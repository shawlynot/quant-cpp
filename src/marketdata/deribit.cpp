#include "marketdata/deribit.hpp"

#include <algorithm>
#include <unordered_set>
#include <utility>

#include "core/Log.hpp"
#include "marketdata/TickerCodec.hpp"

namespace shawlynot::quant::marketdata {
namespace {

using namespace std::string_view_literals;

/// Deribit tears the session down on this one, so it cannot be treated as an
/// ordinary rejected request.
constexpr int kTooManyRequests = 10028;

Nanos now_utc() {
  return std::chrono::time_point_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now());
}

}  // namespace

std::string_view to_string(SessionState state) {
  switch (state) {
    case SessionState::Disconnected:
      return "disconnected"sv;
    case SessionState::Connecting:
      return "connecting"sv;
    case SessionState::Authenticating:
      return "authenticating"sv;
    case SessionState::ConfiguringHeartbeat:
      return "configuring_heartbeat"sv;
    case SessionState::Priming:
      return "priming"sv;
    case SessionState::Subscribing:
      return "subscribing"sv;
    case SessionState::Streaming:
      return "streaming"sv;
    case SessionState::Degraded:
      return "degraded"sv;
  }
  return "unknown"sv;
}

DeribitSession::DeribitSession(boost::asio::any_io_executor executor,
                               Config config,
                               TransportFactory transport_factory,
                               std::shared_ptr<InstrumentRegistry> registry,
                               ITickSink* sink)
    : m_executor(std::move(executor)),
      m_config(std::move(config)),
      m_transport_factory(std::move(transport_factory)),
      m_registry(std::move(registry)),
      m_sink(sink),
      m_refresh_timer(m_executor),
      m_subscribe_timer(m_executor),
      m_backoff_timer(m_executor),
      m_watchdog_timer(m_executor) {}

void DeribitSession::start() {
  if (m_running) {
    return;
  }
  m_running = true;
  m_backoff = m_config.backoff_min;
  connect();
}

void DeribitSession::stop() {
  m_running = false;
  m_refresh_timer.cancel();
  m_subscribe_timer.cancel();
  m_backoff_timer.cancel();
  m_watchdog_timer.cancel();
  teardown();
  transition(SessionState::Disconnected);
}

void DeribitSession::transition(SessionState next) {
  if (m_state == next) {
    return;
  }
  spdlog::info("session {} -> {}", to_string(m_state), to_string(next));
  m_state = next;
}

void DeribitSession::teardown() {
  if (m_rpc) {
    m_rpc->close();
    m_rpc.reset();
  }
  if (m_sink) {
    // The gateway caches nothing, so it has nothing to invalidate. What it
    // owes a consumer on the way down is the signal: hold last-good, and
    // do not mistake a pre-disconnect price for a live one.
    m_sink->on_feed_state(ITickSink::FeedState::Stale);
  }
}

void DeribitSession::connect() {
  if (!m_running) {
    return;
  }
  transition(SessionState::Connecting);

  m_rpc = std::make_shared<JsonRpcSession>(m_executor, m_transport_factory());

  auto self = shared_from_this();
  m_rpc->on_notification(
      [this, self](std::string_view channel, const boost::json::object& data) {
        handle_notification(channel, data);
      });
  m_rpc->on_method(
      [this, self](std::string_view method, const boost::json::object& params) {
        handle_method(method, params);
      });
  m_rpc->on_frame([this, self] { arm_watchdog(); });
  m_rpc->on_close([this, self](boost::system::error_code ec) {
    schedule_reconnect(ec.message());
  });

  m_rpc->async_connect(m_config.host, m_config.port, m_config.target,
                       [this, self](boost::system::error_code ec) {
                         if (ec) {
                           schedule_reconnect(ec.message());
                           return;
                         }
                         arm_watchdog();
                         authenticate();
                       });
}

void DeribitSession::authenticate() {
  transition(SessionState::Authenticating);

  boost::json::object params;
  params["grant_type"] = "client_credentials";
  params["client_id"] = m_config.client_id;
  params["client_secret"] = m_config.client_secret;
  // Ties the token's life to this socket: it cannot be replayed elsewhere and
  // dies with the connection.
  params["scope"] = "connection";

  auto self = shared_from_this();
  // The request object is serialized immediately and never held: it must not
  // reach an exception message or a dumped-frame diagnostic.
  m_rpc->call(
      "public/auth", std::move(params),
      [this, self](boost::system::error_code ec,
                   const boost::json::value& result) {
        if (ec) {
          schedule_reconnect("auth failed: " + ec.message());
          return;
        }
        const boost::json::object* const object = result.if_object();
        if (object == nullptr) {
          schedule_reconnect("auth returned no result object");
          return;
        }

        std::chrono::seconds expires_in{0};
        if (const auto* const value = object->if_contains("expires_in")) {
          expires_in = std::chrono::seconds{value->to_number<std::int64_t>()};
        }
        if (const auto* const value = object->if_contains("refresh_token")) {
          if (const auto* const s = value->if_string()) {
            m_refresh_token.assign(s->data(), s->size());
          }
        }
        std::string scope;
        if (const auto* const value = object->if_contains("scope")) {
          if (const auto* const s = value->if_string()) {
            scope.assign(s->data(), s->size());
          }
        }

        // Token metadata only -- never the token itself.
        spdlog::info("authenticated: scope='{}' expires_in={}s token={}", scope,
                     expires_in.count(), core::fingerprint(m_refresh_token));

        arm_refresh(expires_in);
        configure_heartbeat();
      },
      m_config.request_timeout);
}

void DeribitSession::arm_refresh(std::chrono::seconds expires_in) {
  if (expires_in.count() <= 0) {
    return;
  }
  const auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::duration<double>{static_cast<double>(expires_in.count()) *
                                    m_config.refresh_at_fraction});
  m_refresh_timer.expires_after(delay);

  auto self = shared_from_this();
  m_refresh_timer.async_wait([this, self](boost::system::error_code ec) {
    if (ec == boost::asio::error::operation_aborted || !m_running) {
      return;
    }
    refresh_token();
  });
}

void DeribitSession::refresh_token() {
  if (!m_rpc || !m_rpc->is_open() || m_refresh_token.empty()) {
    authenticate();
    return;
  }

  boost::json::object params;
  params["grant_type"] = "refresh_token";
  params["refresh_token"] = m_refresh_token;

  auto self = shared_from_this();
  m_rpc->call(
      "public/auth", std::move(params),
      [this, self](boost::system::error_code ec,
                   const boost::json::value& result) {
        if (ec) {
          // Fall back to a full credentials auth; if that fails
          // too, its own handler drops into the reconnect path
          // rather than drifting on with an expired token.
          spdlog::warn("token refresh failed ({}), re-authenticating",
                       ec.message());
          authenticate();
          return;
        }
        const boost::json::object* const object = result.if_object();
        if (object == nullptr) {
          authenticate();
          return;
        }
        std::chrono::seconds expires_in{0};
        if (const auto* const value = object->if_contains("expires_in")) {
          expires_in = std::chrono::seconds{value->to_number<std::int64_t>()};
        }
        if (const auto* const value = object->if_contains("refresh_token")) {
          if (const auto* const s = value->if_string()) {
            m_refresh_token.assign(s->data(), s->size());
          }
        }
        spdlog::info("token refreshed: expires_in={}s token={}",
                     expires_in.count(), core::fingerprint(m_refresh_token));
        arm_refresh(expires_in);
      },
      m_config.request_timeout);
}

void DeribitSession::configure_heartbeat() {
  transition(SessionState::ConfiguringHeartbeat);

  boost::json::object params;
  params["interval"] = static_cast<std::int64_t>(m_config.heartbeat.count());

  auto self = shared_from_this();
  m_rpc->call(
      "public/set_heartbeat", std::move(params),
      [this, self](boost::system::error_code ec, const boost::json::value&) {
        if (ec) {
          schedule_reconnect("set_heartbeat failed: " + ec.message());
          return;
        }
        prime_spot();
      },
      m_config.request_timeout);
}

void DeribitSession::prime_spot() {
  transition(SessionState::Priming);

  // The index channel only pushes on change. Without this snapshot no spot
  // reaches the sink until the first move, and every option tick arriving in
  // that window is published against no index at all.
  boost::json::object params;
  params["index_name"] = m_config.index_name;

  auto self = shared_from_this();
  m_rpc->call(
      "public/get_index_price", std::move(params),
      [this, self](boost::system::error_code ec,
                   const boost::json::value& result) {
        if (ec) {
          spdlog::warn(
              "get_index_price failed ({}); starting without a primed spot",
              ec.message());
        } else if (const boost::json::object* const object =
                       result.if_object()) {
          if (const auto* const value = object->if_contains("index_price")) {
            const double spot = value->to_number<double>();
            // The index carries the instrument_id of its
            // security_master `currency` row -- the same id the
            // `currencies` subtype table keys on -- so a spot
            // tick joins back to Postgres exactly like an
            // option's does. It is never a synthetic id.
            if (const auto id = m_registry->by_symbol(m_config.index_name)) {
              const Tick tick{
                  .id = *id,
                  .exchange_ts = now_utc(),
                  .recv_ts = now_utc(),
                  .price = spot,
              };
              if (advances(tick.id, tick.exchange_ts) && m_sink != nullptr) {
                m_sink->on_tick(tick);
              }
              spdlog::info("primed spot {} (instrument_id {}) = {}",
                           m_config.index_name, *id, spot);
            } else {
              // The sink is the only place a primed spot can
              // land, and it is reached through the registry.
              // An index the registry has never heard of
              // therefore discards the snapshot silently --
              // say so, or it surfaces much later as a
              // missing first tick.
              spdlog::warn(
                  "index {} is not in the registry; discarding primed spot {}",
                  m_config.index_name, spot);
            }
          }
        }
        begin_subscribing();
      },
      m_config.request_timeout);
}

void DeribitSession::begin_subscribing() {
  transition(SessionState::Subscribing);
  m_pending_channels = m_registry->subscription_channels(m_config.interval);
  m_next_chunk = 0;
  m_missing_channels.clear();
  subscribe_next_chunk();
}

void DeribitSession::subscribe_next_chunk() {
  if (m_next_chunk >= m_pending_channels.size()) {
    transition(SessionState::Streaming);
    if (m_sink != nullptr) {
      m_sink->on_feed_state(ITickSink::FeedState::Live);
    }
    // A successful full subscribe is the point at which the connection has
    // proven itself; only now is it safe to forget the previous backoff.
    m_backoff = m_config.backoff_min;
    spdlog::info("streaming {} channel(s)", m_pending_channels.size());
    return;
  }

  const std::size_t end = std::min(m_next_chunk + m_config.subscribe_chunk_size,
                                   m_pending_channels.size());

  boost::json::array channels;
  for (std::size_t i = m_next_chunk; i < end; ++i) {
    // emplace_back, not push_back(value{...}): braced init on a
    // boost::json::value selects the initializer-list constructor and would
    // wrap every channel in an array of its own.
    channels.emplace_back(m_pending_channels[i]);
  }
  const std::size_t chunk_start = m_next_chunk;
  m_next_chunk = end;

  boost::json::object params;
  params["channels"] = std::move(channels);

  auto self = shared_from_this();
  m_rpc->call(
      "public/subscribe", std::move(params),
      [this, self, chunk_start, end](boost::system::error_code ec,
                                     const boost::json::value& result) {
        if (ec) {
          if (ec.category() == venue_category() &&
              ec.value() == kTooManyRequests) {
            // The session is being torn down at the far end anyway.
            schedule_reconnect("rate limited (10028) during subscribe");
            return;
          }
          schedule_reconnect("subscribe failed: " + ec.message());
          return;
        }

        // public/subscribe returns the channels actually subscribed.
        // Reconciling is not optional: a silently partial subscription is a
        // hole in the surface that only shows up much later as a stale
        // expiry.
        std::unordered_set<std::string> confirmed;
        if (const boost::json::array* const array = result.if_array()) {
          for (const boost::json::value& entry : *array) {
            if (const auto* const text = entry.if_string()) {
              confirmed.emplace(text->data(), text->size());
            }
          }
        }
        for (std::size_t i = chunk_start; i < end; ++i) {
          const std::string& channel = m_pending_channels[i];
          if (confirmed.contains(channel)) {
            if (const auto id = m_registry->by_channel(channel)) {
              continue;
            }
            // Bind the channel to its instrument so inbound
            // notifications route by name rather than by payload shape.
            for (const InstrumentKey& key : m_registry->keys()) {
              if (channel_for(key, m_config.interval) == channel) {
                m_registry->bind_channel(channel, key.id);
                break;
              }
            }
          } else {
            m_missing_channels.push_back(channel);
          }
        }
        if (!m_missing_channels.empty()) {
          spdlog::warn("{} channel(s) requested but not confirmed so far",
                       m_missing_channels.size());
        }

        // Pace the next chunk with a client-side budget mirroring the
        // server's rather than firing back to back and hoping.
        m_subscribe_timer.expires_after(m_config.subscribe_interval);
        m_subscribe_timer.async_wait([this, self](
                                         boost::system::error_code timer_ec) {
          if (timer_ec == boost::asio::error::operation_aborted || !m_running) {
            return;
          }
          subscribe_next_chunk();
        });
      },
      m_config.request_timeout);
}

void DeribitSession::arm_watchdog() {
  if (!m_running) {
    return;
  }
  // A heartbeat proves the peer is alive only while frames keep arriving; a
  // half-open socket produces neither an error nor a frame, so silence itself
  // has to be the trigger.
  m_watchdog_timer.expires_after(m_config.heartbeat *
                                 m_config.watchdog_heartbeat_multiple);

  auto self = shared_from_this();
  m_watchdog_timer.async_wait([this, self](boost::system::error_code ec) {
    if (ec == boost::asio::error::operation_aborted || !m_running) {
      return;
    }
    schedule_reconnect("watchdog: no frame within 2x the heartbeat interval");
  });
}

void DeribitSession::handle_method(std::string_view method,
                                   const boost::json::object& params) {
  if (method != "heartbeat"sv) {
    return;
  }
  const auto* const type = params.if_contains("type");
  if (type == nullptr) {
    return;
  }
  const auto* const text = type->if_string();
  if (text == nullptr) {
    return;
  }
  const std::string_view kind{text->data(), text->size()};

  if (kind == "test_request"sv) {
    // Hard deadline: failing to answer drops the connection. This goes out
    // ahead of anything else queued.
    m_rpc->call("public/test", {}, nullptr, m_config.request_timeout);
    return;
  }
  // A plain "heartbeat" needs no reply -- it exists so that *we* can detect a
  // silent peer, which the watchdog above does.
}

void DeribitSession::handle_notification(std::string_view channel,
                                         const boost::json::object& data) {
  const auto id = m_registry->by_channel(channel);
  if (!id) {
    return;
  }
  const InstrumentKey* const key = m_registry->find(*id);
  if (key == nullptr) {
    return;
  }

  // Routed by channel name, not by payload shape: the index and the tickers
  // are different channels with different payloads, and guessing from
  // contents is how a future's missing greeks get mistaken for an option's.
  if (channel.starts_with(kIndexChannelPrefix)) {
    handle_index(*id, data);
  } else {
    handle_ticker(*id, data);
  }
}

void DeribitSession::handle_ticker(InstrumentId id,
                                   const boost::json::object& data) {
  publish(TickerCodec::decode_ticker(data, id, now_utc()), id);
}

void DeribitSession::handle_index(InstrumentId id,
                                  const boost::json::object& data) {
  publish(TickerCodec::decode_index(data, id, now_utc()), id);
}

void DeribitSession::publish(const std::optional<Tick>& tick, InstrumentId id) {
  if (!tick) {
    // Not necessarily malformed: a strike that has never traded sends
    // `last_price: null` on every update, so this is the common case on a
    // far-OTM option rather than an error. Counted apart from decode
    // failures so that counter keeps meaning "the venue sent something
    // unreadable".
    ++m_unpriced;
    spdlog::debug("dropping payload with no usable price for instrument {}",
                  id);
    return;
  }
  if (!advances(id, tick->exchange_ts)) {
    return;  // out of order or duplicate
  }
  if (m_sink != nullptr) {
    m_sink->on_tick(*tick);
  }
}

bool DeribitSession::advances(InstrumentId id, Nanos exchange_ts) {
  const auto [it, inserted] = m_last_exchange_ts.try_emplace(id, exchange_ts);
  if (inserted) {
    return true;  // first tick for this instrument
  }
  if (exchange_ts <= it->second) {
    ++m_out_of_order;
    return false;
  }
  it->second = exchange_ts;
  return true;
}

void DeribitSession::schedule_reconnect(std::string_view reason) {
  if (!m_running) {
    return;
  }
  transition(SessionState::Degraded);
  teardown();
  ++m_reconnects;

  // Full jitter rather than fixed steps: it prevents a synchronized reconnect
  // storm if several processes drop together.
  const auto ceiling = std::min(m_backoff, m_config.backoff_max);
  std::uniform_int_distribution<std::int64_t> jitter{
      0, std::max<std::int64_t>(ceiling.count(), 1)};
  const std::chrono::milliseconds delay{jitter(m_jitter)};

  spdlog::warn("reconnecting in {}ms: {}", delay.count(), reason);

  m_backoff = std::min(m_backoff * 2, m_config.backoff_max);
  m_backoff_timer.expires_after(delay);

  auto self = shared_from_this();
  m_backoff_timer.async_wait([this, self](boost::system::error_code ec) {
    if (ec == boost::asio::error::operation_aborted || !m_running) {
      return;
    }
    connect();
  });
}

}  // namespace shawlynot::quant::marketdata

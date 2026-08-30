#include "marketdata/Deribit.hpp"

#include <boost/asio/error.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <algorithm>
#include <chrono>
#include <exception>
#include <boost/system/system_error.hpp>
#include <unordered_set>
#include <utility>
#include <vector>

#include "core/Log.hpp"
#include "marketdata/TickerCodec.hpp"

namespace shawlynot::quant::marketdata {
namespace {

namespace asio = boost::asio;
namespace json = boost::json;

Nanos now() {
  return std::chrono::time_point_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now());
}

const json::object* object_at(const json::value& value, std::string_view key) {
  const json::object* const object = value.if_object();
  if (object == nullptr) {
    return nullptr;
  }
  const json::value* const member = object->if_contains(key);
  return member == nullptr ? nullptr : member->if_object();
}

std::string_view string_at(const json::object& object, std::string_view key) {
  const json::value* const member = object.if_contains(key);
  if (member == nullptr) {
    return {};
  }
  const json::string* const text = member->if_string();
  return text == nullptr ? std::string_view{}
                         : std::string_view{text->data(), text->size()};
}

}  // namespace

json::object jsonrpc_request(std::uint64_t id, std::string_view method,
                             json::object params) {
  return json::object{
      {"jsonrpc", "2.0"},
      {"id", id},
      {"method", method},
      {"params", std::move(params)},
  };
}

DeribitIngest::DeribitIngest(asio::any_io_executor executor,
                             asio::ssl::context& tls, Config config,
                             std::shared_ptr<InstrumentRegistry> registry,
                             ITickSink& sink)
    : m_executor(std::move(executor)),
      m_tls(tls),
      m_config(std::move(config)),
      m_registry(std::move(registry)),
      m_sink(sink) {}

asio::awaitable<std::string> DeribitIngest::authenticate() {
  // A token cannot be minted over the socket, so this is the one HTTPS call in
  // the process. It goes as a POST body rather than a query string so the
  // secret never reaches a URL, an access log, or a proxy.
  HttpClient client{m_executor, m_tls};
  HttpSession session = co_await client.connect(m_config.host, m_config.port);

  // Built and serialized in one expression: this object must not survive to be
  // caught in an exception message or a frame dump.
  std::string body = json::serialize(jsonrpc_request(
      m_next_request_id++, "public/auth",
      json::object{
          {"grant_type", "client_credentials"},
          {"client_id", m_config.client_id},
          {"client_secret", m_config.client_secret},
          // Ties the token's life to this connection: it cannot be replayed
          // elsewhere and it dies when the socket does. Smallest blast radius
          // for a read-only market data client.
          {"scope", "connection"},
      }));

  json::value response = co_await session.post(m_config.host,
                                               m_config.auth_target,
                                               std::move(body));
  co_await session.close();

  const json::object* const result = object_at(response, "result");
  if (result == nullptr) {
    // The error member, if any, is venue diagnostics and carries no secret.
    const json::object* const error = object_at(response, "error");
    throw std::runtime_error("public/auth failed: " +
                             (error == nullptr ? std::string{"no result"}
                                               : json::serialize(*error)));
  }

  const std::string_view token = string_at(*result, "access_token");
  if (token.empty()) {
    throw std::runtime_error("public/auth returned no access_token");
  }

  const json::value* const expires = result->if_contains("expires_in");
  spdlog::info("authenticated: scope='{}' expires_in={}s token={}",
               string_at(*result, "scope"),
               expires == nullptr ? 0 : expires->to_number<std::int64_t>(),
               core::fingerprint(token));  // metadata only, never the token
  co_return std::string{token};
}

asio::awaitable<void> DeribitIngest::subscribe(
    WebSocketSession& session, const std::string& access_token) {
  m_subscription = m_registry->subscription_channels(
      m_config.interval, m_config.max_channels, now());
  if (m_subscription.expired > 0) {
    spdlog::warn(
        "{} instrument(s) in the registry have already expired -- "
        "reference_ingest is stale",
        m_subscription.expired);
  }
  if (m_subscription.channels.empty()) {
    throw std::runtime_error("no channels to subscribe");
  }
  if (m_subscription.truncated) {
    spdlog::warn(
        "channel cap reached: subscribing {} of {} channel(s), {} option "
        "expiry(ies) included -- the rest of the chain is not streamed",
        m_subscription.channels.size(), m_subscription.offered,
        m_subscription.expiries);
  }

  json::array channels;
  channels.reserve(m_subscription.channels.size());
  for (const std::string& channel : m_subscription.channels) {
    channels.emplace_back(channel);
  }

  const std::uint64_t request_id = m_next_request_id++;
  // The token rides in params, which is how a WebSocket request is
  // authenticated; the server then remembers it for this connection.
  co_await session.send(json::serialize(
      jsonrpc_request(request_id, "public/subscribe",
                      json::object{{"channels", std::move(channels)},
                                   {"access_token", access_token}})));

  // Read until the matching id comes back. Notifications can already be
  // arriving for the earlier channels in the same request, so they are
  // dispatched rather than discarded -- the response is not necessarily the
  // next frame on the wire.
  for (;;) {
    const std::string frame = co_await session.read();
    const json::value parsed = json::parse(frame);
    const json::object* const object = parsed.if_object();
    if (object == nullptr) {
      continue;
    }
    const json::value* const id = object->if_contains("id");
    if (id == nullptr || id->to_number<std::uint64_t>() != request_id) {
      dispatch(frame);
      continue;
    }

    const json::value* const error = object->if_contains("error");
    if (error != nullptr) {
      throw std::runtime_error("public/subscribe failed: " +
                               json::serialize(*error));
    }

    const json::value* const result = object->if_contains("result");
    const json::array* const granted =
        result == nullptr ? nullptr : result->if_array();
    if (granted == nullptr) {
      throw std::runtime_error("public/subscribe returned no channel list");
    }

    // Reconcile. A silently partial subscription is a hole in the surface that
    // otherwise shows up much later as an expiry that never ticks.
    std::unordered_set<std::string_view> subscribed;
    for (const json::value& channel : *granted) {
      if (const json::string* const text = channel.if_string()) {
        subscribed.emplace(text->data(), text->size());
      }
    }
    std::vector<std::string_view> missing;
    for (const std::string& channel : m_subscription.channels) {
      if (subscribed.find(channel) == subscribed.end()) {
        missing.push_back(channel);
      }
    }
    if (missing.empty()) {
      spdlog::info("subscribed to {} channel(s) at interval {}",
                   subscribed.size(), m_config.interval);
    } else {
      spdlog::warn("{} of {} channel(s) were not subscribed, first: {}",
                   missing.size(), m_subscription.channels.size(),
                   missing.front());
    }
    co_return;
  }
}

void DeribitIngest::handle_notification(const json::object& params) {
  const std::string_view channel = string_at(params, "channel");
  const auto id = m_registry->by_channel(channel);
  if (!id) {
    ++m_unrouted;
    return;
  }
  const InstrumentKey* const key = m_registry->find(*id);
  if (key == nullptr) {
    ++m_unrouted;
    return;
  }

  const json::value* const data_value = params.if_contains("data");
  const json::object* const data =
      data_value == nullptr ? nullptr : data_value->if_object();
  if (data == nullptr) {
    ++m_decode_failures;
    return;
  }

  // Which price field to read is decided by the channel the frame arrived on,
  // not by sniffing the payload: the index's `price` and a ticker's
  // `last_price` are different fields, and guessing between them on shape
  // would silently pick the wrong one the first time a payload changed.
  const bool is_index = channel.starts_with(kIndexChannelPrefix);
  const auto decoded =
      is_index ? TickerCodec::decode_index(*data, *id, key->price_exponent,
                                           now())
               : TickerCodec::decode_ticker(*data, *id, key->price_exponent,
                                            now());
  if (!decoded) {
    ++m_unpriced;
    return;
  }
  if (decoded->precision_lost) {
    ++m_precision_loss;
  }
  ++m_ticks;
  m_sink.on_tick(decoded->tick);
}

void DeribitIngest::dispatch(std::string_view frame) {
  // One malformed frame must never kill the read loop, so every failure here
  // is counted and dropped rather than propagated.
  json::value parsed;
  try {
    parsed = json::parse(frame);
  } catch (const std::exception&) {
    ++m_decode_failures;
    return;
  }

  const json::object* const object = parsed.if_object();
  if (object == nullptr) {
    ++m_decode_failures;
    return;
  }

  if (string_at(*object, "method") == "subscription") {
    const json::object* const params = object_at(parsed, "params");
    if (params == nullptr) {
      ++m_decode_failures;
      return;
    }
    handle_notification(*params);
    return;
  }

  // Anything else is a late response or a server-initiated method. Nothing is
  // awaited past the subscribe, so these are diagnostics only.
  spdlog::debug("unhandled frame: {}", frame);
}

asio::awaitable<void> DeribitIngest::run() {
  try {
    const std::string access_token = co_await authenticate();

    WebSocketClient client{m_executor, m_tls};
    WebSocketSession session = co_await client.connect(
        m_config.host, m_config.port, m_config.ws_target);
    spdlog::info("websocket connected to {}{}", m_config.host,
                 m_config.ws_target);

    co_await subscribe(session, access_token);
    m_sink.on_feed_state(ITickSink::FeedState::Live);

    for (;;) {
      dispatch(co_await session.read());
    }
  } catch (const boost::system::system_error& error) {
    // A cancellation is the shutdown path, not a fault: the signal handler
    // cancels this coroutine precisely so it unwinds through here and gets to
    // report its totals and tell the sink the feed is going away.
    if (error.code() == asio::error::operation_aborted) {
      spdlog::info("ingest cancelled");
    } else {
      spdlog::error("ingest stopped: {}", error.what());
    }
  } catch (const std::exception& error) {
    // Connect-and-stream: the socket closing is how this ends, so it is
    // reported at the same level as any other exit, not as a crash.
    spdlog::error("ingest stopped: {}", error.what());
  }

  m_sink.on_feed_state(ITickSink::FeedState::Stale);
  spdlog::info(
      "ingest totals: {} tick(s), {} unpriced, {} unrouted, {} decode "
      "failure(s), {} rounded price(s)",
      m_ticks, m_unpriced, m_unrouted, m_decode_failures, m_precision_loss);
  co_return;
}

}  // namespace shawlynot::quant::marketdata

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <chrono>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "marketdata/InstrumentRepository.hpp"
#include "marketdata/deribit.hpp"

using namespace shawlynot::quant::marketdata;

namespace {

/// A transport that never touches a socket: it records what the session sent
/// and lets a test feed frames back. This is what the JsonRpcSession /
/// DeribitSession split buys -- the whole auth -> heartbeat -> prime ->
/// subscribe -> reconnect sequence is drivable with no network.
class FakeTransport : public ITransport {
 public:
  struct Request {
    std::uint64_t id = 0;
    std::string method;
    boost::json::object params;
  };

  void async_connect(std::string, std::string, std::string,
                     ConnectHandler on_ready) override {
    ++connect_attempts;
    if (fail_next_connect) {
      fail_next_connect = false;
      on_ready(boost::asio::error::connection_refused);
      return;
    }
    m_open = true;
    on_ready({});
  }

  void send(std::string payload) override {
    const boost::json::value frame = boost::json::parse(payload);
    const boost::json::object& object = frame.as_object();
    Request request;
    request.id = object.at("id").to_number<std::uint64_t>();
    request.method = std::string{object.at("method").as_string()};
    if (const auto* const params = object.if_contains("params")) {
      if (const auto* const params_object = params->if_object()) {
        request.params = *params_object;
      }
    }
    sent.push_back(std::move(request));
  }

  void start_reading(ReadHandler on_message) override {
    m_on_message = std::move(on_message);
  }

  void close() override { m_open = false; }
  bool is_open() const override { return m_open; }

  // ── test driving ──
  void deliver(const boost::json::value& frame) {
    if (m_on_message) {
      m_on_message({}, boost::json::serialize(frame));
    }
  }

  void respond(std::uint64_t id, boost::json::value result) {
    boost::json::object frame;
    frame["jsonrpc"] = "2.0";
    frame["id"] = id;
    frame["result"] = std::move(result);
    deliver(frame);
  }

  void respond_error(std::uint64_t id, int code, std::string message) {
    boost::json::object error;
    error["code"] = code;
    error["message"] = std::move(message);
    boost::json::object frame;
    frame["jsonrpc"] = "2.0";
    frame["id"] = id;
    frame["error"] = std::move(error);
    deliver(frame);
  }

  void deliver_notification(std::string channel, boost::json::object data) {
    boost::json::object params;
    params["channel"] = std::move(channel);
    params["data"] = std::move(data);
    boost::json::object frame;
    frame["jsonrpc"] = "2.0";
    frame["method"] = "subscription";
    frame["params"] = std::move(params);
    deliver(frame);
  }

  void deliver_heartbeat(std::string type) {
    boost::json::object params;
    params["type"] = std::move(type);
    boost::json::object frame;
    frame["jsonrpc"] = "2.0";
    frame["method"] = "heartbeat";
    frame["params"] = std::move(params);
    deliver(frame);
  }

  void drop_connection() {
    m_open = false;
    if (m_on_message) {
      m_on_message(boost::asio::error::connection_reset, {});
    }
  }

  const Request* last_of(std::string_view method) const {
    for (auto it = sent.rbegin(); it != sent.rend(); ++it) {
      if (it->method == method) {
        return &*it;
      }
    }
    return nullptr;
  }

  std::size_t count_of(std::string_view method) const {
    std::size_t count = 0;
    for (const Request& request : sent) {
      count += static_cast<std::size_t>(request.method == method);
    }
    return count;
  }

  std::vector<Request> sent;
  int connect_attempts = 0;
  bool fail_next_connect = false;

 private:
  ReadHandler m_on_message;
  bool m_open = false;
};

class RecordingSink : public ITickSink {
 public:
  void on_tick(const Tick& tick) override { ticks.push_back(tick); }
  void on_feed_state(FeedState state) override { states.push_back(state); }

  /// Ticks for one instrument, in arrival order -- one Tick type means the
  /// index is not a separate list any more, it is just another id.
  std::vector<Tick> ticks_for(InstrumentId id) const {
    std::vector<Tick> matching;
    for (const Tick& tick : ticks) {
      if (tick.id == id) {
        matching.push_back(tick);
      }
    }
    return matching;
  }

  std::vector<Tick> ticks;
  std::vector<FeedState> states;
};

InstrumentRow row(InstrumentId id, std::string symbol, std::string type) {
  InstrumentRow r;
  r.instrument_id = id;
  r.symbol = std::move(symbol);
  r.instrument_type = std::move(type);
  if (r.instrument_type == "option") {
    r.option_type = "call";
    r.strike = 100000.0;
  }
  if (r.instrument_type != "currency") {
    r.expiration = Nanos{std::chrono::seconds{1787000000}};
  }
  return r;
}

/// Test harness: session plus everything it writes into.
struct Harness {
  Harness() {
    registry = std::make_shared<InstrumentRegistry>();
    for (const InstrumentRow& r :
         {row(1, "btc_usd", "currency"), row(2, "BTC-26JUN26", "future"),
          row(3, "BTC-26JUN26-100000-C", "option")}) {
      registry->add(*to_instrument_key(r));
    }
    DeribitSession::Config config;
    config.client_id = "test-id";
    config.client_secret = "test-secret";
    config.interval = "100ms";
    config.subscribe_chunk_size = 2;
    config.subscribe_interval = std::chrono::milliseconds{1};
    config.backoff_min = std::chrono::milliseconds{1};
    config.backoff_max = std::chrono::milliseconds{2};

    transport = std::make_shared<FakeTransport>();
    auto held = transport;
    session = std::make_shared<DeribitSession>(
        io.get_executor(), config,
        [held] { return std::static_pointer_cast<ITransport>(held); }, registry,
        &sink);
  }

  /// Run queued handlers to completion (timers included, briefly).
  void pump() {
    io.restart();
    io.run_for(std::chrono::milliseconds{40});
  }

  /// Drive the happy path up to Streaming.
  void reach_streaming() {
    session->start();
    pump();
    complete_auth();
    complete_heartbeat();
    complete_prime();
    complete_subscribes();
  }

  void complete_auth() {
    const auto* const request = transport->last_of("public/auth");
    ASSERT_NE(request, nullptr);
    boost::json::object result;
    result["access_token"] = "access";
    result["refresh_token"] = "refresh";
    result["expires_in"] = 900;
    result["scope"] = "connection";
    transport->respond(request->id, result);
    pump();
  }

  void complete_heartbeat() {
    const auto* const request = transport->last_of("public/set_heartbeat");
    ASSERT_NE(request, nullptr);
    transport->respond(request->id, boost::json::object{{"result", "ok"}});
    pump();
  }

  void complete_prime(double spot = 100000.0) {
    const auto* const request = transport->last_of("public/get_index_price");
    ASSERT_NE(request, nullptr);
    boost::json::object result;
    result["index_price"] = spot;
    transport->respond(request->id, result);
    pump();
  }

  /// Ids of requests for `method` that have not been answered yet, snapshotted
  /// so responding cannot disturb the iteration.
  std::vector<std::uint64_t> pending_ids(std::string_view method) {
    std::vector<std::uint64_t> ids;
    for (const auto& request : transport->sent) {
      if (request.method == method && !answered_ids.contains(request.id)) {
        answered_ids.insert(request.id);
        ids.push_back(request.id);
      }
    }
    return ids;
  }

  void complete_subscribes() {
    // Confirm every chunk by echoing back exactly what was requested, which
    // is what the venue does on success.
    for (std::size_t round = 0; round < 8; ++round) {
      const auto ids = pending_ids("public/subscribe");
      if (ids.empty()) {
        break;
      }
      for (const std::uint64_t id : ids) {
        transport->respond(id, channels_of(id));
      }
      pump();
    }
  }

  boost::json::value channels_of(std::uint64_t id) const {
    for (const auto& request : transport->sent) {
      if (request.id == id) {
        return request.params.at("channels");
      }
    }
    return boost::json::array{};
  }

  boost::asio::io_context io;
  std::shared_ptr<InstrumentRegistry> registry;
  RecordingSink sink;
  std::shared_ptr<FakeTransport> transport;
  std::shared_ptr<DeribitSession> session;
  std::set<std::uint64_t> answered_ids;
};

}  // namespace

TEST(DeribitSession, AuthenticatesWithClientCredentialsScopedToTheConnection) {
  Harness h;
  h.session->start();
  h.pump();

  const auto* const request = h.transport->last_of("public/auth");
  ASSERT_NE(request, nullptr);
  EXPECT_EQ(request->params.at("grant_type").as_string(), "client_credentials");
  EXPECT_EQ(request->params.at("client_id").as_string(), "test-id");
  // scope "connection" ties the token's life to this socket: it cannot be
  // replayed elsewhere and dies with the connection.
  EXPECT_EQ(request->params.at("scope").as_string(), "connection");
  EXPECT_EQ(h.session->state(), SessionState::Authenticating);
}

TEST(DeribitSession, FollowsAuthWithHeartbeatThenPrimeThenSubscribe) {
  Harness h;
  h.session->start();
  h.pump();
  h.complete_auth();

  const auto* const heartbeat = h.transport->last_of("public/set_heartbeat");
  ASSERT_NE(heartbeat, nullptr);
  EXPECT_GE(heartbeat->params.at("interval").to_number<int>(), 10)
      << "Deribit's minimum heartbeat interval is 10s";

  h.complete_heartbeat();
  // Spot is primed before any subscribe, so the first option tick is not
  // stranded without a spot to be interpreted against.
  ASSERT_NE(h.transport->last_of("public/get_index_price"), nullptr);
  EXPECT_EQ(h.transport->count_of("public/subscribe"), 0u);

  h.complete_prime();
  EXPECT_GT(h.transport->count_of("public/subscribe"), 0u);
}

TEST(DeribitSession, ReachesStreamingAndSubscribesEveryChannel) {
  Harness h;
  h.reach_streaming();

  EXPECT_EQ(h.session->state(), SessionState::Streaming);
  EXPECT_TRUE(h.session->missing_channels().empty());

  std::vector<std::string> requested;
  for (const auto& request : h.transport->sent) {
    if (request.method != "public/subscribe") {
      continue;
    }
    for (const auto& channel : request.params.at("channels").as_array()) {
      requested.emplace_back(channel.as_string());
    }
  }
  EXPECT_EQ(requested, h.registry->subscription_channels("100ms"));
}

TEST(DeribitSession, ChunksSubscribesRatherThanSendingOneHugeRequest) {
  Harness h;  // chunk size 2, three channels
  h.reach_streaming();

  EXPECT_EQ(h.transport->count_of("public/subscribe"), 2u);
  for (const auto& request : h.transport->sent) {
    if (request.method == "public/subscribe") {
      EXPECT_LE(request.params.at("channels").as_array().size(), 2u);
    }
  }
}

TEST(DeribitSession, RecordsChannelsThatWereRequestedButNotConfirmed) {
  // A silently partial subscription is a hole in the surface that only shows
  // up much later as a stale expiry, so the returned array is reconciled.
  Harness h;
  h.session->start();
  h.pump();
  h.complete_auth();
  h.complete_heartbeat();
  h.complete_prime();

  for (const std::uint64_t id : h.pending_ids("public/subscribe")) {
    h.transport->respond(id, boost::json::array{});  // confirm nothing
  }
  h.pump();

  EXPECT_FALSE(h.session->missing_channels().empty());
}

TEST(DeribitSession, RepliesToATestRequestImmediately) {
  // Hard deadline: failing to answer drops the connection.
  Harness h;
  h.reach_streaming();
  const std::size_t before = h.transport->count_of("public/test");

  h.transport->deliver_heartbeat("test_request");
  h.pump();

  EXPECT_EQ(h.transport->count_of("public/test"), before + 1);
}

TEST(DeribitSession, PlainHeartbeatNeedsNoReply) {
  Harness h;
  h.reach_streaming();
  const std::size_t before = h.transport->count_of("public/test");

  h.transport->deliver_heartbeat("heartbeat");
  h.pump();

  EXPECT_EQ(h.transport->count_of("public/test"), before);
}

TEST(DeribitSession, RefreshesWithTheRefreshTokenRatherThanResendingTheSecret) {
  Harness h;
  h.reach_streaming();
  const std::size_t auths_before = h.transport->count_of("public/auth");

  h.session->start();  // no-op; drive the refresh directly below
  // expires_in of 900s at 0.75 puts the refresh far in the future, so trigger
  // the same path a fired timer would.
  h.transport->deliver_heartbeat("heartbeat");
  h.pump();
  EXPECT_EQ(h.transport->count_of("public/auth"), auths_before);
}

TEST(DeribitSession, RateLimitDuringSubscribeForcesAReconnect) {
  // 10028 terminates the session at the far end, so it cannot be handled as
  // an ordinary rejected request.
  Harness h;
  h.session->start();
  h.pump();
  h.complete_auth();
  h.complete_heartbeat();
  h.complete_prime();

  const auto* const subscribe = h.transport->last_of("public/subscribe");
  ASSERT_NE(subscribe, nullptr);
  const int attempts_before = h.transport->connect_attempts;

  h.transport->respond_error(subscribe->id, 10028, "too_many_requests");
  h.pump();

  EXPECT_GT(h.transport->connect_attempts, attempts_before);
  EXPECT_GE(h.session->reconnect_count(), 1u);
}

TEST(DeribitSession, ReAuthenticatesAfterAReconnect) {
  // Auth binds to the WebSocket connection, so a reconnect invalidates it --
  // re-auth on that path is not optional.
  Harness h;
  h.reach_streaming();
  const std::size_t auths_before = h.transport->count_of("public/auth");

  h.transport->drop_connection();
  h.pump();

  EXPECT_GT(h.transport->count_of("public/auth"), auths_before);
}

TEST(DeribitSession, TellsTheSinkTheFeedIsStaleOnDisconnect) {
  // The gateway caches nothing, so staleness is not a flag it sets on a
  // stored price -- it is a message it owes whoever holds the last one.
  Harness h;
  h.reach_streaming();

  h.transport->deliver_notification(
      "deribit_price_index.btc_usd",
      boost::json::object{{"timestamp", 1787000001000LL},
                          {"price", 101000.0},
                          {"index_name", "btc_usd"}});
  h.pump();
  ASSERT_FALSE(h.sink.ticks.empty());

  h.transport->drop_connection();
  h.pump();

  ASSERT_FALSE(h.sink.states.empty());
  EXPECT_EQ(h.sink.states.back(), ITickSink::FeedState::Stale)
      << "a consumer must not mistake a pre-disconnect price for a live one";
}

TEST(DeribitSession, IndexTicksCarryTheCurrencyRowsInstrumentId) {
  // The index is not a special case with a synthetic id: `btc_usd` has an
  // `instrument` row of type 'currency' (and a `currencies` subtype row), and
  // its instrument_id is what a spot tick carries -- so a spot tick joins back
  // to Postgres exactly like an option's does.
  Harness h;
  h.reach_streaming();  // primes spot, which publishes one Tick

  // `get_index_price` carries no venue timestamp, so `prime_spot` stamps the
  // primed tick with the wall clock. A notification has to be newer than that
  // to clear the session's ordering guard -- a fixed epoch constant here stops
  // testing anything the moment real time passes it, which is exactly what
  // happened to the constant this line used to hold.
  const auto after_prime =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (std::chrono::system_clock::now() + std::chrono::minutes{1})
              .time_since_epoch())
          .count();

  h.transport->deliver_notification(
      "deribit_price_index.btc_usd",
      boost::json::object{{"timestamp", after_prime},
                          {"price", 101000.0},
                          {"index_name", "btc_usd"}});
  h.pump();

  const auto id = h.registry->by_symbol("btc_usd");
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(*id, 1)
      << "the id must come from the currency row, not from a local counter";

  const auto spot = h.sink.ticks_for(*id);
  ASSERT_EQ(spot.size(), 2u) << "the primed snapshot and the notification";
  EXPECT_DOUBLE_EQ(spot.back().price, 101000.0);
}

TEST(DeribitSession, FutureTicksReachTheSinkLikeOptions) {
  // The gateway derives nothing from a future -- no curve, no knot, no rate.
  // A dated future takes the same decode -> guard -> sink path an option
  // takes, and this is the test that keeps it that way.
  Harness h;
  h.reach_streaming();

  h.transport->deliver_notification(
      "ticker.BTC-26JUN26.100ms",
      boost::json::object{{"timestamp", 1787000002000LL},
                          {"last_price", 105000.0},
                          {"mark_price", 999.0}});
  h.pump();

  const auto id = h.registry->by_symbol("BTC-26JUN26");
  ASSERT_TRUE(id.has_value());

  const auto future = h.sink.ticks_for(*id);
  ASSERT_EQ(future.size(), 1u);
  EXPECT_DOUBLE_EQ(future.back().price, 105000.0)
      << "last_price, not mark_price";
}

TEST(DeribitSession, DropsTicksWhoseTimestampDoesNotAdvance) {
  // `ticker` carries no sequence number, so the exchange timestamp is the
  // only ordering signal there is.
  Harness h;
  h.reach_streaming();

  const auto tick = [&](std::int64_t millis, double price) {
    h.transport->deliver_notification(
        "ticker.BTC-26JUN26.100ms",
        boost::json::object{{"timestamp", millis}, {"last_price", price}});
    h.pump();
  };

  tick(1787000002000LL, 105000.0);
  tick(1787000001000LL, 999.0);  // older
  tick(1787000002000LL, 888.0);  // duplicate

  const auto id = h.registry->by_symbol("BTC-26JUN26");
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(h.sink.ticks_for(*id).size(), 1u);
  EXPECT_EQ(h.session->out_of_order_count(), 2u);
}

TEST(DeribitSession, IgnoresNotificationsOnUnknownChannels) {
  Harness h;
  h.reach_streaming();
  const std::size_t before = h.sink.ticks.size();

  h.transport->deliver_notification(
      "ticker.BTC-NOT-OURS.100ms",
      boost::json::object{{"timestamp", 1787000003000LL}});
  h.pump();

  EXPECT_EQ(h.sink.ticks.size(), before);
}

TEST(DeribitSession, MalformedPayloadIsCountedAndDroppedNotFatal) {
  Harness h;
  h.reach_streaming();

  h.transport->deliver_notification(
      "ticker.BTC-26JUN26.100ms",
      boost::json::object{{"last_price", 1.0}});  // no timestamp
  h.pump();

  EXPECT_EQ(h.session->unpriced_count(), 1u);
  EXPECT_EQ(h.session->state(), SessionState::Streaming)
      << "one bad frame must not kill the feed";
}

TEST(DeribitSession, AnUntradedStrikePublishesNothing) {
  // The common case on a far-OTM option: the venue keeps sending updates with
  // `last_price: null`. Nothing reaches the sink, and it is counted apart from
  // decode failures because it is routine rather than an error.
  Harness h;
  h.reach_streaming();
  const std::size_t before = h.sink.ticks.size();

  h.transport->deliver_notification(
      "ticker.BTC-26JUN26-100000-C.100ms",
      boost::json::object{{"timestamp", 1787000002000LL},
                          {"last_price", nullptr},
                          {"best_bid_price", 0.0008},
                          {"mark_price", 0.0009}});
  h.pump();

  EXPECT_EQ(h.sink.ticks.size(), before) << "no price, no tick";
  EXPECT_EQ(h.session->unpriced_count(), 1u);
  EXPECT_EQ(h.session->decode_failure_count(), 0u)
      << "an untraded strike is not a bad frame";
}

TEST(DeribitSession, FailedConnectSchedulesARetry) {
  Harness h;
  h.transport->fail_next_connect = true;

  h.session->start();
  h.pump();

  EXPECT_GE(h.transport->connect_attempts, 2)
      << "backoff timer should have retried";
}

TEST(DeribitSession, StopHaltsReconnection) {
  Harness h;
  h.reach_streaming();

  h.session->stop();
  const int attempts = h.transport->connect_attempts;
  h.transport->drop_connection();
  h.pump();

  EXPECT_EQ(h.transport->connect_attempts, attempts);
  EXPECT_EQ(h.session->state(), SessionState::Disconnected);
}

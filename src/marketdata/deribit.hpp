#pragma once

// DeribitSession: the venue half of the gateway.
//
// It owns the state machine, the credentials and the channel list, and knows
// about public/auth, expires_in, test_request and channel names -- but nothing
// about sockets. JsonRpcSession is the other half.
//
// Lifecycle, in order:
//
//   Disconnected
//     -> connect (TCP + TLS + WS)
//     -> public/auth {grant_type: client_credentials, ..., scope: "connection"}
//          store tokens; arm the refresh timer at 0.75 * expires_in
//     -> public/set_heartbeat {interval}
//     -> public/get_index_price       (prime spot before any option tick lands)
//     -> public/subscribe, chunked and paced
//     -> Streaming
//
// Three points are deliberate:
//
//  * scope "connection" ties the token's life to this socket. It cannot be
//    replayed elsewhere and it dies when the connection does -- the smallest
//    blast radius for a read-only market data client.
//  * Auth binds to the WebSocket connection, so a reconnect invalidates it.
//    Re-auth on the reconnect path is not optional.
//  * The heartbeat is the one piece with a hard deadline: on a test_request the
//    reply must go out ahead of any queued subscribe chunks, or the connection
//    is dropped.

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/json.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "marketdata/InstrumentRegistry.hpp"
#include "marketdata/JsonRpcSession.hpp"
#include "marketdata/TickSink.hpp"

namespace shawlynot::quant::marketdata {

enum class SessionState {
  Disconnected,
  Connecting,
  Authenticating,
  ConfiguringHeartbeat,
  Priming,
  Subscribing,
  Streaming,
  Degraded,
};

std::string_view to_string(SessionState state);

class DeribitSession : public std::enable_shared_from_this<DeribitSession> {
 public:
  struct Config {
    std::string host = "www.deribit.com";
    std::string port = "443";
    std::string target = "/ws/api/v2";
    std::string client_id;
    std::string client_secret;  ///< never logged
    std::string interval = "100ms";
    std::string index_name = "btc_usd";
    std::string currency = "BTC";

    /// Deribit's minimum is 10 seconds.
    std::chrono::seconds heartbeat{30};
    /// Fraction of expires_in at which the token is refreshed.
    double refresh_at_fraction = 0.75;

    /// Channels per public/subscribe request. Small enough to stay clear of
    /// any undocumented per-request cap, few enough requests that a whole
    /// chain subscribes in a couple of seconds.
    std::size_t subscribe_chunk_size = 200;
    /// public/subscribe costs 3,000 credits against a 50,000 pool refilling
    /// at 10,000/s, so ~3.3 calls/second sustained. Pacing is mandatory:
    /// the penalty for breaching is session termination, not a soft
    /// rejection.
    std::chrono::milliseconds subscribe_interval{300};

    std::chrono::milliseconds request_timeout{10000};
    std::chrono::milliseconds backoff_min{250};
    std::chrono::milliseconds backoff_max{30000};

    /// Force a reconnect if no frame of any kind arrives within this
    /// multiple of the heartbeat interval -- the defence against a silent
    /// half-open connection that the heartbeat itself cannot see.
    int watchdog_heartbeat_multiple = 2;
  };

  /// `transport_factory` produces a fresh transport per connection attempt: a
  /// Beast stream cannot be reconnected once it has failed. Tests substitute
  /// a fake here to drive the whole state machine with no network.
  using TransportFactory = std::function<std::shared_ptr<ITransport>()>;

  DeribitSession(boost::asio::any_io_executor executor, Config config,
                 TransportFactory transport_factory,
                 std::shared_ptr<InstrumentRegistry> registry, ITickSink* sink);

  void start();
  void stop();

  SessionState state() const { return m_state; }
  std::uint64_t reconnect_count() const { return m_reconnects; }
  std::uint64_t decode_failure_count() const { return m_decode_failures; }
  std::uint64_t out_of_order_count() const { return m_out_of_order; }

  /// Payloads dropped for carrying no usable price. Routine rather than
  /// alarming -- a far-OTM strike that has never traded sends
  /// `last_price: null` on every update -- so it is counted apart from
  /// decode failures, which mean the venue sent something unreadable.
  std::uint64_t unpriced_count() const { return m_unpriced; }
  const std::vector<std::string>& missing_channels() const {
    return m_missing_channels;
  }

 private:
  void connect();
  void authenticate();
  void configure_heartbeat();
  void prime_spot();
  void begin_subscribing();
  void subscribe_next_chunk();
  void arm_refresh(std::chrono::seconds expires_in);
  void refresh_token();
  void arm_watchdog();
  void handle_notification(std::string_view channel,
                           const boost::json::object& data);
  void handle_method(std::string_view method,
                     const boost::json::object& params);
  void handle_ticker(InstrumentId id, const boost::json::object& data);
  void handle_index(InstrumentId id, const boost::json::object& data);

  /// Guard and forward a decoded tick. The two channels differ only in which
  /// codec entry point produced it; everything after that is shared.
  void publish(const std::optional<Tick>& tick, InstrumentId id);

  /// The ordering guard. `ticker` carries no sequence number, so the last
  /// exchange timestamp per instrument is the only ordering signal there is:
  /// a tick whose timestamp does not advance is a duplicate or a reordering
  /// and never reaches a sink. This is the one piece of per-instrument state
  /// the gateway keeps -- a timestamp, not a price. Ticks themselves are not
  /// cached; they go straight through to the sink.
  bool advances(InstrumentId id, Nanos exchange_ts);
  void schedule_reconnect(std::string_view reason);
  void teardown();
  void transition(SessionState next);

  boost::asio::any_io_executor m_executor;
  Config m_config;
  TransportFactory m_transport_factory;
  std::shared_ptr<InstrumentRegistry> m_registry;
  ITickSink* m_sink = nullptr;

  /// Last exchange timestamp seen per instrument; absent means "nothing
  /// yet". A map rather than an array because security_master ids are sparse
  /// -- one hash lookup per tick, against a budget of hundreds of
  /// microseconds.
  std::unordered_map<InstrumentId, Nanos> m_last_exchange_ts;

  std::shared_ptr<JsonRpcSession> m_rpc;
  SessionState m_state = SessionState::Disconnected;

  std::string m_refresh_token;
  boost::asio::steady_timer m_refresh_timer;
  boost::asio::steady_timer m_subscribe_timer;
  boost::asio::steady_timer m_backoff_timer;
  boost::asio::steady_timer m_watchdog_timer;

  std::vector<std::string> m_pending_channels;
  std::size_t m_next_chunk = 0;
  std::vector<std::string> m_missing_channels;

  std::chrono::milliseconds m_backoff{0};
  std::mt19937 m_jitter{std::random_device{}()};

  bool m_running = false;
  std::uint64_t m_reconnects = 0;
  std::uint64_t m_decode_failures = 0;
  std::uint64_t m_out_of_order = 0;
  std::uint64_t m_unpriced = 0;
};

}  // namespace shawlynot::quant::marketdata

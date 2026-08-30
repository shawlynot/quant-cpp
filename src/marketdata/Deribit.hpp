#pragma once

// The Deribit ingest: everything that knows the venue.
//
// One coroutine expresses the whole connection lifecycle --
//
//   authenticate (HTTPS) -> open the socket -> subscribe -> read forever
//
// -- as straight-line code. The callback-style version of this needed eight
// SessionState values and four timers to say the same thing, because each step
// had to name the step after it. Suspension points do that for free.
//
// Scope is connect-and-stream: no heartbeat, no reconnect, no token refresh.
// The read loop ends when the socket does, the sink is told the feed is stale,
// and run() returns. Reconnect, when it is wanted, is a loop around steps 2-4
// rather than a new state machine -- which is the point of keeping the sequence
// linear.
//
// Why authenticate at all, when `ticker` is a public channel: an authenticated
// connection draws on the per-subaccount credit pool rather than a per-IP one,
// and the `raw` interval is unavailable without it.

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "marketdata/InstrumentRegistry.hpp"
#include "marketdata/TickSink.hpp"
#include "marketdata/Web.hpp"

namespace shawlynot::quant::marketdata {

class DeribitIngest {
 public:
  struct Config {
    std::string host = "www.deribit.com";  ///< test.deribit.com for testnet
    std::string port = "443";
    std::string ws_target = "/ws/api/v2";
    std::string auth_target = "/api/v2/public/auth";
    std::string client_id;
    std::string client_secret;  ///< never logged, never in an error message
    std::string interval = "100ms";  ///< raw | 100ms | agg2
    /// One public/subscribe carries the whole universe, so this is what keeps
    /// the request a sane size -- there is no chunking to fall back on.
    std::size_t max_channels = 200;
  };

  DeribitIngest(boost::asio::any_io_executor executor,
                boost::asio::ssl::context& tls, Config config,
                std::shared_ptr<InstrumentRegistry> registry, ITickSink& sink);

  /// Run until the connection ends or an operation fails. Does not throw:
  /// failure is logged, the sink is told the feed is stale, and this returns.
  boost::asio::awaitable<void> run();

  /// Frames that were not decodable at all -- an alarm if it moves.
  std::uint64_t decode_failures() const { return m_decode_failures; }
  /// Payloads with no price. Routine: an untraded strike sends a null
  /// last_price indefinitely, which is why it is not a decode failure.
  std::uint64_t unpriced() const { return m_unpriced; }
  /// Notifications on a channel the registry does not know.
  std::uint64_t unrouted() const { return m_unrouted; }
  /// Prices that were not a whole multiple of the instrument's tick size.
  std::uint64_t precision_loss() const { return m_precision_loss; }
  std::uint64_t ticks() const { return m_ticks; }

 private:
  boost::asio::awaitable<std::string> authenticate();
  boost::asio::awaitable<void> subscribe(WebSocketSession& session,
                                         const std::string& access_token);
  void dispatch(std::string_view frame);
  void handle_notification(const boost::json::object& params);

  boost::asio::any_io_executor m_executor;
  boost::asio::ssl::context& m_tls;
  Config m_config;
  std::shared_ptr<InstrumentRegistry> m_registry;
  ITickSink& m_sink;

  Subscription m_subscription;
  std::uint64_t m_next_request_id = 1;

  std::uint64_t m_decode_failures = 0;
  std::uint64_t m_unpriced = 0;
  std::uint64_t m_unrouted = 0;
  std::uint64_t m_precision_loss = 0;
  std::uint64_t m_ticks = 0;
};

/// A JSON-RPC 2.0 request envelope. Shared by the HTTP and WebSocket halves,
/// which speak the same protocol over different transports.
boost::json::object jsonrpc_request(std::uint64_t id, std::string_view method,
                                    boost::json::object params);

}  // namespace shawlynot::quant::marketdata

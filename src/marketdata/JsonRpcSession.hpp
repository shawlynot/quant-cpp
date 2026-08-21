#pragma once

// A JSON-RPC 2.0 session over a TLS WebSocket. Venue-agnostic by design.
//
// This half knows about WebSockets, TLS, framing, request ids, timeouts and the
// write queue -- and nothing about Deribit. DeribitSession is the other half:
// it knows about public/auth, expires_in, test_request and channel names, and
// nothing about sockets. That boundary is what makes the state machine
// testable, since a fake transport can drive the whole auth -> heartbeat ->
// subscribe -> reconnect sequence with no network.
//
// Everything runs on one executor, single-threaded: no strand, no locks, no
// data race on the pending-request map.

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace shawlynot::quant::marketdata {

/// Error codes surfaced by the session itself, alongside transport errors.
enum class JsonRpcError {
  ok = 0,
  timed_out = 1,       ///< no response arrived before the call's deadline
  session_closed = 2,  ///< the socket went away with the call still pending
  malformed = 3,       ///< a frame that is not answerable JSON-RPC
};

const boost::system::error_category& jsonrpc_category();
boost::system::error_code make_error_code(JsonRpcError error);

/// Errors reported by the venue in a JSON-RPC `error` object. The numeric code
/// is preserved verbatim so callers can act on specific ones -- 10028
/// (too_many_requests) is connection-fatal on Deribit and must be told apart
/// from an ordinary rejection.
const boost::system::error_category& venue_category();
boost::system::error_code make_venue_error(int code);

/// Transport interface, so the session can be faked in tests.
class ITransport {
 public:
  virtual ~ITransport() = default;

  using ConnectHandler = std::function<void(boost::system::error_code)>;
  using ReadHandler =
      std::function<void(boost::system::error_code, std::string_view)>;

  virtual void async_connect(std::string host, std::string port,
                             std::string target, ConnectHandler on_ready) = 0;
  /// Enqueue one text frame. Implementations must serialize writes: Beast
  /// permits exactly one in-flight async_write per stream, and auth,
  /// heartbeat replies and chunked subscribes all want to write at once.
  virtual void send(std::string payload) = 0;
  virtual void start_reading(ReadHandler on_message) = 0;
  virtual void close() = 0;
  virtual bool is_open() const = 0;
};

/// The real transport: Beast WebSocket over TLS, with an outbound write queue.
class WebSocketTransport
    : public ITransport,
      public std::enable_shared_from_this<WebSocketTransport> {
 public:
  WebSocketTransport(boost::asio::any_io_executor executor,
                     boost::asio::ssl::context& ssl);

  void async_connect(std::string host, std::string port, std::string target,
                     ConnectHandler on_ready) override;
  void send(std::string payload) override;
  void start_reading(ReadHandler on_message) override;
  void close() override;
  bool is_open() const override;

 private:
  void write_next();
  void read_next();
  void fail(boost::system::error_code ec);

  using Stream = boost::beast::websocket::stream<
      boost::beast::ssl_stream<boost::beast::tcp_stream>>;

  boost::asio::any_io_executor m_executor;
  boost::asio::ip::tcp::resolver m_resolver;
  Stream m_stream;
  boost::beast::flat_buffer m_buffer;
  std::deque<std::string> m_outbox;
  bool m_writing = false;
  bool m_open = false;
  ConnectHandler m_on_ready;
  ReadHandler m_on_message;
};

class JsonRpcSession : public std::enable_shared_from_this<JsonRpcSession> {
 public:
  using ResponseHandler = std::function<void(boost::system::error_code,
                                             const boost::json::value& result)>;
  using NotificationHandler = std::function<void(
      std::string_view channel, const boost::json::object& data)>;
  using MethodHandler = std::function<void(std::string_view method,
                                           const boost::json::object& params)>;
  using ConnectHandler = std::function<void(boost::system::error_code)>;
  using CloseHandler = std::function<void(boost::system::error_code)>;
  using FrameHandler = std::function<void()>;

  JsonRpcSession(boost::asio::any_io_executor executor,
                 std::shared_ptr<ITransport> transport);

  void async_connect(std::string host, std::string port, std::string target,
                     ConnectHandler on_ready);

  /// Send a request and register its handler. Every call gets a deadline: a
  /// response that never arrives must fail its handler rather than leak an
  /// entry in the pending map forever, or a dropped-but-not-closed socket
  /// leaves the session wedged with no error surfaced.
  std::uint64_t call(std::string_view method, boost::json::object params,
                     ResponseHandler handler,
                     std::chrono::milliseconds timeout);

  void on_notification(NotificationHandler handler);
  void on_method(MethodHandler handler);
  void on_close(CloseHandler handler);
  /// Invoked for every inbound frame, whatever its shape -- the signal a
  /// staleness watchdog keys off.
  void on_frame(FrameHandler handler);

  void close();
  bool is_open() const;
  std::size_t pending_calls() const;

 private:
  struct Pending {
    ResponseHandler handler;
    std::unique_ptr<boost::asio::steady_timer> deadline;
  };

  void handle_message(boost::system::error_code ec, std::string_view payload);
  void dispatch(const boost::json::value& frame);
  void complete(std::uint64_t id, boost::system::error_code ec,
                const boost::json::value& result);
  void fail_all_pending(boost::system::error_code ec);

  boost::asio::any_io_executor m_executor;
  std::shared_ptr<ITransport> m_transport;
  std::unordered_map<std::uint64_t, Pending> m_pending;
  std::uint64_t m_next_id = 1;

  NotificationHandler m_on_notification;
  MethodHandler m_on_method;
  CloseHandler m_on_close;
  FrameHandler m_on_frame;
};

}  // namespace shawlynot::quant::marketdata

namespace boost::system {
template <>
struct is_error_code_enum<shawlynot::quant::marketdata::JsonRpcError> : std::true_type {};
}  // namespace boost::system

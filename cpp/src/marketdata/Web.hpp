#pragma once

// TLS transport for the Deribit ingest: an HTTPS request/response pair and a
// WebSocket text stream. Venue-agnostic -- nothing here knows what JSON-RPC is,
// what a channel is, or that Deribit exists.
//
// Scope is deliberately the minimum the ingest needs, and each omission is a
// decision rather than an oversight:
//
//   * No write queue. Beast permits exactly one in-flight async_write per
//     stream, which normally forces an outbound queue. Here the subscribe is
//     the only write and it happens before the read loop starts, so there is
//     one writer by construction. Adding a queue would be machinery guarding
//     against a concurrency this design does not have.
//   * No reconnect, no backoff, no heartbeat. Those are connection *policy* and
//     live in Deribit.cpp with the rest of the ingest lifecycle.
//   * No enable_shared_from_this. Callback-style Asio needs a shared_ptr to
//     outlive its handlers (docs/transport-lifetime.md walks through why); a
//     coroutine does not, because the awaiting frame owns the session for the
//     whole of every suspended operation. The sessions are ordinary move-only
//     RAII objects.
//
// Clients own what is shared per process -- the executor, the TLS context, a
// resolver -- and hand back a connected session. Sessions own one stream and do
// message-level I/O only.
//
// Every operation throws boost::system::system_error on failure rather than
// returning an error_code, so one try/catch around the ingest coroutine covers
// connect, send and read alike.

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json/value.hpp>
#include <string>
#include <string_view>

namespace shawlynot::quant::marketdata {

/// A TLS context with peer verification on and the system trust store loaded.
/// Both clients need one and neither should invent its own policy.
boost::asio::ssl::context make_tls_context();

/// One HTTPS connection, good for a request/response pair.
class HttpSession {
 public:
  using Stream = boost::beast::ssl_stream<boost::beast::tcp_stream>;

  explicit HttpSession(Stream stream) : m_stream(std::move(stream)) {}

  HttpSession(HttpSession&&) = default;
  HttpSession& operator=(HttpSession&&) = delete;
  HttpSession(const HttpSession&) = delete;
  HttpSession& operator=(const HttpSession&) = delete;

  /// POST a JSON body and parse the JSON response. `host` goes in the Host
  /// header. Throws on transport failure, a non-200 status, or a body that is
  /// not JSON.
  boost::asio::awaitable<boost::json::value> post(std::string host,
                                                  std::string target,
                                                  std::string body);

  /// Best-effort TLS shutdown. Never throws: the response is already in hand
  /// by the time this runs, and a peer that closes without a close_notify is
  /// not a failure worth propagating.
  boost::asio::awaitable<void> close();

 private:
  Stream m_stream;
  boost::beast::flat_buffer m_buffer;
};

class HttpClient {
 public:
  HttpClient(boost::asio::any_io_executor executor,
             boost::asio::ssl::context& tls)
      : m_executor(std::move(executor)), m_resolver(m_executor), m_tls(tls) {}

  boost::asio::awaitable<HttpSession> connect(std::string host,
                                              std::string port);

 private:
  boost::asio::any_io_executor m_executor;
  boost::asio::ip::tcp::resolver m_resolver;
  boost::asio::ssl::context& m_tls;
};

/// One WebSocket connection carrying UTF-8 text frames.
class WebSocketSession {
 public:
  using Stream = boost::beast::websocket::stream<
      boost::beast::ssl_stream<boost::beast::tcp_stream>>;

  explicit WebSocketSession(Stream stream) : m_stream(std::move(stream)) {}

  WebSocketSession(WebSocketSession&&) = default;
  WebSocketSession& operator=(WebSocketSession&&) = delete;
  WebSocketSession(const WebSocketSession&) = delete;
  WebSocketSession& operator=(const WebSocketSession&) = delete;

  boost::asio::awaitable<void> send(std::string_view text);

  /// Read exactly one frame. Throws boost::beast::websocket::error::closed
  /// when the peer closes, which is how the read loop ends.
  boost::asio::awaitable<std::string> read();

  /// Best-effort close, safe to call on an already-dead stream.
  void close();

 private:
  Stream m_stream;
  boost::beast::flat_buffer m_buffer;
};

class WebSocketClient {
 public:
  WebSocketClient(boost::asio::any_io_executor executor,
                  boost::asio::ssl::context& tls)
      : m_executor(std::move(executor)), m_resolver(m_executor), m_tls(tls) {}

  boost::asio::awaitable<WebSocketSession> connect(std::string host,
                                                   std::string port,
                                                   std::string target);

 private:
  boost::asio::any_io_executor m_executor;
  boost::asio::ip::tcp::resolver m_resolver;
  boost::asio::ssl::context& m_tls;
};

}  // namespace shawlynot::quant::marketdata

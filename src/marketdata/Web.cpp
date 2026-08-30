#include "marketdata/Web.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <boost/asio/buffer.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/parse.hpp>
#include <boost/system/system_error.hpp>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace shawlynot::quant::marketdata {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;

using asio::use_awaitable;
using tcp = asio::ip::tcp;

/// Connect and TLS timeout. The websocket layer installs its own policy once
/// it is up, so this covers only the handshakes.
constexpr std::chrono::seconds kHandshakeTimeout{15};

/// Set the SNI hostname on the ClientHello.
///
/// Neither Asio nor Beast wraps this, and omitting it is *the* Deribit/Beast
/// failure mode: a multi-tenant edge cannot pick a certificate without it and
/// the handshake fails with no useful diagnostic.
void set_sni(beast::ssl_stream<beast::tcp_stream>& stream,
             const std::string& host) {
  if (SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()) != 1) {
    const boost::system::error_code ec{
        static_cast<int>(::ERR_get_error()),
        asio::error::get_ssl_category()};  // renders the OpenSSL code readably
    throw boost::system::system_error{ec, "SNI"};
  }
  stream.set_verify_callback(asio::ssl::host_name_verification(host));
}

/// DNS -> TCP -> TLS, shared by both clients.
asio::awaitable<void> tls_connect(tcp::resolver& resolver,
                                  beast::ssl_stream<beast::tcp_stream>& stream,
                                  const std::string& host,
                                  const std::string& port) {
  const auto endpoints = co_await resolver.async_resolve(host, port,
                                                         use_awaitable);

  // tcp_stream carries its own timeout timer; a plain socket would need a
  // separate steady_timer and cancellation logic for the same guarantee.
  beast::get_lowest_layer(stream).expires_after(kHandshakeTimeout);
  co_await beast::get_lowest_layer(stream).async_connect(endpoints,
                                                         use_awaitable);

  set_sni(stream, host);
  co_await stream.async_handshake(asio::ssl::stream_base::client,
                                  use_awaitable);
}

}  // namespace

asio::ssl::context make_tls_context() {
  asio::ssl::context tls{asio::ssl::context::tls_client};
  tls.set_verify_mode(asio::ssl::verify_peer);
  tls.set_default_verify_paths();
  return tls;
}

// ── HTTP ────────────────────────────────────────────────────────────

asio::awaitable<HttpSession> HttpClient::connect(std::string host,
                                                 std::string port) {
  HttpSession::Stream stream{m_executor, m_tls};
  co_await tls_connect(m_resolver, stream, host, port);
  co_return HttpSession{std::move(stream)};
}

asio::awaitable<boost::json::value> HttpSession::post(std::string host,
                                                      std::string target,
                                                      std::string body) {
  http::request<http::string_body> request{http::verb::post, target, 11};
  request.set(http::field::host, host);
  request.set(http::field::content_type, "application/json");
  request.set(http::field::accept, "application/json");
  request.body() = std::move(body);
  request.prepare_payload();

  beast::get_lowest_layer(m_stream).expires_after(kHandshakeTimeout);
  co_await http::async_write(m_stream, request, use_awaitable);

  http::response<http::string_body> response;
  co_await http::async_read(m_stream, m_buffer, response, use_awaitable);

  if (response.result() != http::status::ok) {
    // The body may echo the request, so it is not reproduced here: this call
    // is only ever used for public/auth and its request carries the secret.
    throw std::runtime_error("HTTP " + std::to_string(response.result_int()) +
                             " from " + target);
  }

  boost::system::error_code ec;
  boost::json::value parsed = boost::json::parse(response.body(), ec);
  if (ec) {
    throw std::runtime_error("response from " + target + " is not JSON");
  }
  co_return parsed;
}

asio::awaitable<void> HttpSession::close() {
  boost::system::error_code ec;
  co_await m_stream.async_shutdown(
      asio::redirect_error(use_awaitable, ec));  // discarded on purpose
  beast::get_lowest_layer(m_stream).close();
  co_return;
}

// ── WebSocket ───────────────────────────────────────────────────────

asio::awaitable<WebSocketSession> WebSocketClient::connect(
    std::string host, std::string port, std::string target) {
  WebSocketSession::Stream stream{m_executor, m_tls};
  co_await tls_connect(m_resolver, stream.next_layer(), host, port);

  // Disarm the tcp_stream timer: the websocket layer installs its own, and two
  // independent timers on one socket race -- the tcp one would fire mid-session
  // and kill a healthy connection.
  beast::get_lowest_layer(stream).expires_never();
  stream.set_option(
      websocket::stream_base::timeout::suggested(beast::role_type::client));

  co_await stream.async_handshake(host, target, use_awaitable);
  co_return WebSocketSession{std::move(stream)};
}

asio::awaitable<void> WebSocketSession::send(std::string_view text) {
  m_stream.text(true);
  co_await m_stream.async_write(asio::buffer(text), use_awaitable);
  co_return;
}

asio::awaitable<std::string> WebSocketSession::read() {
  m_buffer.clear();
  co_await m_stream.async_read(m_buffer, use_awaitable);
  co_return beast::buffers_to_string(m_buffer.data());
}

void WebSocketSession::close() {
  boost::system::error_code ec;
  m_stream.close(websocket::close_code::normal, ec);
  if (ec) {
    beast::get_lowest_layer(m_stream).close();
  }
}

}  // namespace shawlynot::quant::marketdata

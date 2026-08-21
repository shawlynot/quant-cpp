#include "marketdata/JsonRpcSession.hpp"

#include <openssl/ssl.h>

#include <boost/asio/connect.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <utility>

#include "core/Log.hpp"

namespace shawlynot::quant::marketdata {
namespace {

class JsonRpcCategory : public boost::system::error_category {
 public:
  const char* name() const noexcept override { return "quant.jsonrpc"; }

  std::string message(int value) const override {
    switch (static_cast<JsonRpcError>(value)) {
      case JsonRpcError::ok:
        return "ok";
      case JsonRpcError::timed_out:
        return "request timed out";
      case JsonRpcError::session_closed:
        return "session closed with request pending";
      case JsonRpcError::malformed:
        return "malformed JSON-RPC frame";
    }
    return "unknown";
  }
};

class VenueCategory : public boost::system::error_category {
 public:
  const char* name() const noexcept override { return "quant.venue"; }

  std::string message(int value) const override {
    // 10028 is the one worth naming: Deribit tears the session down on it,
    // so it has to be handled as connection-fatal rather than as a
    // rejected request.
    if (value == 10028) {
      return "too_many_requests (session terminated)";
    }
    return "venue error " + std::to_string(value);
  }
};

}  // namespace

const boost::system::error_category& jsonrpc_category() {
  static const JsonRpcCategory category;
  return category;
}

boost::system::error_code make_error_code(JsonRpcError error) {
  return {static_cast<int>(error), jsonrpc_category()};
}

const boost::system::error_category& venue_category() {
  static const VenueCategory category;
  return category;
}

boost::system::error_code make_venue_error(int code) {
  return {code, venue_category()};
}

// ── WebSocketTransport ────────────────────────────────────────────────────

WebSocketTransport::WebSocketTransport(boost::asio::any_io_executor executor,
                                       boost::asio::ssl::context& ssl)
    : m_executor(std::move(executor)),
      m_resolver(m_executor),
      m_stream(m_executor, ssl) {}

void WebSocketTransport::async_connect(std::string host, std::string port,
                                       std::string target,
                                       ConnectHandler on_ready) {
  m_on_ready = std::move(on_ready);

  auto self = shared_from_this();
  m_resolver.async_resolve(
      host, port,
      [this, self, host, target](
          boost::system::error_code ec,
          boost::asio::ip::tcp::resolver::results_type results) {
        if (ec) {
          fail(ec);
          return;
        }

        boost::beast::get_lowest_layer(m_stream).expires_after(
            std::chrono::seconds(15));
        boost::beast::get_lowest_layer(m_stream).async_connect(
            results,
            [this, self, host, target](boost::system::error_code connect_ec,
                                       const boost::asio::ip::tcp::resolver::
                                           results_type::endpoint_type&) {
              if (connect_ec) {
                fail(connect_ec);
                return;
              }

              // Without SNI the edge cannot select a certificate and the
              // handshake fails with an opaque error -- the classic
              // Beast/Deribit trap.
              if (SSL_set_tlsext_host_name(
                      m_stream.next_layer().native_handle(), host.c_str()) ==
                  0) {
                fail(boost::system::error_code{
                    static_cast<int>(::ERR_get_error()),
                    boost::asio::error::get_ssl_category()});
                return;
              }

              m_stream.next_layer().async_handshake(
                  boost::asio::ssl::stream_base::client,
                  [this, self, host, target](boost::system::error_code tls_ec) {
                    if (tls_ec) {
                      fail(tls_ec);
                      return;
                    }

                    // Beast's own timeouts take over from here; the
                    // tcp_stream timer must not also fire mid-session.
                    boost::beast::get_lowest_layer(m_stream).expires_never();
                    m_stream.set_option(
                        boost::beast::websocket::stream_base::timeout::
                            suggested(boost::beast::role_type::client));

                    m_stream.async_handshake(
                        host, target,
                        [this, self](boost::system::error_code ws_ec) {
                          if (ws_ec) {
                            fail(ws_ec);
                            return;
                          }
                          m_open = true;
                          if (auto handler =
                                  std::exchange(m_on_ready, nullptr)) {
                            handler({});
                          }
                        });
                  });
            });
      });
}

void WebSocketTransport::send(std::string payload) {
  if (!m_open) {
    return;
  }
  m_outbox.push_back(std::move(payload));
  // Exactly one write may be in flight; the completion handler chains the
  // next. Skipping this queue produces intermittent, load-dependent frame
  // corruption.
  if (!m_writing) {
    write_next();
  }
}

void WebSocketTransport::write_next() {
  if (m_outbox.empty()) {
    m_writing = false;
    return;
  }
  m_writing = true;

  auto self = shared_from_this();
  m_stream.text(true);
  m_stream.async_write(boost::asio::buffer(m_outbox.front()),
                       [this, self](boost::system::error_code ec, std::size_t) {
                         if (ec) {
                           m_writing = false;
                           fail(ec);
                           return;
                         }
                         m_outbox.pop_front();
                         write_next();
                       });
}

void WebSocketTransport::start_reading(ReadHandler on_message) {
  m_on_message = std::move(on_message);
  read_next();
}

void WebSocketTransport::read_next() {
  auto self = shared_from_this();
  m_stream.async_read(m_buffer,
                      [this, self](boost::system::error_code ec, std::size_t) {
                        if (ec) {
                          fail(ec);
                          return;
                        }
                        const auto data = m_buffer.data();
                        const std::string_view payload{
                            static_cast<const char*>(data.data()), data.size()};
                        if (m_on_message) {
                          m_on_message({}, payload);
                        }
                        m_buffer.consume(m_buffer.size());
                        read_next();
                      });
}

void WebSocketTransport::close() {
  if (!m_open) {
    return;
  }
  m_open = false;
  boost::system::error_code ec;
  m_stream.close(boost::beast::websocket::close_code::normal, ec);
  boost::beast::get_lowest_layer(m_stream).close();
}

bool WebSocketTransport::is_open() const { return m_open; }

void WebSocketTransport::fail(boost::system::error_code ec) {
  const bool was_open = m_open;
  m_open = false;
  m_outbox.clear();

  if (auto handler = std::exchange(m_on_ready, nullptr)) {
    handler(ec);
    return;
  }
  if (was_open && m_on_message) {
    m_on_message(ec, {});
  }
}

// ── JsonRpcSession ────────────────────────────────────────────────────────

JsonRpcSession::JsonRpcSession(boost::asio::any_io_executor executor,
                               std::shared_ptr<ITransport> transport)
    : m_executor(std::move(executor)), m_transport(std::move(transport)) {}

void JsonRpcSession::async_connect(std::string host, std::string port,
                                   std::string target,
                                   ConnectHandler on_ready) {
  auto self = shared_from_this();
  m_transport->async_connect(
      std::move(host), std::move(port), std::move(target),
      [this, self,
       on_ready = std::move(on_ready)](boost::system::error_code ec) {
        if (!ec) {
          m_transport->start_reading(
              [this, self](boost::system::error_code read_ec,
                           std::string_view payload) {
                handle_message(read_ec, payload);
              });
        }
        if (on_ready) {
          on_ready(ec);
        }
      });
}

std::uint64_t JsonRpcSession::call(std::string_view method,
                                   boost::json::object params,
                                   ResponseHandler handler,
                                   std::chrono::milliseconds timeout) {
  const std::uint64_t id = m_next_id++;

  boost::json::object request;
  request["jsonrpc"] = "2.0";
  request["id"] = id;
  request["method"] = std::string{method};
  request["params"] = std::move(params);

  Pending pending;
  pending.handler = std::move(handler);
  pending.deadline = std::make_unique<boost::asio::steady_timer>(m_executor);
  pending.deadline->expires_after(timeout);

  auto self = shared_from_this();
  pending.deadline->async_wait([this, self, id](boost::system::error_code ec) {
    if (ec == boost::asio::error::operation_aborted) {
      return;
    }
    complete(id, make_error_code(JsonRpcError::timed_out), {});
  });

  m_pending.emplace(id, std::move(pending));
  m_transport->send(boost::json::serialize(request));
  return id;
}

void JsonRpcSession::on_notification(NotificationHandler handler) {
  m_on_notification = std::move(handler);
}

void JsonRpcSession::on_method(MethodHandler handler) {
  m_on_method = std::move(handler);
}

void JsonRpcSession::on_close(CloseHandler handler) {
  m_on_close = std::move(handler);
}

void JsonRpcSession::on_frame(FrameHandler handler) {
  m_on_frame = std::move(handler);
}

void JsonRpcSession::close() {
  m_transport->close();
  fail_all_pending(make_error_code(JsonRpcError::session_closed));
}

bool JsonRpcSession::is_open() const { return m_transport->is_open(); }

std::size_t JsonRpcSession::pending_calls() const { return m_pending.size(); }

void JsonRpcSession::handle_message(boost::system::error_code ec,
                                    std::string_view payload) {
  if (ec) {
    fail_all_pending(ec);
    if (m_on_close) {
      m_on_close(ec);
    }
    return;
  }

  if (m_on_frame) {
    m_on_frame();
  }

  boost::system::error_code parse_ec;
  const boost::json::value frame = boost::json::parse(payload, parse_ec);
  if (parse_ec) {
    // One bad frame must never kill the read loop.
    spdlog::debug("dropping unparseable frame ({} bytes): {}", payload.size(),
                  parse_ec.message());
    return;
  }
  dispatch(frame);
}

void JsonRpcSession::dispatch(const boost::json::value& frame) {
  const boost::json::object* const object = frame.if_object();
  if (object == nullptr) {
    return;
  }

  if (const boost::json::value* const id = object->if_contains("id")) {
    std::uint64_t request_id = 0;
    if (const auto* const u = id->if_uint64()) {
      request_id = *u;
    } else if (const auto* const i = id->if_int64()) {
      request_id = static_cast<std::uint64_t>(*i);
    } else {
      return;
    }

    if (const boost::json::value* const error = object->if_contains("error")) {
      int code = 0;
      std::string message;
      if (const auto* const error_object = error->if_object()) {
        if (const auto* const c = error_object->if_contains("code")) {
          code = static_cast<int>(c->to_number<std::int64_t>());
        }
        if (const auto* const m = error_object->if_contains("message")) {
          if (const auto* const s = m->if_string()) {
            message.assign(s->data(), s->size());
          }
        }
      }
      spdlog::warn("request {} failed: venue error {} ({})", request_id, code,
                   message);
      complete(request_id, make_venue_error(code), {});
      return;
    }

    const boost::json::value* const result = object->if_contains("result");
    complete(request_id, {},
             result == nullptr ? boost::json::value{} : *result);
    return;
  }

  const boost::json::value* const method = object->if_contains("method");
  if (method == nullptr) {
    return;
  }
  const auto* const method_name = method->if_string();
  if (method_name == nullptr) {
    return;
  }
  const std::string_view name{method_name->data(), method_name->size()};

  const boost::json::value* const params = object->if_contains("params");
  const boost::json::object* const params_object =
      params == nullptr ? nullptr : params->if_object();
  if (params_object == nullptr) {
    return;
  }

  if (name == "subscription") {
    const boost::json::value* const channel =
        params_object->if_contains("channel");
    const boost::json::value* const data = params_object->if_contains("data");
    if (channel == nullptr || data == nullptr) {
      return;
    }
    const auto* const channel_name = channel->if_string();
    const boost::json::object* const data_object = data->if_object();
    if (channel_name == nullptr || data_object == nullptr) {
      return;
    }
    if (m_on_notification) {
      m_on_notification(
          std::string_view{channel_name->data(), channel_name->size()},
          *data_object);
    }
    return;
  }

  if (m_on_method) {
    m_on_method(name, *params_object);
  }
}

void JsonRpcSession::complete(std::uint64_t id, boost::system::error_code ec,
                              const boost::json::value& result) {
  const auto it = m_pending.find(id);
  if (it == m_pending.end()) {
    return;
  }
  Pending pending = std::move(it->second);
  m_pending.erase(it);

  if (pending.deadline) {
    pending.deadline->cancel();
  }
  if (pending.handler) {
    pending.handler(ec, result);
  }
}

void JsonRpcSession::fail_all_pending(boost::system::error_code ec) {
  auto pending = std::move(m_pending);
  m_pending.clear();
  for (auto& [id, entry] : pending) {
    if (entry.deadline) {
      entry.deadline->cancel();
    }
    if (entry.handler) {
      entry.handler(ec, {});
    }
  }
}

}  // namespace shawlynot::quant::marketdata

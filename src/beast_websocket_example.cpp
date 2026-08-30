//
// Copyright (c) 2016-2019 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/boostorg/beast
//

//------------------------------------------------------------------------------
//
// Example: WebSocket client, asynchronous
//
//------------------------------------------------------------------------------

#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>

//------------------------------------------------------------------------------

// Report a failure
void fail(boost::beast::error_code ec, char const* what) {
  std::cerr << what << ": " << ec.message() << "\n";
}

// Sends a WebSocket message and prints the response
class session : public std::enable_shared_from_this<session> {
  boost::asio::ip::tcp::resolver resolver_;
  boost::beast::websocket::stream<boost::beast::tcp_stream> ws_;
  boost::beast::flat_buffer buffer_;
  std::string host_;
  std::string text_;

 public:
  // Resolver and socket require an io_context
  explicit session(boost::asio::io_context& ioc)
      : resolver_(boost::asio::make_strand(ioc)),
        ws_(boost::asio::make_strand(ioc)) {}

  // Start the asynchronous operation
  void run(char const* host, char const* port, char const* text) {
    // Save these for later
    host_ = host;
    text_ = text;

    // Look up the domain name
    resolver_.async_resolve(host, port,
                            boost::beast::bind_front_handler(
                                &session::on_resolve, shared_from_this()));
  }

  void on_resolve(boost::beast::error_code ec,
                  boost::asio::ip::tcp::resolver::results_type results) {
    if (ec) return fail(ec, "resolve");

    // Set the timeout for the operation
    boost::beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));

    // Make the connection on the IP address we get from a lookup
    boost::beast::get_lowest_layer(ws_).async_connect(
        results, boost::beast::bind_front_handler(&session::on_connect,
                                                  shared_from_this()));
  }

  void on_connect(
      boost::beast::error_code ec,
      boost::asio::ip::tcp::resolver::results_type::endpoint_type ep) {
    if (ec) return fail(ec, "connect");

    // Turn off the timeout on the tcp_stream, because
    // the websocket stream has its own timeout system.
    boost::beast::get_lowest_layer(ws_).expires_never();

    // Set suggested timeout settings for the websocket
    ws_.set_option(boost::beast::websocket::stream_base::timeout::suggested(
        boost::beast::role_type::client));

    // Set a decorator to change the User-Agent of the handshake
    ws_.set_option(boost::beast::websocket::stream_base::decorator(
        [](boost::beast::websocket::request_type& req) {
          req.set(boost::beast::http::field::user_agent,
                  std::string(BOOST_BEAST_VERSION_STRING) +
                      " websocket-client-async");
        }));

    // Update the host_ string. This will provide the value of the
    // Host HTTP header during the WebSocket handshake.
    // See https://tools.ietf.org/html/rfc7230#section-5.4
    host_ += ':' + std::to_string(ep.port());

    // Perform the websocket handshake
    ws_.async_handshake(host_, "/",
                        boost::beast::bind_front_handler(&session::on_handshake,
                                                         shared_from_this()));
  }

  void on_handshake(boost::beast::error_code ec) {
    if (ec) return fail(ec, "handshake");

    // Send the message
    ws_.async_write(boost::asio::buffer(text_),
                    boost::beast::bind_front_handler(&session::on_write,
                                                     shared_from_this()));
  }

  void on_write(boost::beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) return fail(ec, "write");

    // Read a message into our buffer
    ws_.async_read(buffer_, boost::beast::bind_front_handler(
                                &session::on_read, shared_from_this()));
  }

  void on_read(boost::beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) return fail(ec, "read");

    // Close the WebSocket connection
    ws_.async_close(boost::beast::websocket::close_code::normal,
                    boost::beast::bind_front_handler(&session::on_close,
                                                     shared_from_this()));
  }

  void on_close(boost::beast::error_code ec) {
    if (ec) return fail(ec, "close");

    // If we get here then the connection is closed gracefully

    // The make_printable() function helps print a ConstBufferSequence
    std::cout << boost::beast::make_printable(buffer_.data()) << std::endl;
  }
};

//------------------------------------------------------------------------------

int main(int argc, char** argv) {
  // Check command line arguments.
  if (argc != 4) {
    std::cerr << "Usage: websocket-client-async <host> <port> <text>\n"
              << "Example:\n"
              << "    websocket-client-async echo.websocket.org 80 \"Hello, "
                 "world!\"\n";
    return EXIT_FAILURE;
  }
  auto const host = argv[1];
  auto const port = argv[2];
  auto const text = argv[3];

  // The io_context is required for all I/O
  boost::asio::io_context ioc;

  // Launch the asynchronous operation
  std::make_shared<session>(ioc)->run(host, port, text);

  // Run the I/O service. The call will return when
  // the socket is closed.
  ioc.run();

  return EXIT_SUCCESS;
}

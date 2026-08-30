# Boost calls in `WebSocketTransport::async_connect`

Reference notes for `src/marketdata/WebSocketTransport.cpp:17` — a four-stage async
chain where each stage talks to a different layer of the stream.

The type behind it all (`src/marketdata/WebSocketTransport.hpp:68`) is a three-layer
onion:

```
websocket::stream< ssl_stream< tcp_stream > >
       ^                ^           ^
   framing/upgrade    TLS      TCP + timeouts
```

Every Boost call below is picking one of those layers to talk to.

---

## The layer accessors

**`boost::beast::get_lowest_layer(m_stream)`** (:90, :92, :124) — peels *all* the
way down to the innermost `tcp_stream`, skipping the TLS layer. Beast provides
this so you don't have to write `m_stream.next_layer().next_layer()`, which
breaks whenever you add or remove a layer. Use it for anything that is a
property of the socket itself: connecting, timeouts, hard close.

**`m_stream.next_layer()`** (:106, :114) — peels down exactly *one* layer, giving
the `ssl_stream`. That's the object that owns the `SSL*` and performs the TLS
handshake.

---

## Stage 1 — DNS

**`m_resolver.async_resolve(host, port, handler)`** (:80) —
`boost::asio::ip::tcp::resolver`. Async DNS; the handler receives a
`results_type`, which is a *range* of candidate endpoints (v4 and v6, multiple A
records). It doesn't touch the socket at all.

## Stage 2 — TCP

**`get_lowest_layer(m_stream).expires_after(std::chrono::seconds(15))`** (:90) —
`tcp_stream` is Beast's socket wrapper whose whole reason to exist is a built-in
timeout timer. This arms it: if the next operation on that layer hasn't
completed in 15s, it is cancelled with `beast::error::timeout`. Plain
`asio::ip::tcp::socket` has no such thing — you'd need a separate `steady_timer`
and cancellation logic.

**`get_lowest_layer(m_stream).async_connect(results, handler)`** (:92) — this is
Beast's `tcp_stream::async_connect`, *not* the free function
`boost::asio::async_connect` from `<boost/asio/connect.hpp>`. It's a composed
operation: it walks the resolver results in order, trying each until one
connects — which is why the handler's second parameter is the `endpoint_type`
that actually won (:95, unnamed here since it's unused). It also participates in
the `expires_after` timer above.

## Stage 3 — TLS

**`SSL_set_tlsext_host_name(...)`** (:105) — the one non-Boost call. Raw OpenSSL,
because neither Asio nor Beast wraps SNI. It writes the hostname into the
ClientHello so a multi-tenant edge (Deribit's, Cloudflare's, anyone's) can pick
the right certificate. `m_stream.next_layer().native_handle()` hands you the
underlying `SSL*` to set it on. Omit this and you get a handshake failure with
no useful diagnostic.

**`boost::asio::error::get_ssl_category()`** (:110) — pairs `ERR_get_error()` (an
OpenSSL error code) with the Asio error category that knows how to render it, so
the resulting `error_code` prints something meaningful rather than a bare
integer.

**`m_stream.next_layer().async_handshake(ssl::stream_base::client, handler)`**
(:114) — the TLS handshake on the `ssl_stream`. `stream_base::client` selects
the client role (vs `server`): send ClientHello and validate the peer's chain
rather than present one.

## Stage 4 — WebSocket

**`get_lowest_layer(m_stream).expires_never()`** (:124) — disarms the
`tcp_stream` timer. Necessary because Beast's websocket layer has its *own*
timeout machinery, and two independent timers on the same socket will race: the
tcp_stream one would fire mid-session and kill a healthy connection.

**`m_stream.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client))`**
(:125) — installs that websocket-level policy. `suggested(client)` is a preset
giving a 30s handshake timeout, a 30s idle timeout, and automatic ping
keep-alive — Beast pings when the connection goes quiet and tears down if there
is no pong. `role_type::client` vs `server` changes the defaults (a server
doesn't keep-alive-ping by default).

**`m_stream.async_handshake(host, target, handler)`** (:129) — the WebSocket
upgrade: sends `GET <target> HTTP/1.1` with `Upgrade: websocket` and the
`Sec-WebSocket-Key`, validates the `101` response. `host` goes into the `Host:`
header; `target` is the request path (`/ws/api/v2`). After this completes the
stream is in message mode and `async_read`/`async_write` become legal.

---

## Lifetime mechanics threaded through all of it

**`shared_from_this()`** (:79) — captured as `self` in every nested lambda. Asio
handlers execute later, off the `io_context`, long after `async_connect` has
returned; the captured `shared_ptr` keeps the transport alive until the last
handler runs. The `this` capture alongside it is just for convenient member
access — `self` is what actually owns the object.

**`std::exchange(m_on_ready, nullptr)`** (:138, and in `fail` at :222) — not
Boost, but it is the invariant that makes the chain safe: it hands you the
handler and nulls the member in one step, so `on_ready` fires exactly once
whether the chain succeeds at :137 or bails out through `fail()` at any of the
four failure points.

---

## Incidental observation

`#include <boost/asio/connect.hpp>` at :5 covers the free-function
`async_connect`, but this code uses the `tcp_stream` member overload, which
comes in via `<boost/beast/core.hpp>`. The include is harmless but no longer
earning its place.

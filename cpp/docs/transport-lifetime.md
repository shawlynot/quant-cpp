# Transport lifetime: why `self` is captured, and how to stop advertising it

Notes on `src/marketdata/WebSocketTransport.cpp:110,131` — the
`auto self = shared_from_this()` captures in `write_next()` and `read_next()`,
the objection to them, and the refactor that resolves it.

---

## What `self` does today

`WebSocketTransport` derives from `std::enable_shared_from_this`
(`src/marketdata/WebSocketTransport.hpp:50`). Both async operations capture a
`shared_ptr` to themselves into the completion handler:

```cpp
auto self = shared_from_this();
m_stream.async_read(m_buffer,
                    [this, self](boost::system::error_code ec, std::size_t) {
                      ...   // body uses `this`, never `self`
                    });
```

`self` is never referenced in the body. The capture *is* the effect: Asio owns
the handler object until it is invoked and destroyed, so the copied `shared_ptr`
holds the refcount above zero and guarantees `this` is still valid when the
handler runs — even if every other owner has dropped the transport in the
meantime.

## The objection

The requirement to allocate via `make_shared` has leaked out of the class and
into its callers:

- `DeribitSession::TransportFactory` is typed
  `std::function<std::shared_ptr<ITransport>()>`
  (`src/marketdata/deribit.hpp:129`), and `m_transport` is a `shared_ptr`
  (`:223`) even though the session is the sole owner.
- `src/main.cpp:126-129` needs a `static_pointer_cast` to satisfy that factory
  signature.

Nothing about the transport's contract should announce how it was allocated.
That part of the criticism is correct.

## Why "make the destructor wait for in-flight I/O" does not work here

The obvious RAII alternative — block in `~WebSocketTransport()` until pending
reads/writes finish — is unworkable in this program, for mechanical reasons
rather than stylistic ones.

Cancellation in Asio is itself asynchronous. `close()`/`cancel()` does not
retract a pending handler; it makes that handler complete *later* with
`operation_aborted`, dispatched through the `io_context` queue like any other.
So "wait until I/O is complete" means "wait until the `io_context` runs those
handlers". Three consequences:

1. **The teardown path runs on the `io_context` thread.**
   `DeribitSession::teardown()` calls `m_transport.reset()`
   (`src/marketdata/deribit.cpp:152`), reached from `schedule_reconnect()` at
   `:203, :350, :389, :495, :599, :667` — every one of those fires from inside a
   completion handler. A blocking destructor there waits for handlers that only
   the blocked thread can dispatch. Deadlock on the ordinary reconnect path, not
   a corner case.

2. **One of those handlers belongs to the transport itself.** A read error at
   `deribit.cpp:203` is delivered from the read lambda at
   `WebSocketTransport.cpp:131`; that lambda calls into the session, which
   resets the transport. The destructor would be waiting on the very read
   operation whose handler is currently on the stack. `self` is what prevents
   this today: `reset()` drops the session's reference, and the object survives
   until the handler returns.

3. **At shutdown there is no runner at all.** `main` calls `io.stop()`
   (`src/main.cpp:138`) before anything unwinds. Queued handlers never run, so a
   joining destructor hangs forever.

A joining destructor is sound where the waiter is not the worker —
`asio::thread_pool::~thread_pool()`, `std::jthread`, thread-per-connection with
blocking I/O. It does not transfer to a single-threaded proactor.

## Proposal: keep the shared lifetime, stop exposing it

Split the class into a public handle and a private implementation. The handle is
an ordinary RAII object; the `shared_ptr` becomes an implementation detail.

```cpp
class WebSocketTransport : public ITransport {
 public:
  WebSocketTransport(boost::asio::any_io_executor executor,
                     boost::asio::ssl::context& ssl);
  ~WebSocketTransport() override { m_impl->detach(); }  // close + drop;
                                                        // impl frees itself
                                                        // when handlers drain

  void async_connect(std::string host, std::string port, std::string target,
                     ConnectHandler on_ready) override;
  void send(std::string payload) override;
  void start_reading(ReadHandler on_message) override;
  void close() override;
  bool is_open() const override;

 private:
  struct Impl;                    // stream, outbox, enable_shared_from_this
  std::shared_ptr<Impl> m_impl;
};
```

`Impl` keeps the `self` captures, because that is genuinely how Asio handler
lifetime works. `WebSocketTransport` becomes destructible from anywhere,
including from inside a handler, without blocking.

Follow-on cleanups this unlocks:

- `TransportFactory` returns `std::unique_ptr<ITransport>`.
- `DeribitSession::m_transport` becomes a `unique_ptr`.
- The `static_pointer_cast` in `src/main.cpp` disappears.

`detach()` needs to be safe to call from within a handler: mark the impl
detached, `close()` the stream so pending operations complete with
`operation_aborted`, and let the last handler's `self` copy free it. It must
never block.

**Cost:** a forwarding layer of roughly 40 lines and one extra indirection per
call, none of it on a hot path.

**Alternative considered:** C++20 coroutines with `co_spawn`. This does not help
— the coroutine frame still needs the object alive across suspension points, so
the same lifetime question reappears in a different syntax.

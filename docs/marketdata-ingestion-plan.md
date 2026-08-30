# Market Data Ingestion Plan — Deribit `ticker.{instrument_name}.{interval}`

**Status:** design only, not implemented
**Target files:** `src/marketdata/model.{hpp,cpp}`, `src/marketdata/deribit.{hpp,cpp}` (both currently empty), plus new files listed in §5
**Scope:** Phase 0 of `quant-architecture.md` — the market data gateway. Authenticate to Deribit with `CLIENT_ID`/`CLIENT_SECRET`, hold one WebSocket connection, and stream three things: the `ticker` channel for every active BTC **option**, the `ticker` channel for every dated BTC **future**, and `deribit_price_index.btc_usd` for the **underlying spot**. Normalize all three into one POD `Tick` — id, exchange timestamp, receive timestamp, price — and hand them to a downstream sink.

**Universe:** options, dated futures, and the spot index — all three are normalized and published to the sink the same way; the gateway derives nothing from any of them. All three map onto `instrument_type` values `security_master` already allows (`option`, `future`, `currency`), though only `option` and `currency` have rows today (§7.3).

---

## 1. What already exists

| Piece | State |
|---|---|
| `src/marketdata/{deribit,model}.{hpp,cpp}` | Empty stubs, already listed in `add_executable` |
| `src/{greeks,pricer,queue,volfitter}/` | Empty directories — the intended component split |
| `CMakeLists.txt` | C++20, finds Boost 1.83 (`json`), OpenSSL, Threads. **Only links `Boost::json`** |
| `tests/` | GoogleTest via FetchContent, links no project code |
| `py/src/shawlynot/quant/` | Async REST client + `OptionContract` model + Postgres `security_master` upsert (`instrument`, `option`, `currencies`). Seeds one `currencies` row: symbol **`btc_usd`**. Fetches `kind=option` only — **no future rows exist yet** (§7.3) |
| `.env` | `CLIENT_ID`, `CLIENT_SECRET`, `POSTGRES_*` — loaded by the Python layer via `python-dotenv`; the C++ gateway reads the process environment only (§6) |

The Python layer owns *reference* data (what instruments exist); this plan covers *streaming* data (what they are priced at). They are now coupled rather than parallel: the gateway takes its instrument identity from `security_master` at startup (§7.3), which makes the Python ingest a prerequisite rather than a sibling.

---

## 2. Protocol facts this design depends on

Verified against `docs.deribit.com` rather than recalled:

- **Endpoint:** `wss://www.deribit.com/ws/api/v2`, JSON-RPC 2.0. Testnet: `wss://test.deribit.com/ws/api/v2`.
- **`public/auth`** takes `grant_type` ∈ {`client_credentials`, `client_signature`, `refresh_token`}. With `client_credentials` you send `client_id` + `client_secret`. The result carries `access_token`, `token_type` (`bearer`), `expires_in` (seconds), `refresh_token`, `scope`, optional `sid`, `enabled_features`. Renew with `grant_type=refresh_token` — no need to resend the secret.
- **`scope`** is an optional request param supporting `connection`, `session:name`, `expires:NUMBER`, and read/write grants per area.
- **Why authenticate at all** (the `ticker` channel is nominally public):
  1. **The `raw` interval requires an authenticated connection** — unauthenticated clients cannot subscribe to `raw` at all.
  2. Authenticated connections draw on a **per-subaccount credit pool with tier-scaled limits**; unauthenticated ones are **rate-limited per IP** with less predictable headroom.
  3. It is the prerequisite for any private channel later (positions, own orders).
- **Rate limits (non-matching-engine):** 500 credits/request, 50,000 credit pool, 10,000 credits/s refill ⇒ ~20 req/s sustained, ~100 burst. **`public/subscribe` costs 3,000 credits with a burst of 10** ⇒ ~3.3 subscribe calls/second sustained. Breaching returns `too_many_requests` (**code 10028**) *and terminates the session*.
- **`public/set_heartbeat`** takes `interval` in seconds, **minimum 10**. The server then sends two message types: `heartbeat` and `test_request`. On `test_request` the client **must** issue `public/test` or the connection is dropped immediately. `public/disable_heartbeat` turns it off. WebSocket-only method.
- **Notification envelope:**
  ```json
  {"jsonrpc":"2.0","method":"subscription","params":{"channel":"...","data":{...}}}
  ```
- **`ticker.{instrument_name}.{interval}` intervals:** `raw` (1 ms, authorized only), `100ms`, `agg2` (2 s).
- **`ticker` payload (option instrument):**
  - Common: `instrument_name`, `timestamp` (ms since epoch), `state` (`open`/`settlement`/`delivered`/`inactive`/`locked`/`halted`/`archivized`), `open_interest`, `best_bid_price`, `best_bid_amount`, `best_ask_price`, `best_ask_amount`, `index_price`, `min_price`, `max_price`, `mark_price`, `last_price`, `estimated_delivery_price`
  - `stats`: `volume`, `low`, `high`, `price_change`, `volume_usd` (futures only)
  - Options-only: `underlying_price`, `underlying_index`, `interest_rate`, `bid_iv`, `ask_iv`, `mark_iv`
  - `greeks` (options only): `delta`, `gamma`, `vega`, `theta`, `rho`
  - Futures-only: `delivery_price` (when `state` is closed), `settlement_price` (when open). `stats.volume_usd` is also futures-only.
  - Perpetual-only: `current_funding`, `funding_8h`, `interest_value` — out of scope; `BTC-PERPETUAL` is neither ingested nor subscribed (§7.1).
  - A **future's** ticker carries no `greeks` and no `mark_iv`/`bid_iv`/`ask_iv`; those are options-only.
- **`public/get_instruments`** takes `kind` ∈ {`future`, `option`, `spot`, `future_combo`, `option_combo`}. **`kind=future` returns dated futures *and* the perpetual**; they are separated by `settlement_period` — `"month"`/`"week"` for dated, `"perpetual"` for `BTC-PERPETUAL`. Filter on that field, not on the name: `BTC-PERPETUAL` is excluded everywhere in this design (§7.1), and name-matching would miss a future whose symbol convention changes.
- **`public/subscribe`** takes `channels: string[]` and returns the array of channels actually subscribed. The docs do **not** publish a hard per-request channel cap — so treat the cap as unknown, keep the subscribed universe small enough that one request is comfortably under it (§8), and always reconcile the returned array against what was requested.

- **`deribit_price_index.{index_name}`** — the underlying spot index. **No interval suffix**, public, and the payload is just three fields:
  ```json
  {"timestamp": 1750000000000, "price": 118234.56, "index_name": "btc_usd"}
  ```
  `index_name` accepts 47 values; the one this project needs is **`btc_usd`** — which is character-for-character the `symbol` already stored in `security_master.currencies`, so the DB row maps to a channel with no translation layer.
- **`public/get_index_price`** — snapshot form of the same value, public, returns `index_price` and `estimated_delivery_price`. Useful to prime the sink at startup so the first option tick is not stranded without a spot.
- **`estimated_expiration_price.{index_name}`** — `{seconds, price, is_estimated, left_ticks, total_ticks}`. Only meaningful near an expiry boundary; not needed for Phase 1.

### Four payload gotchas that will bite silently

1. **IVs are percentages.** `mark_iv: 62.5` means 62.5 %, not 0.625. The gateway no longer carries IVs at all (§5.1), so this cannot bite today — but it is recorded because the moment a quote-shaped message is added back for the fitter, the conversion has to happen **once**, at the codec boundary, or every downstream vol number is off by 100×.
2. **Option prices are in coin.** `last_price` for `BTC-…-C` is quoted in BTC, not USD (`quant-architecture.md` §1.4). The gateway should **not** convert. It normalizes *shape*, never *unit of account* — it records what the venue sent, and the pricer does the USD conversion where the inverse-settlement logic lives. Note what this implies for the single `Tick.price` field: **an option's price and the index's price are in different units**, and only `kind` on the registry entry tells them apart. A consumer that averages or compares them without checking `kind` is comparing BTC to USD.
3. **`underlying_index` is a string, despite the docs typing it `number`.** It holds either a future's name (`"BTC-27JUN25"`) or the literal `"index_price"`. The gateway does not decode it — it derives nothing from an option's stated underlying — but anything downstream that reaches for the field must parse it as a string and branch on the literal, or the first spot-referencing expiry will throw.
4. **Deribit's `instrument_type` is not `security_master.instrument_type`.** In a `get_instruments` response the field means `"linear"` or `"reversed"` — the settlement convention. Your column means `option`/`future`/`perpetual`/`currency`. The Python ingest hardcodes `'option'` today so nothing is wrong yet, but the obvious move when adding futures is to reach for `payload["instrument_type"]`, which silently writes `"reversed"` and violates the CHECK constraint. Derive the column from `kind` + `settlement_period` instead.

---

## 3. Library choices

| Concern | Choice | Why |
|---|---|---|
| WebSocket + TLS | **Boost.Beast** over **Boost.Asio** + **OpenSSL** | Already installed (Boost 1.83, OpenSSL 3.0), header-only, and it is the choice `quant-architecture.md` §12 already commits to. Gives full control of the event loop, which matters because heartbeat replies, token refresh, and the staleness watchdog all need timers on the same executor as the socket. |
| JSON parsing | **Boost.JSON** now, `simdjson` later | Already found and linked by CMake, so zero new dependency. Use `boost::json::stream_parser` with a reusable `boost::json::monotonic_resource` so per-message parsing does no heap allocation. Keep parsing behind `TickerCodec` (§5) so swapping in `simdjson::ondemand` is a one-file change when tick volume justifies it. |
| Timers, backoff, refresh | `boost::asio::steady_timer` | Same executor as the socket ⇒ no cross-thread synchronization needed. |
| Config / secrets | **`std::getenv`, nothing more** | The process environment is the interface; *populating* it is a platform concern (systemd `EnvironmentFile`, container env, `direnv`, or a shell that sourced `.env`). The gateway reads and validates, and owns no file format. |
| Logging | **spdlog** via `FetchContent` | Accepted. Async sink keeps formatting off the read loop, which matters because the io thread has a heartbeat deadline. `FetchContent` rather than apt, for consistency with how googletest is already pinned. |
| Postgres client | **libpqxx** (`apt install libpqxx-dev`) | Needed now that the instrument cache loads from `security_master` (§7.3). Proper RAII/exception C++ API over libpq, which is **already installed**. If you would rather add no package at all, raw libpq suffices for what is a single startup `SELECT` — but it is manual `PQclear`/`PQfinish` for no real gain. |
| Tests | **GoogleTest** (already wired) | Codec and parser tests run on captured JSON fixtures — no network. |

### Rejected alternatives

- **IXWebSocket** — simpler API, but owns its own thread and reconnect policy. That fights the single-`io_context` model and hides the timer scheduling this design depends on.
- **libwebsockets** — C API, callback-based, awkward TLS/lifetime management from C++20.
- **nlohmann/json** — slower than Boost.JSON on this shape, and would be a *new* dependency when one is already linked.
- **cpprestsdk** — effectively unmaintained.
- **A REST polling loop** — `ticker` is push-only for a reason; polling gives worse latency at higher rate-limit cost.

---

## 4. Threading and execution model

**One `io_context`, one thread, for the whole gateway.** Everything — socket reads, socket writes, heartbeat replies, the token-refresh timer, the subscribe pacer, the reconnect backoff timer — runs on that single executor. This is the model `quant-architecture.md` §8 specifies for the gateway, and it removes an entire class of bug: no strand needed, no locks around session state, no data race on the pending-request map.

Two consequences to design around explicitly:

1. **Beast permits exactly one in-flight `async_write` per stream.** With auth, heartbeat replies, and the subscribe all wanting to write, an outbound **write queue** is mandatory: `std::deque<std::string>`, push and start the write only if the queue was empty, and chain the next write from the completion handler. Getting this wrong produces intermittent, load-dependent corruption that is miserable to debug.
2. **No expensive work on the read loop.** Parsing a ticker message is cheap and stays inline, and with no tick store to write through (§5.4) decode → guard → sink is the entire inline path. Anything heavier (persistence, Protobuf encode + publish) is handed to a consumer thread through the bounded queue in `src/queue/`, so a slow sink can never stall the socket or delay a `test_request` reply.

**The queue is a plain `std::mutex` + `std::condition_variable` + `std::deque`** — no lock-free structure for now. At Deribit's tick rates a few hundred instruments produce thousands of messages per second, not millions, and an uncontended mutex acquisition costs tens of nanoseconds against a budget of hundreds of microseconds. A lock-free SPSC ring is a later optimization (see §5.5), taken when a measurement asks for it rather than on principle.

---

## 5. Component architecture

```
src/core/Config.{hpp,cpp}                 read + validate required env vars
src/core/Log.hpp                          spdlog facade (async sink, one include site)

src/marketdata/model.{hpp,cpp}            <-- the POD Tick, enums, symbol parsing
src/marketdata/InstrumentRepository.{hpp,cpp} one startup SELECT from security_master
src/marketdata/InstrumentRegistry.{hpp,cpp}   symbol <-> ids, built from the repository
src/marketdata/WebSocketTransport.{hpp,cpp}   ITransport + Beast WS over TLS, write queue
src/marketdata/deribit.{hpp,cpp}          DeribitSession: JSON-RPC correlation, auth, heartbeat, subscribe, dispatch
src/marketdata/TickerCodec.{hpp,cpp}      params.data -> Tick
src/marketdata/TickSink.hpp               ITickSink: where normalized ticks go
src/queue/BlockingQueue.hpp               bounded mutex/condvar handoff to the sink thread (phase 4)
```

The split that matters most is **`ITransport` vs `DeribitSession`**. The transport knows about WebSockets, TLS, framing, and the write queue — it moves text frames and knows nothing about what is inside them. `DeribitSession` owns everything above that line: JSON-RPC request ids, the pending-call map and its timeouts, and the venue itself — `public/auth`, `expires_in`, `test_request`, channel names. That one seam is what makes the whole thing unit-testable: a `FakeTransport` satisfying `ITransport` lets you drive the entire auth → heartbeat → subscribe → reconnect state machine in a GoogleTest with no network. There is no venue-agnostic session type in between: correlation and the state machine share every piece of state they touch (the socket, the request ids, the reconnect), and separating them bought two hop-through handler layers rather than a second implementation.

### 5.1 `model.hpp` — the data model

**There is one tick type.** An option, a dated future and the spot index all arrive as a `Tick` carrying four fields — instrument id, exchange timestamp, receive timestamp, price — and differ only in which instrument the id resolves to. There is no per-kind struct and no per-kind field.

The struct stays **trivially copyable** and fixed-size (32 bytes). Today's queue (§5.5) would tolerate a `std::string` member, so this is a forward-looking constraint rather than one imposed by the current design: per `quant-architecture.md` §5.3 the tick may end up in a Protobuf encoder or a shared-memory segment, and a fixed-size POD is what makes those a copy rather than a rewrite. Symbols live in the registry, keyed by instrument id.

```cpp
namespace shawlynot::quant::marketdata {

using InstrumentId   = std::int64_t;    // security_master.instrument.instrument_id --
                                        // the only instrument id in the system
using Nanos          = std::chrono::sys_time<std::chrono::nanoseconds>;

enum class OptionRight    : std::uint8_t { Call, Put };
// Option, Future and Index are the whole subscribed universe. Perpetual is
// recognised but never ingested or subscribed (§7.1) — it exists so a stray
// row or symbol is classified rather than rejected.
enum class InstrumentKind : std::uint8_t { Option, Future, Index, Perpetual, Unknown };

// Identity — one per instrument, built once at subscription time, never on the hot path.
struct InstrumentKey {
    std::string    symbol;        // "BTC-27JUN25-100000-C" | "BTC-27JUN25" | "btc_usd"
    std::string    base_ccy;      // "BTC"
    InstrumentId   id;            // security_master.instrument_id; 0 = no DB row
    InstrumentKind kind;
    Nanos          expiry;        // epoch for an index / perpetual
    double         strike;        // NaN unless kind == Option
    OptionRight    right;         // meaningless unless kind == Option
};

// The hot path, and the whole of it. Trivially copyable, 32 bytes.
struct Tick {
    InstrumentId id;              // resolves through the registry to symbol + kind
    Nanos  exchange_ts;           // from `timestamp` (ms), widened to ns
    Nanos  recv_ts;               // local receive stamp, for latency + staleness
    double price;                 // `last_price` on a ticker, `price` on the index
};

// Free functions in model.cpp
std::optional<InstrumentKey> parse_symbol(std::string_view symbol);
std::string_view             to_string(InstrumentKind);

}  // namespace shawlynot::quant::marketdata
```

**One `Tick` serves every instrument.** An option, a dated future and the spot
index reduce to the same four fields. `kind` on the registry entry is what tells
a consumer which it is holding — the same discrimination options and futures
already relied on, now extended to the index rather than special-cased for it.

**Why not one wide struct with the quotes on it.** The obvious alternative was
to keep the full ticker payload — bids, asks, mark, the three IVs, the greeks,
`open_interest`, `state` — and let the index fill four fields and leave the rest
NaN. That is what this design used to do, and the reason it was wrong is
visible in its own comments: a future left `greeks` and `*_iv` permanently NaN,
and an index tick would have left a dozen fields permanently NaN. A struct whose
fields are meaningless for most of the instruments flowing through it is not a
model of the data, it is a union with the tag left off. Collapsing to the fields
every instrument genuinely has removes the NaN-as-a-convention problem outright
rather than documenting it.

**`price` means `last_price` for a ticker and `price` for the index.** Those are
the two channels' names for the same thing — what it traded at. The codec is the
only place that distinction exists (§5.1.1); past it, a tick is a tick.

**What this drops, and it is not nothing.** Bid/ask, mark price, the venue's
IVs, its greeks, `index_price`/`underlying_price`, `interest_rate`,
`open_interest` and `state` are no longer carried, and `TickerState`
disappeared with the field that held it. Two consequences worth stating plainly
rather than discovering later:

- **This gateway can no longer feed a vol fitter on its own.** Inverting an
  implied vol needs a price to invert *and* a forward, and the put-call parity
  regression that `quant-architecture.md` §1.3.1 builds the forward from needs
  paired call and put quotes. `last_price` alone supplies neither reliably.
  Restoring that is a matter of adding a second, quote-shaped message later —
  the deliberate reading of this change is that the gateway is a **price feed**
  first, and the fitter's input is a separate concern.
- **`last_price` is sparse and can be very stale.** A far-OTM crypto strike may
  not trade for days, so its `last_price` is `null` (no tick is published at all,
  §5.1.1) or is a print from last week carrying a fresh `timestamp`. Consumers
  must treat an option's price as "last trade, whenever that was", not as a
  current value. The index and the liquid futures do not have this problem.

### 5.1.1 The codec's one job

`TickerCodec` has two entry points that differ in exactly one string:
`decode_ticker` reads `last_price`, `decode_index` reads `price`. Both require a
`timestamp` and a finite price, and **return `nullopt` otherwise** — a `Tick`
whose single payload field is NaN carries nothing, and publishing it would put
the same guard in every consumer. That rejection is routine, not an error: an
untraded strike sends `last_price: null` on every update, so the session counts
it in `unpriced_count()` separately from `decode_failure_count()`, which keeps
meaning "the venue sent something unreadable".

### 5.1.2 Instrument identity

**One id, and it is Postgres's.** `security_master.instrument_id` is the sole instrument identifier in the gateway. A tick carries it, the registry keys on it, the Python ingest wrote it, and a published message will forward it — so a tick joins back to the database with no translation step anywhere.

The alternative, and what this design used to do, was to carry a *second* dense id (`0..N-1`, assigned at load) alongside it as an array index, bridged by an `unordered_map` built at startup. That is worth being explicit about, because the tradeoff is real but it now falls the other way:

- **What the dense id bought:** `instrument_id` is a `bigserial`, so after a few months of chain rolls the live set is a sparse window up in the tens of thousands. A dense id is what lets per-instrument state be a flat array subscript instead of a hash lookup.
- **Why that no longer pays.** With the tick store gone (§5.4) the only per-instrument state left is the ordering guard — one timestamp per instrument. That is a hash lookup on a path that runs a few thousand times a second, against a per-tick budget of hundreds of microseconds. Buying a few nanoseconds there costs a second numbering scheme to assign at load, map in both directions, keep in step as the chain rolls, and translate at every boundary where an id leaves the process.
- **What one id buys:** exactly one answer to "which instrument is this", in the gateway, on the wire, and in the database. Every `id` in a log line, a fixture, or a published tick can be pasted straight into a `WHERE instrument_id =` with no lookup table.

**The corollary, and it is a real constraint:** an instrument with no `security_master` row has no identity here at all. `InstrumentRegistry::add` returns `nullopt` for a key whose `id` is 0 rather than inventing one, which means the "subscribe a newly-listed instrument immediately with a provisional local id" option in §7.4 is no longer available — a new listing waits for the next `reference_ingest` run. That is a deliberate trade of coverage for canonical ids; §7.4 records it.

`parse_symbol` handles all four forms: `BTC-27JUN25-100000-C` (option), `BTC-27JUN25` (dated future), `btc_usd` (index), and `BTC-PERPETUAL` — the last purely defensively, since it is filtered out upstream (§7.3) and should never appear. It returns `optional` rather than throwing: a new listing with an unexpected shape should be logged and skipped, not crash the gateway.

**Why NaN and not 0 for nulls:** `best_bid_price` is genuinely `null` when there are no bids, which is routine on far-OTM crypto strikes. A zero there silently becomes a bid of zero and poisons a fit; a NaN propagates visibly and gets filtered at the fitter's OTM/vega stage.

### 5.2 `WebSocketTransport` and the JSON-RPC layer

`ITransport` is the only seam below the session: `async_connect`, `send`, `start_reading`, `close`, `is_open`. `WebSocketTransport` implements it over Beast — TLS, the WebSocket handshake, and an outbound queue that serializes writes, since Beast permits exactly one in-flight `async_write` per stream and auth, heartbeat replies and the subscribe all want to write at once.

JSON-RPC correlation lives inside `DeribitSession`:

```cpp
std::uint64_t call(std::string_view method, boost::json::object params,
                   ResponseHandler);   // deadline from Config::request_timeout
```

Inbound dispatch classifies each frame into one of four shapes:

| Shape | Action |
|---|---|
| `{"id":N,"result":…}` | look up `N`, cancel its timeout, invoke handler |
| `{"id":N,"error":{"code","message"}}` | same, with the error code mapped to an `error_code` |
| `{"method":"subscription","params":{"channel","data"}}` | `handle_notification` → codec → sink |
| `{"method":"heartbeat","params":{"type":…}}` | `handle_method` → reply to `test_request` |

Every outbound `call` gets a timeout timer. A response that never arrives must fail its handler rather than leak an entry in the pending map forever — otherwise a dropped-but-not-closed socket leaves the session wedged in `Authenticating` with no error surfaced.

**TLS detail worth writing down now:** set the SNI hostname via `SSL_set_tlsext_host_name` before the handshake and enable peer verification with the default certificate store. Omitting SNI is the classic Beast/Deribit failure — the handshake fails with an opaque error because the edge cannot select a certificate.

### 5.3 `DeribitSession`

Owns the state machine, the credentials, and the channel list.

```cpp
enum class SessionState { Disconnected, Connecting, Authenticating,
                          ConfiguringHeartbeat, Subscribing, Streaming, Degraded };

class DeribitSession {
public:
    struct Config {
        std::string  host = "www.deribit.com";   // test.deribit.com for testnet
        std::string  port = "443";
        std::string  target = "/ws/api/v2";
        std::string  client_id, client_secret;   // from the environment — never logged
        std::string  interval = "100ms";         // raw | 100ms | agg2
        std::chrono::seconds heartbeat{30};      // >= 10
        double       refresh_at_fraction = 0.75; // of expires_in
    };
    void start();
    void stop();
    SessionState state() const;
};
```

**Lifecycle, in order:**

```
Disconnected
  → resolve + TCP connect + TLS handshake + WS handshake
  → public/auth  {grant_type: client_credentials, client_id, client_secret, scope: "connection"}
       store access_token / refresh_token / expires_in; arm refresh timer at 0.75 * expires_in
  → public/set_heartbeat {interval: 30}
  → public/get_instruments (over the same socket) → build InstrumentRegistry
  → public/subscribe, one request capped at max_channels (§8)
  → Streaming
```

Three things worth being deliberate about:

- **`scope: "connection"`** ties the token's life to this socket. It cannot be replayed elsewhere, and it dies when the connection does. Smallest blast radius for a read-only market data client, and this gateway needs no trading or wallet grants.
- **Auth binds to the WebSocket connection.** Once `public/auth` succeeds on the socket, subsequent calls on that socket are authenticated — the token does not need to be repeated per request. It follows that **a reconnect invalidates authentication**, so re-auth is not optional on the reconnect path.
- **Refresh via `grant_type=refresh_token`** at 75 % of `expires_in`, on the live socket. If refresh fails, fall back to a full `client_credentials` auth; if *that* fails, tear down and enter the reconnect backoff rather than drifting on with an expired token.

**Heartbeat handling** is the one piece with a hard deadline attached: on `params.type == "test_request"`, send `public/test` immediately, ahead of anything else queued. Failure to respond drops the connection. `params.type == "heartbeat"` needs no reply — it exists so *you* can detect a silent peer.

### 5.4 `ITickSink` — ticks go straight to the sink

**There is no tick store.** The gateway holds no latest-value cache: a decoded tick goes codec → ordering guard → sink, and the gateway keeps no copy of it. This is a deliberate departure from CTP's `MarketDataCache` (and from the store `quant-architecture.md` §3.2 sketches), and it is worth being explicit about what it buys and what it costs.

**What it buys.** A cache is only worth its keep if something *reads* it, and nothing in this process does — the gateway derives nothing from a tick, it normalizes and forwards. Keeping one would mean a second copy of every tick, a second lifetime to reason about, and a "who owns the latest price" question with two possible answers. With the store gone there is exactly one path a tick can take and exactly one place it lives: whichever sink is attached. It also removes a split that was always awkward: the gateway held the prices but published the staleness *about* them to something else. Now it publishes ticks and feed state on the same channel, and whoever holds the last price is the one told it has gone stale.

**What it costs, and where the cost lands.** The store's real job was the **snapshot** half of the snapshot-then-stream join (`quant-architecture.md` §3.2): a late-joining subscriber needs the current state of the chain before the delta stream means anything. That requirement has not gone away — it has *moved downstream*, to whichever component publishes to the network. That is the correct home for it: the snapshot has to be consistent with the sequence numbering of the stream it precedes, and only the publisher knows that numbering. A snapshot assembled here would have to be re-serialized and re-sequenced there anyway. Until the ZMQ publisher exists (Phase 4), no consumer is joining late, so nothing needs it yet.

**What the gateway does keep: an ordering guard, not a cache.** `ticker` carries no sequence number, so the resilience rule in §9 — drop any tick whose exchange timestamp does not advance — needs the last timestamp per instrument. `DeribitSession` keeps exactly that and nothing else: an `unordered_map<InstrumentId, Nanos>` holding **a timestamp, not a price** (a map rather than an array because the ids are Postgres's and therefore sparse, §5.1). Eight bytes per instrument against a few hundred instruments, and it answers one question (is this tick newer?) rather than standing in as a second source of truth for market data. `out_of_order_count()` on the session exports the rejections as a metric.

**Staleness on disconnect is now a message, not a flag.** The store used to mark its slots stale on the way down. With nothing cached there is nothing to mark, so teardown emits `on_feed_state(FeedState::Stale)` and that is the whole mechanism — the consumer holding the last-good price is the one that needs to know it has gone stale, and it is the one told.

`ITickSink` carries **one method**, `on_tick(const Tick&)`, because there is one tick type (§5.1). A consumer that cares about only the index, or only the options, filters on the registry's `kind` — the same discrimination options and futures already required. The queue behind `QueueSink` holds `Tick` directly as a result, with no variant and no visit. The gateway's downstream is then a policy, not a hard-wired decision:

- Phase 1: `ConsoleSink` (print, eyeball correctness against Deribit's UI)
- Phase 4: `QueueSink` → `BlockingQueue` → consumer thread → Protobuf/ZMQ publish per `quant-architecture.md` §5. **This is where a latest-value snapshot belongs if one is needed** — the publisher owns the sequence numbering a snapshot has to agree with.
- Optional: `PgSink` writing a `market_data.ticker` hypertable alongside `security_master`

### 5.5 `BlockingQueue` — the handoff to the sink thread

A bounded queue built from the standard library, nothing more:

```cpp
template <typename T>
class BlockingQueue {
public:
    explicit BlockingQueue(std::size_t capacity);

    // Producer (io thread). Never blocks on capacity — see the drop policy below.
    // Returns false if the item displaced an older one.
    bool push(const T& item);

    // Consumer thread. Blocks until an item is available or the queue is closed.
    bool pop(T& out);

    void close();                      // wake the consumer for shutdown
    std::uint64_t dropped() const;     // monotonic counter, exported as a metric

private:
    mutable std::mutex      m_mutex;
    std::condition_variable m_not_empty;
    std::deque<T>           m_items;
    std::size_t             m_capacity;
    std::uint64_t           m_dropped = 0;
    bool                    m_closed  = false;
};
```

Three properties carry the design, and they matter more than the data structure underneath:

1. **Bounded, and the producer never waits for space.** This is the whole reason the queue exists. A `push` that blocks when the consumer falls behind puts backpressure onto the io thread, which is precisely the stall §4 is trying to prevent — and with a heartbeat deadline attached, a producer stuck waiting on a slow Postgres write can get the connection dropped. The producer takes the lock (uncontended, tens of nanoseconds) but never waits on a capacity condition.
2. **Overflow drops the oldest and counts it.** For a latest-value feed, a superseded tick has no value — dropping the *oldest* is the correct policy, not the least-bad one. Export `dropped()`: a non-zero and growing counter is the signal that the consumer needs optimizing or the lock-free replacement is finally justified.
3. **One `condition_variable`, notified outside the lock.** The consumer waits on non-empty; only shutdown needs `close()` to wake it. There is no not-full condition to wait on, which is what keeps the producer side simple.

**Why not lock-free yet.** A few hundred instruments at `100ms` aggregation produce on the order of thousands of messages per second. A mutex handoff is three orders of magnitude away from being the bottleneck there, and the mutex version is straightforward to get right where an SPSC ring needs careful memory-ordering reasoning to avoid a subtle, load-dependent bug. Keep the interface (`push`/`pop`/`close`) narrow enough that swapping in a lock-free ring later touches one header and no call sites — that is the real design requirement, and it is satisfied by any queue with these four methods.

---

## 6. Configuration and secrets

`Config::from_env()` reads the process environment via `std::getenv` and validates it. **It does not parse any file.** How the variables get into the environment — systemd `EnvironmentFile=`, container env, `direnv`, or a shell that sourced `.env` — is a deployment concern, and baking a `.env` parser into the binary would make the gateway's behaviour depend on its working directory.

Required: `CLIENT_ID`, `CLIENT_SECRET`, and — now that the instrument cache loads from Postgres (§7.3) — `POSTGRES_HOST`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`. Optional: `DERIBIT_WS_HOST` (so pointing at `test.deribit.com` is a config change, not a recompile), `DERIBIT_TICKER_INTERVAL`, `QUANT_LOG_LEVEL`.

`POSTGRES_HOST` carries an optional `:port` suffix in this project — `Settings.from_env` on the Python side already partitions on `:` and defaults to 5432. Parse it the same way; two components disagreeing about the format of a shared variable is a needless failure mode.

`POSTGRES_PASSWORD` joins the never-log list below.

Rules that need to hold from the first commit, because retrofitting them is how secrets leak:

- **Never log `client_secret`, `access_token`, `refresh_token`, or `POSTGRES_PASSWORD`** — not at debug level, not in an error path. Log token *metadata* only: scope, `expires_in`, and a fingerprint (first 6 chars of a hash) if you need to correlate.
- The auth request object is built and immediately serialized; do not include it in any exception message or dumped-frame diagnostic. If you add raw-frame tracing for debugging, exclude frames whose method is `public/auth`.
- `Config::from_env()` fails loudly at startup on a missing or empty variable, naming every one that is absent in a single message. A gateway that boots unauthenticated and silently loses `raw` access is worse than one that refuses to start.

---

## 7. Instrument universe, underlying spot, and identity

### 7.1 What to subscribe to

| Instrument | `security_master.instrument_type` | Channel | Count |
|---|---|---|---|
| Every active BTC option | `option` | `ticker.{instrument_name}.100ms` | several hundred |
| Every dated BTC future | `future` | `ticker.{instrument_name}.100ms` | ~6–8 |
| BTC/USD spot index | `currency` | `deribit_price_index.btc_usd` | 1 |

That is the complete universe — **no `BTC-PERPETUAL`**. The futures add under ten channels to a subscription already several hundred wide (negligible against the rate-limit budget in §8), and in exchange every listed forward is published to the sink as an observed price rather than left for a consumer to infer.

**`kind=future` returns the perpetual too, so it has to be filtered out explicitly.** `settlement_period` is the discriminator: keep `"month"` and `"week"`, drop `"perpetual"`. The filter belongs in the Python ingest (§7.3) so the perpetual never reaches `security_master` at all, which means the gateway's `SELECT` cannot accidentally subscribe to it later.

**No perpetual — decided.** `BTC-PERPETUAL` is outside the streamed universe. What it would have offered downstream is `current_funding`/`funding_8h`, and carrying it would mean a second, differently-shaped rate mechanic (and a funding field that fits nowhere in a four-field `Tick`) for a benefit nobody has measured. It stays out, filtered at the Python ingest so it never enters `security_master` (§7.3).

### 7.2 Getting the underlying spot in real time

**Subscribe to `deribit_price_index.btc_usd`.** This is the direct answer, and it is a different shape from everything else in the gateway:

- It is **not** a `ticker` channel and takes **no interval suffix** — `deribit_price_index.btc_usd`, nothing more. Appending `.100ms` fails.
- The payload is three fields: `{"timestamp": <ms>, "price": <number>, "index_name": "btc_usd"}` → straight into a `Tick`, taking `price` (§5.1.1).
- It is public, so it needs no authentication of its own — though the connection is authenticated anyway for the `raw` option interval and the higher credit tier.
- The channel suffix **is** the `security_master` symbol. `btc_usd` in the DB is `btc_usd` on the wire, so the registry maps the currency row to its channel with no translation table.

**The index's instrument id comes from the database, like everything else.** `btc_usd` is an ordinary `security_master.instrument` row with `instrument_type = 'currency'`, plus a `security_master.currencies` subtype row keyed on the same `instrument_id` — which is why the startup `SELECT` (§7.3) includes `'currency'` in its `IN` list. A spot `Tick` carries that `instrument_id`, so it joins back to Postgres exactly like an option's does. There is no synthetic id, no reserved constant, and no "the index is id 0" special case; the index resolves through `InstrumentRegistry::by_symbol("btc_usd")` and through channel binding on the same code path as every other instrument. If the `currency` row is missing — `reference_ingest` never run, or run against a different venue — the primed spot is discarded with a warning naming the symbol, because a spot tick with no identity is worse than no spot tick.

**Prime it at startup with `public/get_index_price`** (`{"index_name": "btc_usd"}` → `index_price`, `estimated_delivery_price`). The index channel only pushes on change; without a snapshot no spot reaches the sink until the first move, and every option tick arriving in that window is published with no index behind it.

**Do not use `markprice.options.btc_usd` as the spot source.** It is tempting — one channel covering the whole chain — but it delivers `{instrument_name, mark_price, iv, timestamp}`, i.e. **Deribit's already-fitted IV**. Feeding that into your own fitter is the circularity `quant-architecture.md` §3.1 warns about. Keep it subscribed only if you want a benchmark to score your fit against.

### 7.3 Where the instrument list comes from

**Decided: Postgres.** At startup the gateway opens one connection to `security_master`, runs a single `SELECT`, builds the `InstrumentRegistry` in memory, and closes the connection. Deribit's `get_instruments` is not the source of identity.

```sql
SELECT i.instrument_id, i.symbol, i.instrument_type, i.tick_size,
       o.option_type, o.strike, o.expiration_timestamp
FROM   security_master.instrument i
JOIN   security_master.venue v USING (venue_id)
LEFT   JOIN security_master.option o USING (instrument_id)
WHERE  v.code = 'DERIBIT'
  AND  i.instrument_type IN ('option', 'future', 'currency');   -- 'perpetual' never populated
```

What this buys, and what it costs:

- **Ids are shared.** `instrument_id` means the same thing in the gateway, the Python ingest, and anything that later reads a published tick — literally the same integer, since the gateway keeps no id of its own (§5.1). No reconciliation layer and no translation at any boundary.
- **Postgres is not in the hot path.** One query, then the connection closes. A database outage blocks *startup* and nothing else — once running, the gateway is unaffected. Worth stating as an explicit property so nobody later adds a per-tick lookup.
- **The DB becomes a staleness risk.** This is the real cost. The registry is only as current as the last `reference_ingest` run, so an expiry listed since then is invisible: the gateway never subscribes to it, and — unlike a bad parse — there is no error, just a silently missing expiry in the surface. Mitigate by calling `get_instruments` anyway at startup **purely as a cross-check**, diffing against the DB set and logging loudly on divergence. It costs two RPCs and turns a silent hole into an alert. §7.4 covers reacting to rolls at runtime.

**Prerequisite: the Python ingest must learn futures.** `security_master` allows `instrument_type = 'future'`, but `reference_ingest.py` fetches `kind=option` only, so the query above returns no futures today and the gateway would subscribe to none. Required work on the Python side, blocking the gateway's M3:

1. `client.get_instruments(currency="BTC", kind="future")` — one extra call; the existing client method already takes `kind`.
2. A `FutureContract` model in `models.py`, mirroring `OptionContract`.
3. A `security_master.future` subtype table alongside `option`, carrying `expiration_timestamp`, `contract_size`, `settlement_period`, `creation_timestamp`.
4. **Filter `settlement_period == "perpetual"` out** and write `instrument_type = 'future'` for what remains. The perpetual is not ingested at all (§7.1), so the `'perpetual'` value stays unused in the CHECK constraint. Derive the column from `kind` + `settlement_period` — **not** from the payload's own `instrument_type` field, which holds `"linear"`/`"reversed"` and would violate the constraint (gotcha 4, §2).

Dropping the perpetual at ingest rather than at subscribe time is the safer placement: it cannot then reach the gateway's `SELECT`, so no later change to that query can accidentally pull it into the subscription.

### 7.4 Chain roll

New listings appear and expiries drop continuously, and with the registry loaded from a database snapshot (§7.3) the gateway will not see them on its own. Subscribe to the instrument-state channel to react without a restart, and re-run `get_instruments` on a slow timer (e.g. hourly) as a reconciliation backstop — diffing against the registry and logging divergence rather than silently carrying on. A newly-listed instrument therefore **waits for the next `reference_ingest` run** before it can be subscribed. This used to be a judgement call — subscribe immediately under a provisional local id (complete surface) versus wait for a canonical one — but with a single Postgres-owned id (§5.1) there is no provisional id to issue, and `add` refuses a key without one. The cost is a window in which a new expiry is invisible; the mitigations are to run `reference_ingest` often enough that the window is short, and to log the `get_instruments` divergence loudly so the gap is visible rather than silent. If the window ever proves too costly, the fix is to make the gateway *write* the missing row (or trigger the ingest) so the id stays canonical — not to reintroduce a local numbering. *Verify the exact instrument-state channel name against the docs before implementing* — `quant-architecture.md` cites `instrument.state.any`, and I have not confirmed that form; the per-kind/per-currency variant may be what the API actually accepts.

---

## 8. Subscribe scope and the channel cap

A full active BTC option chain is several hundred instruments, so the subscribe path is where the rate limiter would bite — and remember the penalty is **session termination**, not a soft rejection. `public/subscribe` costs 3,000 credits against a 50,000 pool refilling at 10,000/s (burst 10, sustained ~3.3/s).

The chosen answer is to **cap the universe rather than pace the requests**: subscribe in a **single `public/subscribe`** carrying at most `max_channels` channels (**200** by default, configurable on `DeribitSession::Config`). One call cannot breach the credit budget, so there is no token bucket, no inter-chunk timer, and no pacing bug to have — the whole class of failure the pacing existed to avoid is designed out instead of managed. 200 also stays well clear of any undocumented per-request channel cap.

- **Truncation drops from the tail.** `subscription_channels()` orders spot → dated futures → options, so a universe over the cap loses the far end of the option chain and never the index every option tick is read against. The session logs both counts (offered and subscribed) when it truncates, so a silently narrowed surface is visible at startup.
- **The cost, stated plainly:** with a full BTC chain this means part of the chain is *not* subscribed. That is the deliberate trade for now — the fitter is exercised on a liquid subset, and the cap is one config field to raise. If the whole chain is ever genuinely needed, the honest fix is a wider cap plus a measured per-request limit, not a return to paced chunking.
- **Reconcile the result.** `public/subscribe` returns the channels actually subscribed. Diff it against what was requested and log/retry the difference; a silently partial subscription is a hole in the surface that shows up much later as a stale expiry.
- Handle `10028 too_many_requests` as a **connection-fatal** event: back off and reconnect, since the session is being torn down anyway.

**Interval: `100ms`** for options and futures — decided. `raw` is available (that is a benefit of authenticating), but it multiplies message volume across several hundred instruments for no fitting benefit, since the fitter runs on a multi-second cadence. Keep it configurable via `DERIBIT_TICKER_INTERVAL` so `raw` is reachable for a small watchlist if a latency measurement ever justifies it.

**The index channel has no interval and is not subject to this choice** — `deribit_price_index.btc_usd` pushes at its own rate.

**Subscribe spot and futures first, options after.** Both are tiny — one channel plus under ten — so they survive any truncation with room to spare, and spot is the reference every option tick carries. Subscribing them last would mean the first seconds of option ticks are published with no index behind them, which is a startup transient you simply do not need to have.

---

## 9. Resilience

Consolidating `quant-architecture.md` §3.3 into concrete mechanisms:

| Failure | Handling |
|---|---|
| Silent half-open connection | Heartbeat: server `test_request` → immediate `public/test`. Independently, a watchdog timer that fires if no frame of any kind arrives within 2× the heartbeat interval forces a reconnect. |
| Disconnect / handshake failure | Exponential backoff **250 ms → 30 s with full jitter**. Full jitter, not fixed steps — it prevents a synchronized reconnect storm if several processes drop together. |
| Reconnect | Re-auth (auth does not survive the socket) → re-`set_heartbeat` → re-`get_instruments` → re-subscribe → resume. The gateway caches nothing to invalidate, so what it owes on the way down is the signal: `on_feed_state(STALE)` to the sink (§5.4). |
| Token expiry | Refresh at 0.75 × `expires_in`; on failure, full re-auth; on repeat failure, drop into the reconnect path. |
| Out-of-order / duplicate ticks | `ticker` carries no sequence number. `DeribitSession` keeps the last `exchange_ts` per instrument — a timestamp, not a cached tick (§5.4) — and **drops any tick whose timestamp does not advance**, counting the rejections in `out_of_order_count()`. |
| Stale feed | Every `Tick` carries `recv_ts`. A consumer detects staleness itself; the gateway additionally emits a `feed_state = STALE` control message when the circuit breaker trips, so downstream holds last-good rather than republishing garbage. This is the *only* staleness mechanism now — with no store there are no cached slots to flag (§5.4), and the consumer holding the last-good price is the one told. **Watch spot and the futures specifically**: a quiet channel is indistinguishable from a broken one on payload alone, and a stale spot or a stale future is published as a live price that whatever consumes it will price a whole expiry off, rather than corrupting one instrument. |
| Malformed / unexpected payload | Codec returns `optional`, counter incremented, message logged at debug and dropped. One bad frame must never kill the read loop. |
| Payload with no usable price | Same path, but counted in `unpriced_count()` rather than `decode_failure_count()` — an untraded strike sending `last_price: null` is routine, and folding it into the failure counter would make that counter useless as an alarm (§5.1.1). |

---

## 10. Build changes

Current `CMakeLists.txt` links only `Boost::json` — Beast needs more, and `tests/` compiles no project code, so nothing in `src/` is testable today. Proposed:

```cmake
find_package(Boost 1.81 REQUIRED COMPONENTS json)   # Beast/Asio are header-only
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(libpqxx REQUIRED)                     # apt install libpqxx-dev

FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.14.1)
FetchContent_MakeAvailable(spdlog)

add_library(quant_marketdata STATIC
    src/core/Config.cpp
    src/marketdata/model.cpp
    src/marketdata/InstrumentRepository.cpp
    src/marketdata/InstrumentRegistry.cpp
    src/marketdata/WebSocketTransport.cpp
    src/marketdata/deribit.cpp
    src/marketdata/TickerCodec.cpp
)
target_include_directories(quant_marketdata PUBLIC src include)
target_link_libraries(quant_marketdata PUBLIC
    Boost::json Boost::headers OpenSSL::SSL OpenSSL::Crypto Threads::Threads
    libpqxx::pqxx spdlog::spdlog)

add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE quant_marketdata)
```

and in `tests/CMakeLists.txt`, link `quant_marketdata` so the codec, symbol parser, and session state machine are actually covered.

`FetchContent` is currently included from `tests/CMakeLists.txt` for googletest; pulling spdlog at the top level moves `include(FetchContent)` up to the root file. `setup.sh` needs no change, but the `libpqxx-dev` prerequisite belongs in it or the README — a `find_package` failure on a fresh clone should point at the fix.

Two smaller items:

- `CMAKE_CXX_FLAGS_DEBUG` currently hard-codes `-fsanitize=address,undefined`. ASan and TSan are mutually exclusive, and the producer/consumer handoff in Phase 4 wants TSan — a mutex does not exempt you, since TSan is what catches the tick being read after it was handed over, or shutdown racing a blocked `pop`. Move sanitizers behind a `QUANT_SANITIZER` cache variable (`none|address|thread`) instead of baking them into the Debug config.
- `-march=native` in Release is fine locally but makes the binary non-portable — worth a comment before anything ships to another machine.

---

## 11. Testing

| Level | What |
|---|---|
| Unit — `model` | `parse_symbol` across option/future/perpetual/index forms, plus malformed input returning `nullopt` |
| Unit — `TickerCodec` | Captured real `ticker` frames as fixtures under `tests/fixtures/`. Assert `last_price` is the field taken, ms → ns timestamps, a null or absent price yields `nullopt`, and that **gutting the frame of every other field leaves the decoded Tick identical** — the guarantee that makes one Tick honest |
| Unit — index codec | `deribit_price_index` frames → `Tick` taking `price`; assert the channel is routed by name rather than payload shape, and that the two entry points are **not** interchangeable (an index frame has no `last_price`) |
| Unit — future ticker | A dated-future `ticker` frame through the same codec and the same field; a future differs from an option only in which instrument its id resolves to |
| Unit — `InstrumentRepository` | Row mapping from a fixture result set: `instrument_type` → `InstrumentKind`, `instrument_id` carried onto the key verbatim, a key with no id refused by `add`, a missing `option` join row on a future does not produce a bogus strike. Needs no live database |
| Unit — `BlockingQueue` | Bounded capacity honoured; overflow drops the **oldest** and increments `dropped()`; `pop` blocks then wakes on push; `close()` releases a blocked consumer. Run the multi-threaded cases under TSan |
| Unit — `DeribitSession` | Drive the full state machine over a `FakeTransport`: auth success/failure, refresh at threshold, refresh failure → re-auth, `test_request` → `public/test`, a single subscribe truncated to the channel cap, `10028` → reconnect, out-of-order tick dropped, an untraded strike publishing nothing, disconnect → `FeedState::Stale` reaches the sink, and **a spot tick carrying the `currency` row's `instrument_id`** (§7.2). A `RecordingSink` is the only place to assert a tick arrived, since the gateway keeps no copy of one |
| Integration | Against **testnet** (`test.deribit.com`) — connect, auth, subscribe one instrument, assert ticks arrive within N seconds. Tagged so it does not run in the default suite |
| Soak | Multi-hour run against production public data, watching for descriptor leaks, unbounded pending-request map growth, and refresh-boundary behaviour (needs >1 token lifetime to be meaningful) |

Capture the fixtures during the M1 milestone — dump raw frames to a file behind a flag, then freeze a representative set. Hand-written fixtures encode your assumptions about the payload rather than the payload.

---

## 12. Milestones

| # | Deliverable | Done when |
|---|---|---|
| **P0** *(Python)* | `reference_ingest` learns `kind=future`: `FutureContract`, `security_master.future` table, `settlement_period == "perpetual"` filtered out | A run populates dated `future` rows and no perpetual row; the §7.3 `SELECT` returns them, so the gateway can subscribe the futures that are part of the streamed universe. **Blocks M3** |
| **M0** | `Config` + TLS/WS connect + `public/auth` | Connects to testnet and prints scope + `expires_in` (token itself redacted) |
| **M1** | Heartbeat + `deribit_price_index.btc_usd` + one hardcoded option and one dated future `ticker` | Spot, one option and one future print for 10 minutes with no disconnect; raw frames of all three shapes captured as fixtures |
| **M2** | `model.hpp` (`Tick`) + `TickerCodec` + unit tests | Fixtures parse; `last_price`/`price` selection, no-price rejection and ms→ns conversion asserted; tests link `quant_marketdata` |
| **M3** | Full universe: `InstrumentRepository` load, `get_index_price` prime, registry, capped single subscribe, ordering guard, reconnect, `get_instruments` cross-check | Whole BTC chain + dated futures + spot streaming off DB-sourced ids; the cross-check reports zero divergence against Deribit; survives a forced disconnect and re-establishes without manual intervention |
| **M4** | `ITickSink` + `BlockingQueue` handoff | A separate consumer thread receives ticks with the io thread never blocking on capacity; `dropped()` exported and observed at zero under normal load; ready for the Protobuf/ZMQ publisher of `quant-architecture.md` §5 |

M0–M2 are the ones that de-risk the design. M3 is mostly volume and bookkeeping; M4 is where this rejoins the broader CVP architecture.

---

## 13. Decisions

### Settled

| # | Decision | Where it lands |
|---|---|---|
| 1 | **Interval `100ms`** for options and futures; `raw` reachable via config | §8 |
| 2 | **No ETH** — BTC only for Phase 1 | §7.1 |
| 3 | **Ticks are not persisted** — no Postgres tick storage until the ZMQ publisher exists | §5.4, §5.5 |
| 7 | **No tick store** — a decoded tick goes codec → ordering guard → sink and the gateway keeps no copy; the snapshot-then-stream join moves to the publisher | §5.4 |
| 8 | **One instrument id** — `security_master.instrument_id` throughout; no dense gateway-local id, and an instrument with no DB row is not registered | §5.1.2, §7.4 |
| 9 | **One `Tick`, four fields** — id, exchange ts, recv ts, price; `last_price` for options and futures, `price` for the index. Quotes, IVs, greeks and `state` are not carried | §5.1 |
| 4 | **Instruments load from Postgres into a startup cache**, not from `get_instruments` | §7.3 |
| 5 | **Dependencies accepted** — spdlog (FetchContent) and libpqxx (apt) | §3, §10 |
| 6 | **No perpetual** — filtered at the Python ingest, never reaches `security_master` | §7.1, §7.3 |

Decision 4 pulls **P0** (§12) forward into required work: the Python ingest has to learn futures, or the gateway has none to subscribe. It also puts `POSTGRES_*` into the gateway's required environment (§6), and makes DB staleness a live concern the `get_instruments` cross-check exists to catch (§7.3).

### Watch, don't decide

Nothing is blocking. Two consequences are worth measuring rather than pre-solving:

- **Whether the perpetual is ever missed.** (Decision 6.) Only a downstream consumer modelling the very short end could want it, and only via funding. Nothing in the gateway needs it, so the question stays open only for whatever prices off these ticks.
- **Whether the fitter needs a second, quote-shaped message.** (Decision 9.) It almost certainly will: put-call parity needs paired bid/ask, and IV inversion needs a price you can trust to be current, neither of which `last_price` gives. The thing worth *not* pre-solving is its shape — a wider `Quote` on its own channel, or `Tick` plus a sidecar. Decide it when the fitter exists and its actual inputs are known, rather than re-widening `Tick` speculatively and ending up back at the permanently-NaN struct this decision removed.
- **Where the snapshot-then-stream join ends up living.** (Decision 7.) Nothing joins late today, so nothing needs a snapshot. When the ZMQ publisher lands it will need one, and the open question is only whether it assembles it from its own accumulated state or asks the gateway to replay — decidable then, with the sequence-numbering scheme in hand, and not usefully guessable now.

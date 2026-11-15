# AGENTS.md

CVP (Crypto Vol Platform): a  real-time vol-surface fitting, pricing
and Greeks system built on Deribit data. 

The repo has two halves that share one Postgres database:

- **`cpp/`** is the low-latency C++20 services. Today that is the market data
  gateway.
- **`py/`** is Python tooling: reference-data ingest, and later research and
  backtesting.

Python writes the instrument universe into `security_master`. The C++ gateway
reads it once at startup and then streams ticks for those instruments.

## Layout

```
.
├── AGENTS.md                 this file
├── CLAUDE.md                 pulls in the private design docs (.claude/docs_private/)
├── .env                      local secrets / config (gitignored; never commit or log)
├── cpp/                      C++ project root (CMake source dir)
│   ├── CMakeLists.txt        top-level build: quant_marketdata lib + quant_cpp exe
│   ├── setup.sh              configure cpp/build and symlink compile_commands.json
│   ├── .clang-format         Google style, 2-space, 80 col
│   ├── include/              extra public include dir (currently empty)
│   ├── docs/                 design notes on the Asio/Beast transport
│   ├── src/
│   │   ├── main.cpp          gateway entry point: config → load instruments → stream
│   │   ├── core/             Config (env-only), Log (async spdlog facade)
│   │   ├── marketdata/       everything venue/feed related (see below)
│   │   └── queue/            BlockingQueue: bounded io-thread → sink handoff
│   └── tests/                GoogleTest suite + JSON fixtures
└── py/                       Python package `shawlynot.quant` (uv-managed)
    ├── pyproject.toml
    ├── src/shawlynot/quant/
    │   ├── config.py         env-based Settings
    │   ├── db/               security_master schema.sql + repository functions
    │   └── deribit/          async REST client, typed models, reference_ingest CLI
    └── tests/                pytest suite + Deribit JSON fixtures
```

### `cpp/src/marketdata/`

| File | Role |
|------|------|
| `model.hpp` | POD `Tick` (id, exchange/receive ns, scaled int price, exponent) and `InstrumentKey` |
| `InstrumentRepository.*` | One startup `SELECT` from `security_master` → `InstrumentRow`s (libpqxx). Not on the hot path |
| `InstrumentRegistry.*` | Instrument identity table keyed by Postgres `instrument_id`; `kind` = Option/Future/Index |
| `TickerCodec.*` | Decodes Deribit `params.data` JSON into `Tick`. Never throws; returns `optional` |
| `Web.*` | Venue-agnostic TLS transport: HTTPS request and WebSocket session (Boost.Beast, coroutines) |
| `Deribit.*` | `DeribitIngest`: authenticate → connect → subscribe → read loop, as one coroutine |
| `TickSink.hpp` | `ITickSink` interface, where normalized ticks go |
| `ConsoleSink.hpp` | Prints ticks; currently wired in `main.cpp` |
| `QueueSink.hpp` | Hands ticks to a consumer thread over `BlockingQueue` (the planned path to Protobuf/ZMQ) |

Gateway threading model: one `io_context` on one thread. Socket I/O and decode
run on that thread with no strands or locks. Anything heavier than parsing
belongs behind a `QueueSink`.

## Build & test

### C++

Run these from `cpp/`:

```bash
./setup.sh                                   # first time: configure build/ + compile_commands symlink
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build                       # GoogleTest via gtest_discover_tests
./build/quant_cpp                            # needs Postgres populated + env config
```

- Requires Boost ≥ 1.81 (json), OpenSSL and libpq. spdlog, libpqxx and
  googletest come in through `FetchContent`.
- Sanitizers: `-DQUANT_SANITIZER=address|thread`. ASan and TSan are mutually
  exclusive; use TSan for anything that touches `queue/` or the sinks.
- Release builds use `-march=native`.
- Build dirs (`build/`, `build-debug/`) and `compile_commands.json` are
  gitignored.

### Python

Run these from `py/`:

```bash
uv sync --extra dev
uv run pytest
uv run ruff check . && uv run mypy
uv run deribit-reference-ingest              # fetch BTC options/futures → upsert security_master
```

## Conventions

- **Identity comes from Postgres.** `security_master.instrument_id` is the
  single instrument id across the DB, Python, C++ and published messages. Never
  derive instrument facts by parsing symbol strings.
- **Config is environment only.** `core::Config::from_env()` parses no files.
  Names in `is_secret_variable()` must never be logged. Use
  `core::fingerprint()` to correlate tokens in logs.
- **Ticks stay POD and trivially copyable** so they can go to Protobuf or shm
  without a rewrite. Option prices stay in coin at the gateway; USD conversion
  belongs in the pricer.
- C++ namespace is `shawlynot::quant::{core,marketdata,queue}`. Includes are
  rooted at `cpp/src` (e.g. `#include "marketdata/TickSink.hpp"`). Format with
  `cpp/.clang-format`.
- New C++ sources go into the `quant_marketdata` target in
  `cpp/CMakeLists.txt`. New tests go into `cpp/tests/CMakeLists.txt`; fixtures
  are loaded via `QUANT_TEST_FIXTURE_DIR`.

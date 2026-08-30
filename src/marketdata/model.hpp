#pragma once

// The POD tick type and the instrument identity it resolves through.
//
// One Tick carries every instrument: an option, a dated future and the spot
// index all reduce to (id, exchange time, receive time, price, exponent).
// There is no per-kind struct and no per-kind field, so nothing on the hot path
// is permanently unset because it belongs to a different instrument -- which is
// what two structs, or one wide struct with an option-only half, both ended up
// doing.
//
// Tick is trivially copyable and fixed-size. Nothing here forces that today,
// but a tick is expected to end up in a Protobuf encoder or a shared-memory
// segment, and staying a POD is what makes those a copy rather than a rewrite.
// Symbols therefore live in the registry, keyed by instrument id, and never on
// the tick itself; `kind` on the registry entry is what tells a consumer
// whether a given price is an option's, a future's or the index's.
//
// There is no symbol parser. Identity is Postgres's -- id, symbol, type and
// tick size all arrive from security_master at startup -- so re-deriving any of
// it from the symbol string would be a second source of truth for facts the
// database already states.

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

namespace shawlynot::quant::marketdata {

/// security_master.instrument.instrument_id -- the only instrument id in the
/// system. Stable, shared with the Python layer, and what travels in published
/// messages, so a tick joins straight back to Postgres with no translation.
///
/// It is a bigserial, so the live set is a sparse window up in the tens of
/// thousands rather than a dense 0..N-1 range. That rules out indexing a flat
/// array by it, which is why every registry lookup is a hash.
using InstrumentId = std::int64_t;

using Nanos = std::chrono::sys_time<std::chrono::nanoseconds>;

/// Option, Future and Index are the whole subscribed universe. Perpetual is
/// recognised but never ingested or subscribed -- it exists so that a stray row
/// is classified rather than rejected.
enum class InstrumentKind : std::uint8_t {
  Option,
  Future,
  Index,
  Perpetual,
  Unknown
};

/// Identity, built once at registry load -- never constructed on the hot path.
///
/// Deliberately minimal: strike, right and base currency are not here because
/// nothing in the ingest reads them, and a field nobody reads is a field that
/// silently goes wrong. `expiry` earns its place only because the subscribe
/// order is expiry-ascending (see InstrumentRegistry::subscription_channels).
struct InstrumentKey {
  std::string symbol;   ///< "BTC-27JUN25-100000-C" | "BTC-27JUN25" | "btc_usd"
  InstrumentId id = 0;  ///< 0 means no security_master row: no identity
  InstrumentKind kind = InstrumentKind::Unknown;
  /// Decimal exponent for this instrument's prices, derived from
  /// instrument.tick_size. -4 for a 0.0001 option tick, -1 for a 2.5 future
  /// tick, -2 for the index's 0.01.
  std::int32_t price_exponent = 0;
  Nanos expiry{};  ///< epoch for the index; a real date for options/futures
};

/// One price update, whatever the instrument.
///
/// `price` is the venue's `last_price` for an option or a dated future, and its
/// `price` for the spot index -- the two channels' payloads are shaped
/// differently but this is the field each one means by "what it traded at". The
/// codec is the only place that distinction exists; past it, a tick is a tick.
///
/// The value is `price * 10^exponent`, exactly. The exponent comes from the
/// instrument's tick size rather than from the frame, so two ticks for the same
/// instrument are always directly comparable as integers -- which is the whole
/// reason for carrying a scale rather than a double.
///
/// What is *not* normalized is the unit of account: an option's price is in
/// coin and the index's is in USD. Only `kind` on the registry entry tells them
/// apart, so a consumer that compares them without checking `kind` is comparing
/// BTC to USD.
struct Tick {
  InstrumentId id = 0;
  Nanos exchange_ts{};  ///< venue `timestamp` (ms), widened to ns
  Nanos recv_ts{};      ///< local receive stamp, for latency and staleness
  std::int64_t price = 0;
  std::int32_t exponent = 0;
};

static_assert(std::is_trivially_copyable_v<Tick>);

constexpr std::string_view to_string(InstrumentKind kind) {
  switch (kind) {
    case InstrumentKind::Option:
      return "option";
    case InstrumentKind::Future:
      return "future";
    case InstrumentKind::Index:
      return "index";
    case InstrumentKind::Perpetual:
      return "perpetual";
    case InstrumentKind::Unknown:
      break;
  }
  return "unknown";
}

}  // namespace shawlynot::quant::marketdata

#pragma once

// The POD tick type and symbol parsing for the Deribit market data gateway.
//
// One Tick carries every instrument: an option, a dated future and the spot
// index all reduce to (id, exchange time, receive time, price). There is no
// per-kind struct and no per-kind field, so nothing on the hot path is
// permanently NaN because it belongs to a different instrument -- which is what
// two structs, or one wide struct with an option-only half, both ended up
// doing.
//
// Tick is trivially copyable and fixed-size. Nothing here forces that today --
// the queue in src/queue would tolerate a std::string member -- but a tick is
// expected to end up in a Protobuf encoder or a shared-memory segment, and
// staying a POD is what makes those a copy rather than a rewrite. Symbols
// therefore live in the registry, keyed by instrument id, and never on the tick
// itself; `kind` on the registry entry is what tells a consumer whether a given
// price is an option's, a future's or the index's.

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

namespace shawlynot::quant::marketdata {

/// security_master.instrument.instrument_id -- the only instrument id in the
/// system. Stable, shared with the Python layer, and what travels in published
/// messages, so a tick joins straight back to Postgres with no translation.
///
/// The gateway deliberately keeps no dense id of its own alongside it. A second
/// numbering would have to be built at load, mapped both ways, and kept in step
/// on every chain roll -- and the only thing it bought was array indexing on a
/// hot path that no longer indexes into anything (the tick store is gone; the
/// ordering guard is a hash lookup). One id means one answer to "which
/// instrument is this", in the gateway, on the wire, and in the database.
using InstrumentId = std::int64_t;

using Nanos = std::chrono::sys_time<std::chrono::nanoseconds>;

/// Absent venue fields are NaN, never 0: `best_bid_price` is genuinely null
/// when there are no bids, which is routine on far-OTM crypto strikes. A zero
/// there silently becomes a bid of zero and poisons a fit, where a NaN
/// propagates visibly and is filtered at the fitter's OTM/vega stage.
inline constexpr double kNoValue = std::numeric_limits<double>::quiet_NaN();

enum class OptionRight : std::uint8_t { Call, Put };

/// Option, Future and Index are the whole subscribed universe. Perpetual is
/// recognised but never ingested or subscribed -- it exists so that a stray row
/// or symbol is classified rather than rejected.
enum class InstrumentKind : std::uint8_t {
  Option,
  Future,
  Index,
  Perpetual,
  Unknown
};

/// Identity, built once at registry load -- never constructed on the hot path.
struct InstrumentKey {
  std::string symbol;    ///< "BTC-27JUN25-100000-C" | "BTC-27JUN25" | "btc_usd"
  std::string base_ccy;  ///< "BTC"
  InstrumentId id = 0;   ///< 0 when parsed from a symbol alone: no identity yet
  InstrumentKind kind = InstrumentKind::Unknown;
  Nanos expiry{};                         ///< epoch for an index or perpetual
  double strike = kNoValue;               ///< NaN unless kind == Option
  OptionRight right = OptionRight::Call;  ///< meaningless unless kind == Option
};

/// One price update, whatever the instrument.
///
/// `price` is the venue's `last_price` for an option or a dated future, and its
/// `price` for the spot index -- the two channels' payloads are shaped
/// differently but this is the field each one means by "what it traded at". The
/// codec is the only place that distinction exists; past it, a tick is a tick.
///
/// `id` resolves through the registry to the symbol and `kind`, so nothing that
/// can be looked up is carried here. That keeps Tick 32 bytes and keeps the
/// registry the single source of identity.
struct Tick {
  InstrumentId id = 0;
  Nanos exchange_ts{};  ///< venue `timestamp` (ms), widened to ns
  Nanos recv_ts{};      ///< local receive stamp, for latency and staleness
  double price = kNoValue;
};

static_assert(std::is_trivially_copyable_v<Tick>);

/// Parse a Deribit instrument name into its identity.
///
/// Handles all four forms: `BTC-27JUN25-100000-C` (option), `BTC-27JUN25`
/// (dated future), `btc_usd` (index), and `BTC-PERPETUAL` -- the last purely
/// defensively, since the perpetual is filtered out at the Python ingest and
/// should never appear.
///
/// Returns nullopt rather than throwing: a new listing with an unexpected shape
/// should be logged and skipped, not crash the gateway.
std::optional<InstrumentKey> parse_symbol(std::string_view symbol);

std::string_view to_string(InstrumentKind kind);

/// Deribit expiries are at 08:00 UTC on the named date.
inline constexpr std::chrono::hours kDeribitExpiryHourUtc{8};

}  // namespace shawlynot::quant::marketdata

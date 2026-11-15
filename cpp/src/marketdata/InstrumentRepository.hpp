#pragma once

// Loads the instrument universe from security_master at startup.
//
// Postgres is deliberately *not* in the hot path: one query, then the
// connection closes. A database outage blocks startup and nothing else -- once
// running, the gateway is unaffected. Stating that as a property is the point;
// nobody should later add a per-tick lookup.
//
// The cost of sourcing identity from the DB is staleness: the registry is only
// as current as the last reference_ingest run, so an expiry listed since then
// is invisible -- and unlike a bad parse there is no error, just a silently
// missing expiry in the surface. Detecting that drift is the job of a separate
// reconciliation process, not of the gateway's startup path.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "marketdata/model.hpp"

namespace shawlynot::quant::marketdata {

/// One row of the startup SELECT, still in database terms.
///
/// Kept separate from InstrumentKey so the mapping below is a pure function
/// that can be tested against a fixture result set with no live database.
struct InstrumentRow {
  InstrumentId instrument_id = 0;
  std::string symbol;
  /// security_master.instrument_type: 'option' | 'future' | 'currency'.
  /// This is *not* Deribit's `instrument_type`, which means the settlement
  /// convention ("linear"/"reversed").
  std::string instrument_type;
  /// instrument.tick_size as text, exactly as Postgres rendered the numeric.
  /// Text and not a double on purpose -- see exponent_from_tick_size.
  std::string tick_size;
  std::optional<Nanos> expiration;  ///< option and future rows
};

/// The decimal exponent implied by a tick size, i.e. minus the number of
/// decimal places it is written with, ignoring trailing zeros.
///
///   "0.00010000" -> -4      "2.50000000" -> -1
///   "0.01000000" -> -2      "5"          ->  0
///
/// This is pure string work by design. `tick_size` is numeric(20,8) and the
/// whole reason a tick carries an integer plus an exponent is to avoid binary
/// floating point; routing the scale itself through a double would put the
/// rounding straight back in. Returns nullopt for anything that is not a plain
/// decimal literal.
std::optional<std::int32_t> exponent_from_tick_size(std::string_view tick_size);

/// Map a database row onto gateway identity.
///
/// Returns nullopt for a row that cannot be made sense of -- an unknown
/// instrument_type, a tick size that will not parse, an option or future with
/// no expiry. A bad row is logged and skipped rather than aborting the load.
std::optional<InstrumentKey> to_instrument_key(const InstrumentRow& row);

/// The startup query. Exposed so a test can assert the shape it expects, and so
/// there is exactly one place the column order is defined.
extern const char* const kInstrumentQuery;

class InstrumentRepository {
 public:
  /// Open a connection, run one SELECT, close it.
  /// Throws on connection or query failure: without an instrument universe
  /// there is nothing to subscribe to, so failing the boot is correct.
  static std::vector<InstrumentRow> load(
      const std::string& conninfo, const std::string& venue_code = "DERIBIT");
};

}  // namespace shawlynot::quant::marketdata

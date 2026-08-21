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

#include <optional>
#include <string>
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
  std::optional<std::string> option_type;  ///< 'call' | 'put', option rows only
  std::optional<double> strike;            ///< option rows only
  std::optional<Nanos> expiration;         ///< option and future rows
};

/// Map a database row onto gateway identity.
///
/// Returns nullopt for a row that cannot be made sense of -- an unknown
/// instrument_type, an option missing its strike or right, a future missing its
/// expiry. A bad row is logged and skipped rather than aborting the load.
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

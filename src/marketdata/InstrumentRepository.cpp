#include "marketdata/InstrumentRepository.hpp"

#include <chrono>
#include <pqxx/pqxx>

#include "core/Log.hpp"

namespace shawlynot::quant::marketdata {

// 'perpetual' is never populated by the Python ingest -- the perpetual is
// filtered there so it cannot reach security_master at all -- but the filter is
// restated here so a future change to the ingest cannot quietly pull it into
// the subscribed universe.
// Expiries come back as epoch seconds rather than a timestamptz text
// rendering, which would drag a timezone-aware date parser into the gateway for
// no gain.
const char* const kInstrumentQuery = R"sql(
SELECT i.instrument_id,
       i.symbol,
       i.instrument_type,
       o.option_type,
       o.strike,
       EXTRACT(EPOCH FROM COALESCE(o.expiration_timestamp,
                                   f.expiration_timestamp)) AS expiration_epoch
FROM   security_master.instrument i
JOIN   security_master.venue v USING (venue_id)
LEFT   JOIN security_master.option o USING (instrument_id)
LEFT   JOIN security_master.future f USING (instrument_id)
WHERE  v.code = $1
  AND  i.instrument_type IN ('option', 'future', 'currency')
ORDER BY i.instrument_id
)sql";

std::optional<InstrumentKey> to_instrument_key(const InstrumentRow& row) {
  if (row.symbol.empty()) {
    return std::nullopt;
  }

  // The symbol carries the shape (strike, right, expiry); the DB carries the
  // identity (instrument_id) and the authoritative type. Parse first, then
  // reconcile -- a disagreement means the row is not what it claims to be.
  std::optional<InstrumentKey> key = parse_symbol(row.symbol);
  if (!key) {
    return std::nullopt;
  }
  key->id = row.instrument_id;

  if (row.instrument_type == "option") {
    if (key->kind != InstrumentKind::Option) {
      return std::nullopt;
    }
    // Prefer the database's own strike/right/expiry where present: it is
    // what the rest of the platform joins against.
    if (row.strike) {
      key->strike = *row.strike;
    }
    if (row.option_type) {
      if (*row.option_type == "call") {
        key->right = OptionRight::Call;
      } else if (*row.option_type == "put") {
        key->right = OptionRight::Put;
      } else {
        return std::nullopt;
      }
    }
    if (row.expiration) {
      key->expiry = *row.expiration;
    }
    return key;
  }

  if (row.instrument_type == "future") {
    // A perpetual row must never be ingested, whatever it claims.
    if (key->kind != InstrumentKind::Future) {
      return std::nullopt;
    }
    if (row.expiration) {
      key->expiry = *row.expiration;
    }
    // A future has no strike; a LEFT JOIN miss on `option` must not leave a
    // bogus one behind.
    key->strike = kNoValue;
    return key;
  }

  if (row.instrument_type == "currency") {
    if (key->kind != InstrumentKind::Index) {
      return std::nullopt;
    }
    key->strike = kNoValue;
    return key;
  }

  return std::nullopt;
}

std::vector<InstrumentRow> InstrumentRepository::load(
    const std::string& conninfo, const std::string& venue_code) {
  pqxx::connection connection{conninfo};
  pqxx::work transaction{connection};

  const pqxx::result result =
      transaction.exec_params(kInstrumentQuery, venue_code);
  transaction.commit();

  std::vector<InstrumentRow> rows;
  rows.reserve(result.size());
  for (const auto& record : result) {
    InstrumentRow row;
    row.instrument_id = record[0].as<InstrumentId>();
    row.symbol = record[1].as<std::string>();
    row.instrument_type = record[2].as<std::string>();
    if (!record[3].is_null()) {
      row.option_type = record[3].as<std::string>();
    }
    if (!record[4].is_null()) {
      row.strike = record[4].as<double>();
    }
    if (!record[5].is_null()) {
      const auto epoch_seconds = record[5].as<double>();
      row.expiration =
          Nanos{std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::duration<double>{epoch_seconds})};
    }
    rows.push_back(std::move(row));
  }

  spdlog::info("loaded {} instrument row(s) from security_master for venue {}",
               rows.size(), venue_code);
  return rows;
}

}  // namespace shawlynot::quant::marketdata

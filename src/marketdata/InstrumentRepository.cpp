#include "marketdata/InstrumentRepository.hpp"

#include <chrono>
#include <pqxx/pqxx>

#include "core/Log.hpp"

namespace shawlynot::quant::marketdata {

// 'perpetual' is never populated by the Python ingest -- the perpetual is
// filtered there so it cannot reach security_master at all -- but the filter is
// restated here so a future change to the ingest cannot quietly pull it into
// the subscribed universe.
//
// tick_size is cast to text rather than read as a numeric: the exponent is
// derived from how many decimal places it is written with, and a double in the
// middle of that would defeat the point (see exponent_from_tick_size).
//
// Expiries come back as epoch seconds rather than a timestamptz text
// rendering, which would drag a timezone-aware date parser into the gateway for
// no gain.
const char* const kInstrumentQuery = R"sql(
SELECT i.instrument_id,
       i.symbol,
       i.instrument_type,
       i.tick_size::text AS tick_size,
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

std::optional<std::int32_t> exponent_from_tick_size(
    std::string_view tick_size) {
  if (tick_size.empty()) {
    return std::nullopt;
  }
  if (tick_size.front() == '+' || tick_size.front() == '-') {
    tick_size.remove_prefix(1);  // a negative tick size is nonsense, but the
  }                              // sign is not what decides the scale
  if (tick_size.empty()) {
    return std::nullopt;
  }

  const std::size_t point = tick_size.find('.');
  const std::string_view whole = tick_size.substr(0, point);
  std::string_view fraction =
      point == std::string_view::npos ? std::string_view{}
                                      : tick_size.substr(point + 1);

  const auto all_digits = [](std::string_view text) {
    for (const char c : text) {
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  };
  if (whole.empty() || !all_digits(whole) || !all_digits(fraction)) {
    return std::nullopt;
  }

  // numeric(20,8) renders as "0.00010000"; the trailing zeros are the column's
  // scale, not the instrument's, so they say nothing about the tick.
  while (!fraction.empty() && fraction.back() == '0') {
    fraction.remove_suffix(1);
  }
  return -static_cast<std::int32_t>(fraction.size());
}

std::optional<InstrumentKey> to_instrument_key(const InstrumentRow& row) {
  if (row.symbol.empty() || row.instrument_id == 0) {
    return std::nullopt;
  }

  const auto exponent = exponent_from_tick_size(row.tick_size);
  if (!exponent) {
    return std::nullopt;
  }

  InstrumentKey key;
  key.symbol = row.symbol;
  key.id = row.instrument_id;
  key.price_exponent = *exponent;

  if (row.instrument_type == "option") {
    key.kind = InstrumentKind::Option;
  } else if (row.instrument_type == "future") {
    key.kind = InstrumentKind::Future;
  } else if (row.instrument_type == "currency") {
    key.kind = InstrumentKind::Index;
  } else {
    // 'perpetual' lands here along with anything unrecognised. Both are
    // skipped rather than classified, since neither is subscribable.
    return std::nullopt;
  }

  // The subscribe order is expiry-ascending, so a dated instrument with no
  // expiry cannot be placed. The index has none by nature and keeps the epoch,
  // which sorts it first -- harmlessly, since it is emitted ahead of the
  // dated instruments anyway.
  if (key.kind != InstrumentKind::Index) {
    if (!row.expiration) {
      return std::nullopt;
    }
    key.expiry = *row.expiration;
  }

  return key;
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
    row.tick_size = record[3].as<std::string>();
    if (!record[4].is_null()) {
      const auto epoch_seconds = record[4].as<double>();
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

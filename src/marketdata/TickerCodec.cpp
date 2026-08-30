#include "marketdata/TickerCodec.hpp"

#include <boost/json/value.hpp>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string_view>

namespace shawlynot::quant::marketdata {
namespace {

/// A price that is not a multiple of the instrument's tick by more than this
/// fraction of one tick is reported as rounded. Loose enough to ignore the
/// double's own representation error on a value the venue wrote in decimal.
constexpr double kRoundingTolerance = 1e-6;

/// Exact powers of ten. std::pow is not required to be exactly representable
/// even where the result is, and this is the multiply that decides the integer
/// a tick carries.
constexpr int kMaxExponent = 18;
constexpr double kPow10[] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8, 1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18};

/// A missing key, an explicit null, or a non-numeric value all mean "no value".
std::optional<double> number(const boost::json::object& data,
                             std::string_view key) {
  const boost::json::value* const value = data.if_contains(key);
  if (value == nullptr) {
    return std::nullopt;
  }
  if (const auto* const d = value->if_double()) {
    return *d;
  }
  if (const auto* const i = value->if_int64()) {
    return static_cast<double>(*i);
  }
  if (const auto* const u = value->if_uint64()) {
    return static_cast<double>(*u);
  }
  return std::nullopt;
}

/// Venue timestamps are milliseconds since epoch; the rest of the system is ns.
std::optional<Nanos> timestamp_ms(const boost::json::object& data) {
  const boost::json::value* const value = data.if_contains("timestamp");
  if (value == nullptr) {
    return std::nullopt;
  }
  std::int64_t millis = 0;
  if (const auto* const i = value->if_int64()) {
    millis = *i;
  } else if (const auto* const u = value->if_uint64()) {
    millis = static_cast<std::int64_t>(*u);
  } else if (const auto* const d = value->if_double()) {
    millis = static_cast<std::int64_t>(*d);
  } else {
    return std::nullopt;
  }
  return Nanos{std::chrono::milliseconds{millis}};
}

/// The shape both channels share: a timestamp and one price, or nothing.
///
/// A payload missing either is not a degraded tick, it is not a tick -- a
/// far-OTM strike that has never traded sends `last_price: null` indefinitely,
/// and forwarding that would put the same guard in every consumer.
std::optional<Decoded> decode_priced(const boost::json::object& data,
                                     std::string_view price_key,
                                     InstrumentId id, std::int32_t exponent,
                                     Nanos recv_ts) {
  const auto exchange_ts = timestamp_ms(data);
  if (!exchange_ts) {
    return std::nullopt;
  }
  const auto price = number(data, price_key);
  if (!price || !std::isfinite(*price)) {
    return std::nullopt;
  }

  // An exponent outside the table is a corrupt registry entry, not a corrupt
  // frame -- but it is still not decodable, so it fails the same way.
  if (exponent > 0 || exponent < -kMaxExponent) {
    return std::nullopt;
  }
  const double scaled = *price * kPow10[-exponent];
  if (std::abs(scaled) >
      static_cast<double>(std::numeric_limits<std::int64_t>::max())) {
    return std::nullopt;
  }

  const double rounded = std::round(scaled);
  Decoded decoded;
  decoded.precision_lost = std::abs(scaled - rounded) > kRoundingTolerance;
  decoded.tick = Tick{
      .id = id,
      .exchange_ts = *exchange_ts,
      .recv_ts = recv_ts,
      .price = static_cast<std::int64_t>(rounded),
      .exponent = exponent,
  };
  return decoded;
}

}  // namespace

std::optional<Decoded> TickerCodec::decode_ticker(
    const boost::json::object& data, InstrumentId id, std::int32_t exponent,
    Nanos recv_ts) {
  return decode_priced(data, "last_price", id, exponent, recv_ts);
}

std::optional<Decoded> TickerCodec::decode_index(
    const boost::json::object& data, InstrumentId id, std::int32_t exponent,
    Nanos recv_ts) {
  return decode_priced(data, "price", id, exponent, recv_ts);
}

}  // namespace shawlynot::quant::marketdata

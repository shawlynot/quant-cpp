#include "marketdata/TickerCodec.hpp"

#include <boost/json/value.hpp>
#include <cmath>
#include <cstdint>

namespace shawlynot::quant::marketdata {
namespace {

/// A missing key, an explicit null, or a non-numeric value all mean "no value".
double number_or_nan(const boost::json::object& data, std::string_view key) {
  const boost::json::value* const value = data.if_contains(key);
  if (value == nullptr) {
    return kNoValue;
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
  return kNoValue;
}

std::string_view string_or_empty(const boost::json::object& data,
                                 std::string_view key) {
  const boost::json::value* const value = data.if_contains(key);
  if (value == nullptr) {
    return {};
  }
  const auto* const s = value->if_string();
  return s == nullptr ? std::string_view{}
                      : std::string_view{s->data(), s->size()};
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
/// and forwarding that as NaN would put the same guard in every consumer.
std::optional<Tick> decode_priced(const boost::json::object& data,
                                  std::string_view price_key, InstrumentId id,
                                  Nanos recv_ts) {
  const auto exchange_ts = timestamp_ms(data);
  if (!exchange_ts) {
    return std::nullopt;
  }
  const double price = number_or_nan(data, price_key);
  if (std::isnan(price)) {
    return std::nullopt;
  }
  return Tick{
      .id = id,
      .exchange_ts = *exchange_ts,
      .recv_ts = recv_ts,
      .price = price,
  };
}

}  // namespace

std::optional<Tick> TickerCodec::decode_ticker(const boost::json::object& data,
                                               InstrumentId id, Nanos recv_ts) {
  return decode_priced(data, "last_price", id, recv_ts);
}

std::optional<Tick> TickerCodec::decode_index(const boost::json::object& data,
                                              InstrumentId id, Nanos recv_ts) {
  return decode_priced(data, "price", id, recv_ts);
}

std::optional<std::string_view> TickerCodec::instrument_name(
    const boost::json::object& data) {
  const std::string_view name = string_or_empty(data, "instrument_name");
  if (name.empty()) {
    return std::nullopt;
  }
  return name;
}

}  // namespace shawlynot::quant::marketdata

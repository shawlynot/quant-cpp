#pragma once

// Decodes Deribit `params.data` payloads into the Tick in model.hpp.
//
// This is the one boundary at which venue conventions are normalized, and it is
// deliberately the only one:
//
//   * `timestamp` is milliseconds since epoch and is widened to nanoseconds.
//   * The price field differs by channel -- `last_price` on a ticker,
//     `price` on the index -- and that is the *only* difference between the two
//     entry points below. Past the codec, a tick is a tick.
//   * A payload with no usable price is rejected outright rather than decoded
//     into a NaN. A Tick whose one payload field is NaN carries nothing, and
//     publishing it would make every consumer re-implement the same guard.
//
// What it does *not* normalize is the unit of account. Option prices are quoted
// in coin and they stay in coin -- the gateway records what the venue sent, so
// the pricer can do the USD conversion where the inverse-settlement logic
// lives.
//
// Every entry point returns optional and never throws: one malformed frame must
// not kill the read loop.

#include <boost/json/object.hpp>
#include <optional>
#include <string_view>

#include "marketdata/model.hpp"

namespace shawlynot::quant::marketdata {

class TickerCodec {
 public:
  /// Decode a `ticker.{instrument}.{interval}` payload, taking `last_price`
  /// as the price. `id` and `recv_ts` are supplied by the caller: the codec
  /// knows nothing about the registry or the clock.
  ///
  /// Returns nullopt when `last_price` is absent or null, which is routine on
  /// a far-OTM strike that has never traded.
  static std::optional<Tick> decode_ticker(const boost::json::object& data,
                                           InstrumentId id, Nanos recv_ts);

  /// Decode a `deribit_price_index.{index_name}` payload, taking `price`.
  static std::optional<Tick> decode_index(const boost::json::object& data,
                                          InstrumentId id, Nanos recv_ts);

  /// `instrument_name` out of a ticker payload -- how a tick is matched to a
  /// registry entry before `decode_ticker` is called.
  static std::optional<std::string_view> instrument_name(
      const boost::json::object& data);
};

}  // namespace shawlynot::quant::marketdata

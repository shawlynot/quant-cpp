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
//   * The venue's JSON number is scaled to an integer by the instrument's own
//     exponent, which the caller supplies from the registry. The codec never
//     picks a scale: one derived from the payload would vary tick to tick and
//     stop two prices for the same instrument being comparable as integers.
//   * A payload with no usable price is rejected outright rather than decoded
//     into a zero. A Tick whose one payload field is meaningless carries
//     nothing, and publishing it would make every consumer re-implement the
//     same guard.
//
// What it does *not* normalize is the unit of account. Option prices are quoted
// in coin and they stay in coin -- the gateway records what the venue sent, so
// the pricer can do the USD conversion where the inverse-settlement logic
// lives.
//
// Every entry point returns optional and never throws: one malformed frame must
// not kill the read loop.

#include <boost/json/object.hpp>
#include <cstdint>
#include <optional>

#include "marketdata/model.hpp"

namespace shawlynot::quant::marketdata {

/// A decoded tick plus the one thing the tick itself cannot say.
struct Decoded {
  Tick tick;
  /// The venue price was not a whole multiple of the instrument's tick, so
  /// scaling it rounded. Deribit's `tick_size_steps` shrinks the tick in low
  /// price bands while security_master stores only the base tick size, so this
  /// is the visible edge of that gap rather than a corrupt frame.
  bool precision_lost = false;
};

class TickerCodec {
 public:
  /// Decode a `ticker.{instrument}.{interval}` payload, taking `last_price`
  /// as the price. `id`, `exponent` and `recv_ts` are supplied by the caller:
  /// the codec knows nothing about the registry or the clock.
  ///
  /// Returns nullopt when `last_price` is absent or null, which is routine on
  /// a far-OTM strike that has never traded.
  static std::optional<Decoded> decode_ticker(const boost::json::object& data,
                                              InstrumentId id,
                                              std::int32_t exponent,
                                              Nanos recv_ts);

  /// Decode a `deribit_price_index.{index_name}` payload, taking `price`.
  static std::optional<Decoded> decode_index(const boost::json::object& data,
                                             InstrumentId id,
                                             std::int32_t exponent,
                                             Nanos recv_ts);
};

}  // namespace shawlynot::quant::marketdata

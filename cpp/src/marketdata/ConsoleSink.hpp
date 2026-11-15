#pragma once

// The first sink: print ticks so correctness can be eyeballed against Deribit's
// own UI. Deliberately trivial -- it exists to prove the pipeline, not to be
// fast.
//
// The price is rendered back out of the integer and its exponent rather than
// printed raw, because reading "5" and knowing it means 0.0005 BTC is exactly
// the step that a wrong exponent would hide.

#include <spdlog/spdlog.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "marketdata/InstrumentRegistry.hpp"
#include "marketdata/TickSink.hpp"

namespace shawlynot::quant::marketdata {

/// `price * 10^exponent` as a decimal string, without going through a double.
inline std::string format_scaled(std::int64_t price, std::int32_t exponent) {
  if (exponent >= 0) {
    return std::to_string(price) +
           std::string(static_cast<std::size_t>(exponent), '0');
  }

  const bool negative = price < 0;
  // Negate in unsigned so INT64_MIN does not overflow on the way.
  const auto unsigned_price = static_cast<std::uint64_t>(price);
  const std::uint64_t magnitude =
      negative ? ~unsigned_price + 1U : unsigned_price;
  std::string digits = std::to_string(magnitude);

  const auto places = static_cast<std::size_t>(-exponent);
  if (digits.size() <= places) {
    digits.insert(0, places - digits.size() + 1, '0');
  }
  digits.insert(digits.size() - places, 1, '.');
  return negative ? "-" + digits : digits;
}

class ConsoleSink : public ITickSink {
 public:
  explicit ConsoleSink(std::shared_ptr<const InstrumentRegistry> registry)
      : m_registry(std::move(registry)) {}

  void on_tick(const Tick& tick) override {
    const InstrumentKey* const key = m_registry->find(tick.id);
    static constexpr std::string_view kUnknown = "?";
    spdlog::info("{:8} {:<28} id={} price={}",
                 key == nullptr ? kUnknown : to_string(key->kind),
                 key == nullptr ? kUnknown : std::string_view{key->symbol},
                 tick.id, format_scaled(tick.price, tick.exponent));
  }

 private:
  std::shared_ptr<const InstrumentRegistry> m_registry;
};

}  // namespace shawlynot::quant::marketdata

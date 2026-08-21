#pragma once

// The M1 sink: print ticks so correctness can be eyeballed against Deribit's
// own UI. Deliberately trivial -- it exists to prove the pipeline, not to be
// fast.

#include <spdlog/spdlog.h>

#include "marketdata/InstrumentRegistry.hpp"
#include "marketdata/TickSink.hpp"

namespace shawlynot::quant::marketdata {

class ConsoleSink : public ITickSink {
 public:
  explicit ConsoleSink(std::shared_ptr<InstrumentRegistry> registry)
      : m_registry(std::move(registry)) {}

  void on_tick(const Tick& tick) override {
    const InstrumentKey* const key = m_registry->find(tick.id);
    spdlog::debug("{} {} price={}", key == nullptr ? "?" : to_string(key->kind),
                  key == nullptr ? "?" : key->symbol.c_str(), tick.price);
  }

  void on_feed_state(FeedState state) override {
    spdlog::info("feed state: {}", state == FeedState::Live ? "LIVE" : "STALE");
  }

 private:
  std::shared_ptr<InstrumentRegistry> m_registry;
};

}  // namespace shawlynot::quant::marketdata

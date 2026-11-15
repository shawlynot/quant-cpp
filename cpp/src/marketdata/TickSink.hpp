#pragma once

// Where normalized ticks go once the gateway is done with them.
//
// One method, because there is now one tick type: an option, a dated future and
// the spot index all arrive as a Tick and differ only in which instrument the
// id resolves to. A consumer that cares about only one of them filters on the
// registry's `kind` -- that is the same discrimination options and futures
// already relied on, extended to the index rather than special-cased for it.
//
// The gateway's downstream is a policy, not a hard-wired decision: ConsoleSink
// today, a queue handing off to a Protobuf/ZMQ publisher next.

#include "marketdata/model.hpp"

namespace shawlynot::quant::marketdata {

class InstrumentRegistry;

class ITickSink {
 public:
  virtual ~ITickSink() = default;

  virtual void on_tick(const Tick& tick) = 0;
};

}  // namespace shawlynot::quant::marketdata

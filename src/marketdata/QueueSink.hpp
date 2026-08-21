#pragma once

// The M4 sink: hand ticks to a consumer thread over a bounded queue, so the io
// thread is never behind a slow downstream.
//
// Anything heavier than parsing -- persistence, Protobuf encode, a publish --
// belongs on the far side of this handoff. Parsing a ticker message is cheap
// and stays inline; a blocking write on the read loop is what delays a
// test_request reply and gets the connection dropped.

#include <atomic>
#include <functional>
#include <thread>

#include "marketdata/TickSink.hpp"
#include "queue/BlockingQueue.hpp"

namespace shawlynot::quant::marketdata {

/// One tick type means the queue holds Ticks directly -- no variant, no visit,
/// and the element stays trivially copyable.
class QueueSink : public ITickSink {
 public:
  using TickHandler = std::function<void(const Tick&)>;

  explicit QueueSink(std::size_t capacity) : m_queue(capacity) {}

  ~QueueSink() override { stop(); }

  /// Start the consumer thread. The handler runs on that thread, never on the
  /// io thread.
  void start(TickHandler on_tick) {
    m_on_tick = std::move(on_tick);
    m_consumer = std::thread([this] { consume(); });
  }

  void stop() {
    m_queue.close();
    if (m_consumer.joinable()) {
      m_consumer.join();
    }
  }

  void on_tick(const Tick& tick) override { m_queue.push(tick); }

  void on_feed_state(FeedState state) override {
    m_feed_state.store(state, std::memory_order_relaxed);
  }

  FeedState feed_state() const {
    return m_feed_state.load(std::memory_order_relaxed);
  }

  /// Non-zero and growing means the consumer needs optimizing.
  std::uint64_t dropped() const { return m_queue.dropped(); }

 private:
  void consume() {
    Tick item;
    while (m_queue.pop(item)) {
      if (m_on_tick) {
        m_on_tick(item);
      }
    }
  }

  queue::BlockingQueue<Tick> m_queue;
  std::thread m_consumer;
  TickHandler m_on_tick;
  std::atomic<FeedState> m_feed_state{FeedState::Stale};
};

}  // namespace shawlynot::quant::marketdata

#pragma once

// Bounded producer/consumer handoff from the io thread to a sink thread.
//
// A plain mutex + condition_variable + deque, and deliberately so: a few
// hundred instruments at 100ms aggregation produce thousands of messages per
// second, not millions, and an uncontended mutex acquisition costs tens of
// nanoseconds against a budget of hundreds of microseconds. A lock-free SPSC
// ring needs careful memory-ordering reasoning to avoid a subtle,
// load-dependent bug; it is the optimization taken when a measurement asks for
// it. Keeping this interface to push/pop/close is what makes that swap touch
// one header and no call sites.

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <utility>

namespace shawlynot::quant::queue {

template <typename T>
class BlockingQueue {
 public:
  explicit BlockingQueue(std::size_t capacity) : m_capacity(capacity) {}

  BlockingQueue(const BlockingQueue&) = delete;
  BlockingQueue& operator=(const BlockingQueue&) = delete;

  /// Producer side (the io thread). Never waits for space.
  ///
  /// This is the whole reason the queue exists. A push that blocked when the
  /// consumer fell behind would put backpressure on the io thread -- exactly
  /// the stall the threading model exists to prevent -- and with a heartbeat
  /// deadline attached, a producer stuck behind a slow sink gets the
  /// connection dropped.
  ///
  /// Returns false when the item displaced an older one. For a latest-value
  /// feed a superseded tick has no value, so dropping the *oldest* is the
  /// correct policy rather than the least-bad one.
  bool push(const T& item) {
    bool dropped = false;
    {
      const std::lock_guard<std::mutex> lock(m_mutex);
      if (m_closed) {
        return false;
      }
      if (m_items.size() >= m_capacity) {
        m_items.pop_front();
        ++m_dropped;
        dropped = true;
      }
      m_items.push_back(item);
    }
    m_not_empty.notify_one();
    return !dropped;
  }

  /// Consumer side. Blocks until an item is available or the queue closes.
  /// Returns false only when the queue is closed and drained.
  bool pop(T& out) {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_not_empty.wait(lock, [this] { return !m_items.empty() || m_closed; });
    if (m_items.empty()) {
      return false;
    }
    out = std::move(m_items.front());
    m_items.pop_front();
    return true;
  }

  /// Wake every blocked consumer for shutdown. Items already queued stay
  /// poppable, so a consumer can drain before exiting.
  void close() {
    {
      const std::lock_guard<std::mutex> lock(m_mutex);
      m_closed = true;
    }
    m_not_empty.notify_all();
  }

  /// Monotonic drop counter, meant to be exported as a metric: non-zero and
  /// growing is the signal that the consumer needs optimizing, or that the
  /// lock-free replacement is finally justified.
  std::uint64_t dropped() const {
    const std::lock_guard<std::mutex> lock(m_mutex);
    return m_dropped;
  }

  std::size_t size() const {
    const std::lock_guard<std::mutex> lock(m_mutex);
    return m_items.size();
  }

  bool closed() const {
    const std::lock_guard<std::mutex> lock(m_mutex);
    return m_closed;
  }

 private:
  mutable std::mutex m_mutex;
  std::condition_variable m_not_empty;
  std::deque<T> m_items;
  std::size_t m_capacity;
  std::uint64_t m_dropped = 0;
  bool m_closed = false;
};

}  // namespace shawlynot::quant::queue

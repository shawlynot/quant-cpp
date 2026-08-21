#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "queue/BlockingQueue.hpp"

using shawlynot::quant::queue::BlockingQueue;

TEST(BlockingQueue, HonoursCapacity) {
  BlockingQueue<int> queue{3};

  for (int i = 0; i < 10; ++i) {
    queue.push(i);
  }

  EXPECT_EQ(queue.size(), 3u);
}

TEST(BlockingQueue, OverflowDropsTheOldest) {
  // For a latest-value feed a superseded tick has no value, so dropping the
  // oldest is correct rather than merely least-bad.
  BlockingQueue<int> queue{3};
  for (int i = 0; i < 5; ++i) {
    queue.push(i);
  }

  int value = -1;
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 2) << "0 and 1 should have been displaced";
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 3);
  ASSERT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 4);
}

TEST(BlockingQueue, CountsDrops) {
  BlockingQueue<int> queue{2};

  EXPECT_TRUE(queue.push(1));
  EXPECT_TRUE(queue.push(2));
  EXPECT_FALSE(queue.push(3)) << "push returns false when it displaced an item";
  EXPECT_FALSE(queue.push(4));

  EXPECT_EQ(queue.dropped(), 2u);
}

TEST(BlockingQueue, ProducerNeverBlocksOnCapacity) {
  // The whole reason the queue exists: a push that waited for space would put
  // backpressure on the io thread, and with a heartbeat deadline attached
  // that gets the connection dropped.
  BlockingQueue<int> queue{4};

  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 100000; ++i) {
    queue.push(i);
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, std::chrono::seconds{5});
  EXPECT_EQ(queue.size(), 4u);
  EXPECT_GT(queue.dropped(), 0u);
}

TEST(BlockingQueue, PopBlocksThenWakesOnPush) {
  BlockingQueue<int> queue{8};
  std::atomic<bool> popped{false};
  int received = -1;

  std::thread consumer([&] {
    int value = 0;
    if (queue.pop(value)) {
      received = value;
      popped.store(true);
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_FALSE(popped.load()) << "consumer should still be waiting";

  queue.push(42);
  consumer.join();

  EXPECT_TRUE(popped.load());
  EXPECT_EQ(received, 42);
}

TEST(BlockingQueue, CloseReleasesABlockedConsumer) {
  BlockingQueue<int> queue{8};
  std::atomic<bool> finished{false};

  std::thread consumer([&] {
    int value = 0;
    EXPECT_FALSE(queue.pop(value));
    finished.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_FALSE(finished.load());

  queue.close();
  consumer.join();

  EXPECT_TRUE(finished.load());
}

TEST(BlockingQueue, ClosedQueueStillDrains) {
  // A consumer should be able to finish what was already queued before it
  // sees the shutdown.
  BlockingQueue<int> queue{8};
  queue.push(1);
  queue.push(2);
  queue.close();

  int value = 0;
  EXPECT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 1);
  EXPECT_TRUE(queue.pop(value));
  EXPECT_EQ(value, 2);
  EXPECT_FALSE(queue.pop(value));
}

TEST(BlockingQueue, ClosedQueueRejectsFurtherPushes) {
  BlockingQueue<int> queue{8};
  queue.close();

  EXPECT_FALSE(queue.push(1));
  EXPECT_EQ(queue.size(), 0u);
}

TEST(BlockingQueue, SurvivesConcurrentProducerAndConsumer) {
  // Run this one under TSan: a mutex does not exempt the handoff from being
  // checked, since what TSan catches here is an item read after it was handed
  // over, or shutdown racing a blocked pop.
  constexpr int kItems = 20000;
  BlockingQueue<int> queue{64};
  std::atomic<int> consumed{0};

  std::thread consumer([&] {
    int value = 0;
    while (queue.pop(value)) {
      consumed.fetch_add(1, std::memory_order_relaxed);
    }
  });

  for (int i = 0; i < kItems; ++i) {
    queue.push(i);
  }
  queue.close();
  consumer.join();

  // Drops are expected and fine; what must hold is that nothing is delivered
  // twice or lost without being counted.
  EXPECT_EQ(consumed.load() + static_cast<int>(queue.dropped()), kItems);
}

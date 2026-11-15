// Registry tests: identity in, subscription out.
//
// The interesting behaviour is all in subscription_channels: what gets
// subscribed when the universe is larger than one request can carry, and in
// what order. A cap that silently kept the wrong 200 channels would show up
// much later as a surface with no usable smile anywhere.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <string>

#include "marketdata/InstrumentRegistry.hpp"

using namespace shawlynot::quant::marketdata;

namespace {

Nanos days(int n) { return Nanos{std::chrono::hours{24 * n}}; }

/// "Now" for the subscription tests: expiries above this are live, below dead.
const Nanos kNow = days(1);

InstrumentKey index_key(InstrumentId id = 1) {
  return InstrumentKey{.symbol = "btc_usd",
                       .id = id,
                       .kind = InstrumentKind::Index,
                       .price_exponent = -2,
                       .expiry = Nanos{}};
}

InstrumentKey future_key(InstrumentId id, const std::string& symbol,
                         int expiry_days) {
  return InstrumentKey{.symbol = symbol,
                       .id = id,
                       .kind = InstrumentKind::Future,
                       .price_exponent = -1,
                       .expiry = days(expiry_days)};
}

InstrumentKey option_key(InstrumentId id, const std::string& symbol,
                         int expiry_days) {
  return InstrumentKey{.symbol = symbol,
                       .id = id,
                       .kind = InstrumentKind::Option,
                       .price_exponent = -4,
                       .expiry = days(expiry_days)};
}

/// `count` options all sharing one expiry.
void add_expiry(InstrumentRegistry& registry, InstrumentId first_id,
                int expiry_days, int count) {
  for (int i = 0; i < count; ++i) {
    registry.add(option_key(first_id + i,
                            "BTC-E" + std::to_string(expiry_days) + "-" +
                                std::to_string(i) + "-C",
                            expiry_days));
  }
}

}  // namespace

TEST(ChannelFor, TheIndexTakesNoIntervalSuffix) {
  // Appending one fails the subscribe outright, so this is not cosmetic.
  EXPECT_EQ(channel_for(index_key(), "100ms"), "deribit_price_index.btc_usd");
  EXPECT_EQ(channel_for(future_key(2, "BTC-20AUG26", 5), "100ms"),
            "ticker.BTC-20AUG26.100ms");
  EXPECT_EQ(channel_for(option_key(3, "BTC-25SEP26-98000-C", 40), "agg2"),
            "ticker.BTC-25SEP26-98000-C.agg2");
}

TEST(InstrumentRegistry, RefusesAnInstrumentWithNoSecurityMasterId) {
  InstrumentRegistry registry;
  InstrumentKey orphan = index_key(0);
  EXPECT_FALSE(registry.add(std::move(orphan)).has_value());
  EXPECT_EQ(registry.size(), 0u);
}

TEST(InstrumentRegistry, ReRegisteringASymbolReturnsTheExistingId) {
  InstrumentRegistry registry;
  ASSERT_EQ(registry.add(index_key(7)), 7);
  EXPECT_EQ(registry.add(index_key(7)), 7);
  EXPECT_EQ(registry.size(), 1u);
}

TEST(InstrumentRegistry, SubscribingBindsEveryChannelForInboundRouting) {
  // This is what lets dispatch route on the channel a frame arrived on instead
  // of re-parsing instrument_name out of every payload.
  InstrumentRegistry registry;
  registry.add(index_key(1));
  registry.add(future_key(2, "BTC-20AUG26", 5));

  EXPECT_FALSE(registry.by_channel("deribit_price_index.btc_usd").has_value())
      << "nothing is routable before it is subscribed";

  const Subscription subscription =
      registry.subscription_channels("100ms", 50, kNow);
  EXPECT_EQ(subscription.channels.size(), 2u);
  EXPECT_EQ(registry.by_channel("deribit_price_index.btc_usd"), 1);
  EXPECT_EQ(registry.by_channel("ticker.BTC-20AUG26.100ms"), 2);
}

TEST(InstrumentRegistry, IndexComesFirstThenFuturesThenOptions) {
  // Spot is the reference every option tick is read against; subscribing it
  // last would leave the first seconds of option ticks with no index behind
  // them.
  InstrumentRegistry registry;
  add_expiry(registry, 100, 7, 2);
  registry.add(future_key(2, "BTC-20AUG26", 5));
  registry.add(index_key(1));

  const Subscription subscription =
      registry.subscription_channels("100ms", 50, kNow);
  ASSERT_EQ(subscription.channels.size(), 4u);
  EXPECT_EQ(subscription.channels[0], "deribit_price_index.btc_usd");
  EXPECT_EQ(subscription.channels[1], "ticker.BTC-20AUG26.100ms");
  EXPECT_FALSE(subscription.truncated);
  EXPECT_EQ(subscription.offered, 4u);
}

TEST(InstrumentRegistry, FuturesAreOrderedNearestExpiryFirst) {
  InstrumentRegistry registry;
  registry.add(future_key(3, "BTC-FAR", 90));
  registry.add(future_key(2, "BTC-NEAR", 5));

  const Subscription subscription =
      registry.subscription_channels("100ms", 50, kNow);
  ASSERT_EQ(subscription.channels.size(), 2u);
  EXPECT_EQ(subscription.channels[0], "ticker.BTC-NEAR.100ms");
  EXPECT_EQ(subscription.channels[1], "ticker.BTC-FAR.100ms");
}

TEST(InstrumentRegistry, OptionsAreAdmittedAWholeExpiryAtATime) {
  // A fitter needs complete front expiries. An arbitrary slice across every
  // expiry would give it a fragment of each and a usable smile for none, so
  // the cap must cut between expiries rather than through one.
  InstrumentRegistry registry;
  registry.add(index_key(1));
  add_expiry(registry, 100, 7, 4);    // near
  add_expiry(registry, 200, 30, 4);   // mid
  add_expiry(registry, 300, 90, 4);   // far

  // Room for the index plus two whole expiries, and half of a third.
  const Subscription subscription =
      registry.subscription_channels("100ms", 11, kNow);
  EXPECT_TRUE(subscription.truncated);
  EXPECT_EQ(subscription.expiries, 2u) << "the third expiry does not fit whole";
  EXPECT_EQ(subscription.channels.size(), 9u) << "index + 4 + 4";
  EXPECT_EQ(subscription.offered, 13u);

  // And it is the *near* expiries that survived.
  const auto contains = [&](const std::string& channel) {
    return std::find(subscription.channels.begin(),
                     subscription.channels.end(),
                     channel) != subscription.channels.end();
  };
  EXPECT_TRUE(contains("ticker.BTC-E7-0-C.100ms"));
  EXPECT_TRUE(contains("ticker.BTC-E30-0-C.100ms"));
  EXPECT_FALSE(contains("ticker.BTC-E90-0-C.100ms"));
}

TEST(InstrumentRegistry, AFirstExpiryWiderThanTheCapIsTakenPartially) {
  // Dropping every option because the front month is large would be worse than
  // a partial front month.
  InstrumentRegistry registry;
  registry.add(index_key(1));
  add_expiry(registry, 100, 7, 20);

  const Subscription subscription =
      registry.subscription_channels("100ms", 6, kNow);
  EXPECT_TRUE(subscription.truncated);
  EXPECT_EQ(subscription.channels.size(), 6u);
  EXPECT_EQ(subscription.channels[0], "deribit_price_index.btc_usd");
}

TEST(InstrumentRegistry, AnUntruncatedSubscriptionSaysSo) {
  InstrumentRegistry registry;
  registry.add(index_key(1));
  add_expiry(registry, 100, 7, 3);

  const Subscription subscription =
      registry.subscription_channels("100ms", 200, kNow);
  EXPECT_FALSE(subscription.truncated);
  EXPECT_EQ(subscription.expiries, 1u);
  EXPECT_EQ(subscription.channels.size(), subscription.offered);
}

TEST(InstrumentRegistry, APerpetualIsNeverSubscribed) {
  InstrumentRegistry registry;
  registry.add(InstrumentKey{.symbol = "BTC-PERPETUAL",
                             .id = 9,
                             .kind = InstrumentKind::Perpetual,
                             .price_exponent = -1,
                             .expiry = Nanos{}});
  registry.add(index_key(1));

  const Subscription subscription =
      registry.subscription_channels("100ms", 50, kNow);
  ASSERT_EQ(subscription.channels.size(), 1u);
  EXPECT_EQ(subscription.channels[0], "deribit_price_index.btc_usd");
  EXPECT_EQ(subscription.offered, 1u) << "it is not even offered";
}

TEST(InstrumentRegistry, AnAlreadyExpiredInstrumentIsNeverSubscribed) {
  // The registry is only as current as the last reference_ingest run and
  // routinely holds settled expiries. Because the order is nearest-first,
  // those would otherwise consume the whole cap and be rejected wholesale by
  // the venue -- which reads as "the subscribe half worked" rather than as
  // stale reference data.
  InstrumentRegistry registry;
  registry.add(index_key(1));
  registry.add(future_key(2, "BTC-DEAD", 0));
  add_expiry(registry, 100, 0, 4);   // settled yesterday
  add_expiry(registry, 200, 30, 4);  // live

  const Subscription subscription =
      registry.subscription_channels("100ms", 200, kNow);
  EXPECT_EQ(subscription.expired, 5u) << "one future and four options";
  EXPECT_EQ(subscription.offered, 5u) << "index plus the live expiry";
  EXPECT_EQ(subscription.channels.size(), 5u);
  EXPECT_EQ(subscription.expiries, 1u);
  for (const std::string& channel : subscription.channels) {
    EXPECT_EQ(channel.find("DEAD"), std::string::npos);
    EXPECT_EQ(channel.find("BTC-E0-"), std::string::npos);
  }
}

TEST(InstrumentRegistry, TheIndexHasNoExpiryAndIsAlwaysLive) {
  InstrumentRegistry registry;
  registry.add(index_key(1));
  const Subscription subscription =
      registry.subscription_channels("100ms", 200, days(10000));
  ASSERT_EQ(subscription.channels.size(), 1u);
  EXPECT_EQ(subscription.expired, 0u);
}

TEST(InstrumentRegistry, FindResolvesATickIdBackToItsIdentity) {
  InstrumentRegistry registry;
  registry.add(option_key(555, "BTC-25SEP26-98000-C", 40));

  const InstrumentKey* const key = registry.find(555);
  ASSERT_NE(key, nullptr);
  EXPECT_EQ(key->symbol, "BTC-25SEP26-98000-C");
  EXPECT_EQ(key->price_exponent, -4);
  EXPECT_EQ(registry.find(556), nullptr);
}

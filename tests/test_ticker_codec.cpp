// Codec tests: captured Deribit frames in, Ticks out.
//
// The assertions are on the exact integer, not on a tolerance, because the
// whole reason a tick carries an int plus an exponent is that the value is
// exact. A test that accepted 68553.49999 would not be testing this design.

#include <gtest/gtest.h>

#include "fixtures.hpp"
#include "marketdata/TickerCodec.hpp"

using namespace shawlynot::quant::marketdata;
using shawlynot::quant::test::load_notification_data;

namespace {

constexpr InstrumentId kId = 4242;
constexpr Nanos kRecv{std::chrono::nanoseconds{1'787'170'728'500'000'000}};

// The exponents the repository derives from security_master.tick_size for the
// three instrument types actually in the database.
constexpr std::int32_t kOptionExponent = -4;  // tick_size 0.0001
constexpr std::int32_t kFutureExponent = -1;  // tick_size 2.5
constexpr std::int32_t kIndexExponent = -2;   // tick_size 0.01

}  // namespace

TEST(TickerCodec, ScalesAnOptionPremiumByItsOwnTickExponent) {
  const auto decoded = TickerCodec::decode_ticker(
      load_notification_data("ticker_option.json"), kId, kOptionExponent,
      kRecv);
  ASSERT_TRUE(decoded.has_value());
  // last_price 0.001 BTC at 1e-4 granularity is exactly 10 ticks.
  EXPECT_EQ(decoded->tick.price, 10) << "0.001 BTC should scale to 10 at 1e-4";
  EXPECT_EQ(decoded->tick.exponent, kOptionExponent);
  EXPECT_EQ(decoded->tick.id, kId);
  EXPECT_FALSE(decoded->precision_lost);
}

TEST(TickerCodec, ScalesAFuturePriceByItsOwnTickExponent) {
  const auto decoded = TickerCodec::decode_ticker(
      load_notification_data("ticker_future.json"), kId, kFutureExponent,
      kRecv);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->tick.price, 685300) << "68530.0 at 1e-1";
  EXPECT_EQ(decoded->tick.exponent, kFutureExponent);
  EXPECT_FALSE(decoded->precision_lost);
}

TEST(TickerCodec, IndexReadsPriceRatherThanLastPrice) {
  const auto decoded = TickerCodec::decode_index(
      load_notification_data("price_index.json"), kId, kIndexExponent, kRecv);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->tick.price, 6855350) << "68553.5 at 1e-2";
  EXPECT_EQ(decoded->tick.exponent, kIndexExponent);
}

TEST(TickerCodec, TheIndexPayloadHasNoLastPriceSoTheTickerEntryPointRejectsIt) {
  // The two channels carry the price under different names. Choosing the entry
  // point by channel rather than by payload shape is what keeps that from
  // being a guess -- this is the failure that guess would produce.
  EXPECT_FALSE(TickerCodec::decode_ticker(
                   load_notification_data("price_index.json"), kId,
                   kIndexExponent, kRecv)
                   .has_value());
}

TEST(TickerCodec, VenueTimestampIsMillisecondsWidenedToNanoseconds) {
  const auto decoded = TickerCodec::decode_index(
      load_notification_data("price_index.json"), kId, kIndexExponent, kRecv);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->tick.exchange_ts,
            Nanos{std::chrono::milliseconds{1787170728407}});
  EXPECT_EQ(decoded->tick.recv_ts, kRecv);
}

TEST(TickerCodec, AnUntradedStrikeSendsNullLastPriceAndDecodesToNothing) {
  // Routine on a far-OTM strike, and the reason this is not a decode failure:
  // folding it into the failure counter would make that counter useless.
  boost::json::object data = load_notification_data("ticker_option.json");
  data["last_price"] = nullptr;
  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, kOptionExponent, kRecv)
                   .has_value());

  data.erase("last_price");
  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, kOptionExponent, kRecv)
                   .has_value());
}

TEST(TickerCodec, APayloadWithNoTimestampIsNotATick) {
  boost::json::object data = load_notification_data("ticker_option.json");
  data.erase("timestamp");
  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, kOptionExponent, kRecv)
                   .has_value());
}

TEST(TickerCodec, APriceFinerThanTheTickIsRoundedAndFlagged) {
  // Deribit's tick_size_steps shrinks the tick in low price bands while
  // security_master stores only the base tick size, so a premium can arrive
  // with more decimals than the exponent can hold. It must round visibly
  // rather than silently.
  boost::json::object data = load_notification_data("ticker_option.json");
  data["last_price"] = 0.00105;
  const auto decoded =
      TickerCodec::decode_ticker(data, kId, kOptionExponent, kRecv);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(decoded->precision_lost);
  EXPECT_EQ(decoded->tick.price, 11) << "0.00105 at 1e-4 rounds to 11";
}

TEST(TickerCodec, AnIntegerJsonPriceIsAcceptedAlongsideADouble) {
  // Boost.JSON types 68530 and 68530.0 differently; the venue writes whichever
  // it likes and the codec must not care.
  boost::json::object data = load_notification_data("ticker_future.json");
  data["last_price"] = 68530;
  const auto decoded =
      TickerCodec::decode_ticker(data, kId, kFutureExponent, kRecv);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->tick.price, 685300);
}

TEST(TickerCodec, ANonsensicalExponentIsRejectedRatherThanIndexedOutOfRange) {
  boost::json::object data = load_notification_data("ticker_future.json");
  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, -19, kRecv).has_value());
  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, 1, kRecv).has_value());
}

#include <gtest/gtest.h>

#include <cmath>

#include "fixtures.hpp"
#include "marketdata/TickerCodec.hpp"

using namespace shawlynot::quant::marketdata;
using shawlynot::quant::test::load_notification_channel;
using shawlynot::quant::test::load_notification_data;

namespace {

constexpr InstrumentId kId = 90502;

Nanos recv_stamp() { return Nanos{std::chrono::seconds{1787170730}}; }

}  // namespace

TEST(TickerCodec, TakesLastPriceAsThePriceOnAnOptionFrame) {
  // A ticker frame carries a dozen prices; `last_price` is the one a Tick
  // means. The rest -- bids, asks, mark, the IVs, the greeks -- are read by
  // nothing and decoded by nothing.
  const auto tick = TickerCodec::decode_ticker(
      load_notification_data("ticker_option.json"), kId, recv_stamp());

  ASSERT_TRUE(tick.has_value());
  EXPECT_EQ(tick->id, kId);
  EXPECT_EQ(tick->recv_ts, recv_stamp());
  EXPECT_DOUBLE_EQ(tick->price, 0.001);
}

TEST(TickerCodec, TakesLastPriceAsThePriceOnAFutureFrame) {
  // Same codec, same field: a future differs from an option only in which
  // instrument its id resolves to.
  const auto tick = TickerCodec::decode_ticker(
      load_notification_data("ticker_future.json"), kId, recv_stamp());

  ASSERT_TRUE(tick.has_value());
  EXPECT_DOUBLE_EQ(tick->price, 68530.0);
}

TEST(TickerCodec, WidensMillisecondTimestampToNanoseconds) {
  const auto tick = TickerCodec::decode_ticker(
      load_notification_data("ticker_option.json"), kId, recv_stamp());

  ASSERT_TRUE(tick.has_value());
  const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
      tick->exchange_ts.time_since_epoch());
  EXPECT_EQ(millis.count(), 1787170728407);
  // Widened, not truncated: the ns representation must hold the exact ms.
  EXPECT_EQ(tick->exchange_ts.time_since_epoch().count(),
            1787170728407LL * 1'000'000);
}

TEST(TickerCodec, RejectsAFrameWithNoLastPrice) {
  // Routine, not malformed: a far-OTM strike that has never traded sends
  // `last_price: null` on every update. A Tick whose one payload field is NaN
  // carries nothing, so it is not produced at all -- otherwise every consumer
  // reimplements this guard.
  boost::json::object data = load_notification_data("ticker_option.json");
  data["last_price"] = nullptr;

  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, recv_stamp()).has_value())
      << "an explicit null price must not decode";

  data.erase("last_price");
  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, recv_stamp()).has_value())
      << "an absent price must not decode";
}

TEST(TickerCodec, RejectsPayloadWithoutTimestamp) {
  boost::json::object data = load_notification_data("ticker_option.json");
  data.erase("timestamp");

  EXPECT_FALSE(TickerCodec::decode_ticker(data, kId, recv_stamp()).has_value());
}

TEST(TickerCodec, IgnoresEveryOtherFieldOnTheFrame) {
  // The guarantee that makes one Tick honest: nothing outside `timestamp` and
  // the price field can change the decode. Gutting the frame of quotes, IVs,
  // greeks and state leaves the tick identical.
  boost::json::object full = load_notification_data("ticker_option.json");
  boost::json::object bare{{"timestamp", full.at("timestamp")},
                           {"last_price", full.at("last_price")}};

  const auto from_full = TickerCodec::decode_ticker(full, kId, recv_stamp());
  const auto from_bare = TickerCodec::decode_ticker(bare, kId, recv_stamp());

  ASSERT_TRUE(from_full.has_value());
  ASSERT_TRUE(from_bare.has_value());
  EXPECT_EQ(from_full->exchange_ts, from_bare->exchange_ts);
  EXPECT_DOUBLE_EQ(from_full->price, from_bare->price);
}

TEST(TickerCodec, ReadsInstrumentName) {
  EXPECT_EQ(TickerCodec::instrument_name(
                load_notification_data("ticker_option.json")),
            "BTC-25SEP26-98000-C");
  EXPECT_EQ(TickerCodec::instrument_name(
                load_notification_data("ticker_future.json")),
            "BTC-20AUG26");
}

TEST(IndexCodec, TakesPriceFromTheIndexPayloadsOwnField) {
  // The index channel has a three-field payload and calls its price `price`,
  // not `last_price`. That one key is the whole difference between the two
  // codec entry points.
  const auto tick = TickerCodec::decode_index(
      load_notification_data("price_index.json"), kId, recv_stamp());

  ASSERT_TRUE(tick.has_value());
  EXPECT_EQ(tick->id, kId);
  EXPECT_DOUBLE_EQ(tick->price, 68553.5);
  EXPECT_EQ(tick->exchange_ts.time_since_epoch().count(),
            1787170728407LL * 1'000'000);
  EXPECT_EQ(tick->recv_ts, recv_stamp());
}

TEST(IndexCodec, RejectsPayloadWithoutAPrice) {
  boost::json::object data = load_notification_data("price_index.json");
  data.erase("price");

  EXPECT_FALSE(TickerCodec::decode_index(data, kId, recv_stamp()).has_value());
}

TEST(IndexCodec, DoesNotFallBackToLastPrice) {
  // The entry points must not be interchangeable: an index frame has no
  // `last_price` and a ticker frame's `price` means nothing.
  boost::json::object index = load_notification_data("price_index.json");
  EXPECT_FALSE(
      TickerCodec::decode_ticker(index, kId, recv_stamp()).has_value());

  boost::json::object ticker = load_notification_data("ticker_option.json");
  EXPECT_FALSE(
      TickerCodec::decode_index(ticker, kId, recv_stamp()).has_value());
}

TEST(IndexCodec, ChannelIsRoutedByNameNotPayloadShape) {
  // With one Tick type the payload shape is the *only* thing that could
  // otherwise distinguish the channels, and it must not be relied on.
  EXPECT_EQ(load_notification_channel("price_index.json"),
            "deribit_price_index.btc_usd");
  EXPECT_EQ(load_notification_channel("ticker_option.json"),
            "ticker.BTC-25SEP26-98000-C.100ms");
  EXPECT_EQ(load_notification_channel("ticker_future.json"),
            "ticker.BTC-20AUG26.100ms");
}

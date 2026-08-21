#include <gtest/gtest.h>

#include <cmath>

#include "marketdata/model.hpp"

using namespace shawlynot::quant::marketdata;

namespace {

std::int64_t epoch_seconds(Nanos when) {
  return std::chrono::duration_cast<std::chrono::seconds>(
             when.time_since_epoch())
      .count();
}

}  // namespace

TEST(ParseSymbol, ParsesOption) {
  const auto key = parse_symbol("BTC-27JUN25-100000-C");

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Option);
  EXPECT_EQ(key->symbol, "BTC-27JUN25-100000-C");
  EXPECT_EQ(key->base_ccy, "BTC");
  EXPECT_DOUBLE_EQ(key->strike, 100000.0);
  EXPECT_EQ(key->right, OptionRight::Call);
}

TEST(ParseSymbol, ParsesPut) {
  const auto key = parse_symbol("BTC-25SEP26-98000-P");

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->right, OptionRight::Put);
  EXPECT_DOUBLE_EQ(key->strike, 98000.0);
}

TEST(ParseSymbol, ParsesDatedFuture) {
  const auto key = parse_symbol("BTC-26JUN26");

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Future);
  EXPECT_EQ(key->base_ccy, "BTC");
  EXPECT_TRUE(std::isnan(key->strike));
}

TEST(ParseSymbol, ParsesSingleDigitDay) {
  // Deribit writes the day without a leading zero: "4SEP26", not "04SEP26".
  const auto key = parse_symbol("BTC-4SEP26");

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Future);
  // 2026-09-04T08:00:00Z
  EXPECT_EQ(epoch_seconds(key->expiry), 1788508800);
}

TEST(ParseSymbol, ExpiryIsEightAmUtc) {
  const auto key = parse_symbol("BTC-27JUN25");

  ASSERT_TRUE(key.has_value());
  // 2025-06-27T08:00:00Z
  EXPECT_EQ(epoch_seconds(key->expiry), 1751011200);
}

TEST(ParseSymbol, ParsesIndex) {
  const auto key = parse_symbol("btc_usd");

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Index);
  EXPECT_EQ(key->symbol, "btc_usd");
  EXPECT_EQ(key->base_ccy, "BTC");
  EXPECT_TRUE(std::isnan(key->strike));
}

TEST(ParseSymbol, RecognisesPerpetualDefensively) {
  // Filtered out upstream and should never appear -- but it must be
  // classified rather than rejected if it does.
  const auto key = parse_symbol("BTC-PERPETUAL");

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Perpetual);
}

TEST(ParseSymbol, RejectsMalformedInput) {
  // A new listing with an unexpected shape is logged and skipped, never fatal.
  EXPECT_FALSE(parse_symbol("").has_value());
  EXPECT_FALSE(parse_symbol("BTC").has_value());
  EXPECT_FALSE(parse_symbol("BTC-NOTADATE").has_value());
  EXPECT_FALSE(parse_symbol("BTC-27JUN25-100000-X").has_value())
      << "right must be C or P";
  EXPECT_FALSE(parse_symbol("BTC-27JUN25-abc-C").has_value())
      << "strike must be numeric";
  EXPECT_FALSE(parse_symbol("BTC-27JUN25-0-C").has_value())
      << "strike must be positive";
  EXPECT_FALSE(parse_symbol("BTC-32JUN25-100-C").has_value())
      << "day 32 is not a date";
  EXPECT_FALSE(parse_symbol("BTC-27XXX25-100-C").has_value())
      << "month must be a name";
  EXPECT_FALSE(parse_symbol("BTC-27JUN25-100-C-EXTRA").has_value());
  EXPECT_FALSE(parse_symbol("_usd").has_value());
  EXPECT_FALSE(parse_symbol("btc_").has_value());
}

TEST(ParseSymbol, StrikeIsNanForNonOptions) {
  // NaN rather than 0, so a future's absent strike cannot be mistaken for a
  // zero-strike option anywhere downstream.
  EXPECT_TRUE(std::isnan(parse_symbol("BTC-26JUN26")->strike));
  EXPECT_TRUE(std::isnan(parse_symbol("btc_usd")->strike));
  EXPECT_TRUE(std::isnan(parse_symbol("BTC-PERPETUAL")->strike));
}

TEST(TickDefaults, AnUnsetPriceIsNanNotZero) {
  // A default-constructed Tick must not read as a real price of zero: the
  // whole point of NaN-for-absent survives the collapse to one field.
  const Tick tick;

  EXPECT_TRUE(std::isnan(tick.price));
  EXPECT_EQ(tick.id, 0);
  EXPECT_EQ(tick.exchange_ts, Nanos{});
  EXPECT_EQ(tick.recv_ts, Nanos{});
}

TEST(TickDefaults, StaysASmallTriviallyCopyablePod) {
  // Tick is expected to end up in a Protobuf encoder or a shared-memory
  // segment; staying a fixed-size POD is what makes those a copy rather than
  // a rewrite.
  static_assert(std::is_trivially_copyable_v<Tick>);
  EXPECT_EQ(sizeof(Tick), 32u);
}

// Repository tests: a database row becomes gateway identity, and a tick size
// becomes the exponent every price for that instrument is scaled by.
//
// No database. `load()` is one SELECT and is exercised by running the gateway;
// what is worth testing without one is the mapping, which is where a wrong
// exponent would silently misprice every tick for an instrument.

#include <gtest/gtest.h>

#include <chrono>

#include "marketdata/InstrumentRepository.hpp"

using namespace shawlynot::quant::marketdata;

namespace {

constexpr Nanos kExpiry{std::chrono::seconds{1787097600}};

InstrumentRow option_row() {
  return InstrumentRow{
      .instrument_id = 101,
      .symbol = "BTC-25SEP26-98000-C",
      .instrument_type = "option",
      .tick_size = "0.00010000",
      .expiration = kExpiry,
  };
}

}  // namespace

TEST(ExponentFromTickSize, ReadsTheScaleOffTheDigitsNotAFloat) {
  // The three tick sizes actually present in security_master.
  EXPECT_EQ(exponent_from_tick_size("0.00010000"), -4) << "option tick";
  EXPECT_EQ(exponent_from_tick_size("2.50000000"), -1) << "future tick";
  EXPECT_EQ(exponent_from_tick_size("0.01000000"), -2) << "index placeholder";
}

TEST(ExponentFromTickSize, TrailingZerosAreTheColumnScaleNotTheInstruments) {
  // numeric(20,8) always renders eight places; only the significant ones say
  // anything about the tick.
  EXPECT_EQ(exponent_from_tick_size("0.10000000"), -1);
  EXPECT_EQ(exponent_from_tick_size("0.1"), -1);
  EXPECT_EQ(exponent_from_tick_size("1.00000000"), 0);
}

TEST(ExponentFromTickSize, AnIntegerTickNeedsNoScaling) {
  EXPECT_EQ(exponent_from_tick_size("5"), 0);
  EXPECT_EQ(exponent_from_tick_size("25"), 0);
}

TEST(ExponentFromTickSize, RejectsAnythingThatIsNotAPlainDecimalLiteral) {
  EXPECT_FALSE(exponent_from_tick_size("").has_value());
  EXPECT_FALSE(exponent_from_tick_size(".5").has_value());
  EXPECT_FALSE(exponent_from_tick_size("1e-4").has_value())
      << "scientific notation would give the wrong scale silently";
  EXPECT_FALSE(exponent_from_tick_size("abc").has_value());
  EXPECT_FALSE(exponent_from_tick_size("1.2.3").has_value());
}

TEST(ToInstrumentKey, MapsEachDatabaseTypeOntoItsKind) {
  InstrumentRow row = option_row();
  ASSERT_TRUE(to_instrument_key(row).has_value());
  EXPECT_EQ(to_instrument_key(row)->kind, InstrumentKind::Option);

  row.instrument_type = "future";
  row.symbol = "BTC-20AUG26";
  row.tick_size = "2.50000000";
  ASSERT_TRUE(to_instrument_key(row).has_value());
  EXPECT_EQ(to_instrument_key(row)->kind, InstrumentKind::Future);
  EXPECT_EQ(to_instrument_key(row)->price_exponent, -1);

  row.instrument_type = "currency";
  row.symbol = "btc_usd";
  row.tick_size = "0.01000000";
  row.expiration.reset();
  ASSERT_TRUE(to_instrument_key(row).has_value());
  EXPECT_EQ(to_instrument_key(row)->kind, InstrumentKind::Index);
  EXPECT_EQ(to_instrument_key(row)->price_exponent, -2);
}

TEST(ToInstrumentKey, CarriesThePostgresIdAndSymbolThrough) {
  const auto key = to_instrument_key(option_row());
  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->id, 101) << "the security_master id is the only id";
  EXPECT_EQ(key->symbol, "BTC-25SEP26-98000-C");
  EXPECT_EQ(key->price_exponent, -4);
  EXPECT_EQ(key->expiry, kExpiry);
}

TEST(ToInstrumentKey, RefusesAPerpetualEvenThoughTheColumnAllowsIt) {
  // The Python ingest filters it, but a subscribed perpetual would be a
  // silently wider universe, so the filter is restated here.
  InstrumentRow row = option_row();
  row.instrument_type = "perpetual";
  row.symbol = "BTC-PERPETUAL";
  EXPECT_FALSE(to_instrument_key(row).has_value());
}

TEST(ToInstrumentKey, RefusesADatedInstrumentWithNoExpiry) {
  // The subscribe order is expiry-ascending, so an option with no expiry
  // cannot be placed in it.
  InstrumentRow row = option_row();
  row.expiration.reset();
  EXPECT_FALSE(to_instrument_key(row).has_value());
}

TEST(ToInstrumentKey, RefusesARowWhoseTickSizeWillNotParse) {
  // Better a missing instrument than one whose every price is off by a power
  // of ten.
  InstrumentRow row = option_row();
  row.tick_size = "";
  EXPECT_FALSE(to_instrument_key(row).has_value());
}

TEST(ToInstrumentKey, RefusesARowWithNoSecurityMasterId) {
  InstrumentRow row = option_row();
  row.instrument_id = 0;
  EXPECT_FALSE(to_instrument_key(row).has_value());
}

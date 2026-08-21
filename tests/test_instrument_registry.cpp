#include <gtest/gtest.h>

#include <cmath>

#include "marketdata/InstrumentRegistry.hpp"
#include "marketdata/InstrumentRepository.hpp"

using namespace shawlynot::quant::marketdata;

namespace {

/// A fixture result set -- the row mapping is a pure function, so none of this
/// needs a live database.
InstrumentRow option_row(InstrumentId id, std::string symbol,
                         std::string option_type, double strike) {
  InstrumentRow row;
  row.instrument_id = id;
  row.symbol = std::move(symbol);
  row.instrument_type = "option";
  row.option_type = std::move(option_type);
  row.strike = strike;
  row.expiration = Nanos{std::chrono::seconds{1782633600}};
  return row;
}

InstrumentRow future_row(InstrumentId id, std::string symbol) {
  InstrumentRow row;
  row.instrument_id = id;
  row.symbol = std::move(symbol);
  row.instrument_type = "future";
  row.expiration = Nanos{std::chrono::seconds{1782633600}};
  return row;
}

InstrumentRow currency_row(InstrumentId id, std::string symbol) {
  InstrumentRow row;
  row.instrument_id = id;
  row.symbol = std::move(symbol);
  row.instrument_type = "currency";
  return row;
}

}  // namespace

TEST(InstrumentRepository, MapsOptionRow) {
  const auto key = to_instrument_key(
      option_row(101, "BTC-26JUN26-100000-C", "call", 100000.0));

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Option);
  EXPECT_EQ(key->id, 101);
  EXPECT_EQ(key->right, OptionRight::Call);
  EXPECT_DOUBLE_EQ(key->strike, 100000.0);
}

TEST(InstrumentRepository, MapsPutRow) {
  const auto key =
      to_instrument_key(option_row(102, "BTC-26JUN26-90000-P", "put", 90000.0));

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->right, OptionRight::Put);
}

TEST(InstrumentRepository, MapsFutureRow) {
  const auto key = to_instrument_key(future_row(201, "BTC-26JUN26"));

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Future);
  EXPECT_EQ(key->id, 201);
}

TEST(InstrumentRepository, MissingOptionJoinDoesNotGiveAFutureABogusStrike) {
  // The startup SELECT LEFT JOINs `option`, so a future row's option columns
  // are null. A 0 strike there would look like a real zero-strike instrument.
  const auto key = to_instrument_key(future_row(201, "BTC-26JUN26"));

  ASSERT_TRUE(key.has_value());
  EXPECT_TRUE(std::isnan(key->strike));
}

TEST(InstrumentRepository, MapsCurrencyRowToTheIndex) {
  const auto key = to_instrument_key(currency_row(301, "btc_usd"));

  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key->kind, InstrumentKind::Index);
  EXPECT_EQ(key->id, 301);
}

TEST(InstrumentRepository, RejectsRowsWhoseTypeContradictsTheirSymbol) {
  InstrumentRow row = future_row(202, "BTC-26JUN26-100000-C");
  EXPECT_FALSE(to_instrument_key(row).has_value())
      << "an option symbol typed as a future";

  InstrumentRow perpetual = future_row(203, "BTC-PERPETUAL");
  EXPECT_FALSE(to_instrument_key(perpetual).has_value())
      << "a perpetual must never be subscribed";

  InstrumentRow unknown =
      option_row(204, "BTC-26JUN26-100000-C", "call", 100000.0);
  unknown.instrument_type = "swaption";
  EXPECT_FALSE(to_instrument_key(unknown).has_value());

  InstrumentRow bad_right =
      option_row(205, "BTC-26JUN26-100000-C", "straddle", 100000.0);
  EXPECT_FALSE(to_instrument_key(bad_right).has_value());

  InstrumentRow bad_symbol = option_row(206, "not-a-symbol", "call", 1.0);
  EXPECT_FALSE(to_instrument_key(bad_symbol).has_value());
}

TEST(InstrumentRepository, DatabaseValuesWinOverTheSymbol) {
  // The symbol carries the shape; the database carries the identity the rest
  // of the platform joins against.
  InstrumentRow row = option_row(107, "BTC-26JUN26-100000-C", "put", 99999.0);

  const auto key = to_instrument_key(row);

  ASSERT_TRUE(key.has_value());
  EXPECT_DOUBLE_EQ(key->strike, 99999.0);
  EXPECT_EQ(key->right, OptionRight::Put);
}

TEST(InstrumentRegistry, RegistersUnderTheSecurityMasterIdVerbatim) {
  // security_master.instrument_id is a bigserial, so the live set is a sparse
  // window up in the tens of thousands. The registry keeps those ids exactly
  // as they are rather than renumbering: the id on a tick is the id the
  // database, the Python layer and a published message all use.
  InstrumentRegistry registry;
  const auto a =
      registry.add(*to_instrument_key(currency_row(90001, "btc_usd")));
  const auto b =
      registry.add(*to_instrument_key(future_row(90502, "BTC-26JUN26")));
  const auto c = registry.add(*to_instrument_key(
      option_row(97788, "BTC-26JUN26-100000-C", "call", 100000.0)));

  EXPECT_EQ(a, 90001);
  EXPECT_EQ(b, 90502);
  EXPECT_EQ(c, 97788);
  EXPECT_EQ(registry.size(), 3u);
}

TEST(InstrumentRegistry, ResolvesSparseIdsBothWays) {
  InstrumentRegistry registry;
  const auto id =
      registry.add(*to_instrument_key(future_row(90502, "BTC-26JUN26")));
  ASSERT_TRUE(id.has_value());

  EXPECT_EQ(registry.by_symbol("BTC-26JUN26"), id);
  EXPECT_FALSE(registry.by_symbol("BTC-NOTLISTED").has_value());
  ASSERT_NE(registry.find(*id), nullptr);
  EXPECT_EQ(registry.find(*id)->id, 90502);
  EXPECT_EQ(registry.find(1), nullptr) << "a neighbouring id must not resolve";
  EXPECT_EQ(registry.find(9999), nullptr);
}

TEST(InstrumentRegistry, RefusesAnInstrumentWithNoSecurityMasterId) {
  // A symbol parsed on its own has no id behind it. There is no dense id to
  // fall back on any more, so registering it would mean inventing an
  // identity -- which is exactly what one-id-only rules out.
  InstrumentRegistry registry;
  auto orphan = parse_symbol("BTC-26JUN26-100000-C");
  ASSERT_TRUE(orphan.has_value());
  EXPECT_EQ(orphan->id, 0);

  EXPECT_FALSE(registry.add(*orphan).has_value());
  EXPECT_EQ(registry.size(), 0u);
}

TEST(InstrumentRegistry, ReregisteringASymbolIsIdempotent) {
  InstrumentRegistry registry;
  const auto first =
      registry.add(*to_instrument_key(future_row(1, "BTC-26JUN26")));
  const auto second =
      registry.add(*to_instrument_key(future_row(1, "BTC-26JUN26")));

  EXPECT_EQ(first, second);
  EXPECT_EQ(registry.size(), 1u);
}

TEST(ChannelNaming, TickerChannelsCarryTheInterval) {
  const auto option = to_instrument_key(
      option_row(1, "BTC-26JUN26-100000-C", "call", 100000.0));
  const auto future = to_instrument_key(future_row(2, "BTC-26JUN26"));

  EXPECT_EQ(channel_for(*option, "100ms"), "ticker.BTC-26JUN26-100000-C.100ms");
  EXPECT_EQ(channel_for(*future, "raw"), "ticker.BTC-26JUN26.raw");
}

TEST(ChannelNaming, IndexChannelTakesNoIntervalSuffix) {
  // Appending one fails the subscribe -- the index is not a ticker channel.
  const auto index = to_instrument_key(currency_row(3, "btc_usd"));

  EXPECT_EQ(channel_for(*index, "100ms"), "deribit_price_index.btc_usd");
}

TEST(ChannelNaming, PerpetualHasNoChannel) {
  InstrumentKey key;
  key.symbol = "BTC-PERPETUAL";
  key.kind = InstrumentKind::Perpetual;

  EXPECT_TRUE(channel_for(key, "100ms").empty());
}

TEST(InstrumentRegistry, SubscribesSpotAndFuturesBeforeOptions) {
  // Both are tiny, and spot is the reference every option tick carries;
  // subscribing them last would leave the first seconds of option ticks
  // published with no index behind them.
  InstrumentRegistry registry;
  registry.add(*to_instrument_key(
      option_row(1, "BTC-26JUN26-100000-C", "call", 100000.0)));
  registry.add(
      *to_instrument_key(option_row(2, "BTC-26JUN26-90000-P", "put", 90000.0)));
  registry.add(*to_instrument_key(future_row(3, "BTC-26JUN26")));
  registry.add(*to_instrument_key(currency_row(4, "btc_usd")));

  const auto channels = registry.subscription_channels("100ms");

  ASSERT_EQ(channels.size(), 4u);
  EXPECT_EQ(channels[0], "deribit_price_index.btc_usd");
  EXPECT_EQ(channels[1], "ticker.BTC-26JUN26.100ms");
  EXPECT_EQ(channels[2], "ticker.BTC-26JUN26-100000-C.100ms");
  EXPECT_EQ(channels[3], "ticker.BTC-26JUN26-90000-P.100ms");
}

TEST(InstrumentRegistry, RoutesNotificationsByBoundChannel) {
  InstrumentRegistry registry;
  const auto id =
      registry.add(*to_instrument_key(future_row(3, "BTC-26JUN26")));
  ASSERT_TRUE(id.has_value());
  registry.bind_channel("ticker.BTC-26JUN26.100ms", *id);

  EXPECT_EQ(registry.by_channel("ticker.BTC-26JUN26.100ms"), id);
  EXPECT_FALSE(registry.by_channel("ticker.BTC-26JUN26.raw").has_value());
}

TEST(InstrumentRegistry, ListsIdsByKind) {
  InstrumentRegistry registry;
  registry.add(*to_instrument_key(currency_row(1, "btc_usd")));
  registry.add(*to_instrument_key(future_row(2, "BTC-26JUN26")));
  registry.add(*to_instrument_key(
      option_row(3, "BTC-26JUN26-100000-C", "call", 100000.0)));

  EXPECT_EQ(registry.ids_of_kind(InstrumentKind::Index).size(), 1u);
  EXPECT_EQ(registry.ids_of_kind(InstrumentKind::Future).size(), 1u);
  EXPECT_EQ(registry.ids_of_kind(InstrumentKind::Option).size(), 1u);
  EXPECT_TRUE(registry.ids_of_kind(InstrumentKind::Perpetual).empty());
}

#pragma once

// The gateway's instrument identity table, built once at startup.
//
// One id, and it is Postgres's: security_master.instrument_id. It is a
// bigserial, so the live set is a sparse window somewhere up in the tens of
// thousands rather than a dense 0..N-1 range -- which rules out indexing a flat
// array by it, and is why every lookup here is a hash rather than a subscript.
// That is the whole cost, and it is a few nanoseconds on a path that runs a few
// thousand times a second.
//
// What it buys is that the id a tick carries is the id the database uses, the
// Python layer uses, and a published message will carry. No second numbering to
// assign at load, map both ways, and keep in step as the chain rolls.
//
// The corollary: an instrument with no Postgres row has no identity here, and
// `add` rejects it. A listing that appears mid-session is invisible until the
// next reference_ingest run gives it a row.

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "marketdata/model.hpp"

namespace shawlynot::quant::marketdata {

/// Channel name prefixes, kept here so subscription and dispatch cannot drift.
inline constexpr std::string_view kTickerChannelPrefix = "ticker.";
inline constexpr std::string_view kIndexChannelPrefix = "deribit_price_index.";

/// The channel an instrument is streamed on.
///
/// Options and futures share `ticker.{name}.{interval}`. The index is the odd
/// one out: `deribit_price_index.{name}` takes *no* interval suffix --
/// appending one fails the subscribe.
std::string channel_for(const InstrumentKey& key, std::string_view interval);

class InstrumentRegistry {
 public:
  /// Register an instrument under its security_master id.
  ///
  /// Returns nullopt for a key with no id (`id == 0`) -- a symbol parsed
  /// without a database row behind it has no identity to register under, and
  /// silently inventing one is what the single-id rule exists to prevent.
  /// Re-registering a symbol returns the existing id rather than duplicating.
  std::optional<InstrumentId> add(InstrumentKey key);

  const InstrumentKey* find(InstrumentId id) const;
  std::optional<InstrumentId> by_symbol(std::string_view symbol) const;

  /// Resolve an inbound notification's channel to the instrument it carries.
  std::optional<InstrumentId> by_channel(std::string_view channel) const;

  /// Every channel the gateway should subscribe to, in subscription order:
  /// the spot index first, then dated futures, then options.
  ///
  /// Spot and the futures are tiny -- one channel plus under ten -- so they
  /// fit inside the first chunk with room to spare, and spot is the reference
  /// every option tick carries. Subscribing them last would leave the first
  /// seconds of option ticks published with no index behind them, a startup
  /// transient not worth having.
  ///
  /// The perpetual is never included, whatever a stray row might say.
  std::vector<std::string> subscription_channels(
      std::string_view interval) const;

  std::vector<InstrumentId> ids_of_kind(InstrumentKind kind) const;

  std::size_t size() const { return m_keys.size(); }
  const std::vector<InstrumentKey>& keys() const { return m_keys; }

  /// Record the channel an instrument was subscribed on, so inbound
  /// notifications can be routed by name rather than by payload shape.
  void bind_channel(std::string channel, InstrumentId id);

 private:
  /// Insertion order, which `subscription_channels` and `ids_of_kind` rely on
  /// for a deterministic subscribe sequence. `m_slot_by_id` is what turns an
  /// id back into an entry, since the ids are sparse.
  std::vector<InstrumentKey> m_keys;
  std::unordered_map<InstrumentId, std::size_t> m_slot_by_id;
  std::unordered_map<std::string, InstrumentId> m_by_symbol;
  std::unordered_map<std::string, InstrumentId> m_by_channel;
};

}  // namespace shawlynot::quant::marketdata

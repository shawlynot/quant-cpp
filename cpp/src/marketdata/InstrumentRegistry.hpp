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

/// What one call to subscription_channels() decided.
struct Subscription {
  std::vector<std::string> channels;
  std::size_t offered = 0;   ///< live channels the universe could have produced
  std::size_t expired = 0;   ///< instruments skipped as already settled
  std::size_t expiries = 0;  ///< option expiries included in full
  bool truncated = false;
};

class InstrumentRegistry {
 public:
  /// Register an instrument under its security_master id.
  ///
  /// Returns nullopt for a key with no id (`id == 0`) -- a symbol with no
  /// database row behind it has no identity to register under, and silently
  /// inventing one is what the single-id rule exists to prevent.
  /// Re-registering a symbol returns the existing id rather than duplicating.
  std::optional<InstrumentId> add(InstrumentKey key);

  const InstrumentKey* find(InstrumentId id) const;
  std::optional<InstrumentId> by_symbol(std::string_view symbol) const;

  /// Resolve an inbound notification's channel to the instrument it carries.
  /// This is why dispatch never has to re-parse `instrument_name` out of a
  /// payload: the channel the frame arrived on already names the instrument.
  std::optional<InstrumentId> by_channel(std::string_view channel) const;

  /// The channels to subscribe, in subscription order, capped at
  /// `max_channels` -- one `public/subscribe` carries the lot, so the cap is
  /// what keeps the universe inside the request rather than any batching.
  ///
  /// Order is the index, then dated futures by expiry, then options grouped by
  /// expiry with the nearest first. Options are admitted a *whole expiry at a
  /// time*: a fitter wants complete front expiries, and an arbitrary slice
  /// across every expiry would give it a fragment of each and a usable smile
  /// for none. The single exception is a first expiry larger than the entire
  /// cap, which is taken partially rather than dropping every option.
  ///
  /// Anything already expired at `as_of` is skipped. The registry is only as
  /// current as the last reference_ingest run, so it routinely holds expiries
  /// that have since settled -- and because the order is nearest-first, those
  /// dead instruments would otherwise consume the entire cap and the venue
  /// would reject every one of them. `as_of` is a parameter rather than a call
  /// to the clock so this is testable without waiting for an expiry to pass.
  ///
  /// Calling this also binds each channel to its instrument (see by_channel),
  /// so the routing table and the subscription can never disagree.
  Subscription subscription_channels(std::string_view interval,
                                     std::size_t max_channels, Nanos as_of);

  std::vector<InstrumentId> ids_of_kind(InstrumentKind kind) const;

  std::size_t size() const { return m_keys.size(); }
  const std::vector<InstrumentKey>& keys() const { return m_keys; }

 private:
  void bind_channel(std::string channel, InstrumentId id);

  /// Whether an instrument is still subscribable at `as_of`. A settled
  /// instrument is not, and the venue rejects it rather than erroring the whole
  /// request -- so an unfiltered universe silently shrinks to whatever happened
  /// to still be alive. The index never settles.
  static bool is_live(const InstrumentKey& key, Nanos as_of);

  /// Insertion order, which `ids_of_kind` relies on. `m_slot_by_id` is what
  /// turns an id back into an entry, since the ids are sparse.
  std::vector<InstrumentKey> m_keys;
  std::unordered_map<InstrumentId, std::size_t> m_slot_by_id;
  std::unordered_map<std::string, InstrumentId> m_by_symbol;
  std::unordered_map<std::string, InstrumentId> m_by_channel;
};

}  // namespace shawlynot::quant::marketdata

#include "marketdata/InstrumentRegistry.hpp"

#include <algorithm>
#include <map>
#include <utility>

namespace shawlynot::quant::marketdata {

std::string channel_for(const InstrumentKey& key, std::string_view interval) {
  switch (key.kind) {
    case InstrumentKind::Option:
    case InstrumentKind::Future:
      return std::string{kTickerChannelPrefix} + key.symbol + "." +
             std::string{interval};
    case InstrumentKind::Index:
      return std::string{kIndexChannelPrefix} + key.symbol;
    case InstrumentKind::Perpetual:
    case InstrumentKind::Unknown:
      break;
  }
  return {};
}

std::optional<InstrumentId> InstrumentRegistry::add(InstrumentKey key) {
  if (key.id == 0) {
    return std::nullopt;  // no security_master row, no identity
  }
  if (const auto existing = by_symbol(key.symbol)) {
    return *existing;
  }

  const InstrumentId id = key.id;
  m_slot_by_id.emplace(id, m_keys.size());
  m_by_symbol.emplace(key.symbol, id);
  m_keys.push_back(std::move(key));
  return id;
}

const InstrumentKey* InstrumentRegistry::find(InstrumentId id) const {
  const auto it = m_slot_by_id.find(id);
  if (it == m_slot_by_id.end()) {
    return nullptr;
  }
  return &m_keys[it->second];
}

std::optional<InstrumentId> InstrumentRegistry::by_symbol(
    std::string_view symbol) const {
  const auto it = m_by_symbol.find(std::string{symbol});
  if (it == m_by_symbol.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<InstrumentId> InstrumentRegistry::by_channel(
    std::string_view channel) const {
  const auto it = m_by_channel.find(std::string{channel});
  if (it == m_by_channel.end()) {
    return std::nullopt;
  }
  return it->second;
}

void InstrumentRegistry::bind_channel(std::string channel, InstrumentId id) {
  m_by_channel.insert_or_assign(std::move(channel), id);
}

std::vector<InstrumentId> InstrumentRegistry::ids_of_kind(
    InstrumentKind kind) const {
  std::vector<InstrumentId> ids;
  for (const InstrumentKey& key : m_keys) {
    if (key.kind == kind) {
      ids.push_back(key.id);
    }
  }
  return ids;
}

bool InstrumentRegistry::is_live(const InstrumentKey& key, Nanos as_of) {
  return key.kind == InstrumentKind::Index || key.expiry > as_of;
}

Subscription InstrumentRegistry::subscription_channels(
    std::string_view interval, std::size_t max_channels, Nanos as_of) {
  Subscription result;

  // Emit one instrument's channel, binding it for inbound routing.
  const auto emit = [&](const InstrumentKey& key) {
    std::string channel = channel_for(key, interval);
    if (channel.empty()) {
      return;
    }
    bind_channel(channel, key.id);
    result.channels.push_back(std::move(channel));
  };

  // Count what the universe could have offered, so a truncation is reported
  // against the real total rather than against what fitted.
  for (const InstrumentKey& key : m_keys) {
    if (key.kind != InstrumentKind::Option &&
        key.kind != InstrumentKind::Future &&
        key.kind != InstrumentKind::Index) {
      continue;
    }
    if (is_live(key, as_of)) {
      ++result.offered;
    } else {
      ++result.expired;
    }
  }

  // The index first. It is a single channel, and it is the reference every
  // option tick is read against -- subscribing it last would leave the first
  // seconds of option ticks with no spot behind them.
  for (const InstrumentKey& key : m_keys) {
    if (key.kind == InstrumentKind::Index) {
      emit(key);
    }
  }

  // Then the dated futures, nearest first. Under twenty channels, so they are
  // never what a cap has to cut.
  std::vector<const InstrumentKey*> futures;
  for (const InstrumentKey& key : m_keys) {
    if (key.kind == InstrumentKind::Future && is_live(key, as_of)) {
      futures.push_back(&key);
    }
  }
  std::sort(futures.begin(), futures.end(),
            [](const InstrumentKey* a, const InstrumentKey* b) {
              return a->expiry < b->expiry;
            });
  for (const InstrumentKey* key : futures) {
    emit(*key);
  }

  // Then options, grouped by expiry and admitted a whole expiry at a time.
  // std::map keeps the groups in ascending expiry order for free.
  std::map<Nanos, std::vector<const InstrumentKey*>> options_by_expiry;
  for (const InstrumentKey& key : m_keys) {
    if (key.kind == InstrumentKind::Option && is_live(key, as_of)) {
      options_by_expiry[key.expiry].push_back(&key);
    }
  }

  for (const auto& [expiry, strikes] : options_by_expiry) {
    const bool fits = result.channels.size() + strikes.size() <= max_channels;
    // A first expiry wider than the whole cap is taken partially: subscribing
    // zero options because the front month is large would be worse than a
    // partial front month.
    const bool first_and_oversized = result.expiries == 0 && !fits;
    if (!fits && !first_and_oversized) {
      result.truncated = true;
      break;
    }
    for (const InstrumentKey* key : strikes) {
      if (result.channels.size() >= max_channels) {
        result.truncated = true;
        break;
      }
      emit(*key);
    }
    ++result.expiries;
    if (result.truncated) {
      break;
    }
  }

  return result;
}

}  // namespace shawlynot::quant::marketdata

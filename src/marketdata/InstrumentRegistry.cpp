#include "marketdata/InstrumentRegistry.hpp"

#include <algorithm>
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

std::vector<std::string> InstrumentRegistry::subscription_channels(
    std::string_view interval) const {
  std::vector<std::string> channels;
  channels.reserve(m_keys.size());

  for (const InstrumentKind kind :
       {InstrumentKind::Index, InstrumentKind::Future,
        InstrumentKind::Option}) {
    for (const InstrumentKey& key : m_keys) {
      if (key.kind != kind) {
        continue;
      }
      std::string channel = channel_for(key, interval);
      if (!channel.empty()) {
        channels.push_back(std::move(channel));
      }
    }
  }
  return channels;
}

}  // namespace shawlynot::quant::marketdata

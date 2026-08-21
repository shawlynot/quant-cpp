#include "marketdata/model.hpp"

#include <array>
#include <cctype>
#include <charconv>
#include <string>
#include <vector>

namespace shawlynot::quant::marketdata {
namespace {

using namespace std::string_view_literals;

constexpr std::array<std::string_view, 12> kMonths = {
    "JAN"sv, "FEB"sv, "MAR"sv, "APR"sv, "MAY"sv, "JUN"sv,
    "JUL"sv, "AUG"sv, "SEP"sv, "OCT"sv, "NOV"sv, "DEC"sv,
};

std::string to_upper(std::string_view text) {
  std::string upper;
  upper.reserve(text.size());
  for (const char c : text) {
    upper.push_back(
        static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return upper;
}

/// Split on '-' into at most `max_parts` pieces. Returns fewer on a shorter
/// symbol; a symbol with more separators than expected yields max_parts and is
/// rejected by the caller's arity check.
std::vector<std::string_view> split(std::string_view text, char sep) {
  std::vector<std::string_view> parts;
  std::size_t start = 0;
  while (true) {
    const std::size_t at = text.find(sep, start);
    if (at == std::string_view::npos) {
      parts.push_back(text.substr(start));
      return parts;
    }
    parts.push_back(text.substr(start, at - start));
    start = at + 1;
  }
}

template <typename T>
std::optional<T> parse_number(std::string_view text) {
  T value{};
  const auto* const first = text.data();
  const auto* const last = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(first, last, value);
  if (ec != std::errc{} || ptr != last) {
    return std::nullopt;
  }
  return value;
}

std::optional<unsigned> month_from_name(std::string_view name) {
  for (unsigned i = 0; i < kMonths.size(); ++i) {
    if (kMonths[i] == name) {
      return i + 1;
    }
  }
  return std::nullopt;
}

/// "27JUN25" / "4SEP26" -> 08:00 UTC on that date. Deribit writes the day
/// without a leading zero, so the numeric prefix is 1 or 2 characters.
std::optional<Nanos> parse_expiry(std::string_view text) {
  if (text.size() < 6 || text.size() > 7) {
    return std::nullopt;
  }
  const std::size_t day_digits =
      text.size() - 5;  // 3 month letters + 2 year digits
  const auto day = parse_number<unsigned>(text.substr(0, day_digits));
  const auto month = month_from_name(text.substr(day_digits, 3));
  const auto year = parse_number<unsigned>(text.substr(day_digits + 3));
  if (!day || !month || !year) {
    return std::nullopt;
  }

  const std::chrono::year_month_day date{
      std::chrono::year{static_cast<int>(2000 + *year)},
      std::chrono::month{*month},
      std::chrono::day{*day},
  };
  if (!date.ok()) {
    return std::nullopt;
  }
  return Nanos{std::chrono::sys_days{date} + kDeribitExpiryHourUtc};
}

}  // namespace

std::optional<InstrumentKey> parse_symbol(std::string_view symbol) {
  if (symbol.empty()) {
    return std::nullopt;
  }

  // Index names are the odd one out: lowercase and underscore-separated
  // ("btc_usd"), which is also character-for-character the symbol stored in
  // security_master.currencies, so no translation layer is needed.
  if (const std::size_t underscore = symbol.find('_');
      underscore != std::string_view::npos) {
    if (underscore == 0 || underscore + 1 == symbol.size()) {
      return std::nullopt;
    }
    InstrumentKey key;
    key.symbol = std::string{symbol};
    key.base_ccy = to_upper(symbol.substr(0, underscore));
    key.kind = InstrumentKind::Index;
    return key;
  }

  const std::vector<std::string_view> parts = split(symbol, '-');
  if (parts.size() != 2 && parts.size() != 4) {
    return std::nullopt;
  }
  if (parts[0].empty()) {
    return std::nullopt;
  }

  InstrumentKey key;
  key.symbol = std::string{symbol};
  key.base_ccy = std::string{parts[0]};

  if (parts.size() == 2) {
    if (parts[1] == "PERPETUAL"sv) {
      key.kind = InstrumentKind::Perpetual;
      return key;
    }
    const auto expiry = parse_expiry(parts[1]);
    if (!expiry) {
      return std::nullopt;
    }
    key.kind = InstrumentKind::Future;
    key.expiry = *expiry;
    return key;
  }

  const auto expiry = parse_expiry(parts[1]);
  const auto strike = parse_number<double>(parts[2]);
  if (!expiry || !strike || !(*strike > 0.0)) {
    return std::nullopt;
  }
  if (parts[3] == "C"sv) {
    key.right = OptionRight::Call;
  } else if (parts[3] == "P"sv) {
    key.right = OptionRight::Put;
  } else {
    return std::nullopt;
  }

  key.kind = InstrumentKind::Option;
  key.expiry = *expiry;
  key.strike = *strike;
  return key;
}

std::string_view to_string(InstrumentKind kind) {
  switch (kind) {
    case InstrumentKind::Option:
      return "option"sv;
    case InstrumentKind::Future:
      return "future"sv;
    case InstrumentKind::Index:
      return "index"sv;
    case InstrumentKind::Perpetual:
      return "perpetual"sv;
    case InstrumentKind::Unknown:
      break;
  }
  return "unknown"sv;
}

}  // namespace shawlynot::quant::marketdata

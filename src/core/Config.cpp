#include "core/Config.hpp"

#include <array>
#include <charconv>
#include <cstdlib>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace shawlynot::quant::core {
namespace {

using namespace std::string_view_literals;

constexpr std::array kSecretVariables = {
    "CLIENT_SECRET"sv,
    "POSTGRES_PASSWORD"sv,
};

constexpr std::uint16_t kDefaultPostgresPort = 5432;

std::optional<std::string> read_env(const char* name) {
  const char* const value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return std::nullopt;
  }
  return std::string{value};
}

std::string read_env_or(const char* name, std::string fallback) {
  return read_env(name).value_or(std::move(fallback));
}

/// Required variables are collected rather than thrown on individually, so one
/// run reports every one that is absent.
std::string require(const char* name, std::vector<std::string_view>& missing) {
  auto value = read_env(name);
  if (!value) {
    missing.emplace_back(name);
    return {};
  }
  return std::move(*value);
}

}  // namespace

bool is_secret_variable(std::string_view name) {
  for (const std::string_view secret : kSecretVariables) {
    if (name == secret) {
      return true;
    }
  }
  return false;
}

std::string Config::postgres_conninfo() const {
  std::ostringstream out;
  out << "host=" << pg_host << " port=" << pg_port << " dbname=" << pg_database
      << " user=" << pg_user << " password=" << pg_password;
  return out.str();
}

Config Config::from_env() {
  std::vector<std::string_view> missing;

  Config config;
  config.client_id = require("CLIENT_ID", missing);
  config.client_secret = require("CLIENT_SECRET", missing);

  // POSTGRES_HOST carries an optional ":port" suffix in this project --
  // Settings.from_env on the Python side partitions on ':' the same way.
  // Two components disagreeing about the format of a shared variable is a
  // needless failure mode.
  const std::string host_spec = require("POSTGRES_HOST", missing);
  if (const std::size_t colon = host_spec.find(':');
      colon != std::string::npos) {
    config.pg_host = host_spec.substr(0, colon);
    const std::string port = host_spec.substr(colon + 1);
    std::uint16_t parsed = 0;
    const auto [ptr, ec] =
        std::from_chars(port.data(), port.data() + port.size(), parsed);
    if (ec != std::errc{} || ptr != port.data() + port.size() || parsed == 0) {
      throw std::runtime_error("POSTGRES_HOST has a malformed port suffix: " +
                               port);
    }
    config.pg_port = parsed;
  } else {
    config.pg_host = host_spec;
    config.pg_port = kDefaultPostgresPort;
  }

  config.pg_database = require("POSTGRES_DATABASE", missing);
  config.pg_user = require("POSTGRES_USER", missing);
  config.pg_password = require("POSTGRES_PASSWORD", missing);

  if (!missing.empty()) {
    std::ostringstream out;
    out << "missing required environment variable(s): ";
    for (std::size_t i = 0; i < missing.size(); ++i) {
      out << (i == 0 ? "" : ", ") << missing[i];
    }
    throw std::runtime_error(out.str());
  }

  config.ws_host = read_env_or("DERIBIT_WS_HOST", std::move(config.ws_host));
  config.ws_port = read_env_or("DERIBIT_WS_PORT", std::move(config.ws_port));
  config.ticker_interval =
      read_env_or("DERIBIT_TICKER_INTERVAL", std::move(config.ticker_interval));
  config.log_level = read_env_or("QUANT_LOG_LEVEL", std::move(config.log_level));

  return config;
}

}  // namespace shawlynot::quant::core

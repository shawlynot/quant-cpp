#pragma once

// Gateway configuration, read from the process environment and nothing else.
//
// This deliberately parses no file. How the variables get into the environment
// -- systemd EnvironmentFile=, container env, direnv, or a shell that sourced
// .env -- is a deployment concern, and baking a .env parser into the binary
// would make the gateway's behaviour depend on its working directory.

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace shawlynot::quant::core {

/// Names that must never be logged, at any level, in any error path.
/// Token *metadata* (scope, expires_in, a short fingerprint) is fine; the
/// secret itself never is.
bool is_secret_variable(std::string_view name);

struct Config {
  // ── Deribit ──
  std::string ws_host = "www.deribit.com";  ///< test.deribit.com for testnet
  std::string ws_port = "443";
  std::string ws_target = "/ws/api/v2";
  std::string client_id;
  std::string client_secret;              ///< never logged
  std::string ticker_interval = "100ms";  ///< raw | 100ms | agg2

  // ── Postgres (startup only -- never on the hot path) ──
  std::string pg_host = "localhost";
  std::uint16_t pg_port = 5432;
  std::string pg_database;
  std::string pg_user;
  std::string pg_password;  ///< never logged

  std::string log_level = "info";

  /// libpq connection string. Built on demand so the password is not held in
  /// a second long-lived copy.
  std::string postgres_conninfo() const;

  /// Read and validate the environment.
  ///
  /// Throws std::runtime_error naming *every* missing variable in one
  /// message. A gateway that boots unauthenticated and silently loses `raw`
  /// access is worse than one that refuses to start.
  static Config from_env();
};

}  // namespace shawlynot::quant::core

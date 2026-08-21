#pragma once

// spdlog facade -- one include site for the whole gateway.
//
// The sink is asynchronous on purpose: formatting happens off the io thread,
// which matters because that thread has a hard heartbeat deadline. A blocking
// stdout write during a burst is exactly the kind of stall that gets the
// connection dropped.

#include <spdlog/spdlog.h>

#include <string>
#include <string_view>

namespace shawlynot::quant::core {

/// Install the async logger. Idempotent; safe to call once at startup.
/// `level` is an spdlog level name ("trace".."critical"); an unrecognised name
/// falls back to "info" rather than failing the boot.
void init_logging(const std::string& level);

/// Flush and shut the async sink down cleanly. Call before exit, or queued
/// messages are lost.
void shutdown_logging();

/// A short, non-reversible fingerprint for correlating a token across logs
/// without ever writing the token itself.
std::string fingerprint(std::string_view secret);

}  // namespace shawlynot::quant::core

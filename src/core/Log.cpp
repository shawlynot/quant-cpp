#include "core/Log.hpp"

#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <cstdio>
#include <functional>
#include <iomanip>
#include <sstream>

namespace shawlynot::quant::core {
namespace {

constexpr std::size_t kQueueSize = 8192;
constexpr std::size_t kWorkerThreads = 1;

}  // namespace

void init_logging(const std::string& level) {
  static bool initialised = false;
  if (initialised) {
    return;
  }
  initialised = true;

  spdlog::init_thread_pool(kQueueSize, kWorkerThreads);
  auto logger =
      spdlog::create_async<spdlog::sinks::stdout_color_sink_mt>("quant");
  logger->set_pattern("%Y-%m-%dT%H:%M:%S.%f %^%l%$ [%n] %v");

  const spdlog::level::level_enum parsed = spdlog::level::from_str(level);
  // from_str yields `off` for anything it does not recognise, which would
  // silence the gateway entirely on a typo -- not the failure mode we want.
  logger->set_level(parsed == spdlog::level::off && level != "off"
                        ? spdlog::level::info
                        : parsed);
  spdlog::set_default_logger(logger);
  spdlog::flush_on(spdlog::level::warn);
}

void shutdown_logging() { spdlog::shutdown(); }

std::string fingerprint(std::string_view secret) {
  const std::size_t hashed = std::hash<std::string_view>{}(secret);
  std::ostringstream out;
  out << std::hex << std::setw(6) << std::setfill('0') << (hashed & 0xFFFFFFu);
  return out.str();
}

}  // namespace shawlynot::quant::core

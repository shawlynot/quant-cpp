// Quant market data gateway.
//
// One process, one Deribit WebSocket connection, one io_context on one thread.
// Socket reads, the subscribe write and every decode run on that executor, so
// there are no strands, no locks and no synchronization around session state.
// The sink is the console and runs inline: printing is cheap, and the queue
// that would keep a slow downstream off the socket is not needed until there
// is one.

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <cstdio>
#include <exception>
#include <memory>
#include <vector>

#include "core/Config.hpp"
#include "core/Log.hpp"
#include "marketdata/ConsoleSink.hpp"
#include "marketdata/Deribit.hpp"
#include "marketdata/InstrumentRegistry.hpp"
#include "marketdata/InstrumentRepository.hpp"
#include "marketdata/Web.hpp"

namespace {

namespace md = shawlynot::quant::marketdata;

/// Build the registry from the startup SELECT, skipping rows that cannot be
/// made sense of rather than failing the boot over one bad listing.
std::shared_ptr<md::InstrumentRegistry> build_registry(
    const std::vector<md::InstrumentRow>& rows) {
  auto registry = std::make_shared<md::InstrumentRegistry>();
  std::size_t skipped = 0;

  for (const md::InstrumentRow& row : rows) {
    auto key = md::to_instrument_key(row);
    if (!key) {
      ++skipped;
      spdlog::warn(
          "skipping unmappable instrument row: id={} symbol='{}' type='{}' "
          "tick_size='{}'",
          row.instrument_id, row.symbol, row.instrument_type, row.tick_size);
      continue;
    }
    if (!registry->add(std::move(*key))) {
      // add() only refuses a key with no security_master id, and
      // to_instrument_key always sets one -- so this is unreachable rather
      // than routine. Count it anyway: silence here would mean an instrument
      // quietly missing from the subscription.
      ++skipped;
      spdlog::warn("instrument row has no id, cannot register: symbol='{}'",
                   row.symbol);
    }
  }

  spdlog::info(
      "registry: {} instrument(s) ({} option, {} future, {} index), {} skipped",
      registry->size(),
      registry->ids_of_kind(md::InstrumentKind::Option).size(),
      registry->ids_of_kind(md::InstrumentKind::Future).size(),
      registry->ids_of_kind(md::InstrumentKind::Index).size(), skipped);
  return registry;
}

}  // namespace

int main() {
  using namespace shawlynot::quant;

  core::Config config;
  try {
    config = core::Config::from_env();
  } catch (const std::exception& error) {
    // Before logging is up, so this goes to stderr directly. A gateway that
    // boots unauthenticated and silently loses `raw` access is worse than one
    // that refuses to start.
    std::fprintf(stderr, "configuration error: %s\n", error.what());
    return 1;
  }

  core::init_logging(config.log_level);
  spdlog::info("quant market data gateway starting (host={} interval={})",
               config.ws_host, config.ticker_interval);

  std::vector<md::InstrumentRow> rows;
  try {
    rows = md::InstrumentRepository::load(config.postgres_conninfo());
  } catch (const std::exception& error) {
    // Postgres is a startup dependency only -- but without an instrument
    // universe there is nothing to subscribe to.
    spdlog::critical("failed to load instruments from security_master: {}",
                     error.what());
    core::shutdown_logging();
    return 1;
  }

  auto registry = build_registry(rows);
  if (registry->size() == 0) {
    spdlog::critical("no instruments loaded; has reference_ingest been run?");
    core::shutdown_logging();
    return 1;
  }

  md::ConsoleSink sink{registry};

  boost::asio::io_context io;
  auto tls = md::make_tls_context();

  md::DeribitIngest::Config ingest_config;
  ingest_config.host = config.ws_host;
  ingest_config.port = config.ws_port;
  ingest_config.ws_target = config.ws_target;
  ingest_config.auth_target = config.auth_target;
  ingest_config.client_id = config.client_id;
  ingest_config.client_secret = config.client_secret;
  ingest_config.interval = config.ticker_interval;
  ingest_config.max_channels = config.max_channels;

  md::DeribitIngest ingest{io.get_executor(), tls, std::move(ingest_config),
                           registry, sink};

  // run() does not throw, so the ingest ending is the process ending: stop the
  // context from the same executor rather than leaving io.run() spinning on an
  // empty queue.
  boost::asio::cancellation_signal cancel;
  boost::asio::co_spawn(
      io, ingest.run(),
      boost::asio::bind_cancellation_slot(
          cancel.slot(), [&io](std::exception_ptr) { io.stop(); }));

  // Cancel rather than stop the context. Stopping it would abandon the ingest
  // coroutine mid-await, so its unwind never runs -- no totals, and the sink
  // never learns the feed went away. Cancelling completes the pending read
  // with operation_aborted and lets run() exit through its own handler.
  boost::asio::signal_set signals{io, SIGINT, SIGTERM};
  signals.async_wait(
      [&cancel](const boost::system::error_code& ec, int number) {
        if (ec) {
          return;
        }
        spdlog::info("signal {} received, shutting down", number);
        cancel.emit(boost::asio::cancellation_type::all);
      });

  io.run();

  const bool streamed = ingest.ticks() > 0;
  spdlog::info("gateway stopped after {} tick(s)", ingest.ticks());
  core::shutdown_logging();
  return streamed ? 0 : 1;
}

// Quant market data gateway.
//
// One process, one Deribit WebSocket connection, one io_context on one thread.
// Everything -- socket reads and writes, heartbeat replies, the token-refresh
// timer, the subscribe pacer, the reconnect backoff -- runs on that executor.
// That is what removes the need for strands, locks, and any synchronization
// around session state. The single exception is the sink handoff, which crosses
// to a consumer thread through a bounded queue precisely so a slow downstream
// can never stall the socket.

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl/context.hpp>
#include <exception>
#include <memory>

#include "core/Config.hpp"
#include "core/Log.hpp"
#include "marketdata/ConsoleSink.hpp"
#include "marketdata/InstrumentRegistry.hpp"
#include "marketdata/InstrumentRepository.hpp"
#include "marketdata/QueueSink.hpp"
#include "marketdata/deribit.hpp"

namespace {

constexpr std::size_t kQueueCapacity = 65536;

/// Build the registry from the startup SELECT, skipping rows that cannot be
/// made sense of rather than failing the boot over one bad listing.
std::shared_ptr<shawlynot::quant::marketdata::InstrumentRegistry> build_registry(
    const std::vector<shawlynot::quant::marketdata::InstrumentRow>& rows) {
  auto registry = std::make_shared<shawlynot::quant::marketdata::InstrumentRegistry>();
  std::size_t skipped = 0;

  for (const shawlynot::quant::marketdata::InstrumentRow& row : rows) {
    auto key = shawlynot::quant::marketdata::to_instrument_key(row);
    if (!key) {
      ++skipped;
      spdlog::warn(
          "skipping unmappable instrument row: id={} symbol='{}' type='{}'",
          row.instrument_id, row.symbol, row.instrument_type);
      continue;
    }
    if (!registry->add(std::move(*key))) {
      // add() only refuses a key with no security_master id, and
      // to_instrument_key always sets one -- so this is unreachable
      // rather than routine. Count it anyway: silence here would mean an
      // instrument quietly missing from the subscription.
      ++skipped;
      spdlog::warn("instrument row has no id, cannot register: symbol='{}'",
                   row.symbol);
    }
  }

  spdlog::info(
      "registry: {} instrument(s) ({} option, {} future, {} index), {} skipped",
      registry->size(),
      registry->ids_of_kind(shawlynot::quant::marketdata::InstrumentKind::Option).size(),
      registry->ids_of_kind(shawlynot::quant::marketdata::InstrumentKind::Future).size(),
      registry->ids_of_kind(shawlynot::quant::marketdata::InstrumentKind::Index).size(),
      skipped);
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
    // boots unauthenticated and silently loses `raw` access is worse than
    // one that refuses to start.
    std::fprintf(stderr, "configuration error: %s\n", error.what());
    return 1;
  }

  core::init_logging(config.log_level);
  spdlog::info("quant market data gateway starting (host={} interval={})",
               config.ws_host, config.ticker_interval);

  std::vector<marketdata::InstrumentRow> rows;
  try {
    rows = marketdata::InstrumentRepository::load(config.postgres_conninfo());
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

  marketdata::ConsoleSink console{registry};
  marketdata::QueueSink sink{kQueueCapacity};
  sink.start(
      [&console](const marketdata::Tick& tick) { console.on_tick(tick); });

  boost::asio::io_context io;
  boost::asio::ssl::context ssl{boost::asio::ssl::context::tlsv12_client};
  ssl.set_default_verify_paths();
  ssl.set_verify_mode(boost::asio::ssl::verify_peer);

  marketdata::DeribitSession::Config session_config;
  session_config.host = config.ws_host;
  session_config.port = config.ws_port;
  session_config.target = config.ws_target;
  session_config.client_id = config.client_id;
  session_config.client_secret = config.client_secret;
  session_config.interval = config.ticker_interval;

  auto session = std::make_shared<marketdata::DeribitSession>(
      io.get_executor(), session_config,
      [&io, &ssl] {
        return std::static_pointer_cast<marketdata::ITransport>(
            std::make_shared<marketdata::WebSocketTransport>(io.get_executor(),
                                                             ssl));
      },
      registry, &sink);

  boost::asio::signal_set signals{io, SIGINT, SIGTERM};
  signals.async_wait(
      [&session, &io](const boost::system::error_code&, int signal) {
        spdlog::info("signal {} received, shutting down", signal);
        session->stop();
        io.stop();
      });

  session->start();
  io.run();

  sink.stop();
  spdlog::info("gateway stopped ({} reconnect(s), {} dropped tick(s))",
               session->reconnect_count(), sink.dropped());
  core::shutdown_logging();
  return 0;
}

#pragma once

// Fixture loading for the codec tests.
//
// The JSON files beside this header are *captured* Deribit frames, not
// hand-written ones -- a hand-written fixture encodes assumptions about the
// payload rather than the payload itself, which is exactly what these tests
// exist to check.

#include <boost/json.hpp>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace shawlynot::quant::test {

inline boost::json::value load_fixture(const std::string& name) {
  const std::string path = std::string{QUANT_TEST_FIXTURE_DIR} + "/" + name;
  std::ifstream file{path};
  if (!file) {
    throw std::runtime_error("cannot open fixture: " + path);
  }
  std::ostringstream contents;
  contents << file.rdbuf();
  return boost::json::parse(contents.str());
}

/// The `params.data` object out of a captured notification frame.
inline boost::json::object load_notification_data(const std::string& name) {
  return load_fixture(name).at("params").at("data").as_object();
}

inline std::string load_notification_channel(const std::string& name) {
  return std::string{load_fixture(name).at("params").at("channel").as_string()};
}

}  // namespace shawlynot::quant::test

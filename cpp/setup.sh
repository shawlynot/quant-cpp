#!/usr/bin/env bash
# One-time project setup for a fresh clone of quant-cpp.
#
# - Configures the CMake build directory (generates build/compile_commands.json)
# - Symlinks compile_commands.json into the repo root so clangd/IDEs find it
#   without needing a .clangd CompilationDatabase override
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"

build_dir="build"

echo "Configuring CMake build in ${build_dir}/ ..."
cmake -S . -B "$build_dir" -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

echo "Linking compile_commands.json into repo root ..."
ln -sf "${build_dir}/compile_commands.json" compile_commands.json

echo "Setup complete."

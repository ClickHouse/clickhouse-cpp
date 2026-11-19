#!/usr/bin/env bash
# Builds the shim and its unit tests with CMake, and prints the path of the
# shim program.
#
# Usage: scripts/build.sh
#
# The build directory is CLICKHOUSE_TEST_RUNNER_BUILD_DIR (default: .build in
# this directory). The build uses the 3.x API of the library
# (-DCH_USE_3X_API=ON, as the root README recommends), so that Bool is a
# ColumnBool. CMAKE_BUILD_TYPE (default: Release), CMAKE_BUILD_PARALLEL_LEVEL
# (default: 4), CC and CXX apply as usual.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="$(cd "${RUNNER_DIR}/../.." && pwd)"
BUILD_DIR="${CLICKHOUSE_TEST_RUNNER_BUILD_DIR:-${RUNNER_DIR}/.build}"

generator=()
if command -v ninja > /dev/null; then
  generator=(-G Ninja)
fi
if [[ ! -f "${BUILD_DIR}/CMakeCache.txt" ]]; then
  cmake "${generator[@]}" -S "${REPO_DIR}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}" \
    -DCH_USE_3X_API=ON \
    -DBUILD_CLICKHOUSE_TEST_RUNNER=ON \
    -DDEBUG_DEPENDENCIES=OFF >&2
fi
cmake --build "${BUILD_DIR}" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-4}" \
  --target clickhouse-test-runner clickhouse-test-runner-ut >&2
echo "${BUILD_DIR}/tests/clickhouse-test-runner/clickhouse"

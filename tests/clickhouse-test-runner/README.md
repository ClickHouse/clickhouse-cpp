# clickhouse-test-runner

This harness runs the ClickHouse core SQL tests
([`tests/queries/0_stateless`](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless))
through clickhouse-cpp.

The upstream test runner
([`tests/clickhouse-test`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/clickhouse-test))
runs each `.sql` test through `clickhouse-client` and compares the output with
the `.reference` file of the test. This harness is a small C++ program that
imitates `clickhouse-client` well enough for the runner to drive it without
changes: the same flags, the same stdin and `--query` behavior, and the
`extract-from-config` shortcut.

The program sends every query through `clickhouse::Client` over the native
protocol. clickhouse-cpp gives each result block as typed columns. The
program reads each value with the typed column class of the column type
(`ColumnInt32::At`, `ColumnNullable::IsNull`, `ColumnArray`, `ColumnTuple`,
`ColumnMap::GetAsColumn`, `ColumnLowCardinalityT<ColumnString>::At`,
`ColumnDecimal::StringAt`, ...), and writes the values as `TabSeparated`
text itself. Thus a test passes only when clickhouse-cpp decodes every value
correctly.

It is a port of the Go harness
([ClickHouse/clickhouse-go#2141](https://github.com/ClickHouse/clickhouse-go/pull/2141)),
and of the [JavaScript](https://github.com/ClickHouse/clickhouse-js/tree/main/tests/clickhouse-test-runner)
and [Java](https://github.com/ClickHouse/clickhouse-java/tree/main/tests/clickhouse-client)
harnesses.

On ClickHouse 26.10.4.207, the allowlist runs **10,004 tests**. **981 tests**
are known failures in **23 groups**: 19 groups of bugs and gaps in
clickhouse-cpp (683 tests) and 4 groups of gaps in the shim (298 tests). See
[Known failures](#known-failures).

## Run the tests locally

You need CMake, a C++17 compiler, Python 3 with `jinja2`, the tz database
with the old zone names (`tzdata` and, on Debian and Ubuntu, `tzdata-legacy`),
`git`, `curl` and Docker.

1. Start the pinned ClickHouse server. The version is in
   [`clickhouse-version`](clickhouse-version):

   ```bash
   docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
     --hostname "$(hostname)" --ulimit nofile=262144:262144 \
     clickhouse/clickhouse-server:$(tests/clickhouse-test-runner/scripts/pinned-version.sh)
   ```

   With `CLICKHOUSE_SKIP_USER_SETUP=1`, the server keeps the `default` user
   of the image's `users.xml`: it accepts network connections and has access
   management and `named_collection_control`. Tests create users, roles and
   named collections, so with another user configuration some allowlisted
   tests fail. `--hostname` gives the server the host name of your machine:
   `tests/clickhouse-test` replaces its own host name with `localhost` in the
   output, and some `.reference` files have `localhost` for `hostName()`.
   Give the server at least 8 GiB of memory for a run of all `.sql` tests.

2. Get the upstream tests of the same version:

   ```bash
   tests/clickhouse-test-runner/scripts/fetch-upstream.sh
   ```

   The script checks out the release tag of the pinned version (for example
   `v26.10.4.207-stable`) into `.upstream/ClickHouse`, sparse and shallow.
   The `.reference` files of that tag were made by the same server build. The
   script also generates [`error_codes_generated.inc`](error_codes_generated.inc)
   from the same checkout (with `sed`; no C++ toolchain is necessary). To try
   another version, give it as the argument (`fetch-upstream.sh 26.11.1.10`),
   and start the server of that version.

   The run script stops when the server and the tests are of different
   versions, because then the `.reference` files do not match. Set
   `ALLOW_VERSION_MISMATCH=1` to run anyway.

3. Run the allowlisted tests:

   ```bash
   tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 4
   ```

   CI uses `-j 4`. At `-j 6`, a full run takes about 11 minutes.

The script builds the program with [`scripts/build.sh`](scripts/build.sh)
(CMake, in `.build`, with `-DBUILD_CLICKHOUSE_TEST_RUNNER=ON -DCH_USE_3X_API=ON`),
links it as `.bin/clickhouse` and `.bin/clickhouse-client`, puts `.bin` first
in `PATH` for the runner only, and starts `tests/clickhouse-test`. It gives
the runner `--no-stateful --no-random-settings --no-random-merge-tree-settings
--dont_retry_failures`; other arguments go to the runner after these. `UPSTREAM_TEST_LIST` selects
other tests, also tests that are not in the allowlist. For example, to run
one test:

```bash
UPSTREAM_TEST_LIST=<(echo 00001_select_1) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

The runner prints the diff of a failed test in its output. It keeps the
output of the test next to the test, in
`.upstream/ClickHouse/tests/queries/0_stateless/<test>[.<pid>].stdout` and
`.stderr` (`.<pid>` for a test of the parallel phase).

The normal build of the library does not build the harness: the CMake option
`BUILD_CLICKHOUSE_TEST_RUNNER` is `OFF` by default. The harness needs
`CH_MAP_BOOL_TO_UINT8=OFF` (`-DCH_USE_3X_API=ON` sets it), because the server
writes `Bool` values as `true` and `false`.

The unit tests of the harness (`clickhouse-test-runner-ut`, gtest, not part
of `clickhouse-cpp-ut`) need no server. The live tests compare the output of
the program with the server's own `TabSeparated` output, and need the server
of step 1:

```bash
tests/clickhouse-test-runner/scripts/build.sh
CLICKHOUSE_TEST_RUNNER_LIVE=1 tests/clickhouse-test-runner/.build/tests/clickhouse-test-runner/clickhouse-test-runner-ut
```

### Change the shim

- Format the sources with the repository's [`.clang-format`](../../.clang-format):
  `clang-format --style=file -i tests/clickhouse-test-runner/*.cpp tests/clickhouse-test-runner/*.h tests/clickhouse-test-runner/ut/*.cpp`.
  No CI job checks the format.
- The root `CMakeLists.txt` compiles with `-Wall -Wextra -Werror` (GCC and
  Clang). Build the shim with both compilers (`CC` and `CXX`), each in its
  own build directory (`CLICKHOUSE_TEST_RUNNER_BUILD_DIR`).
- Run the unit tests and the live tests. Then run the allowlist.
- Update this file, [`AGENTS.md`](AGENTS.md) and the workflow in the same
  change.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `UPSTREAM_CLICKHOUSE_DIR` | `.upstream/ClickHouse` | The checkout of `ClickHouse/ClickHouse`. |
| `UPSTREAM_REPO_URL` | the GitHub repository | The repository that `fetch-upstream.sh` fetches from, for example a mirror. |
| `UPSTREAM_TEST_LIST` | `allowlist.txt` | The file with the tests to run. |
| `SHARD_INDEX`, `SHARD_TOTAL` | `1`, `1` | Run only shard `SHARD_INDEX` of `SHARD_TOTAL` round-robin shards of the list. |
| `KNOWN_FAILURES` | (none) | Run the known failures of these groups (comma-separated IDs, or `all`) instead of the allowlist. |
| `RETRY_FAILED` | `0` | Run the failed tests again, at most this number of times. A test that passes on a retry is a warning. CI does not use it. |
| `ALLOW_VERSION_MISMATCH` | `0` | `1` runs the tests also when the server and the tests are of different versions. |
| `CLICKHOUSE_TEST_RUNNER_BIN` | (none) | The absolute path of a program that is already built; the run script then does not build. |
| `CLICKHOUSE_TEST_RUNNER_BUILD_DIR` | `.build` in this directory | The CMake build directory of `scripts/build.sh`, as an absolute path. |
| `CMAKE_BUILD_TYPE`, `CMAKE_BUILD_PARALLEL_LEVEL` | `Release`, `4` | The build type (used only when the build directory is new) and the number of parallel jobs of `scripts/build.sh`. |
| `CLICKHOUSE_CLIENT_CLI_LOG` | (none) | The absolute path of a file. Each invocation of the program adds its arguments (the `--password` value hidden), statements and errors to it. |
| `CLICKHOUSE_TEST_RUNNER_RECEIVE_TIMEOUT` | `60` | Seconds that the program waits for a packet of the server (`ClientOptions::SetConnectionRecvTimeout`); `0` waits for ever. |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` | `localhost`, `9000`, `8123` | The server. The runner gives `--host` and `--port=$CLICKHOUSE_PORT_TCP`; the run script and the live tests use the HTTP port to read the server version and the server's own output. |
| `CLICKHOUSE_CONTAINER` | `clickhouse` | The server container in which `run-with-real-client.sh` runs the real `clickhouse-client`. |
| `CLICKHOUSE_TEST_RUNNER_LIVE` | (none) | `1` runs the live tests of `clickhouse-test-runner-ut`; without it, they are skipped. |
| `TZDIR` | `/usr/share/zoneinfo` | The tz database that the program uses to write `DateTime` values. |

## The allowlist

[`allowlist.txt`](allowlist.txt) lists the tests that pass reliably, one test
name (the file name without `.sql`) per line. `#` starts a comment. The script
gives the runner anchored regular expressions, so each name selects exactly
one test. (The runner itself matches each name as a substring, so
`00396_uuid` would also select `00396_uuid_v7`.)

clickhouse-cpp has only the native protocol, so there is one list. The file
has two kinds of lines:

| Line | Meaning |
|---|---|
| `00001_select_1` | The test runs. |
| `# 00015_totals_having_constants  # [totals-packet-unimplemented]` | A **known failure**: the test does not run, because of a bug in clickhouse-cpp or a gap in the shim. The ID in brackets names the group. The header of the group, higher in the file, describes the bug and gives a small reproduction. |

To run the known failures of one or more groups, for example to check a fix:

```bash
KNOWN_FAILURES=totals-packet-unimplemented tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
KNOWN_FAILURES=all tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

When a fix makes the tests of a group pass, remove the `# ` at the start and
the `# [<id>]` at the end of their lines. When all tests of a group pass,
remove the group header.

### Grow the allowlist

1. Run the candidate tests through the harness. To try all `.sql` tests:

   ```bash
   ls tests/clickhouse-test-runner/.upstream/ClickHouse/tests/queries/0_stateless \
     | sed -nE 's/\.sql(\.j2)?$//p' | sort -u > /tmp/candidates.txt
   UPSTREAM_TEST_LIST=/tmp/candidates.txt \
     tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 6 --no-long --timeout 120 \
       --max-failures-chain 1000000 | tee /tmp/run.log
   ```

   On 26.10.4.207, this sweep of 12,961 tests takes about 18 minutes at
   `-j 6`. `--max-failures-chain` turns off a limit of the runner: by default
   it stops after 20 failures in a row, which a list of failing tests reaches
   at once. Do not run two sweeps at the same time on one server: the memory
   pressure causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.

2. Keep only the tests that report `[ OK ]`:

   ```bash
   sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' /tmp/run.log | sort -u
   ```

3. Run the new tests again (two or more times, and with `-j` higher than CI
   uses) and remove the tests that do not pass every time.

4. Make sure that the tests also pass on a fresh server, because CI starts
   from one. For example, a test that reads `system.query_log` can depend on
   the queries of other tests.

**Rule:** add only tests that pass reliably. Before you remove a test or
decide not to add it, find out whether it fails because of a bug in
clickhouse-cpp. The harness exists to find those bugs. See
[`AGENTS.md`](AGENTS.md) for the steps.

To compare with the real `clickhouse-client`, run the same tests through it,
in the server container:

```bash
UPSTREAM_TEST_LIST=<(echo 00015_totals_having_constants) \
  tests/clickhouse-test-runner/scripts/run-with-real-client.sh
```

A test that passes there and fails through clickhouse-cpp shows a bug in
clickhouse-cpp or in this program.

## How the program imitates clickhouse-client

The program sends nothing that `clickhouse-client` does not send, because
some tests read their own queries from `system.query_log`.

- **Statements.** The program splits the input into statements as
  `clickhouse-client` does: at a top-level `;`, with the same-line trailing
  comment kept with its statement. It sends each statement with its `;`. The
  inline data of `INSERT ... FORMAT <name>` (other than `Values`) ends at an
  empty line. The `-- Tags:` header is not part of a statement. Every
  statement runs, with or without `--multiquery`.
- **One API for all statements.** Every statement runs with
  `Client::Execute(Query)`. The result blocks come to `Query::OnData`. A
  statement without a result is not special. An `INSERT` keeps its inline
  data in the query text; the server reads it from there.
- **Test hints.** `-- { serverError X }`, `-- { clientError X }` and
  `-- { error X }` (codes or names) make the error the expected result. A hint
  in a leading comment is ignored, as upstream. Through clickhouse-cpp the
  server finds all errors, so a server error can satisfy a `clientError`
  hint. `-- { echo }`, `-- { echoOn }` and `-- { echoOff }` print the
  statements, but not a statement with a syntax error (`clickhouse-client`
  parses before it echoes). A syntax error that a function finds in its
  argument (`formatQuery('...')`) comes after the parse, so that statement
  is echoed: the server message then has `: In scope ` or
  `: While processing `. `-- { retry ... }` is not supported.
- **Session.** The session is the one `clickhouse::Client` of the
  invocation, that is one TCP connection: `SET`, `USE` and temporary tables
  last for the whole invocation. clickhouse-cpp has no connection pool and
  does not reconnect in `Execute`. After a server exception the connection
  stays usable, so the session also lasts after an error that a test hint
  expects. Any other exception stops the program, because then the client
  can be out of sync with the server.
- **Output formats.** The program writes the results itself in the
  `TabSeparated` family (also `TSVRaw` and `...WithNames[AndTypes]`),
  `LineAsString` (the server writes it as `TSVRaw`), `Values` (each row as a
  tuple of values written as inside an array, rows separated by commas, no
  newline) or `Null`. A statement with any other output format fails. The statement goes
  to the server as it is, with its `FORMAT` clause: over the native protocol
  the server ignores the clause and sends `Native` blocks. The names and
  types of `...WithNames[AndTypes]` are `Block::GetColumnName` and
  `Type::GetName` of the client; a Raw format writes them without escapes,
  as its values. After `SET dialect` to another language
  (KQL, PRQL, ...), the program does not look for a `FORMAT` clause.
- **Values.** The rendering follows the column type (`Column::Type()`).
  Strings are escaped at the top level and quoted inside `Array`, `Tuple`
  and `Map`; NULL is `\N` or `NULL`. Floats are the shortest text that reads
  back as the same value, in the server's layout. Decimals have no trailing
  zeros. `DateTime` and `DateTime64` values are written in the time zone of
  the type, or in the server time zone of the handshake
  (`ServerInfo::timezone`) when the type has none (with the tz database of
  the system). The server's fixed-offset zones `Fixed/UTC+HH:MM:SS` and
  `Fixed/UTC-HH:MM:SS` are UTC shifted by the offset. As the server does, a
  date after the year 9999 is written as `9999-12-31 23:59:59`, and a date
  before the year 0 as `0000-01-01 00:00:00` (`Date32`: the date only; a
  `DateTime64` keeps its fractional digits). Maps keep the order of the
  server.
- **Settings.** Of the settings on the command line, only `log_comment`,
  `max_insert_threads`, `max_threads`, `send_logs_level`, `async_insert` and
  `enable_parallel_replicas` go to the server, with each query
  (`Query::SetSetting`), as in `clickhouse-client`. The last two are the
  feature-tag overrides of `tests/clickhouse-test`: a test with the tag
  `no-async-insert` or `no-parallel-replicas` gets `--async_insert 0` or
  `--enable_parallel_replicas 0`, because it needs the feature off
  (`async_insert` is on by default in current servers). The other settings
  of the runner are randomized or client-only, and the program drops them.
  A `SET` of the same setting in the test replaces the command-line value.
  `--param_<name>=<value>` gives a query parameter (`Query::SetParam`).
  Server logs (`send_logs_level`) are not written: `clickhouse-client` writes
  them to the `--client_logs_file` of the runner.
- **Time limit.** An `INSERT` without inline data makes the server ask for
  the data: it sends the header block and waits. `Client::Execute` does not
  send data and waits for ever (see `[insert-without-data-hangs]`). When an
  `INSERT` gets a block in `OnData`, the program stops the statement with an
  error at once. For any other wait, the receive timeout of the connection
  (`CLICKHOUSE_TEST_RUNNER_RECEIVE_TIMEOUT`) makes a hang a clear failure.
- **Errors.** An unexpected error stops the program. It writes the error with
  its code and name to stderr, and the exit code is the error code modulo
  256.
- **Error names.** [`error_codes_generated.inc`](error_codes_generated.inc)
  maps the error names of test hints to codes.
  [`scripts/fetch-upstream.sh`](scripts/fetch-upstream.sh) generates it from
  `src/Common/ErrorCodes.cpp` of the upstream checkout, so it always matches
  the tests. Commit the file when it changes.

## Known failures

The end of [`allowlist.txt`](allowlist.txt) lists the tests that fail
because of a bug in clickhouse-cpp or a gap in this program, in groups. Each
group header describes the cause and gives a small reproduction. A test that
cannot pass for an environment cause (a cluster, ZooKeeper, server
configuration, features of `clickhouse-client` itself) is not listed; see
[`AGENTS.md`](AGENTS.md#environment-causes). The program does not try `.sh`
tests.

Some tests pass in the upstream CI but are not in the allowlist on purpose,
because in a full run on a small server they can fail also through the real
`clickhouse-client`:

- Tests that compare `EXPLAIN PIPELINE` output or thread and stream counts.
  The server makes the plan from its load and its free memory, so the widths
  are smaller than the reference when other tests run at the same time.
- Tests that need more memory than the server has free after the parallel
  phase of the run (the allowlist was checked with an 8 GiB server).
- Tests that attach a fixed table UUID again, or drop a user, directly after
  a `DROP`: an `Atomic` database removes the dropped table asynchronously, so
  the next statement can fail in a full run.

## CI

[`upstream-sql-tests.yml`](../../.github/workflows/upstream-sql-tests.yml)
runs the allowlist nightly, manually, and on pushes and pull requests that
change this directory, the workflow, or the library (`clickhouse/`,
`contrib/`, `cmake/`, the root `CMakeLists.txt`).

- The `build` job builds the shim once with `scripts/build.sh` and runs the
  unit tests. The test jobs use its programs.
- The test jobs are 8 round-robin shards of the allowlist, about 1,250 tests
  each, at `-j 4`. Each job starts the server of the pinned version with
  `CLICKHOUSE_SKIP_USER_SETUP=1` and the host name of the runner (see
  [Run the tests locally](#run-the-tests-locally)). Shard 1 also runs the
  live tests of the harness.
- A failed test fails the job: the script turns off the runner's own
  retries (`--dont_retry_failures`), and CI does not set `RETRY_FAILED`.
- The diffs of the failed tests are in the job log. A failed job also keeps
  the `.stdout` and `.stderr` files of its failed tests, the log of the
  program (`CLICKHOUSE_CLIENT_CLI_LOG`) and the error log of the server as
  an artifact for 14 days.
- The server version and the upstream tests are pinned together in
  [`clickhouse-version`](clickhouse-version). A new ClickHouse release
  changes nothing in CI, so a pull request never fails because of a release
  that it does not touch. A bump is a pull request of its own; see
  [`AGENTS.md`](AGENTS.md#bump-the-server-version).
- The `clickhouse_version` input of a manual run tries another version
  without a commit, for example before a bump.
- When an allowlisted test is not in the checkout (upstream renamed or
  removed it), the run shows a warning with the names.
- If a shard takes much more than a few minutes, increase the number of
  shards: change the `shard` list and `SHARD_TOTAL` together.

## Bump the server version

Bump [`clickhouse-version`](clickhouse-version) in a pull request of its
own. The version must have a server image tag and a `v<version>-stable` tag
upstream. In short:

1. Try the version: a manual run of the workflow with the
   `clickhouse_version` input, or a local run with
   `fetch-upstream.sh <version>` and a fresh server of that version.
2. Find the cause of each new failure, run the known failures
   (`KNOWN_FAILURES=all`), and change the allowlist.
3. Change `clickhouse-version`, run `fetch-upstream.sh`, and commit
   `error_codes_generated.inc` and the new counts with it.

The full steps are in [`AGENTS.md`](AGENTS.md#bump-the-server-version).

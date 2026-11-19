# Instructions for agents: upstream SQL test harness

This harness runs the ClickHouse core SQL tests through clickhouse-cpp. See
[`README.md`](README.md) for how it works and how to run it.

## The goal

The goal is to find bugs in clickhouse-cpp. To pass more tests is not the
goal. A failed test is information. Do not hide it.

## Rules

1. **Find the cause of a failure before you do anything with the test.** A
   test fails for one of three causes:
   - **The client** (the library in `clickhouse/`). This is what the harness
     is for.
   - **The shim** (the program in this directory). It does not do what
     `clickhouse-client` does.
   - **The environment.** The test cannot pass through any client on this
     server (see [Environment causes](#environment-causes)).
2. **Do not fix a client bug in a harness change.** Report it with a small
   reproduction that uses only the public API, and fix it in a pull request
   of its own, with a regression test in the root `ut/` directory
   (`clickhouse-cpp-ut`). Then enable the tests of its group.
3. **The default for a shim bug is a fix in the shim.** Make the shim copy
   `clickhouse-client` more exactly. Do not change the shim to hide a client
   bug, for example by another way to read a value, a setting that the
   client needs to read a type, or a correction of a wrong value. The shim
   must use the client as a user does: `Client::Execute` with a `Query`, and
   the typed column class of each column type. A type that the client cannot
   read is a client gap (a known failure), not something for the shim to work
   around.
4. **Remove a test from the allowlist only when it cannot pass for an
   environment cause.** Write the reason in the commit message.
5. **A test that fails because of a client bug or a shim gap stays
   visible.** Do not delete it. Put it in [`allowlist.txt`](allowlist.txt)
   as a known failure, `# <test>  # [<id>]`, in the group of its cause. Each
   group has a header that describes the bug and gives a small reproduction.
   Make a new group when no group fits. See the README for the format and for
   how to run one group (`KNOWN_FAILURES=<id>`).
6. **Do not list a test that cannot pass for an environment cause.**

## How to find the cause

Do these steps in this order.

1. **Run the test through the harness:**

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
   ```

2. **Run the test through the real `clickhouse-client`** in the server
   container, on the same server:

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-with-real-client.sh
   ```

   If it also fails here, the cause is the environment. If it passes here,
   the cause is the client or the shim.
3. **Find the statement that fails.** Set `CLICKHOUSE_CLIENT_CLI_LOG` to
   the absolute path of a file; the log shows each statement and the error.
   Read the diff in the output of the runner, and the `.stdout` and
   `.stderr` files next to the test.
4. **Compare the shim with the server on that one statement.** Run it
   through the shim
   (`tests/clickhouse-test-runner/.build/tests/clickhouse-test-runner/clickhouse -q "<SQL>"`)
   and through `curl` with `FORMAT TSV`. Then make the statement smaller
   until it is as small as possible.
5. **Decide between the client and the shim with a small C++ program** that
   uses only the public clickhouse-cpp API (`Client::Execute`,
   `Query::OnData`, the column classes). If the program also gives a wrong
   value or an error, the bug is in the client. If the program gives the
   correct value, the bug is in the shim.

### Environment causes

A test cannot pass through any client on the CI server when it needs one of
these:

- A cluster, shards or replicas (`CLUSTER_DOESNT_EXIST`, `test_shard_localhost`).
- ZooKeeper or Keeper (`NO_ZOOKEEPER`, `Replicated*` engines).
- External services: S3, Azure, HDFS, Kafka, MySQL, PostgreSQL, MongoDB, URL
  sources.
- Server configuration of the upstream CI: named collections, dictionaries,
  storage policies, disks, users and grants, macros
  (`NAMED_COLLECTION_DOESNT_EXIST`, `NO_ELEMENTS_IN_CONFIG`, `UNKNOWN_POLICY`,
  `UNKNOWN_DISK`).
- Features that the server build or platform does not have
  (`SUPPORT_IS_DISABLED`).
- Stateful datasets (`--no-stateful`) or long tests (`--no-long`).
- Features of `clickhouse-client` itself that are not part of a driver:
  `INTO OUTFILE`, `FROM INFILE`, client-side parsing of `INSERT` data, and
  client-only settings.
- The tz database. ClickHouse and the system tz database that the shim
  reads with glibc (`TZDIR`, default `/usr/share/zoneinfo`) can give
  different historical offsets for the same zone. An old zone name
  (`US/Samoa`) that the system does not know is not an environment cause:
  install `tzdata-legacy` (Debian and Ubuntu).
- Tests that conflict with other tests of the set: a fixed table UUID, or
  server state that a test leaves behind (failpoints that a failed test did
  not disable, row policies). Such a test passes alone on a fresh server and
  fails in a full run. A failure message with "Injected failure" comes from a
  failpoint of another test.
- Asynchronous drops. An `Atomic` database removes a dropped table some time
  after the `DROP`. A test that attaches the same table UUID again, or drops
  a user that the table used, directly after the `DROP` can fail in a full
  run. The server log shows the removal after the error.
- The load of the server. `EXPLAIN PIPELINE` output and thread and stream
  counts depend on the load and the free memory of the server. Such a test
  can fail in a full run also through the real `clickhouse-client`.
- Memory limits of the server, when the test also fails through the real
  `clickhouse-client`.

These are **not** environment causes. Find the bug:

- A value that is different from the server's value.
- A decode error, `unimplemented <packet>`, `unsupported column type`, or a
  `ProtocolError`.
- A hang or a receive timeout.

## Conditions for the allowlist

- Add a test only when it passes.
- Run new tests three times, also with a higher `-j` than CI, on a **fresh**
  server (remove the server container and start a new one). Tests change the
  state of the server (row policies, settings profiles, users). A test that
  passes only after other tests ran must not go in.
- Do not run two sweeps at the same time on one server. The memory pressure
  causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.
- Do not use `RETRY_FAILED` when you grow or check the allowlist: a test that
  needs a retry is not reliable.
- When a test in the allowlist starts to fail intermittently, find the cause
  first (rule 1). Remove it only when the cause is in the environment or in
  the test itself, and write the reason.

## Bump the server version

The server version and the upstream tests are pinned together in
[`clickhouse-version`](clickhouse-version). A new ClickHouse release changes
nothing in CI until a pull request bumps the pin, so other pull requests do
not fail because of a release. Bump the pin in a pull request of its own,
and make that pull request pass before it merges:

1. Select the version: a full version with a server image tag and a
   `v<version>-stable` tag upstream, normally the newest stable release.
   (An LTS release has only a `v<version>-lts` tag, which the scripts do not
   take.)
2. Try it: start a manual run with the `clickhouse_version` input, or run
   the allowlist locally on that version (`fetch-upstream.sh <version>`, and
   a fresh server of that version).
3. For each allowlisted test that now fails, find the cause (rule 1). A
   release changes the tests and the server output, so many tests can fail
   at the same time with no change in the client. To see whether the client
   is the cause, run the same test through the real client
   (`run-with-real-client.sh`) on the same server.
4. Run the known failures (`KNOWN_FAILURES=all`). Enable the tests that pass
   now.
5. Fix the names of allowlisted tests that the run reports as not in the
   checkout (upstream renamed or removed them).
6. Optionally, try the new tests of the release (see "Grow the allowlist" in
   the README).
7. Change `clickhouse-version`, run `fetch-upstream.sh`, and commit
   `error_codes_generated.inc` with it.
8. Update the counts that depend on the version: the header of
   `allowlist.txt` and the `Tests:` line of each group, the numbers and the
   version at the top of the README and in "Grow the allowlist", and the
   comment of the shard list in the workflow.

## Keep the documents in sync

When you change the behavior of the shim (flags, statement handling, output
formats, settings), update [`README.md`](README.md), this file and the
workflow in the same change. Before you commit a change of the shim, format
it with the repository's `.clang-format` and build it with GCC and Clang
(see "Change the shim" in the README).

---
name: issue-priority
description: >
  Decide the priority of a `ClickHouse/clickhouse-cpp` issue: `Urgent`,
  `High`, `Medium`, or `Low`, for the org issue field **Priority**. Use this
  skill when the task is to "prioritize", "set the priority of", "rate", or
  "triage" an issue, or to explain why an issue has a given priority. It
  defines the reach tiers of the library, the severity rows, the base-level
  matrix, the modifiers, and the confidence.
---

# Issue priority

This skill is the single source of truth for the **Priority** of
`ClickHouse/clickhouse-cpp` issues. People and automated triage use the same
rules.

The value goes into the org issue field **Priority** (`Urgent`, `High`,
`Medium`, `Low`). The field is visible to org members only. An automated tool
sets the value once, never changes a value that is already set, and keeps the
level and its reason out of public comments.

## When to set no priority

Set no priority when one of these is true:

- A maintainer can close the issue with no further action: it is already fixed
  on `master`, a duplicate, works as intended, already supported, answered, or
  obsolete. "Works as intended" includes a failure that shows only when two
  threads use one `Client` (README, "Thread-safety").
- Only a deprecated public API reaches the defect: a `[[deprecated]]`
  attribute, a header that prints a deprecation `#warning` (for example
  `clickhouse/columns/utils.h`), or docs that call the API deprecated. Fixes to
  deprecated code are not released. One exception: the Abseil-backed
  `Int128`/`UInt128` (`CH_ABSEIL_BIGNUM_DEPRECATED`) is deprecated, but it is
  the default of the 2.x build (`CH_USE_ABSEIL_FOR_BIGNUM=ON`). Rate its
  defects normally (section 1).
- The issue is a question, or it is about CI, the release process, or a
  discussion.

## Vulnerabilities that are not public yet

A security issue must not stay public with a description of how to reproduce
it. This section applies when a reproduction or code inspection confirms a
vulnerability that is not public yet. The classification skill of this
repository defines it
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md), "A
vulnerability that is not public yet"): memory corruption is one also when only
a hostile server or network causes it, and a hang, or a crash with no memory
corruption, that only such a server or network causes is not one.

- Report it in a private GitHub vulnerability report for this repository
  (**Security** → **Report a vulnerability**), never in a public issue,
  comment, or pull request. If a public issue reports it, copy the title, the
  text, and the comments of the issue into the report first.
- Set no Priority on the public issue. Close it with a comment that gives no
  details, and ask a repository administrator to delete it: closing does not
  hide the text of the issue.
- Rate the vulnerability with the rules below, and put the level and its reason
  into the private report as the proposed severity.

A vulnerability that is already public (a published CVE or GHSA, for example in
a dependency), and an authentication method that does not work or is missing,
get a Priority on the issue as usual.

## 1. Reach

The reach is the tier of the most-used public path that reaches the defect,
not the tier of the file that contains it. Every `Select()` parses the column
types that the server sends (`clickhouse/types/type_parser.cpp`) and creates
the columns (`clickhouse/columns/factory.cpp`). So a parser defect for a common
type is Core, and a parser defect that only an unusual type name reaches is
Narrow.

| Tier      | Public paths |
| --------- | ------------ |
| Core      | `Client` connect, handshake, and authentication; `Execute()`, `Select()`, `Insert()`, `Ping()`, `ResetConnection()`; endpoint failover (`SetEndpoints`); timeouts and keep-alive in `ClientOptions`; TLS (`SetSSLOptions`, `WITH_OPENSSL`; ClickHouse Cloud needs it); error handling (`ServerException`, `ProtocolError`, and the connection state after an error); `Block` and the Native codec for `(U)Int8`–`(U)Int64`, `Float32`, `Float64`, `String`, `FixedString`, `Date`, `DateTime`, `DateTime64`, `Decimal32`–`Decimal128`, `UUID`, `Nullable(T)`, `Array(T)`, and `LowCardinality(String)`; the CMake build with the default options on a platform and compiler that CI tests |
| Common    | compression (`SetCompressionMethod`: LZ4, ZSTD); multi-block insert (`BeginInsert`, `SendInsertBlock`, `EndInsert`); streaming and cancelable select (`BeginSelect`, `NextBlock`, `SelectCancelable`, `Cancel`); query settings and parameters (`Query::SetSetting`, `SetParam`); `Query` callbacks (progress, profile, server log, profile events, exception); external data (`SelectWithExternalData`); the types `Bool`, `Enum8`, `Enum16`, `Map`, `Tuple`, `IPv4`, `IPv6`, `Int128`, `UInt128`, and the other `LowCardinality` and nested wrappers; column methods (`Append`, `Slice`, `Reserve`, `Clear`, `Swap`, `GetItem`); the build with Bazel, MinGW, shared libraries, system dependencies (`WITH_SYSTEM_*`), `CH_USE_3X_API` and its options, and the `make install` layout |
| Narrow    | rare types (`Point`, `Ring`, `Polygon`, `MultiPolygon`, `JSON`, `Time`, `Time64`, `Nothing`, `Nested`); unusual type names (escaped `Enum` labels, deep nesting); input that a correct server does not send (a malformed packet or type name); caller input that the type or the API does not allow, but that the library does not reject (an undeclared `Enum` value, a column of the wrong type); OpenTelemetry tracing context; a compiler, platform, or dependency version that CI does not test |
| Satellite | `examples/`, `bench/`, `ut/`, `tests/`, `docs/`, `README.md`; packages that other projects maintain (Conan Center, vcpkg, Homebrew, distribution packages) |

The 2.x build uses the Abseil-backed `Int128`/`UInt128` by default. A defect
that only this implementation has is Common (`Int128`, `UInt128`). If the
self-contained implementation does not have the defect, the workaround is the
build flag `-DCH_USE_ABSEIL_FOR_BIGNUM=OFF` (severity "Workaround exists").

## 2. Severity

First name the **impacts** of the issue with the classification skill of this
repository
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md)): none,
one, or more of Security (part vulnerability or part authentication), Data
integrity, Stability, and Performance. Most issues have none.

Then use the first severity row that fits. With more than one impact, the
highest row applies:

| Severity                | Use when |
| ----------------------- | -------- |
| Wrong data or security  | impact **Data integrity**: the client returns or stores wrong or incomplete data with no error (this includes a block stream that goes out of sync with no error), or reports success for a query that failed; or impact **Security** with a vulnerability: a security defect: a leak of credentials or data, a TLS verification that does not occur, SQL built from a caller value without escaping, memory corruption (an out-of-bounds write, a use after free), or a vulnerable dependency in code that the client calls. A hang, or a crash with no memory corruption, that only a hostile server or network causes does not use this row: rate it by the failure |
| Broken, no workaround   | the feature fails (an exception, a hang, a crash, an abort, a leak that ends in a crash or in exhausted sockets), and the caller has no reasonable workaround; also a `Client` that stays unusable after an error, and a build that fails on a supported toolchain; also a wrong or incomplete error with no workaround: the failure is reported, but its type, code, or message is wrong or lost. The impact **Stability** (a hang, a crash, such a leak) uses this row when the caller has no reasonable workaround |
| Workaround exists       | the feature fails, but the caller can avoid the failure with a small change in their own code or build flags (name the workaround in the reason); also impact **Performance** of a defect: a result that is correct but slow or uses too much memory (a request to make an operation faster is a feature request: last row) |
| Cosmetic, docs, feature | wrong log or message text, a compiler warning, documentation, a feature request (this includes support for a new type or a new package manager) |

An issue that has the impact Security with no vulnerability (an authentication
method that does not work or is missing) does not use the first row. Rate an
authentication method that does not work by its failure ("Broken, no
workaround" or "Workaround exists"), and a request for a missing method as a
feature request.

A request to make an operation faster has the impact Performance, but it is a
feature request: use "Cosmetic, docs, feature". Only a defect with the impact
Performance (a correct result that is too slow) uses "Workaround exists".

A vulnerability in a dependency uses "Wrong data or security" when the client
calls the vulnerable code. When it does not, use "Cosmetic, docs, feature": for
a published one (a CVE or GHSA), the issue asks for a dependency update. One
that is not published yet stays private also then (see "Vulnerabilities that
are not public yet"), and this level is the proposed severity of the private
report.

## 3. Base level

| Severity ↓ / Reach →    | Core   | Common | Narrow | Satellite |
| ----------------------- | ------ | ------ | ------ | --------- |
| Wrong data or security  | Urgent | Urgent | High   | Medium    |
| Broken, no workaround   | High   | High   | Medium | Low       |
| Workaround exists       | Medium | Medium | Low    | Low       |
| Cosmetic, docs, feature | Low    | Low    | Low    | Low       |

## 4. Modifiers

Apply them in this order:

1. **Security with a shown path from untrusted input.** The issue or a
   reproduction shows how a value that an application takes from its users
   reaches the defect: `Urgent` at any reach. Skip the other modifiers. A
   defect that puts a caller-supplied value into SQL without quoting or
   escaping is a candidate (for example a table name, a setting, or a query
   parameter): look for such a path. If none is shown, rate the defect by the
   failure that it shows. A vulnerability that is not public yet goes to a
   private vulnerability report, not to a public issue (see "Vulnerabilities
   that are not public yet").
2. **Regression in the latest published release**: up one level. This can give
   `Urgent`. It is a regression only when the latest release shows the problem
   and the release before it does not. The tags are `vX.Y.Z` (for example
   `v2.6.2` and `v2.6.1`). A defect that only `master` has (after the latest
   tag) is not a release regression: no modifier, but write "unreleased
   regression" in the reason, so that it is fixed before the next release.
3. **Customer request in Linear.** The issue links (`linear.app`) a Linear
   issue for a customer request: up one level, but never to `Urgent` (`High`
   stays `High`).

## 5. Confidence

A missing reproduction lowers the confidence, not the level. Record the
confidence with the value when the tool that sets it supports it:

| Confidence | Use when |
| ---------- | -------- |
| `HIGH`     | a run shows the problem (or, for a feature, code inspection confirms its absence on `master`) |
| `MEDIUM`   | code inspection shows a defect, but no reproduction was run (for example, it needs ClickHouse Cloud or a specific platform) |
| `LOW`      | the report lacks what a reproduction needs, or a run did not show the problem |

## 6. Reason

Write one line with the value:

`<tier> (<path>) · <impacts, or "no impact"> · <severity> · <modifiers, or "no modifier"> → <level>.`

Name the workaround when the severity is "Workaround exists". When the
confidence is `LOW`, say what is missing.

## 7. Labels from earlier triage

- `priority-high` and `priority-low` are older labels that a maintainer set.
  When an issue has one, use its level (`High` or `Low`) instead of the table
  value, and write "maintainer label `priority-…`" in the reason. Never add
  these labels.
- `pg_clickhouse` marks a request from the pg_clickhouse PostgreSQL extension.
  pg_clickhouse uses clickhouse-c instead of clickhouse-cpp from its v0.3.1
  release, so the label gives no modifier.

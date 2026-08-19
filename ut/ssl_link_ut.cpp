#include <clickhouse/base/sslsocket.h>

#include <gtest/gtest.h>

// Link/compile guard for the TLS code path.
//
// A library-only `bazel build //:clickhouse` produces a static archive
// and never runs the linker, so missing system libraries (on Windows,
// the Win32 cert store `crypt32` and `user32` that OpenSSL needs) stay
// invisible until a *consumer* links an executable that actually pulls
// in sslsocket.obj. The hermetic unit_tests subset doesn't touch SSL,
// so it didn't surface them either.
//
// Constructing an SSLContext drags sslsocket.obj — and transitively the
// SSL_CTX_*/X509_* symbols and their system-lib dependencies — into the
// test binary's link step. Default-constructed params do no CA loading
// and open no socket, so this runs in the hermetic sandbox while still
// exercising the link that previously only broke for downstream users.
//
// Only compiled when TLS is enabled (excluded for tls=no, where
// sslsocket.cpp isn't part of the library); see ut/BUILD.bazel.
TEST(SSLLink, ConstructContext) {
    clickhouse::SSLParams params{};
    clickhouse::SSLContext context(params);
    SUCCEED();
}

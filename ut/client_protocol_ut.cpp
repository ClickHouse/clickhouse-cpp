#include <clickhouse/client.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/socket.h>
#include <clickhouse/exceptions.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace {

using namespace clickhouse;

/// A socket that serves a pre-recorded byte script as the server response
/// and discards (but keeps) everything written by the client.
class ScriptedSocket : public SocketBase {
public:
    explicit ScriptedSocket(std::vector<uint8_t> script)
        : script_(std::move(script))
    {}

    std::unique_ptr<InputStream> makeInputStream() const override {
        return std::make_unique<ArrayInput>(script_.data(), script_.size());
    }

    std::unique_ptr<OutputStream> makeOutputStream() const override {
        return std::make_unique<BufferOutput>(&written_);
    }

private:
    const std::vector<uint8_t> script_;
    mutable std::vector<uint8_t> written_;
};

class ScriptedSocketFactory : public SocketFactory {
public:
    explicit ScriptedSocketFactory(std::vector<uint8_t> script)
        : script_(std::move(script))
    {}

    std::unique_ptr<SocketBase> connect(const ClientOptions&, const Endpoint&) override {
        return std::make_unique<ScriptedSocket>(script_);
    }

private:
    std::vector<uint8_t> script_;
};

/// Serves the next script on each connect, repeating the last one.
class SequencedSocketFactory : public SocketFactory {
public:
    explicit SequencedSocketFactory(std::vector<std::vector<uint8_t>> scripts) : scripts_(std::move(scripts)) {}

    std::unique_ptr<SocketBase> connect(const ClientOptions&, const Endpoint&) override {
        const size_t index = next_ < scripts_.size() - 1 ? next_++ : next_;
        return std::make_unique<ScriptedSocket>(scripts_[index]);
    }

private:
    const std::vector<std::vector<uint8_t>> scripts_;
    size_t next_ = 0;
};

ClientOptions ScriptedClientOptions() {
    return ClientOptions()
        .SetHost("scripted.test")
        .SetPingBeforeQuery(false)
        .SetSendRetries(0);
}

/// A minimal well-formed ServerHello. Revision 50000 is below
/// DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE (54058), so the server sends only
/// name, version_major, version_minor and revision.
const std::vector<uint8_t> kServerHello = {
    0x00,             // packet type: ServerCodes::Hello (varint)
    0x02, 0x43, 0x48, // server name: "CH" (string, len 2)
    0x01,             // version_major = 1 (varint)
    0x01,             // version_minor = 1 (varint)
    0xD0, 0x86, 0x03, // revision = 50000 (varint)
};

/// A ServerHello at revision 54401 (DBMS_MIN_REVISION_WITH_VERSION_PATCH),
/// the first revision that carries every ServerInfo field.
const std::vector<uint8_t> kServerHelloWithPatch = {
    0x00,                    // packet type: ServerCodes::Hello (varint)
    0x02, 0x43, 0x48,        // server name: "CH" (string, len 2)
    0x02,                    // version_major = 2 (varint)
    0x03,                    // version_minor = 3 (varint)
    0x81, 0xA9, 0x03,        // revision = 54401 (varint)
    0x03, 0x55, 0x54, 0x43,  // timezone: "UTC" (string, len 3)
    0x01, 0x64,              // display_name: "d" (string, len 1)
    0x07,                    // version_patch = 7 (varint)
};

/// An exception packet with a valid code but a corrupt `name` string.
const std::vector<uint8_t> kCorruptExceptionPacket = {
    0x02,                         // packet type: ServerCodes::Exception (varint)
    0x3C, 0x00, 0x00, 0x00,       // code = 60 (int32, little-endian)
    0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // name string length = 0xFFFFFFFF (varint),
                                  // exceeds the 0x00FFFFFF limit in WireFormat::ReadString
};

/// A complete well-formed exception packet.
const std::vector<uint8_t> kWellFormedExceptionPacket = {
    0x02,                         // packet type: ServerCodes::Exception (varint)
    0x3C, 0x00, 0x00, 0x00,       // code = 60 (int32, little-endian)
    0x02, 0x44, 0x42,             // name: "DB" (string, len 2)
    0x04, 0x6F, 0x6F, 0x70, 0x73, // display_text: "oops" (string, len 4)
    0x00,                         // stack_trace: "" (string, len 0)
    0x00,                         // has_nested = false (fixed, 1 byte)
};

/// Performs the handshake against kServerHello, then runs a Select that
/// receives `response` and expects a ProtocolError containing
/// `expected_message`.
void ExpectSelectThrowsProtocolError(const std::vector<uint8_t>& response,
                                     const std::string& expected_message) {
    std::vector<uint8_t> script = kServerHello;
    script.insert(script.end(), response.begin(), response.end());
    // Trailing garbage: decoding must fail on validation, not on end-of-stream.
    script.insert(script.end(), 64, 0xAA);

    // The handshake must succeed; only Select is expected to throw.
    Client client(ScriptedClientOptions(),
                  std::make_unique<ScriptedSocketFactory>(script));

    try {
        client.Select("SELECT 1", [](const Block&) {});
        FAIL() << "expected ProtocolError";
    } catch (const ProtocolError& e) {
        EXPECT_TRUE(std::string(e.what()).find(expected_message) != std::string::npos)
            << "unexpected message: " << e.what();
    }
}

}

TEST(ClientProtocol, MalformedServerHelloThrowsProtocolError) {
    std::vector<uint8_t> script = {
        0x00,                         // packet type: ServerCodes::Hello (varint)
        0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // server-name string length = 0xFFFFFFFF (varint),
                                      // exceeds the 0x00FFFFFF limit in WireFormat::ReadString
    };

    // Trailing garbage: decoding must fail on validation, not on end-of-stream.
    script.insert(script.end(), 64, 0xAA);

    try {
        Client client(ScriptedClientOptions(),
                      std::make_unique<ScriptedSocketFactory>(script));
        FAIL() << "expected ProtocolError";
    } catch (const ProtocolError& e) {
        EXPECT_TRUE(std::string(e.what()).find("fail to connect") != std::string::npos)
            << "unexpected message: " << e.what();
    }
}

TEST(ClientProtocol, MalformedSelectResponseThrowsProtocolError) {
    // Malformed packet type: a varint whose continuation bit never clears,
    // so ReadVarint64 gives up after MAX_VARINT_BYTES.
    const std::vector<uint8_t> response(10, 0x80);
    ExpectSelectThrowsProtocolError(response, "can't read packet type");
}

TEST(ClientProtocol, CorruptProgressPacketThrowsProtocolError) {
    std::vector<uint8_t> response = {
        0x03, // packet type: ServerCodes::Progress (varint)
    };
    // Malformed `rows` varint: the continuation bit never clears.
    response.insert(response.end(), 10, 0x80);
    ExpectSelectThrowsProtocolError(response, "can't read progress packet");
}

TEST(ClientProtocol, CorruptProfileInfoPacketThrowsProtocolError) {
    std::vector<uint8_t> response = {
        0x06, // packet type: ServerCodes::ProfileInfo (varint)
    };
    // Malformed `rows` varint: the continuation bit never clears.
    response.insert(response.end(), 10, 0x80);
    ExpectSelectThrowsProtocolError(response, "can't read profile info packet");
}

TEST(ClientProtocol, CorruptLogPacketThrowsProtocolError) {
    const std::vector<uint8_t> response = {
        0x0A,                         // packet type: ServerCodes::Log (varint)
        0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // log-tag string length = 0xFFFFFFFF (varint),
                                      // exceeds the 0x00FFFFFF limit in WireFormat::SkipString
    };
    ExpectSelectThrowsProtocolError(response, "can't read log packet");
}

TEST(ClientProtocol, CorruptExceptionPacketInSelectThrowsProtocolError) {
    ExpectSelectThrowsProtocolError(kCorruptExceptionPacket, "error details lost");
}

TEST(ClientProtocol, CorruptExceptionPacketInHandshakeThrowsProtocolError) {
    // The exception packet arrives instead of ServerHello.
    std::vector<uint8_t> script = kCorruptExceptionPacket;
    // Trailing garbage: decoding must fail on validation, not on end-of-stream.
    script.insert(script.end(), 64, 0xAA);

    try {
        Client client(ScriptedClientOptions(),
                      std::make_unique<ScriptedSocketFactory>(script));
        FAIL() << "expected ProtocolError";
    } catch (const ProtocolError& e) {
        EXPECT_TRUE(std::string(e.what()).find("exception packet could not be decoded") != std::string::npos)
            << "unexpected message: " << e.what();
    }
}

TEST(ClientProtocol, ExceptionPacketInSelectThrowsServerException) {
    std::vector<uint8_t> script = kServerHello;
    script.insert(script.end(),
                  kWellFormedExceptionPacket.begin(), kWellFormedExceptionPacket.end());

    // The handshake must succeed; only Select is expected to throw.
    Client client(ScriptedClientOptions(),
                  std::make_unique<ScriptedSocketFactory>(script));

    try {
        client.Select("SELECT 1", [](const Block&) {});
        FAIL() << "expected ServerException";
    } catch (const ServerException& e) {
        EXPECT_EQ(e.GetCode(), 60);
        EXPECT_EQ(e.GetException().name, "DB");
        EXPECT_STREQ(e.what(), "oops");
    }
}

TEST(ClientProtocol, ExceptionPacketInHandshakeThrowsServerException) {
    // The exception packet arrives instead of ServerHello.
    try {
        Client client(ScriptedClientOptions(),
                      std::make_unique<ScriptedSocketFactory>(kWellFormedExceptionPacket));
        FAIL() << "expected ServerException";
    } catch (const ServerException& e) {
        EXPECT_EQ(e.GetCode(), 60);
        EXPECT_EQ(e.GetException().name, "DB");
        EXPECT_STREQ(e.what(), "oops");
    }
}

TEST(ClientProtocol, WellFormedSelectResponseSucceeds) {
    std::vector<uint8_t> script = kServerHello;
    const std::vector<uint8_t> response = {
        0x01,       // packet type: ServerCodes::Data (varint)
                    // (revision 50000 has neither temp-table name nor BlockInfo)
        0x00,       // num_columns = 0 (varint)
        0x00,       // num_rows = 0 (varint)
        0x05,       // packet type: ServerCodes::EndOfStream (varint)
    };
    script.insert(script.end(), response.begin(), response.end());

    Client client(ScriptedClientOptions(),
                  std::make_unique<ScriptedSocketFactory>(script));

    size_t blocks = 0;
    EXPECT_NO_THROW(client.Select("SELECT 1", [&blocks](const Block& block) {
        ++blocks;
        EXPECT_EQ(block.GetRowCount(), 0u);
    }));
    EXPECT_EQ(blocks, 1u);
}

TEST(ClientProtocol, OldRevisionServerHelloLeavesMissingFieldsZero) {
    Client client(ScriptedClientOptions(), std::make_unique<ScriptedSocketFactory>(kServerHello));

    const ServerInfo& info = client.GetServerInfo();
    EXPECT_EQ(info.name, "CH");
    EXPECT_EQ(info.version_major, 1u);
    EXPECT_EQ(info.version_minor, 1u);
    EXPECT_EQ(info.revision, 50000u);
    EXPECT_EQ(info.timezone, "");
    EXPECT_EQ(info.display_name, "");
    EXPECT_EQ(info.version_patch, 0u);
}

TEST(ClientProtocol, ReconnectToOldRevisionServerClearsPreviousServerInfo) {
    Client client(ScriptedClientOptions(),
                  std::make_unique<SequencedSocketFactory>(std::vector<std::vector<uint8_t>>{kServerHelloWithPatch, kServerHello}));

    EXPECT_EQ(client.GetServerInfo().timezone, "UTC");
    EXPECT_EQ(client.GetServerInfo().display_name, "d");
    EXPECT_EQ(client.GetServerInfo().version_patch, 7u);

    client.ResetConnection();

    const ServerInfo& info = client.GetServerInfo();
    EXPECT_EQ(info.name, "CH");
    EXPECT_EQ(info.revision, 50000u);
    EXPECT_EQ(info.timezone, "");
    EXPECT_EQ(info.display_name, "");
    EXPECT_EQ(info.version_patch, 0u);
}

TEST(ClientProtocol, ProfileInfoFlagsAcceptAnyNonZeroByte) {
    std::vector<uint8_t> script         = kServerHello;
    const std::vector<uint8_t> response = {
        0x06,  // packet type: ServerCodes::ProfileInfo (varint)
        0x01,  // rows = 1 (varint)
        0x00,  // blocks = 0 (varint)
        0x02,  // bytes = 2 (varint)
        0x05,  // applied_limit: non-canonical true (fixed, 1 byte)
        0x03,  // rows_before_limit = 3 (varint)
        0x00,  // calculated_rows_before_limit = false (fixed, 1 byte)
        0x06,  // packet type: ServerCodes::ProfileInfo (varint)
        0x01, 0x00, 0x02,
        0x00,  // applied_limit = false
        0x03,
        0xFF,  // calculated_rows_before_limit: non-canonical true
        0x05,  // packet type: ServerCodes::EndOfStream (varint)
    };
    script.insert(script.end(), response.begin(), response.end());

    Client client(ScriptedClientOptions(), std::make_unique<ScriptedSocketFactory>(script));

    std::vector<std::pair<uint8_t, uint8_t>> flags;
    Query query("SELECT 1");
    query.OnProfile([&flags](const Profile& profile) {
        EXPECT_EQ(profile.rows, 1u);
        EXPECT_EQ(profile.bytes, 2u);
        EXPECT_EQ(profile.rows_before_limit, 3u);
        // Inspect the object representation: a bool holding anything other
        // than 0 or 1 is undefined behaviour to load.
        flags.emplace_back(*reinterpret_cast<const uint8_t*>(&profile.applied_limit),
                           *reinterpret_cast<const uint8_t*>(&profile.calculated_rows_before_limit));
    });
    client.Select(query);

    ASSERT_EQ(flags.size(), 2u);
    EXPECT_EQ(flags[0].first, 1u);
    EXPECT_EQ(flags[0].second, 0u);
    EXPECT_EQ(flags[1].first, 0u);
    EXPECT_EQ(flags[1].second, 1u);
}

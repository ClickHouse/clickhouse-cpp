#include <city.h>
#include <clickhouse/base/compressed.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/wire_format.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace clickhouse;

namespace {

Buffer MakeCompressedBlockHeader(uint32_t compressed_size) {
    Buffer buffer;
    BufferOutput output(&buffer);

    uint8_t hash[16] = {};
    const uint8_t method = 0x82;
    const uint32_t original_size = 0;

    WireFormat::WriteFixed(output, hash);
    WireFormat::WriteFixed(output, method);
    WireFormat::WriteFixed(output, compressed_size);
    WireFormat::WriteFixed(output, original_size);
    output.Flush();

    return buffer;
}

void WriteCompressedFrame(OutputStream& output, uint8_t method, uint32_t original_size, const std::vector<uint8_t>& payload) {
    Buffer frame;
    {
        BufferOutput frame_output(&frame);
        const uint32_t compressed_size = static_cast<uint32_t>(9 + payload.size());
        WireFormat::WriteFixed(frame_output, method);
        WireFormat::WriteFixed(frame_output, compressed_size);
        WireFormat::WriteFixed(frame_output, original_size);
        WireFormat::WriteBytes(frame_output, payload.data(), payload.size());
        frame_output.Flush();
    }

    WireFormat::WriteFixed(output, cityhash::CityHash128(reinterpret_cast<const char*>(frame.data()), frame.size()));
    WireFormat::WriteBytes(output, frame.data(), frame.size());
}

constexpr uint8_t kLZ4  = 0x82;
constexpr uint8_t kZSTD = 0x90;

// Encodings of an empty input: a single LZ4 token with no literals, and a
// ZSTD frame with Frame_Content_Size 0 holding one empty raw block.
const std::vector<uint8_t> kEmptyLZ4  = {0x00};
const std::vector<uint8_t> kEmptyZSTD = {0x28, 0xB5, 0x2F, 0xFD, 0x20, 0x00, 0x01, 0x00, 0x00};
}

TEST(CodedStreamCase, Varint64) {
    Buffer buf;

    {
        BufferOutput output(&buf);
        WireFormat::WriteVarint64(output, 18446744071965638648ULL);
        output.Flush();
    }

    {
        ArrayInput input(buf.data(), buf.size());
        uint64_t value = 0;
        ASSERT_TRUE(WireFormat::ReadVarint64(input, &value));
        ASSERT_EQ(value, 18446744071965638648ULL);
    }
}

TEST(CompressedInputCase, RejectsBlocksShorterThanHeader) {
    for (const uint32_t compressed_size : {0U, 1U, 8U}) {
        Buffer buffer = MakeCompressedBlockHeader(compressed_size);
        ArrayInput input(buffer.data(), buffer.size());
        CompressedInput compressed_input(&input);
        const void* data = nullptr;

        EXPECT_THROW(compressed_input.Next(&data, 1), CompressionError)
            << "compressed size: " << compressed_size;
    }
}

TEST(CompressedInputCase, SkipsEmptyBlocks) {
    for (const auto& [method, empty_payload] : {std::make_pair(kLZ4, kEmptyLZ4), std::make_pair(kZSTD, kEmptyZSTD)}) {
        Buffer buffer;
        {
            BufferOutput output(&buffer);
            WriteCompressedFrame(output, method, 0, empty_payload);
            CompressedOutput compressed_output(&output, 0, method == kLZ4 ? CompressionMethod::LZ4 : CompressionMethod::ZSTD);
            WireFormat::WriteBytes(compressed_output, "abc", 3);
            compressed_output.Flush();
        }

        ArrayInput input(buffer.data(), buffer.size());
        CompressedInput compressed_input(&input);
        std::string data(3, '\0');

        ASSERT_TRUE(WireFormat::ReadBytes(compressed_input, data.data(), data.size())) << "method: " << int(method);
        EXPECT_EQ(data, "abc");
    }
}

TEST(CompressedInputCase, RejectsEmptyBlockWithNonEmptyContent) {
    // LZ4: a 4-byte literal run. ZSTD: Frame_Content_Size 1 with a 1-byte raw block.
    const std::vector<uint8_t> lz4_payload  = {0x40, 0x61, 0x62, 0x63, 0x64};
    const std::vector<uint8_t> zstd_payload = {0x28, 0xB5, 0x2F, 0xFD, 0x20, 0x01, 0x09, 0x00, 0x00, 0x61};

    for (const auto& [method, payload] : {std::make_pair(kLZ4, lz4_payload), std::make_pair(kZSTD, zstd_payload)}) {
        Buffer buffer;
        {
            BufferOutput output(&buffer);
            WriteCompressedFrame(output, method, 0, payload);
            output.Flush();
        }

        ArrayInput input(buffer.data(), buffer.size());
        CompressedInput compressed_input(&input);
        const void* data = nullptr;

        EXPECT_THROW(compressed_input.Next(&data, 1), CompressionError) << "method: " << int(method);
    }
}

TEST(CompressedInputCase, RejectsBlockShorterThanDeclaredOriginalSize) {
    // Both payloads decode to "abc", but the frame header declares 4 bytes.
    const std::vector<uint8_t> lz4_payload  = {0x30, 0x61, 0x62, 0x63};
    const std::vector<uint8_t> zstd_payload = {0x28, 0xB5, 0x2F, 0xFD, 0x20, 0x03, 0x19, 0x00, 0x00, 0x61, 0x62, 0x63};

    for (const auto& [method, payload] : {std::make_pair(kLZ4, lz4_payload), std::make_pair(kZSTD, zstd_payload)}) {
        Buffer buffer;
        {
            BufferOutput output(&buffer);
            WriteCompressedFrame(output, method, 4, payload);
            output.Flush();
        }

        ArrayInput input(buffer.data(), buffer.size());
        CompressedInput compressed_input(&input);
        const void* data = nullptr;

        EXPECT_THROW(compressed_input.Next(&data, 4), CompressionError) << "method: " << int(method);
    }
}

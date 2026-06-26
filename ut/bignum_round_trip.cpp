#include <clickhouse/client.h>
#include <clickhouse/types/bignum.h>
#include <gtest/gtest.h>
#include "bignum_samples.h"
#include "utils.h"

#include <algorithm>
#include <iterator>

using namespace clickhouse;

const auto LocalHostEndpoint = ClientOptions()
        .SetHost(           getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
        .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
        .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
        .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
        .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"));

namespace {

// Strip leading zeros (and normalize "-0" -> "0") to get the canonical decimal
// representation that ClickHouse's toString() is expected to produce.
std::string TrimLeadingZeros(const std::string& str) {
    std::string ret{};
    ret.reserve(str.size());

    auto it  = str.cbegin();
    auto end = str.cend();

    if (it < end && *it == '-') {
        ret.push_back('-');
        ++it;
    }

    for (; it < end && *it == '0'; ++it);

    std::copy(it, end, std::back_inserter(ret));

    if (ret.empty() || ret == "-") {
        return "0";
    }
    return ret;
}

// `limbs` is given in hi-lo order (most significant 32-bit word first),
// 4 words for a 128-bit integer.
UInt128 ToUInt128(const std::vector<uint32_t>& limbs) {
    const uint64_t hi = (static_cast<uint64_t>(limbs[0]) << 32) | limbs[1];
    const uint64_t lo = (static_cast<uint64_t>(limbs[2]) << 32) | limbs[3];
    return Bignum::MakeUInt128(hi, lo);
}

Int128 ToInt128(const std::vector<uint32_t>& limbs) {
    const uint64_t hi = (static_cast<uint64_t>(limbs[0]) << 32) | limbs[1];
    const uint64_t lo = (static_cast<uint64_t>(limbs[2]) << 32) | limbs[3];
    return Bignum::MakeInt128(static_cast<int64_t>(hi), lo);
}

}  // namespace

class BignumRoundtripCase : public testing::Test {
protected:
    void SetUp() override {
        client_ = std::make_unique<Client>(
            ClientOptions(LocalHostEndpoint).SetPingBeforeQuery(true));
    }

    void TearDown() override {
        client_.reset();
    }

    // Inserts every sample as a row, then selects back the value alongside
    // ClickHouse's own toString() of the column, verifying both the binary
    // round trip and the textual representation.
    template <typename ColumnType, typename Converter>
    void RunRoundtrip(const std::string& table, const std::string& ch_type,
                      const std::vector<TestSample>& samples, Converter convert) {
        using ValueType = typename ColumnType::ValueType;

        client_->Execute("DROP TEMPORARY TABLE IF EXISTS " + table);
        client_->Execute("CREATE TEMPORARY TABLE " + table +
                         " (id UInt64, v " + ch_type + ") ENGINE = Memory");

        std::vector<ValueType> expected_values;
        std::vector<std::string> expected_strings;
        expected_values.reserve(samples.size());
        expected_strings.reserve(samples.size());

        auto id  = std::make_shared<ColumnUInt64>();
        auto col = std::make_shared<ColumnType>();
        for (size_t i = 0; i < samples.size(); ++i) {
            const auto& [value, limbs] = samples[i];
            const ValueType v = convert(limbs);
            id->Append(i);
            col->Append(v);
            expected_values.push_back(v);
            expected_strings.push_back(TrimLeadingZeros(value));
        }

        Block b;
        b.AppendColumn("id", id);
        b.AppendColumn("v", col);
        client_->Insert(table, b);

        size_t total_rows = 0;
        client_->Select("SELECT id, v, toString(v) FROM " + table + " ORDER BY id",
            [&](const Block& block) {
                for (size_t r = 0; r < block.GetRowCount(); ++r) {
                    const auto idx = block[0]->template As<ColumnUInt64>()->At(r);
                    ASSERT_LT(idx, samples.size());
                    SCOPED_TRACE("value = " + samples[idx].first);

                    const auto got_value  = block[1]->template As<ColumnType>()->At(r);
                    const auto got_string = block[2]->template As<ColumnString>()->At(r);

                    EXPECT_EQ(expected_values[idx], got_value);
                    EXPECT_EQ(expected_strings[idx], got_string);
                    ++total_rows;
                }
            });

        EXPECT_EQ(samples.size(), total_rows);
    }

    std::unique_ptr<Client> client_;
};

TEST_F(BignumRoundtripCase, UnsignedInt128) {
    RunRoundtrip<ColumnUInt128>("test_clickhouse_cpp_bignum_u128", "UInt128",
                                UnsignedInt128Samples(), ToUInt128);
}

TEST_F(BignumRoundtripCase, SignedInt128) {
    RunRoundtrip<ColumnInt128>("test_clickhouse_cpp_bignum_i128", "Int128",
                               SignedInt128Samples(), ToInt128);
}

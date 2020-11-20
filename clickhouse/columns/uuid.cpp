#include "uuid.h"
#include "utils.h"

#include <stdexcept>

namespace clickhouse {

ColumnUUID::ColumnUUID() : Column(Type::CreateUUID()), data_(std::make_shared<ColumnUInt64>()) {
}

ColumnUUID::ColumnUUID(ColumnRef data) : Column(Type::CreateUUID()), data_(data->As<ColumnUInt64>()) {
    if (data_->Size() % 2 != 0) {
        throw std::runtime_error(
            "number of entries must be even (two 64-bit numbers for each "
            "UUID)");
    }
}

void ColumnUUID::Append(const UInt128& value) {
    data_->Append(value.first);
    data_->Append(value.second);
}

void ColumnUUID::Clear() {
    data_->Clear();
}

const UInt128 ColumnUUID::At(size_t n) const {
    return UInt128(data_->At(n * 2), data_->At(n * 2 + 1));
}

const UInt128 ColumnUUID::operator[](size_t n) const {
    return UInt128((*data_)[n * 2], (*data_)[n * 2 + 1]);
}

void ColumnUUID::Append(ColumnRef column) {
    if (auto col = column->As<ColumnUUID>()) {
        data_->Append(col->data_);
    }
}

bool ColumnUUID::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows * 2);
}

void ColumnUUID::Save(CodedOutputStream* output) {
    data_->Save(output);
}

size_t ColumnUUID::Size() const {
    return data_->Size() / 2;
}

ColumnRef ColumnUUID::Slice(size_t begin, size_t len) {
    return std::make_shared<ColumnUUID>(data_->Slice(begin * 2, len * 2));
}

void ColumnUUID::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnUUID&>(other);
    data_.swap(col.data_);
}

ItemView ColumnUUID::GetItem(size_t index) const {
    return data_->GetItem(index);
}

extern const char* const hex_byte_to_char_lowercase_table;

void writeHexByteLowercase(uint8_t byte, void* out) {
    memcpy(out, &hex_byte_to_char_lowercase_table[static_cast<size_t>(byte) * 2], 2);
}

const char* const hex_byte_to_char_lowercase_table =
    "000102030405060708090a0b0c0d0e0f"
    "101112131415161718191a1b1c1d1e1f"
    "202122232425262728292a2b2c2d2e2f"
    "303132333435363738393a3b3c3d3e3f"
    "404142434445464748494a4b4c4d4e4f"
    "505152535455565758595a5b5c5d5e5f"
    "606162636465666768696a6b6c6d6e6f"
    "707172737475767778797a7b7c7d7e7f"
    "808182838485868788898a8b8c8d8e8f"
    "909192939495969798999a9b9c9d9e9f"
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
    "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
    "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
    "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
    "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes) {
    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; src_pos < num_bytes; ++src_pos) {
        writeHexByteLowercase(src[src_pos], &dst[dst_pos]);
        dst_pos += 2;
    }
}

void formatUUID(const uint8_t* src16, uint8_t* dst36) {
    formatHex(&src16[0], &dst36[0], 4);
    dst36[8] = '-';
    formatHex(&src16[4], &dst36[9], 2);
    dst36[13] = '-';
    formatHex(&src16[6], &dst36[14], 2);
    dst36[18] = '-';
    formatHex(&src16[8], &dst36[19], 2);
    dst36[23] = '-';
    formatHex(&src16[10], &dst36[24], 6);
}

/** Function used when byte ordering is important when parsing uuid
 *  ex: When we create an UUID type
 */
void formatUUID(std::reverse_iterator<const uint8_t*> src16, uint8_t* dst36) {
    formatHex(src16 + 8, &dst36[0], 4);
    dst36[8] = '-';
    formatHex(src16 + 12, &dst36[9], 2);
    dst36[13] = '-';
    formatHex(src16 + 14, &dst36[14], 2);
    dst36[18] = '-';
    formatHex(src16, &dst36[19], 2);
    dst36[23] = '-';
    formatHex(src16 + 2, &dst36[24], 6);
}

std::ostream& ColumnUUID::Dump(std::ostream& o, size_t index) const {
    char dest[36];
    auto src = At(index);
    formatUUID(std::reverse_iterator<const uint8_t*>(reinterpret_cast<const uint8_t*>(&src) + 16), reinterpret_cast<uint8_t*>(&dest));
    return o << std::string(dest);
}
}  // namespace clickhouse

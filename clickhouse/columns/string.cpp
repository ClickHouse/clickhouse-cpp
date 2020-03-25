#include "string.h"
#include "utils.h"

#include "../base/wire_format.h"

namespace {
const size_t DEFAULT_BLOCK_SIZE = 4096;

template <typename Container>
size_t ComputeTotalSize(const Container & strings, size_t begin = 0, size_t len = -1)
{
    size_t result = 0;
    if (begin < strings.size()) {
        len = std::min(len, strings.size() - begin);

        for (size_t i = begin; i < begin + len; ++i)
            result += strings[i].size();
    }

    return result;
}

}

namespace clickhouse {

ColumnFixedString::ColumnFixedString(size_t n)
    : Column(Type::CreateString(n))
    , string_size_(n)
{
}

void ColumnFixedString::Append(std::string_view str) {
    if (data_.capacity() < str.size())
    {
        // round up to the next block size
        const auto new_size = (((data_.size() + string_size_) / DEFAULT_BLOCK_SIZE) + 1) * DEFAULT_BLOCK_SIZE;
        data_.reserve(new_size);
    }

    data_.insert(data_.size(), str);
}

void ColumnFixedString::Clear() {
    data_.clear();
}

std::string_view ColumnFixedString::At(size_t n) const {
    const auto pos = n * string_size_;
    return std::string_view(&data_.at(pos), string_size_);
}

std::string_view ColumnFixedString::operator [](size_t n) const {
    const auto pos = n * string_size_;
    return std::string_view(&data_[pos], string_size_);
}

size_t ColumnFixedString::FixedSize() const
{
       return string_size_;
}

void ColumnFixedString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnFixedString>()) {
        if (string_size_ == col->string_size_) {
            data_.insert(data_.end(), col->data_.begin(), col->data_.end());
        }
    }
}

bool ColumnFixedString::Load(CodedInputStream* input, size_t rows) {
    data_.resize(string_size_ * rows);
    if (!WireFormat::ReadBytes(input, &data_[0], data_.size())) {
        return false;
    }

    return true;
}

void ColumnFixedString::Save(CodedOutputStream* output) {
    WireFormat::WriteBytes(output, data_.data(), data_.size());
}

size_t ColumnFixedString::Size() const {
    return data_.size() / string_size_;
}

ColumnRef ColumnFixedString::Slice(size_t begin, size_t len) {
    auto result = std::make_shared<ColumnFixedString>(string_size_);

    if (begin < Size()) {
        const auto b = begin * string_size_;
        const auto l = len * string_size_;
        result->data_ = data_.substr(b, std::min(data_.size() - b, l));
    }

    return result;
}

void ColumnFixedString::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnFixedString &>(other);
    std::swap(string_size_, col.string_size_);
    data_.swap(col.data_);
}

ItemView ColumnFixedString::GetItem(size_t index) const {
    return ItemView{Type::FixedString, this->At(index)};
}

struct ColumnString::Block
{
    using CharT = typename std::string::value_type;

    explicit Block(size_t starting_capacity)
        : size(0),
          capacity(starting_capacity),
          data_(new CharT[capacity])
    {}

    inline auto GetAvailble() const
    {
        return capacity - size;
    }

    std::string_view AppendUnsafe(std::string_view str)
    {
        const auto pos = &data_[size];

        memcpy(pos, str.data(), str.size());
        size += str.size();

        return std::string_view(pos, str.size());
    }

    auto GetCurrentWritePos()
    {
        return &data_[size];
    }

    std::string_view ConsumeTailAsStringViewUnsafe(size_t len)
    {
        const auto start = &data_[size];
        size += len;
        return std::string_view(start, len);
    }

    size_t size;
    const size_t capacity;
    std::unique_ptr<CharT[]> data_;
};

ColumnString::ColumnString()
    : Column(Type::CreateString())
{
}

ColumnString::ColumnString(const std::vector<std::string> & data)
    : Column(Type::CreateString())
{
    items_.reserve(data.size());
    blocks_.emplace_back(ComputeTotalSize(data));

    for (const auto & s : data)
    {
        AppendUnsafe(s);
    }
}

ColumnString::~ColumnString()
{}

void ColumnString::Append(std::string_view str) {
    if (blocks_.size() == 0 || blocks_.back().GetAvailble() < str.length())
    {
        blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, str.size()));
    }

    items_.emplace_back(blocks_.back().AppendUnsafe(str));
}

void ColumnString::AppendUnsafe(std::string_view str)
{
    items_.emplace_back(blocks_.back().AppendUnsafe(str));
}

void ColumnString::Clear() {
    items_.clear();
    blocks_.clear();
}

std::string_view ColumnString::At(size_t n) const {
    return items_.at(n);
}

std::string_view ColumnString::operator [] (size_t n) const {
    return items_[n];
}

void ColumnString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnString>()) {
        const auto total_size = ComputeTotalSize(col->items_);

        // TODO: fill up existing block with some items and then add a new one for the rest of items
        if (blocks_.size() == 0 || blocks_.back().GetAvailble() < total_size)
            blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, total_size));
        items_.reserve(items_.size() + col->Size());

        for (size_t i = 0; i < column->Size(); ++i) {
            this->AppendUnsafe((*col)[i]);
        }
    }
}

bool ColumnString::Load(CodedInputStream* input, size_t rows) {
    items_.clear();
    blocks_.clear();

    items_.reserve(rows);
    Block * block = nullptr;

    // TODO(performance): unroll a loop to a first row (to get rid of `blocks_.size() == 0` check) and the rest.
    for (size_t i = 0; i < rows; ++i) {
        uint64_t len;
        if (!WireFormat::ReadUInt64(input, &len))
            return false;

        if (blocks_.size() == 0 || len > block->GetAvailble())
            block = &blocks_.emplace_back(std::max<size_t>(DEFAULT_BLOCK_SIZE, len));

        if (!WireFormat::ReadBytes(input, block->GetCurrentWritePos(), len))
            return false;

        items_.emplace_back(block->ConsumeTailAsStringViewUnsafe(len));
    }

    return true;
}

void ColumnString::Save(CodedOutputStream* output) {
    for (const auto & item : items_) {
        WireFormat::WriteString(output, item);
    }
}

size_t ColumnString::Size() const {
    return items_.size();
}

ColumnRef ColumnString::Slice(size_t begin, size_t len) {
    auto result = std::make_shared<ColumnString>();

    if (begin < items_.size()) {
        len = std::min(len, items_.size() - begin);

        result->blocks_.emplace_back(ComputeTotalSize(items_, begin, len));
        for (size_t i = begin; i < begin + len; ++i)
        {
            result->Append(items_[i]);
        }
    }

    return result;
}

void ColumnString::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnString &>(other);
    items_.swap(col.items_);
    blocks_.swap(col.blocks_);
}

ItemView ColumnString::GetItem(size_t index) const {
    return ItemView{Type::String, this->At(index)};
}

}

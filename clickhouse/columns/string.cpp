#include "string.h"
#include "utils.h"

#include "../base/wire_format.h"

namespace {

constexpr size_t DEFAULT_BLOCK_SIZE = 4096;

template <typename Container>
size_t ComputeTotalSize(const Container & strings, size_t begin = 0, size_t len = -1) {
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
    if (str.size() > string_size_) {
        throw ValidationError("Expected string of length not greater than "
                                 + std::to_string(string_size_) + " bytes, received "
                                 + std::to_string(str.size()) + " bytes.");
    }

    if (data_.capacity() - data_.size() < str.size())
    {
        // round up to the next block size
        const auto new_size = (((data_.size() + string_size_) / DEFAULT_BLOCK_SIZE) + 1) * DEFAULT_BLOCK_SIZE;
        data_.reserve(new_size);
    }

    data_.insert(data_.size(), str);
    // Pad up to string_size_ with zeroes.
    const auto padding_size = string_size_ - str.size();
    data_.resize(data_.size() + padding_size, char(0));
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

size_t ColumnFixedString::FixedSize() const {
       return string_size_;
}

void ColumnFixedString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnFixedString>()) {
        if (string_size_ == col->string_size_) {
            data_.insert(data_.end(), col->data_.begin(), col->data_.end());
        }
    }
}

bool ColumnFixedString::LoadBody(InputStream * input, size_t rows) {
    data_.resize(string_size_ * rows);
    if (!WireFormat::ReadBytes(*input, &data_[0], data_.size())) {
        return false;
    }

    return true;
}

void ColumnFixedString::SaveBody(OutputStream* output) {
    WireFormat::WriteBytes(*output, data_.data(), data_.size());
}

size_t ColumnFixedString::Size() const {
    return data_.size() / string_size_;
}

ColumnRef ColumnFixedString::Slice(size_t begin, size_t len) const {
    auto result = std::make_shared<ColumnFixedString>(string_size_);

    if (begin < Size()) {
        const auto b = begin * string_size_;
        const auto l = len * string_size_;
        result->data_ = data_.substr(b, std::min(data_.size() - b, l));
    }

    return result;
}

ColumnRef ColumnFixedString::CloneEmpty() const {
    return std::make_shared<ColumnFixedString>(string_size_);
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

    inline auto GetAvailable() const
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

ColumnString::ColumnString(const std::vector<std::string>& data)
    : ColumnString()
{
    items_.reserve(data.size());
    blocks_.emplace_back(ComputeTotalSize(data));

    for (const auto & s : data)
    {
        AppendUnsafe(s);
    }
};

ColumnString::ColumnString(std::vector<std::string>&& data)
    : ColumnString()
{
    items_.reserve(data.size());

    for (auto&& d : data) {
        append_data_.emplace_back(std::move(d));
        auto& last_data = append_data_.back();
        items_.emplace_back(std::string_view{ last_data.data(),last_data.length() });
    }
}

ColumnString::~ColumnString()
{}

void ColumnString::Append(std::string_view str) {
    if (blocks_.size() == 0 || blocks_.back().GetAvailable() < str.length())
    {
        blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, str.size()));
    }

    items_.emplace_back(blocks_.back().AppendUnsafe(str));
}

void ColumnString::Append(const char* str) {
    Append(std::string_view{ str ,strlen(str) });
}

void ColumnString::Append(std::string&& steal_value) {
    append_data_.emplace_back(std::move(steal_value));
    auto& last_data = append_data_.back();
    items_.emplace_back(std::string_view{ last_data.data(),last_data.length() });
}

void ColumnString::AppendNoManagedLifetime(std::string_view str) {
    items_.emplace_back(str);
}

void ColumnString::AppendUnsafe(std::string_view str) {
    items_.emplace_back(blocks_.back().AppendUnsafe(str));
}

void ColumnString::Clear() {
    items_.clear();
    blocks_.clear();
    append_data_.clear();
    append_data_.shrink_to_fit();
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
        if (blocks_.size() == 0 || blocks_.back().GetAvailable() < total_size)
            blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, total_size));
        items_.reserve(items_.size() + col->Size());

        for (size_t i = 0; i < column->Size(); ++i) {
            this->AppendUnsafe((*col)[i]);
        }
    }
}

void ColumnString::AppendWithMove(ColumnRef column) {
    if (auto col = column->As<ColumnString>()) {
        for (auto&& block : col->blocks_) {
            blocks_.emplace_back(std::move(block));
        }
        col->blocks_.clear();
        col->blocks_.shrink_to_fit();

        for (auto&& ad : col->append_data_) {
            append_data_.emplace_back(std::move(ad));
        }
        col->append_data_.clear();
        col->append_data_.shrink_to_fit();

        items_.reserve(items_.size() + col->Size());
        for (auto&& item : col->items_) {
            items_.emplace_back(std::move(item));
        }
        col->items_.clear();
        col->items_.shrink_to_fit();
    }
}

bool ColumnString::LoadBody(InputStream* input, size_t rows) {
    items_.clear();
    blocks_.clear();

    items_.reserve(rows);
    Block * block = nullptr;

    // TODO(performance): unroll a loop to a first row (to get rid of `blocks_.size() == 0` check) and the rest.
    for (size_t i = 0; i < rows; ++i) {
        uint64_t len;
        if (!WireFormat::ReadUInt64(*input, &len))
            return false;

        if (blocks_.size() == 0 || len > block->GetAvailable())
            block = &blocks_.emplace_back(std::max<size_t>(DEFAULT_BLOCK_SIZE, len));

        if (!WireFormat::ReadBytes(*input, block->GetCurrentWritePos(), len))
            return false;

        items_.emplace_back(block->ConsumeTailAsStringViewUnsafe(len));
    }

    return true;
}

void ColumnString::SaveBody(OutputStream* output) {
    for (const auto & item : items_) {
        WireFormat::WriteString(*output, item);
    }
}

size_t ColumnString::Size() const {
    return items_.size();
}

ColumnRef ColumnString::Slice(size_t begin, size_t len) const {
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

ColumnRef ColumnString::CloneEmpty() const {
    return std::make_shared<ColumnString>();
}

void ColumnString::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnString &>(other);
    items_.swap(col.items_);
    blocks_.swap(col.blocks_);
    append_data_.swap(col.append_data_);
}

ItemView ColumnString::GetItem(size_t index) const {
    return ItemView{Type::String, this->At(index)};
}

}

#include "string.h"

#include "../base/wire_format.h"

#include <cstring>

namespace {

constexpr size_t DEFAULT_BLOCK_SIZE = 4096;

// Upper bound on what LoadBody allocates ahead of the data, since row counts and
// string lengths come from the peer.
constexpr size_t MAX_PREALLOCATED_BYTES = 1 << 20;

// Grows `items` eightfold rather than twofold so that loading many rows copies little.
void AppendItem(std::vector<std::string_view>& items, size_t rows, std::string_view item) {
    if (items.size() == items.capacity()) {
        items.reserve(clickhouse::WireFormat::NextReadCapacity(items.size(), rows, size_t(1)));
    }
    items.emplace_back(item);
}

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

void ColumnFixedString::Reserve(size_t new_cap) {
    data_.reserve(string_size_ * new_cap);
}

void ColumnFixedString::Append(std::string_view str) {
    if (str.size() > string_size_) {
        throw ValidationError("Expected string of length not greater than "
                                 + std::to_string(string_size_) + " bytes, received "
                                 + std::to_string(str.size()) + " bytes.");
    }

    if (data_.capacity() - data_.size() < str.size()) {
        // round up to the next block size
        const auto new_size = (((data_.size() + string_size_) / DEFAULT_BLOCK_SIZE) + 1) * DEFAULT_BLOCK_SIZE;
        data_.reserve(new_size);
    }

    data_.insert(data_.size(), str);
    // Pad up to string_size_ with zeroes.
    if (str.size() < string_size_) {
        const auto padding_size = string_size_ - str.size();
        data_.resize(data_.size() + padding_size, char(0));
    }
}

void ColumnFixedString::Clear() {
    data_.clear();
}

std::string_view ColumnFixedString::At(size_t n) const {
    const auto pos = n * string_size_;
    return std::string_view(&data_.at(pos), string_size_);
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
    return WireFormat::ReadPODArray(*input, &data_, string_size_ * rows);
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
    if (string_size_ != col.string_size_) {
        throw ValidationError("Can't swap FixedString columns when sizes are not the same: "
            + std::to_string(string_size_) + "(this) != "
            + std::to_string(col.string_size_) + "(that)");
    }
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

    // Moves the data, so it must not be called once a string_view points into the block.
    void Grow(size_t new_capacity) {
        std::unique_ptr<CharT[]> new_data(new CharT[new_capacity]);
        if (size > 0) {
            memcpy(new_data.get(), data_.get(), size);
        }
        data_    = std::move(new_data);
        capacity = new_capacity;
    }

    inline auto GetAvailable() const {
        return capacity - size;
    }

    std::string_view AppendUnsafe(std::string_view str) {
        const auto pos = &data_[size];

        // memcpy's source pointer is declared nonnull regardless of the
        // size argument, so an empty string_view backed by std::string()
        // (where data() may be null) trips UBSan on every empty append.
        if (str.size() > 0) {
            memcpy(pos, str.data(), str.size());
            size += str.size();
        }

        return std::string_view(pos, str.size());
    }

    auto GetCurrentWritePos() {
        return &data_[size];
    }

    std::string_view ConsumeTailAsStringViewUnsafe(size_t len) {
        const auto start = &data_[size];
        size += len;
        return std::string_view(start, len);
    }

    size_t size;
    size_t capacity;
    std::unique_ptr<CharT[]> data_;
};

ColumnString::ColumnString()
    : Column(Type::CreateString())
{
}

ColumnString::ColumnString(size_t element_count)
    : Column(Type::CreateString())
{
    items_.reserve(element_count);
    // 16 is arbitrary number, assumption that string values are about ~256 bytes long.
    blocks_.reserve(std::max<size_t>(1, element_count / 16));
}

ColumnString::ColumnString(const std::vector<std::string>& data)
    : ColumnString()
{
    items_.reserve(data.size());
    blocks_.emplace_back(ComputeTotalSize(data));

    for (const auto & s : data) {
        AppendUnsafe(s);
    }
}

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

void ColumnString::Reserve(size_t new_cap) {
    items_.reserve(new_cap);
    // 16 is arbitrary number, assumption that string values are about ~256 bytes long.
    blocks_.reserve(std::max<size_t>(1, new_cap / 16));
}

void ColumnString::Append(std::string_view str) {
    if (blocks_.size() == 0 || blocks_.back().GetAvailable() < str.length()) {
        blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, str.size()));
    }

    items_.emplace_back(blocks_.back().AppendUnsafe(str));
}

void ColumnString::Append(const char* str) {
    Append(std::string_view(str, strlen(str)));
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
}

std::string_view ColumnString::At(size_t n) const {
    return items_.at(n);
}

void ColumnString::Append(ColumnRef column) {
    if (auto col = column->As<ColumnString>()) {
        const auto total_size = ComputeTotalSize(col->items_);

        // TODO: fill up existing block with some items and then add a new one for the rest of items
        if (blocks_.size() == 0 || blocks_.back().GetAvailable() < total_size)
            blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, total_size));

        // Intentionally not doing items_.reserve() since that cripples performance.
        for (size_t i = 0; i < column->Size(); ++i) {
            this->AppendUnsafe((*col)[i]);
        }
    }
}

bool ColumnString::LoadBody(InputStream* input, size_t rows) {
    if (rows == 0) {
        items_.clear();
        blocks_.clear();

        return true;
    }

    decltype(items_) new_items;
    decltype(blocks_) new_blocks;

    new_items.reserve(std::min(rows, MAX_PREALLOCATED_BYTES / sizeof(std::string_view)));

    // Suboptimzal if the first row string is >DEFAULT_BLOCK_SIZE, but that must be a very rare case.
    Block * block = &new_blocks.emplace_back(DEFAULT_BLOCK_SIZE);

    for (size_t i = 0; i < rows; ++i) {
        uint64_t len;
        if (!WireFormat::ReadUInt64(*input, &len))
            return false;

        if (len > MAX_PREALLOCATED_BYTES) {
            if (len > SIZE_MAX) return false;

            // A block of its own, grown as the bytes arrive.
            block = &new_blocks.emplace_back(MAX_PREALLOCATED_BYTES);
            while (block->size < len) {
                if (block->GetAvailable() == 0)
                    block->Grow(WireFormat::NextReadCapacity(block->capacity, static_cast<size_t>(len), size_t(0)));

                const size_t chunk = std::min<size_t>(static_cast<size_t>(len) - block->size, block->GetAvailable());
                if (!WireFormat::ReadBytes(*input, block->GetCurrentWritePos(), chunk)) return false;
                block->size += chunk;
            }

            AppendItem(new_items, rows, std::string_view(block->data_.get(), block->size));
            continue;
        }

        if (len > block->GetAvailable())
            block = &new_blocks.emplace_back(std::max<size_t>(DEFAULT_BLOCK_SIZE, len));

        if (!WireFormat::ReadBytes(*input, block->GetCurrentWritePos(), len))
            return false;

        AppendItem(new_items, rows, block->ConsumeTailAsStringViewUnsafe(len));
    }

    items_.swap(new_items);
    blocks_.swap(new_blocks);

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
        result->items_.reserve(len);

        result->blocks_.emplace_back(ComputeTotalSize(items_, begin, len));
        for (size_t i = begin; i < begin + len; ++i) {
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

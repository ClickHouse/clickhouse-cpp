#include "block.h"

namespace clickhouse {

Block::Iterator::Iterator(const Block& block)
    : block_(block)
    , idx_(0)
{
}

const std::string& Block::Iterator::Name() const {
    return block_.columns_[idx_].name;
}

const std::string& Block::Iterator::Type() const {
    return block_.columns_[idx_].type;
}

ColumnRef Block::Iterator::Column() const {
    return block_.columns_[idx_].column;
}

void Block::Iterator::Next() {
    ++idx_;
}

bool Block::Iterator::IsValid() const {
    return idx_ < block_.columns_.size();
}


Block::Block()
    : rows_(0)
{
}

Block::Block(size_t cols, size_t rows)
    : rows_(rows)
{
    columns_.reserve(cols);
}

Block::~Block() = default;

void Block::AppendColumn(
    const std::string& name,
    const std::string& type,
    const ColumnRef& col)
{
    columns_.push_back(ColumnItem{name, type, col});
}

/// Count of columns in the block.
size_t Block::Columns() const {
    return columns_.size();
}

/// Count of rows in the block.
size_t Block::Rows() const {
    return rows_;
}

}

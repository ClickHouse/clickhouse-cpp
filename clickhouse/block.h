#pragma once

#include "columns.h"

namespace clickhouse {

/**
 * Represents one row of a block.
 */
class Row {
public:
    /// Number of fields in the row.
    size_t Size() const;
};

class Block {
public:
    class Iterator {
    public:
        Iterator(const Block& block);

        const std::string& Name() const;

        const std::string& Type() const;

        ColumnRef Column() const;

        void Next();

        bool IsValid() const;

    private:
        Iterator() = delete;

        const Block& block_;
        size_t idx_;
    };

public:
     Block();
     Block(size_t cols, size_t rows);
    ~Block();

    /// Append named column to the block.
    void AppendColumn(const std::string& name,
                      const std::string& type,
                      const ColumnRef& col);

    /// Count of columns in the block.
    size_t Columns() const;

    /// Count of rows in the block.
    size_t Rows() const;

private:
    struct ColumnItem {
        std::string name;
        std::string type;
        ColumnRef   column;
    };

    std::vector<ColumnItem> columns_;
    /// Count of rows in the block.
    size_t rows_;
};

}

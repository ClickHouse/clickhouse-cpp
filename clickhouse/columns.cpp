#include "columns.h"
#include "type_parser.h"
#include "wire_format.h"

#include <iostream>

namespace clickhouse {

ColumnFixedString::ColumnFixedString(size_t n)
    : string_size_(n)
{
}

void ColumnFixedString::Append(const std::string& str) {
    data_.push_back(str);
    data_.back().resize(string_size_);
}

size_t ColumnFixedString::Size() const {
    return data_.size();
}

bool ColumnFixedString::Print(std::basic_ostream<char>& output, size_t row) {
    output << data_.at(row);
    return true;
}

bool ColumnFixedString::Load(CodedInputStream* input, size_t rows) {
    for (size_t i = 0; i < rows; ++i) {
        std::string s;
        s.resize(string_size_);

        if (!WireFormat::ReadBytes(input, &s[0], s.size())) {
            return false;
        }

        data_.push_back(s);
    }

    return true;
}

void ColumnFixedString::Save(CodedOutputStream* output) {
    for (size_t i = 0; i < data_.size(); ++i) {
        WireFormat::WriteBytes(output, data_[i].data(), string_size_);
    }
}


void ColumnString::Append(const std::string& str) {
    data_.push_back(str);
}

size_t ColumnString::Size() const {
    return data_.size();
}

bool ColumnString::Print(std::basic_ostream<char>& output, size_t row) {
    output << data_.at(row);
    return true;
}

bool ColumnString::Load(CodedInputStream* input, size_t rows) {
    for (size_t i = 0; i < rows; ++i) {
        std::string s;

        if (!WireFormat::ReadString(input, &s)) {
            return false;
        }

        data_.push_back(s);
    }

    return true;
}

void ColumnString::Save(CodedOutputStream* output) {
    for (size_t i = 0; i < data_.size(); ++i) {
        WireFormat::WriteString(output, data_[i]);
    }
}


ColumnTuple::ColumnTuple(const std::vector<ColumnRef>& columns)
    : columns_(columns)
{
}

size_t ColumnTuple::Size() const {
    return columns_.empty() ? 0 : columns_[0]->Size();
}

bool ColumnTuple::Print(std::basic_ostream<char>& output, size_t row) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ) {
        if (!(*ci)->Print(output, row)) {
            return false;
        }

        if (++ci != columns_.end()) {
            output << ", ";
        }
    }

    return true;
}

bool ColumnTuple::Load(CodedInputStream* input, size_t rows) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
        if (!(*ci)->Load(input, rows)) {
            return false;
        }
    }

    return true;
}

void ColumnTuple::Save(CodedOutputStream* output) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
        (*ci)->Save(output);
    }
}


static ColumnRef CreateColumnFromAst(const TypeAst& ast);

static ColumnRef CreateTupleColumn(const TypeAst& ast) {
    std::vector<ColumnRef> columns;

    for (const auto& elem : ast.elements) {
        if (auto col = CreateColumnFromAst(elem)) {
            columns.push_back(col);
        } else {
            return nullptr;
        }
    }

    return std::make_shared<ColumnTuple>(columns);
}

static ColumnRef CreateTerminalColumn(const TypeAst& ast) {
    if (ast.name == "UInt8")
        return std::make_shared<ColumnUInt8>();
    if (ast.name == "UInt16")
        return std::make_shared<ColumnUInt16>();
    if (ast.name == "UInt32")
        return std::make_shared<ColumnUInt32>();
    if (ast.name == "UInt64")
        return std::make_shared<ColumnUInt64>();

    if (ast.name == "Int8")
        return std::make_shared<ColumnInt8>();
    if (ast.name == "Int16")
        return std::make_shared<ColumnInt16>();
    if (ast.name == "Int32")
        return std::make_shared<ColumnInt32>();
    if (ast.name == "Int64")
        return std::make_shared<ColumnInt64>();

    if (ast.name == "Float32")
        return std::make_shared<ColumnFloat32>();
    if (ast.name == "Float64")
        return std::make_shared<ColumnFloat64>();

    if (ast.name == "String")
        return std::make_shared<ColumnString>();
    if (ast.name == "FixedString")
        return std::make_shared<ColumnFixedString>(ast.elements.front().size);

    if (ast.name == "DateTime")
        return std::make_shared<ColumnDateTime>();
    if (ast.name == "Date")
        return std::make_shared<ColumnDate>();

    return nullptr;
}

static ColumnRef CreateColumnFromAst(const TypeAst& ast) {
    if (ast.meta == TypeAst::Terminal) {
        return CreateTerminalColumn(ast);
    }
    if (ast.meta == TypeAst::Tuple) {
        return CreateTupleColumn(ast);
    }
    return nullptr;
}

ColumnRef CreateColumnByType(const std::string& type_name) {
    TypeParser parser(type_name);
    TypeAst ast;

    if (parser.Parse(&ast)) {
        if (ast.meta == TypeAst::Terminal) {
            return CreateTerminalColumn(ast);
        }
        if (ast.meta == TypeAst::Tuple) {
            return CreateTupleColumn(ast);
        }
        // TODO
    }

    return nullptr;
}

}

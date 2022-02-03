#include "ip4.h"

#include "../base/socket.h" // for platform-specific IPv4-related functions
#include <stdexcept>

namespace clickhouse {

ColumnIPv4::ColumnIPv4()
    : Column(Type::CreateIPv4())
    , data_(std::make_shared<ColumnUInt32>())
{
}

ColumnIPv4::ColumnIPv4(ColumnRef data)
    : Column(Type::CreateIPv4())
    , data_(data->As<ColumnUInt32>())
{
    if (data_->Size() != 0) {
        throw std::runtime_error("number of entries must be even (32-bit numbers for each IPv4)");
    }
}

void ColumnIPv4::Append(const std::string& str) {
    uint32_t address;
    if (inet_pton(AF_INET, str.c_str(), &address) != 1)
        throw std::runtime_error("invalid IPv4 format, ip: " + str);
    data_->Append(htonl(address));
}

void ColumnIPv4::Append(uint32_t ip) {
    data_->Append(htonl(ip));
}

void ColumnIPv4::Append(in_addr ip)
{
    data_->Append(htonl(ip.s_addr));
}

void ColumnIPv4::Clear() {
    data_->Clear();
}

in_addr ColumnIPv4::At(size_t n) const {
    in_addr addr;
    addr.s_addr = ntohl(data_->At(n));
    return addr;
}

in_addr ColumnIPv4::operator [] (size_t n) const {
    in_addr addr;
    addr.s_addr = ntohl(data_->operator[](n));
    return addr;
}

std::string ColumnIPv4::AsString(size_t n) const {
    return inet_ntoa(this->At(n));
}

void ColumnIPv4::Append(ColumnRef column) {
    if (auto col = column->As<ColumnIPv4>()) {
        data_->Append(col->data_);
    }
}

bool ColumnIPv4::Load(InputStream * input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnIPv4::Save(OutputStream* output) {
    data_->Save(output);
}

size_t ColumnIPv4::Size() const {
    return data_->Size();
}

ColumnRef ColumnIPv4::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnIPv4>(data_->Slice(begin, len));
}

void ColumnIPv4::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnIPv4 &>(other);
    data_.swap(col.data_);
}

ItemView ColumnIPv4::GetItem(size_t index) const {
    return data_->GetItem(index);
}

}

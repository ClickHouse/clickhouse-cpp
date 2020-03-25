
#include "ip6.h"

#include <stdexcept>

namespace clickhouse {

static_assert(sizeof(struct in6_addr) == 16, "sizeof in6_addr should be 16 bytes");

ColumnIPv6::ColumnIPv6()
    : Column(Type::CreateIPv6())
    , data_(std::make_shared<ColumnFixedString>(16))
{
}

ColumnIPv6::ColumnIPv6(ColumnRef data)
    : Column(Type::CreateIPv6())
    , data_(data->As<ColumnFixedString>())
{
    if (data_->Size() != 0) {
        throw std::runtime_error("number of entries must be even (two 64-bit numbers for each IPv6)");
    }
}

void ColumnIPv6::Append(const std::string& ip) {
    unsigned char buf[16];
    if (inet_pton(AF_INET6, ip.c_str(), buf) != 1) {
        throw std::runtime_error("invalid IPv6 format, ip: " + ip);
    }
    data_->Append(std::string((const char*)buf, 16));
}

void ColumnIPv6::Append(const in6_addr* addr) {
    data_->Append(std::string((const char*)addr->s6_addr, 16));
}

void ColumnIPv6::Clear() {
    data_->Clear();
}

std::string ColumnIPv6::AsString (size_t n) const{
    const auto& addr = data_->At(n);
    char buf[INET6_ADDRSTRLEN];
    const char* ip_str = inet_ntop(AF_INET6, addr.data(), buf, INET6_ADDRSTRLEN);
    if (ip_str == nullptr) {
        throw std::runtime_error("invalid IPv6 format: " + std::string(addr));
    }
    return ip_str;
}

in6_addr ColumnIPv6::At(size_t n) const {
    return *reinterpret_cast<const in6_addr*>(data_->At(n).data());
}

in6_addr ColumnIPv6::operator [] (size_t n) const {
    return *reinterpret_cast<const in6_addr*>(data_->At(n).data());
}

void ColumnIPv6::Append(ColumnRef column) {
    if (auto col = column->As<ColumnIPv6>()) {
        data_->Append(col->data_);
    }
}

bool ColumnIPv6::Load(CodedInputStream* input, size_t rows) {
    return data_->Load(input, rows);
}

void ColumnIPv6::Save(CodedOutputStream* output) {
    data_->Save(output);
}

size_t ColumnIPv6::Size() const {
    return data_->Size();
}

ColumnRef ColumnIPv6::Slice(size_t begin, size_t len) {
    return std::make_shared<ColumnIPv6>(data_->Slice(begin, len));
}

void ColumnIPv6::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnIPv6 &>(other);
    data_.swap(col.data_);
}

ItemView ColumnIPv6::GetItem(size_t index) const {
    return data_->GetItem(index);
}

}

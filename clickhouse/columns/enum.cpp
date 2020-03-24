#include "enum.h"
#include "utils.h"

namespace clickhouse {

template <typename T>
ColumnEnum<T>::ColumnEnum(TypeRef type)
    : Column(type)
{
}

template <typename T>
ColumnEnum<T>::ColumnEnum(TypeRef type, const std::vector<T>& data)
    : Column(type)
    , data_(data)
{
}

template <typename T>
void ColumnEnum<T>::Append(const T& value, bool checkValue) {
    if  (checkValue) {
        // TODO: type_->HasEnumValue(value), "Enum type doesn't have value " + std::to_string(value);
    }
    data_.push_back(value);
}

template <typename T>
void ColumnEnum<T>::Append(const std::string& name) {
    data_.push_back(type_->As<EnumType>()->GetEnumValue(name));
}

template <typename T>
void ColumnEnum<T>::Clear() {
    data_.clear();
}

template <typename T>
const T& ColumnEnum<T>::At(size_t n) const {
    return data_.at(n);
}

template <typename T>
const std::string ColumnEnum<T>::NameAt(size_t n) const {
    return type_->As<EnumType>()->GetEnumName(data_.at(n));
}

template <typename T>
const T& ColumnEnum<T>::operator[] (size_t n) const {
    return data_[n];
}

template <typename T>
void ColumnEnum<T>::SetAt(size_t n, const T& value, bool checkValue) {
    if (checkValue) {
        // TODO: type_->HasEnumValue(value), "Enum type doesn't have value " + std::to_string(value);
    }
    data_.at(n) = value;
}

template <typename T>
void ColumnEnum<T>::SetNameAt(size_t n, const std::string& name) {
    data_.at(n) = type_->As<EnumType>()->GetEnumValue(name);
}

template <typename T>
void ColumnEnum<T>::Append(ColumnRef column) {
    if (auto col = column->As<ColumnEnum<T>>()) {
        data_.insert(data_.end(), col->data_.begin(), col->data_.end());
    }
}

template <typename T>
bool ColumnEnum<T>::Load(CodedInputStream* input, size_t rows) {
    data_.resize(rows);
    return input->ReadRaw(data_.data(), data_.size() * sizeof(T));
}

template <typename T>
void ColumnEnum<T>::Save(CodedOutputStream* output) {
    output->WriteRaw(data_.data(), data_.size() * sizeof(T));
}

template <typename T>
size_t ColumnEnum<T>::Size() const {
    return data_.size();
}

template <typename T>
ColumnRef ColumnEnum<T>::Slice(size_t begin, size_t len) {
    return std::make_shared<ColumnEnum<T>>(type_, SliceVector(data_, begin, len));
}

template <typename T>
void ColumnEnum<T>::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnEnum<T> &>(other);
    data_.swap(col.data_);
}

template <typename T>
ItemView ColumnEnum<T>::GetItem(size_t index) const {
    return ItemView{type_->GetCode(), data_[index]};
}

template class ColumnEnum<int8_t>;
template class ColumnEnum<int16_t>;

}

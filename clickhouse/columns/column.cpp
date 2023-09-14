#include "column.h"

#include "../base/wire_format.h"

namespace clickhouse {

bool Column::Load(InputStream* input, size_t rows) {
    assert(serialization_);
    return serialization_->LoadPrefix(this, input, rows)
        && serialization_->LoadBody(this, input, rows);
}

/// Saves column data to output stream.
void Column::Save(OutputStream* output) {
    assert(serialization_);
    serialization_->SavePrefix(this, output);
    serialization_->SaveBody(this,output);
}

bool Column::LoadSerializationKind(InputStream* input) {
    uint8_t kind;
    if (!WireFormat::ReadFixed(*input, &kind)) {
        return false;
    }
    SetSerializationKind(static_cast<Serialization::Kind>(kind));
    return true;
}

void Column::SaveSerializationKind(OutputStream* output) {
    assert(serialization_);
    WireFormat::WriteFixed(*output, static_cast<uint8_t>(serialization_->GetKind()));
}

SerializationRef Column::GetSerialization() {
    assert(serialization_);
    return serialization_;
}

bool Column::HasCustomSerialization() const {
    assert(serialization_);
    return serialization_->GetKind() != Serialization::Kind::DEFAULT;
}

}

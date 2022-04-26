#include "column.h"

namespace clickhouse {

bool Column::LoadPrefix([[maybe_unused]] InputStream* input, [[maybe_unused]] size_t rows) {
    /// does nothing by default
    return true;
}

bool Column::LoadBody([[maybe_unused]] InputStream* input, [[maybe_unused]] size_t rows) {
    /// does nothing by default
    return true;
}

bool Column::Load(InputStream* input, size_t rows) {
    return LoadPrefix(input, rows) && LoadBody(input, rows);
}

void Column::SavePrefix([[maybe_unused]] OutputStream* output) {
    /// does nothing by default
}

void Column::SaveBody([[maybe_unused]] OutputStream* output) {
    /// does nothing by default
}

void Column::SaveSuffix([[maybe_unused]] OutputStream* output) {
    /// does nothing by default
}

/// Saves column data to output stream.
void Column::Save(OutputStream* output) {
    SavePrefix(output);
    SaveBody(output);
    SaveSuffix(output);
}

}

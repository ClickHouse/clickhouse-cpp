#include <clickhouse/columns/numeric.h>

#include <cstdlib>
#include <new>

#include "utils.h"

namespace {
thread_local bool track_allocations    = false;
thread_local size_t largest_allocation = 0;
}  // namespace

// The default operator new[], operator delete[], and nothrow forms forward to these.
void* operator new(std::size_t size) {
    if (track_allocations && size > largest_allocation) {
        largest_allocation = size;
    }
    if (void* ptr = std::malloc(size ? size : 1)) {
        return ptr;
    }
    throw std::bad_alloc();
}

void operator delete(void* ptr) noexcept {
    std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept {
    std::free(ptr);
}

LargestAllocationScope::LargestAllocationScope() {
    largest_allocation = 0;
    track_allocations  = true;
}

LargestAllocationScope::~LargestAllocationScope() {
    track_allocations = false;
}

size_t LargestAllocationScope::Get() const {
    return largest_allocation;
}

bool LargestAllocationScope::ObservesLibraryAllocations() {
    constexpr size_t rows = 1 << 20;
    LargestAllocationScope allocations;
    clickhouse::ColumnUInt64 column;
    column.Reserve(rows);
    return allocations.Get() >= rows * sizeof(uint64_t);
}

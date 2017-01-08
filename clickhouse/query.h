#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace clickhouse {

/**
 * Settings of individual query.
 */
struct QuerySettings {
    /// Максимальное количество потоков выполнения запроса. По-умолчанию - определять автоматически.
    int max_threads = 0;
    /// Считать минимумы и максимумы столбцов результата
    bool extremes = false;
    /// Тихо пропускать недоступные шарды.
    bool skip_unavailable_shards = false;
    /// Write statistics about read rows, bytes, time elapsed, etc.
    bool output_format_write_statistics = true;
    /// Use client timezone for interpreting DateTime string values, instead of adopting server timezone.
    bool use_client_time_zone = false;

    // connect_timeout
    // max_block_size
    // distributed_group_by_no_merge = false
    // strict_insert_defaults = 0
    // network_compression_method = LZ4
    // priority = 0
};


struct Exception {
    int code = 0;
    std::string name;
    std::string display_text;
    std::string stack_trace;
    /// Pointer tp nested exception.
    std::unique_ptr<Exception> nested;
};


struct Profile {
    uint64_t rows = 0;
    uint64_t blocks = 0;
    uint64_t bytes = 0;
    uint64_t rows_before_limit = 0;
    bool applied_limit = false;
    bool calculated_rows_before_limit = false;
};


struct Progress {
    uint64_t rows = 0;
    uint64_t bytes = 0;
    uint64_t total_rows = 0;
};


class QueryEvents {
public:
    virtual ~QueryEvents()
    { }

    /// Some data was received.
    virtual void OnData() = 0;

    virtual void OnServerException(const Exception& e) = 0;

    virtual void OnProfile(const Profile& profile) = 0;

    virtual void OnProgress(const Progress& progress) = 0;
};


}

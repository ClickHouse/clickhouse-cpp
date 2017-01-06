#pragma once

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


}

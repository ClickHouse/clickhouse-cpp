#pragma once

#include "block.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace clickhouse {

/**
 * Settings of individual query.
 */
struct QuerySettings {
    /// Максимальное количество потоков выполнения запроса. По-умолчанию - определять автоматически.
    int max_threads = 0;
    /// Считать минимумы и максимумы столбцов результата.
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
    /// Pointer to nested exception.
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
    virtual void OnData(const Block& block) = 0;
    virtual bool OnDataCancelable(const Block& block) = 0;

    virtual void OnServerException(const Exception& e) = 0;

    virtual void OnProfile(const Profile& profile) = 0;

    virtual void OnProgress(const Progress& progress) = 0;

    virtual void OnFinish() = 0;
};


using ExceptionCallback        = std::function<void(const Exception& e)>;
using ProgressCallback         = std::function<void(const Progress& progress)>;
using SelectCallback           = std::function<void(const Block& block)>;
using SelectCancelableCallback = std::function<bool(const Block& block)>;


class Query : public QueryEvents {
public:
     Query();
     Query(const char* query);
     Query(const std::string& query);
    ~Query();

    ///
    inline std::string GetText() const {
        return query_;
    }

    /// Set handler for receiving result data.
    inline Query& OnData(SelectCallback cb) {
        select_cb_ = cb;
        return *this;
    }

    inline Query& OnDataCancelable(SelectCancelableCallback cb) {
        select_cancelable_cb_ = cb;
        return *this;
    }

    /// Set handler for receiving server's exception.
    inline Query& OnException(ExceptionCallback cb) {
        exception_cb_ = cb;
        return *this;
    }


    /// Set handler for receiving a progress of query exceution.
    inline Query& OnProgress(ProgressCallback cb) {
        progress_cb_ = cb;
        return *this;
    }

private:
    void OnData(const Block& block) override {
        if (select_cb_) {
            select_cb_(block);
        }
    }

    bool OnDataCancelable(const Block& block) override {
        if (select_cancelable_cb_) {
            return select_cancelable_cb_(block);
        } else {
            return true;
        }
    }

    void OnServerException(const Exception& e) override {
        if (exception_cb_) {
            exception_cb_(e);
        }
    }

    void OnProfile(const Profile& profile) override {
        (void)profile;
    }

    void OnProgress(const Progress& progress) override {
        if (progress_cb_) {
            progress_cb_(progress);
        }
    }

    void OnFinish() override {
    }

private:
    std::string query_;
    ExceptionCallback exception_cb_;
    ProgressCallback progress_cb_;
    SelectCallback select_cb_;
    SelectCancelableCallback select_cancelable_cb_;
};

}

#pragma once

#include "client.h"

namespace clickhouse {
class ThreadSafeClient {
public:
    explicit ThreadSafeClient(const ClientOptions& opts);

    /// Intends for execute arbitrary queries.
    void Execute(const Query& query);

    /// Intends for execute select queries.  Data will be returned with
    /// one or more call of \p cb.
    void Select(const std::string& query, SelectCallback cb);

    /// Executes a select query which can be canceled by returning false from
    /// the data handler function \p cb.
    void SelectCancelable(const std::string& query, SelectCancelableCallback cb);

    /// Alias for Execute.
    void Select(const Query& query);

    /// Intends for insert block of data into a table \p table_name.
    void Insert(const std::string& table_name, const Block& block);

    /// Ping server for aliveness.
    void Ping();

    /// Reset connection with initial params.
    void ResetConnection();

    [[nodiscard]] const ServerInfo& GetServerInfo() const;

private:
    Client client_;

    std::recursive_mutex stream_mutex_;
};
}  // namespace clickhouse

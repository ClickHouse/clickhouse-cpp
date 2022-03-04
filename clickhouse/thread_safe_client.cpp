#include "thread_safe_client.h"

#include <utility>

namespace clickhouse {

ThreadSafeClient::ThreadSafeClient(const ClientOptions& opts) : client_(opts) {
}

void ThreadSafeClient::Execute(const Query& query) {
    auto lock = std::unique_lock(stream_mutex_);
    client_.Execute(query);
}

void ThreadSafeClient::Select(const std::string& query, SelectCallback cb) {
    auto lock = std::unique_lock(stream_mutex_);
    client_.Select(query, std::move(cb));
}

void ThreadSafeClient::SelectCancelable(const std::string& query, SelectCancelableCallback cb) {
    auto lock = std::unique_lock(stream_mutex_);
    client_.SelectCancelable(query, std::move(cb));
}

void ThreadSafeClient::Select(const Query& query) {
    auto lock = std::unique_lock(stream_mutex_);
    client_.Select(query);
}

void ThreadSafeClient::Insert(const std::string& table_name, const Block& block) {
    auto lock = std::unique_lock(stream_mutex_);
    client_.Insert(table_name, block);
}

void ThreadSafeClient::Ping() {
    auto lock = std::unique_lock(stream_mutex_);
    client_.Ping();
}

void ThreadSafeClient::ResetConnection() {
    auto lock = std::unique_lock(stream_mutex_);
    client_.ResetConnection();
}

const ServerInfo& ThreadSafeClient::GetServerInfo() const {
    return client_.GetServerInfo();
}

}  // namespace clickhouse
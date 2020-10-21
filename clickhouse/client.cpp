#include "client.h"
#include "protocol.h"

#include "base/coded.h"
#include "base/compressed.h"
#include "base/socket.h"
#include "base/wire_format.h"

#include "columns/factory.h"

#include <cityhash/city.h>
#include <lz4/lz4.h>

#include <assert.h>
#include <atomic>
#include <system_error>
#include <thread>
#include <vector>
#include <sstream>
#include <stdexcept>

#define DBMS_NAME                                       "ClickHouse"
#define DBMS_VERSION_MAJOR                              1
#define DBMS_VERSION_MINOR                              2

#define REVISION                                        54405

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         50264
#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS   51554
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO               51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO              54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060
//#define DBMS_MIN_REVISION_WITH_TABLES_STATUS            54226
//#define DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE 54337
#define DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME      54372
#define DBMS_MIN_REVISION_WITH_VERSION_PATCH            54401
#define DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE     54405

namespace clickhouse {

struct ClientInfo {
    uint8_t iface_type = 1; // TCP
    uint8_t query_kind;
    std::string initial_user;
    std::string initial_query_id;
    std::string quota_key;
    std::string os_user;
    std::string client_hostname;
    std::string client_name;
    std::string initial_address = "[::ffff:127.0.0.1]:0";
    uint64_t client_version_major = 0;
    uint64_t client_version_minor = 0;
    uint64_t client_version_patch = 0;
    uint32_t client_revision = 0;
};

std::ostream& operator<<(std::ostream& os, const ClientOptions& opt) {
    os << "Client(" << opt.user << '@' << opt.host << ":" << opt.port
       << " ping_before_query:" << opt.ping_before_query
       << " send_retries:" << opt.send_retries
       << " retry_timeout:" << opt.retry_timeout.count()
       << " compression_method:"
       << (opt.compression_method == CompressionMethod::LZ4 ? "LZ4" : "None")
       << ")";
    return os;
}

class Client::Impl {
public:
     Impl(const ClientOptions& opts);
    ~Impl();

    void ExecuteQuery(Query query);

    void SendCancel();

    void Insert(const std::string& table_name, const Block& block);

    void Ping();

    void ResetConnection();

    const ServerInfo& GetServerInfo() const;

private:
    bool Handshake();

    bool ReceivePacket(uint64_t* server_packet = nullptr);

    void SendQuery(const std::string& query);

    void SendData(const Block& block);

    bool SendHello();

    bool ReadBlock(Block* block, CodedInputStream* input);

    bool ReceiveHello();

    /// Reads data packet form input stream.
    bool ReceiveData();

    /// Reads exception packet form input stream.
    bool ReceiveException(bool rethrow = false);

    void WriteBlock(const Block& block, CodedOutputStream* output);

private:
    /// In case of network errors tries to reconnect to server and
    /// call fuc several times.
    void RetryGuard(std::function<void()> fuc);

private:
    class EnsureNull {
    public:
        inline EnsureNull(QueryEvents* ev, QueryEvents** ptr)
            : ptr_(ptr)
        {
            if (ptr_) {
                *ptr_ = ev;
            }
        }

        inline ~EnsureNull() {
            if (ptr_) {
                *ptr_ = nullptr;
            }
        }

    private:
        QueryEvents** ptr_;

    };


    const ClientOptions options_;
    QueryEvents* events_;
    int compression_ = CompressionState::Disable;

    SocketHolder socket_;

    SocketInput socket_input_;
    BufferedInput buffered_input_;
    CodedInputStream input_;

    SocketOutput socket_output_;
    BufferedOutput buffered_output_;
    CodedOutputStream output_;

    ServerInfo server_info_;
};


Client::Impl::Impl(const ClientOptions& opts)
    : options_(opts)
    , events_(nullptr)
    , socket_(-1)
    , socket_input_(socket_)
    , buffered_input_(&socket_input_)
    , input_(&buffered_input_)
    , socket_output_(socket_)
    , buffered_output_(&socket_output_)
    , output_(&buffered_output_)
{
    // TODO: throw on big-endianness of platform

    for (unsigned int i = 0; ; ) {
        try {
            ResetConnection();
            break;
        } catch (const std::system_error&) {
            if (++i > options_.send_retries) {
                throw;
            }

            std::this_thread::sleep_for(options_.retry_timeout);
        }
    }

    if (options_.compression_method != CompressionMethod::None) {
        compression_ = CompressionState::Enable;
    }
}

Client::Impl::~Impl()
{ }

void Client::Impl::ExecuteQuery(Query query) {
    EnsureNull en(static_cast<QueryEvents*>(&query), &events_);

    if (options_.ping_before_query) {
        RetryGuard([this]() { Ping(); });
    }

    SendQuery(query.GetText());

    while (ReceivePacket()) {
        ;
    }
}

void Client::Impl::Insert(const std::string& table_name, const Block& block) {
    if (options_.ping_before_query) {
        RetryGuard([this]() { Ping(); });
    }

    std::vector<std::string> fields;
    fields.reserve(block.GetColumnCount());

    // Enumerate all fields
    for (unsigned int i = 0; i < block.GetColumnCount(); i++) {
        fields.push_back(block.GetColumnName(i));
    }

    std::stringstream fields_section;

    for (auto elem = fields.begin(); elem != fields.end(); ++elem) {
        if (std::distance(elem, fields.end()) == 1) {
            fields_section << *elem;
        } else {
            fields_section << *elem << ",";
        }
    }

    SendQuery("INSERT INTO " + table_name + " ( " + fields_section.str() + " ) VALUES");

    uint64_t server_packet;
    // Receive data packet.
    while (true) {
        bool ret = ReceivePacket(&server_packet);

        if (!ret) {
            throw std::runtime_error("fail to receive data packet");
        }
        if (server_packet == ServerCodes::Data) {
            break;
        }
        if (server_packet == ServerCodes::Progress) {
            continue;
        }
    }

    // Send data.
    SendData(block);
    // Send empty block as marker of
    // end of data.
    SendData(Block());

    // Wait for EOS.
    uint64_t eos_packet{0};
    while (ReceivePacket(&eos_packet)) {
        ;
    }

    if (eos_packet != ServerCodes::EndOfStream && eos_packet != ServerCodes::Exception
        && eos_packet != ServerCodes::Log && options_.rethrow_exceptions) {
        throw std::runtime_error(std::string{"unexpected packet from server while receiving end of query, expected (expected Exception, EndOfStream or Log, got: "}
                            + (eos_packet ? std::to_string(eos_packet) : "nothing") + ")");
    }
}

void Client::Impl::Ping() {
    WireFormat::WriteUInt64(&output_, ClientCodes::Ping);
    output_.Flush();

    uint64_t server_packet;
    const bool ret = ReceivePacket(&server_packet);

    if (!ret || server_packet != ServerCodes::Pong) {
        throw std::runtime_error("fail to ping server");
    }
}

void Client::Impl::ResetConnection() {
    SocketHolder s(SocketConnect(NetworkAddress(options_.host, std::to_string(options_.port))));

    if (s.Closed()) {
        throw std::system_error(errno, std::system_category());
    }

    if (options_.tcp_keepalive) {
        s.SetTcpKeepAlive(options_.tcp_keepalive_idle.count(),
                          options_.tcp_keepalive_intvl.count(),
                          options_.tcp_keepalive_cnt);
    }

    socket_ = std::move(s);
    socket_input_ = SocketInput(socket_);
    socket_output_ = SocketOutput(socket_);
    buffered_input_.Reset();
    buffered_output_.Reset();

    if (!Handshake()) {
        throw std::runtime_error("fail to connect to " + options_.host);
    }
}

const ServerInfo& Client::Impl::GetServerInfo() const {
    return server_info_;
}

bool Client::Impl::Handshake() {
    if (!SendHello()) {
        return false;
    }
    if (!ReceiveHello()) {
        return false;
    }
    return true;
}

bool Client::Impl::ReceivePacket(uint64_t* server_packet) {
    uint64_t packet_type = 0;

    if (!input_.ReadVarint64(&packet_type)) {
        return false;
    }
    if (server_packet) {
        *server_packet = packet_type;
    }

    switch (packet_type) {
    case ServerCodes::Data: {
        if (!ReceiveData()) {
            throw std::runtime_error("can't read data packet from input stream");
        }
        return true;
    }

    case ServerCodes::Exception: {
        ReceiveException();
        return false;
    }

    case ServerCodes::ProfileInfo: {
        Profile profile;

        if (!WireFormat::ReadUInt64(&input_, &profile.rows)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(&input_, &profile.blocks)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(&input_, &profile.bytes)) {
            return false;
        }
        if (!WireFormat::ReadFixed(&input_, &profile.applied_limit)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(&input_, &profile.rows_before_limit)) {
            return false;
        }
        if (!WireFormat::ReadFixed(&input_, &profile.calculated_rows_before_limit)) {
            return false;
        }

        if (events_) {
            events_->OnProfile(profile);
        }

        return true;
    }

    case ServerCodes::Progress: {
        Progress info;

        if (!WireFormat::ReadUInt64(&input_, &info.rows)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(&input_, &info.bytes)) {
            return false;
        }
        if (REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) {
            if (!WireFormat::ReadUInt64(&input_, &info.total_rows)) {
                return false;
            }
        }

        if (events_) {
            events_->OnProgress(info);
        }

        return true;
    }

    case ServerCodes::Pong: {
        return true;
    }

    case ServerCodes::EndOfStream: {
        if (events_) {
            events_->OnFinish();
        }
        return false;
    }

    default:
        throw std::runtime_error("unimplemented " + std::to_string((int)packet_type));
        break;
    }

    return false;
}

bool Client::Impl::ReadBlock(Block* block, CodedInputStream* input) {
    // Additional information about block.
    if (REVISION >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
        uint64_t num;
        BlockInfo info;

        // BlockInfo
        if (!WireFormat::ReadUInt64(input, &num)) {
            return false;
        }
        if (!WireFormat::ReadFixed(input, &info.is_overflows)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input, &num)) {
            return false;
        }
        if (!WireFormat::ReadFixed(input, &info.bucket_num)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input, &num)) {
            return false;
        }

        // TODO use data
    }

    uint64_t num_columns = 0;
    uint64_t num_rows = 0;

    if (!WireFormat::ReadUInt64(input, &num_columns)) {
        return false;
    }
    if (!WireFormat::ReadUInt64(input, &num_rows)) {
        return false;
    }

    for (size_t i = 0; i < num_columns; ++i) {
        std::string name;
        std::string type;

        if (!WireFormat::ReadString(input, &name)) {
            return false;
        }
        if (!WireFormat::ReadString(input, &type)) {
            return false;
        }

        if (ColumnRef col = CreateColumnByType(type)) {
            if (num_rows && !col->Load(input, num_rows)) {
                throw std::runtime_error("can't load");
            }

            block->AppendColumn(name, col);
        } else {
            throw std::runtime_error(std::string("unsupported column type: ") + type);
        }
    }

    return true;
}

bool Client::Impl::ReceiveData() {
    Block block;

    if (REVISION >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
        std::string table_name;

        if (!WireFormat::ReadString(&input_, &table_name)) {
            return false;
        }
    }

    if (compression_ == CompressionState::Enable) {
        CompressedInput compressed(&input_);
        CodedInputStream coded(&compressed);

        if (!ReadBlock(&block, &coded)) {
            return false;
        }
    } else {
        if (!ReadBlock(&block, &input_)) {
            return false;
        }
    }

    if (events_) {
        events_->OnData(block);
        if (!events_->OnDataCancelable(block)) {
            SendCancel();
        }
    }

    return true;
}

bool Client::Impl::ReceiveException(bool rethrow) {
    std::unique_ptr<Exception> e(new Exception);
    Exception* current = e.get();

    bool exception_received = true;
    do {
        bool has_nested = false;

        if (!WireFormat::ReadFixed(&input_, &current->code)) {
           exception_received = false;
           break;
        }
        if (!WireFormat::ReadString(&input_, &current->name)) {
            exception_received = false;
            break;
        }
        if (!WireFormat::ReadString(&input_, &current->display_text)) {
            exception_received = false;
            break;
        }
        if (!WireFormat::ReadString(&input_, &current->stack_trace)) {
            exception_received = false;
            break;
        }
        if (!WireFormat::ReadFixed(&input_, &has_nested)) {
            exception_received = false;
            break;
        }

        if (has_nested) {
            current->nested.reset(new Exception);
            current = current->nested.get();
        } else {
            break;
        }
    } while (true);

    if (events_) {
        events_->OnServerException(*e);
    }

    if (rethrow || options_.rethrow_exceptions) {
        throw ServerException(std::move(e));
    }

    return exception_received;
}

void Client::Impl::SendCancel() {
    WireFormat::WriteUInt64(&output_, ClientCodes::Cancel);
    output_.Flush();
}

void Client::Impl::SendQuery(const std::string& query) {
    WireFormat::WriteUInt64(&output_, ClientCodes::Query);
    WireFormat::WriteString(&output_, std::string());

    /// Client info.
    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO) {
        ClientInfo info;

        info.query_kind = 1;
        info.client_name = "ClickHouse client";
        info.client_version_major = DBMS_VERSION_MAJOR;
        info.client_version_minor = DBMS_VERSION_MINOR;
        info.client_revision = REVISION;


        WireFormat::WriteFixed(&output_, info.query_kind);
        WireFormat::WriteString(&output_, info.initial_user);
        WireFormat::WriteString(&output_, info.initial_query_id);
        WireFormat::WriteString(&output_, info.initial_address);
        WireFormat::WriteFixed(&output_, info.iface_type);

        WireFormat::WriteString(&output_, info.os_user);
        WireFormat::WriteString(&output_, info.client_hostname);
        WireFormat::WriteString(&output_, info.client_name);
        WireFormat::WriteUInt64(&output_, info.client_version_major);
        WireFormat::WriteUInt64(&output_, info.client_version_minor);
        WireFormat::WriteUInt64(&output_, info.client_revision);

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
            WireFormat::WriteString(&output_, info.quota_key);
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
            WireFormat::WriteUInt64(&output_, info.client_version_patch);
        }
    }

    /// Per query settings.
    //if (settings)
    //    settings->serialize(*out);
    //else
    WireFormat::WriteString(&output_, std::string());

    WireFormat::WriteUInt64(&output_, Stages::Complete);
    WireFormat::WriteUInt64(&output_, compression_);
    WireFormat::WriteString(&output_, query);
    // Send empty block as marker of
    // end of data
    SendData(Block());

    output_.Flush();
}


void Client::Impl::WriteBlock(const Block& block, CodedOutputStream* output) {
    // Additional information about block.
    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
        WireFormat::WriteUInt64(output, 1);
        WireFormat::WriteFixed (output, block.Info().is_overflows);
        WireFormat::WriteUInt64(output, 2);
        WireFormat::WriteFixed (output, block.Info().bucket_num);
        WireFormat::WriteUInt64(output, 0);
    }

    WireFormat::WriteUInt64(output, block.GetColumnCount());
    WireFormat::WriteUInt64(output, block.GetRowCount());

    for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
        WireFormat::WriteString(output, bi.Name());
        WireFormat::WriteString(output, bi.Type()->GetName());

        bi.Column()->Save(output);
    }
}

void Client::Impl::SendData(const Block& block) {
    WireFormat::WriteUInt64(&output_, ClientCodes::Data);

    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
        WireFormat::WriteString(&output_, std::string());
    }

    if (compression_ == CompressionState::Enable) {
        switch (options_.compression_method) {
            case CompressionMethod::None: {
                assert(false);
                break;
            }

            case CompressionMethod::LZ4: {
                Buffer tmp;
                // Serialize block's data
                {
                    BufferOutput out(&tmp);
                    CodedOutputStream coded(&out);
                    WriteBlock(block, &coded);
                }
                // Reserver space for data
                Buffer buf;
                buf.resize(9 + LZ4_compressBound(tmp.size()));

                // Compress data
                int size = LZ4_compress((const char*)tmp.data(), (char*)buf.data() + 9, tmp.size());
                buf.resize(9 + size);

                // Fill header
                uint8_t* p = buf.data();
                // Compression method
                WriteUnaligned(p, (uint8_t)0x82); p += 1;
                // Compressed data size with header
                WriteUnaligned(p, (uint32_t)buf.size()); p += 4;
                // Original data size
                WriteUnaligned(p, (uint32_t)tmp.size());

                WireFormat::WriteFixed(&output_, CityHash128(
                                    (const char*)buf.data(), buf.size()));
                WireFormat::WriteBytes(&output_, buf.data(), buf.size());
                break;
            }
        }
    } else {
        WriteBlock(block, &output_);
    }

    output_.Flush();
}

bool Client::Impl::SendHello() {
    WireFormat::WriteUInt64(&output_, ClientCodes::Hello);
    WireFormat::WriteString(&output_, std::string(DBMS_NAME) + " client");
    WireFormat::WriteUInt64(&output_, DBMS_VERSION_MAJOR);
    WireFormat::WriteUInt64(&output_, DBMS_VERSION_MINOR);
    WireFormat::WriteUInt64(&output_, REVISION);
    WireFormat::WriteString(&output_, options_.default_database);
    WireFormat::WriteString(&output_, options_.user);
    WireFormat::WriteString(&output_, options_.password);

    output_.Flush();

    return true;
}

bool Client::Impl::ReceiveHello() {
    uint64_t packet_type = 0;

    if (!input_.ReadVarint64(&packet_type)) {
        return false;
    }

    if (packet_type == ServerCodes::Hello) {
        if (!WireFormat::ReadString(&input_, &server_info_.name)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(&input_, &server_info_.version_major)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(&input_, &server_info_.version_minor)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(&input_, &server_info_.revision)) {
            return false;
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
            if (!WireFormat::ReadString(&input_, &server_info_.timezone)) {
                return false;
            }
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME) {
            if (!WireFormat::ReadString(&input_, &server_info_.display_name)) {
                return false;
            }
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
            if (!WireFormat::ReadUInt64(&input_, &server_info_.version_patch)) {
                return false;
            }
        }

        return true;
    } else if (packet_type == ServerCodes::Exception) {
        ReceiveException(true);
        return false;
    }

    return false;
}

void Client::Impl::RetryGuard(std::function<void()> func) {
    for (unsigned int i = 0; ; ++i) {
        try {
            func();
            return;
        } catch (const std::system_error&) {
            bool ok = true;

            try {
                std::this_thread::sleep_for(options_.retry_timeout);
                ResetConnection();
            } catch (...) {
                ok = false;
            }

            if (!ok && i == options_.send_retries) {
                throw;
            }
        }
    }
}

Client::Client(const ClientOptions& opts)
    : options_(opts)
    , impl_(new Impl(opts))
{
}

Client::~Client()
{ }

void Client::Execute(const Query& query) {
    impl_->ExecuteQuery(query);
}

void Client::Select(const std::string& query, SelectCallback cb) {
    Execute(Query(query).OnData(cb));
}

void Client::SelectCancelable(const std::string& query, SelectCancelableCallback cb) {
    Execute(Query(query).OnDataCancelable(cb));
}

void Client::Select(const Query& query) {
    Execute(query);
}

void Client::Insert(const std::string& table_name, const Block& block) {
    impl_->Insert(table_name, block);
}

void Client::Ping() {
    impl_->Ping();
}

void Client::ResetConnection() {
    impl_->ResetConnection();
}

const ServerInfo& Client::GetServerInfo() const {
    return impl_->GetServerInfo();
}

}

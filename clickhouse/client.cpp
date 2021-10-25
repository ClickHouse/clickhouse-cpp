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

#if WITH_OPENSSL
#include "base/sslsocket.h"
#endif

#define DBMS_NAME                                       "ClickHouse"
#define DBMS_VERSION_MAJOR                              1
#define DBMS_VERSION_MINOR                              2

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         50264
#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS   51554
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO               51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO              54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060
//#define DBMS_MIN_REVISION_WITH_TABLES_STATUS            54226
#define DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME      54372
#define DBMS_MIN_REVISION_WITH_VERSION_PATCH            54401
#define DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE     54405
#define DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE 54337

#define REVISION  DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE

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
       << (opt.compression_method == CompressionMethod::LZ4 ? "LZ4" : "None");
#if WITH_OPENSSL
    if (opt.ssl_options.use_ssl) {
        const auto & ssl_options = opt.ssl_options;
        os << " SSL ("
           << " ssl_context: " << (ssl_options.ssl_context ? "provided by user" : "created internally")
           << " use_default_ca_locations: " << ssl_options.use_default_ca_locations
           << " use_default_ca_locations: " << ssl_options.use_default_ca_locations
           << " path_to_ca_files: " << ssl_options.path_to_ca_files.size() << " items"
           << " path_to_ca_directory: " << ssl_options.path_to_ca_directory
           << " min_protocol_version: " << ssl_options.min_protocol_version
           << " min_protocol_version: " << ssl_options.max_protocol_version
           << " context_options: " << ssl_options.context_options
           << ")";
    }
#endif
    os << ")";
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

    std::unique_ptr<InputStream> socket_input_;
    std::unique_ptr<BufferedInput> buffered_input_;
    std::unique_ptr<CodedInputStream> input_;

    std::unique_ptr<OutputStream> socket_output_;
    std::unique_ptr<BufferedOutput> buffered_output_;
    std::unique_ptr<CodedOutputStream> output_;

    std::unique_ptr<Socket> socket_;

#if WITH_OPENSSL
    std::unique_ptr<SSLContext> ssl_context_;
#endif

    ServerInfo server_info_;
};


Client::Impl::Impl(const ClientOptions& opts)
    : options_(opts)
    , events_(nullptr)
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

std::string NameToQueryString(const std::string &input)
{
    std::string output;
    output.reserve(input.size() + 2);
    output += '`';

    for (const auto & c : input) {
        if (c == '`') {
            //escape ` with ``
            output.append("``");
        } else {
            output.push_back(c);
        }
    }

    output += '`';
    return output;
}

void Client::Impl::Insert(const std::string& table_name, const Block& block) {
    if (options_.ping_before_query) {
        RetryGuard([this]() { Ping(); });
    }

    std::stringstream fields_section;
		const auto num_columns = block.GetColumnCount();

    for (unsigned int i = 0; i < num_columns; ++i) {
        if (i == num_columns - 1) {
            fields_section << NameToQueryString(block.GetColumnName(i));
        } else {
            fields_section << NameToQueryString(block.GetColumnName(i)) << ",";
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
    WireFormat::WriteUInt64(output_.get(), ClientCodes::Ping);
    output_->Flush();

    uint64_t server_packet;
    const bool ret = ReceivePacket(&server_packet);

    if (!ret || server_packet != ServerCodes::Pong) {
        throw std::runtime_error("fail to ping server");
    }
}

void Client::Impl::ResetConnection() {

    std::unique_ptr<Socket> socket;

    const auto address = NetworkAddress(options_.host, std::to_string(options_.port));
#if WITH_OPENSSL
    // TODO: maybe do not re-create context multiple times upon reconnection - that doesn't make sense.
    std::unique_ptr<SSLContext> ssl_context;
    if (options_.ssl_options.use_ssl) {
        const auto ssl_options = options_.ssl_options;
        const auto ssl_params = SSLParams {
                ssl_options.path_to_ca_files,
                ssl_options.path_to_ca_directory,
                ssl_options.use_default_ca_locations,
                ssl_options.context_options,
                ssl_options.min_protocol_version,
                ssl_options.max_protocol_version,
                ssl_options.use_sni
        };

        if (ssl_options.ssl_context)
            ssl_context = std::make_unique<SSLContext>(*ssl_options.ssl_context);
        else {
            ssl_context = std::make_unique<SSLContext>(ssl_params);
        }

        socket = std::make_unique<SSLSocket>(address, ssl_params, *ssl_context);
    }
    else
#endif
        socket = std::make_unique<Socket>(address);

    if (options_.tcp_keepalive) {
        socket->SetTcpKeepAlive(options_.tcp_keepalive_idle.count(),
                          options_.tcp_keepalive_intvl.count(),
                          options_.tcp_keepalive_cnt);
    }
    if (options_.tcp_nodelay) {
        socket->SetTcpNoDelay(options_.tcp_nodelay);
    }

    auto socket_input = socket->makeInputStream();
    auto socket_output = socket->makeOutputStream();
    auto buffered_input = std::make_unique<BufferedInput>(socket_input.get());
    auto buffered_output = std::make_unique<BufferedOutput>(socket_output.get());
    auto input = std::make_unique<CodedInputStream>(buffered_input.get());
    auto output = std::make_unique<CodedOutputStream>(buffered_output.get());

    std::swap(socket_input, socket_input_);
    std::swap(socket_output, socket_output_);
    std::swap(buffered_input, buffered_input_);
    std::swap(buffered_output, buffered_output_);
    std::swap(input, input_);
    std::swap(output, output_);
    std::swap(socket, socket_);

#if WITH_OPENSSL
    std::swap(ssl_context_, ssl_context);
#endif

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

    if (!input_->ReadVarint64(&packet_type)) {
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

        if (!WireFormat::ReadUInt64(input_.get(), &profile.rows)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input_.get(), &profile.blocks)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input_.get(), &profile.bytes)) {
            return false;
        }
        if (!WireFormat::ReadFixed(input_.get(), &profile.applied_limit)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input_.get(), &profile.rows_before_limit)) {
            return false;
        }
        if (!WireFormat::ReadFixed(input_.get(), &profile.calculated_rows_before_limit)) {
            return false;
        }

        if (events_) {
            events_->OnProfile(profile);
        }

        return true;
    }

    case ServerCodes::Progress: {
        Progress info;

        if (!WireFormat::ReadUInt64(input_.get(), &info.rows)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input_.get(), &info.bytes)) {
            return false;
        }
        if (REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) {
            if (!WireFormat::ReadUInt64(input_.get(), &info.total_rows)) {
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

    CreateColumnByTypeSettings create_column_settings;
    create_column_settings.low_cardinality_as_wrapped_column = options_.backward_compatibility_lowcardinality_as_wrapped_column;

    std::string name;
    std::string type;
    for (size_t i = 0; i < num_columns; ++i) {
        if (!WireFormat::ReadString(input, &name)) {
            return false;
        }
        if (!WireFormat::ReadString(input, &type)) {
            return false;
        }

        if (ColumnRef col = CreateColumnByType(type, create_column_settings)) {
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
        if (!WireFormat::SkipString(input_.get())) {
            return false;
        }
    }

    if (compression_ == CompressionState::Enable) {
        CompressedInput compressed(input_.get());
        CodedInputStream coded(&compressed);

        if (!ReadBlock(&block, &coded)) {
            return false;
        }
    } else {
        if (!ReadBlock(&block, input_.get())) {
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

        if (!WireFormat::ReadFixed(input_.get(), &current->code)) {
           exception_received = false;
           break;
        }
        if (!WireFormat::ReadString(input_.get(), &current->name)) {
            exception_received = false;
            break;
        }
        if (!WireFormat::ReadString(input_.get(), &current->display_text)) {
            exception_received = false;
            break;
        }
        if (!WireFormat::ReadString(input_.get(), &current->stack_trace)) {
            exception_received = false;
            break;
        }
        if (!WireFormat::ReadFixed(input_.get(), &has_nested)) {
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
    WireFormat::WriteUInt64(output_.get(), ClientCodes::Cancel);
    output_->Flush();
}

void Client::Impl::SendQuery(const std::string& query) {
    WireFormat::WriteUInt64(output_.get(), ClientCodes::Query);
    WireFormat::WriteString(output_.get(), std::string());

    /// Client info.
    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO) {
        ClientInfo info;

        info.query_kind = 1;
        info.client_name = "ClickHouse client";
        info.client_version_major = DBMS_VERSION_MAJOR;
        info.client_version_minor = DBMS_VERSION_MINOR;
        info.client_revision = REVISION;


        WireFormat::WriteFixed(output_.get(), info.query_kind);
        WireFormat::WriteString(output_.get(), info.initial_user);
        WireFormat::WriteString(output_.get(), info.initial_query_id);
        WireFormat::WriteString(output_.get(), info.initial_address);
        WireFormat::WriteFixed(output_.get(), info.iface_type);

        WireFormat::WriteString(output_.get(), info.os_user);
        WireFormat::WriteString(output_.get(), info.client_hostname);
        WireFormat::WriteString(output_.get(), info.client_name);
        WireFormat::WriteUInt64(output_.get(), info.client_version_major);
        WireFormat::WriteUInt64(output_.get(), info.client_version_minor);
        WireFormat::WriteUInt64(output_.get(), info.client_revision);

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
            WireFormat::WriteString(output_.get(), info.quota_key);
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
            WireFormat::WriteUInt64(output_.get(), info.client_version_patch);
        }
    }

    /// Per query settings.
    //if (settings)
    //    settings->serialize(*out);
    //else
    WireFormat::WriteString(output_.get(), std::string());

    WireFormat::WriteUInt64(output_.get(), Stages::Complete);
    WireFormat::WriteUInt64(output_.get(), compression_);
    WireFormat::WriteString(output_.get(), query);
    // Send empty block as marker of
    // end of data
    SendData(Block());

    output_->Flush();
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
    WireFormat::WriteUInt64(output_.get(), ClientCodes::Data);

    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
        WireFormat::WriteString(output_.get(), std::string());
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
                int size = LZ4_compress_default((const char*)tmp.data(), (char*)buf.data() + 9, tmp.size(), buf.size() - 9);
                buf.resize(9 + size);

                // Fill header
                uint8_t* p = buf.data();
                // Compression method
                WriteUnaligned(p, (uint8_t)0x82); p += 1;
                // Compressed data size with header
                WriteUnaligned(p, (uint32_t)buf.size()); p += 4;
                // Original data size
                WriteUnaligned(p, (uint32_t)tmp.size());

                WireFormat::WriteFixed(output_.get(), CityHash128(
                                    (const char*)buf.data(), buf.size()));
                WireFormat::WriteBytes(output_.get(), buf.data(), buf.size());
                break;
            }
        }
    } else {
        WriteBlock(block, output_.get());
    }

    output_->Flush();
}

bool Client::Impl::SendHello() {
    WireFormat::WriteUInt64(output_.get(), ClientCodes::Hello);
    WireFormat::WriteString(output_.get(), std::string(DBMS_NAME) + " client");
    WireFormat::WriteUInt64(output_.get(), DBMS_VERSION_MAJOR);
    WireFormat::WriteUInt64(output_.get(), DBMS_VERSION_MINOR);
    WireFormat::WriteUInt64(output_.get(), REVISION);
    WireFormat::WriteString(output_.get(), options_.default_database);
    WireFormat::WriteString(output_.get(), options_.user);
    WireFormat::WriteString(output_.get(), options_.password);

    output_->Flush();

    return true;
}

bool Client::Impl::ReceiveHello() {
    uint64_t packet_type = 0;

    if (!input_->ReadVarint64(&packet_type)) {
        return false;
    }

    if (packet_type == ServerCodes::Hello) {
        if (!WireFormat::ReadString(input_.get(), &server_info_.name)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input_.get(), &server_info_.version_major)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input_.get(), &server_info_.version_minor)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input_.get(), &server_info_.revision)) {
            return false;
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
            if (!WireFormat::ReadString(input_.get(), &server_info_.timezone)) {
                return false;
            }
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME) {
            if (!WireFormat::ReadString(input_.get(), &server_info_.display_name)) {
                return false;
            }
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
            if (!WireFormat::ReadUInt64(input_.get(), &server_info_.version_patch)) {
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

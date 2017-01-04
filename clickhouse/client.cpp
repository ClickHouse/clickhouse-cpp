#include "client.h"
#include "protocol.h"
#include "varint.h"

#include "base/platform.h"
#include "net/socket.h"

#include <system_error>
#include <vector>
#include <iostream>

#if defined(_win_)
#   include <winsock2.h>
#   include <ws2tcpip.h>
#else
#   include <arpa/inet.h>
#   include <sys/types.h>
#   include <sys/socket.h>
#   include <errno.h>
#   include <fcntl.h>
#   include <memory.h>
#   include <netdb.h>
#   include <poll.h>
#   include <unistd.h>
#endif

#define DBMS_NAME               "ClickHouse"
#define DBMS_VERSION_MAJOR      1
#define DBMS_VERSION_MINOR      1
#define REVISION                54126

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         50264
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO               51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO              54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060

namespace clickhouse {

struct ClientInfo {
    uint8_t interface = 1; // TCP
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
    uint32_t client_revision = 0;
};

class Client::Impl {
public:
    Impl(const std::string& host, const std::string& port)
        : socket_(-1)
    {
        struct addrinfo* info;
        struct addrinfo hints;
        memset(&hints, 0, sizeof(hints));

        hints.ai_family = PF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        // TODO if not local addr hints.ai_flags |= AI_ADDRCONFIG;

        const int error = getaddrinfo(host.c_str(), port.c_str(), &hints, &info);

        if (error) {
            throw std::system_error(errno, std::system_category());
        }

        if (!Connect(info)) {
            freeaddrinfo(info);
            throw std::system_error(errno, std::system_category());
        }

        // TODO use uniqe_ptr
        freeaddrinfo(info);
    }

    ~Impl() {
        Disconnect();
    }

    void Handshake() {
        sendHello();
        receiveHello();
    }

    void SendQuery(const std::string& query) {
        std::vector<char> data(64 << 10);
        char* p = data.data();

        p = writeVarUInt(ClientCodes::Query, p);
        p = writeStringBinary("1"/*query_id*/, p);

        /// Client info.
        if (server_revision_ >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        {
            ClientInfo info;

            info.query_kind = 1;
            info.client_name = "ClickHouse client";
            info.client_version_major = DBMS_VERSION_MAJOR;
            info.client_version_minor = DBMS_VERSION_MINOR;
            info.client_revision = REVISION;

            p = writeBinary(info.query_kind, p);
            p = writeStringBinary(info.initial_user, p);
            p = writeStringBinary(info.initial_query_id, p);
            p = writeStringBinary(info.initial_address, p);
            p = writeBinary(info.interface, p);

            p = writeStringBinary(info.os_user, p);
            p = writeStringBinary(info.client_hostname, p);
            p = writeStringBinary(info.client_name, p);
            p = writeVarUInt(info.client_version_major, p);
            p = writeVarUInt(info.client_version_minor, p);
            p = writeVarUInt(info.client_revision, p);

            if (server_revision_ >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
                p = writeStringBinary(info.quota_key, p);
        }

        /// Per query settings.
        //if (settings)
        //    settings->serialize(*out);
        //else
        p = writeStringBinary("", p);

        uint64_t stage = 2; // Complete
        p = writeVarUInt(stage, p);
        p = writeVarUInt(CompressionState::Disable, p);
        p = writeStringBinary(query, p);

        // Empty block
        {
            p = writeVarUInt(ClientCodes::Data, p);

            if (server_revision_ >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
                p = writeStringBinary("", p);
            }
            /// Дополнительная информация о блоке.
            //if (client_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
            //    block.info.write(ostr);
            //}

            /// Размеры
            p = writeVarUInt(0, p);
            p = writeVarUInt(0, p);
            p = writeVarUInt(0, p);
        }

        // TODO result
        ::send(socket_, data.data(), p - data.data(), 0);


        while (ReceiveQuery())
        { }
    }

private:
    bool ReceiveQuery() {
        uint64_t packet_type;
        readVarUInt(socket_, packet_type);

        std::cerr << "receive packet " << packet_type << std::endl;
        switch (packet_type) {
            case ServerCodes::Data: {
                if (REVISION >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
                    std::string table_name;
                    readStringBinary(socket_, table_name);
                    std::cerr << "temporary_table_name : " << table_name << std::endl;
                }
                /// Дополнительная информация о блоке.
                if (REVISION >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
                    uint64_t num;
                    int32_t bucket_num = 0;
                    uint8_t is_overflows = 0;

                    // BlockInfo
                    readVarUInt(socket_, num);
                    readBinary(socket_, is_overflows);
                    readVarUInt(socket_, num);
                    readBinary(socket_, bucket_num);
                    readVarUInt(socket_, num);

                    std::cerr << "bucket_num : " << bucket_num << std::endl;
                    std::cerr << "is_overflows : " << bool(is_overflows) << std::endl;
                }

                uint64_t num_columns = 0;
                uint64_t num_rows = 0;
                std::string name;

                readVarUInt(socket_, num_columns);
                readVarUInt(socket_, num_rows);
                std::cerr << "num_columns : " << num_columns << std::endl;
                std::cerr << "num_rows : " << num_rows << std::endl;

                for (size_t i = 0; i < num_columns; ++i) {
                    readStringBinary(socket_, name);
                    std::cerr << "name : " << name << std::endl;

                    readStringBinary(socket_, name);
                    std::cerr << "type : " << name << std::endl;

                    if (num_rows) {
                        // type.deserializeBinary(column, istr, rows, 0);
                        throw std::runtime_error("unimplemented");
                    }
                }
                return true;
            }

        default:
            throw std::runtime_error("unimplemented");
            break;
        }

        return false;
    }

    void sendHello() {
        std::vector<char> data(64 << 10);
        char* p = data.data();

        p = writeVarUInt((int)ClientCodes::Hello, p);
        p = writeStringBinary(std::string(DBMS_NAME) + " client", p);
        p = writeVarUInt(DBMS_VERSION_MAJOR, p);
        p = writeVarUInt(DBMS_VERSION_MINOR, p);
        p = writeVarUInt(REVISION, p);
        p = writeStringBinary("system", p); // default_database
        p = writeStringBinary("default", p);            // user
        p = writeStringBinary("", p);  // password

        // TODO result
        ::send(socket_, data.data(), p - data.data(), 0);
    }

    void receiveHello() {
        uint64_t packet_type = 0;

        readVarUInt(socket_, packet_type);

        if (packet_type == (uint64_t)ServerCodes::Hello) {
            readStringBinary(socket_, server_name_);
            readVarUInt(socket_, server_version_major_);
            readVarUInt(socket_, server_version_minor_);
            readVarUInt(socket_, server_revision_);
            if (server_revision_ >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
                readStringBinary(socket_, server_timezone_);
            }

            std::cerr << server_name_ << std::endl;
            std::cerr << server_revision_ << std::endl;
            std::cerr << server_timezone_ << std::endl;
        } else if (packet_type == ServerCodes::Exception) {
            std::cerr << "got exception" << std::endl;
            //receiveException()->rethrow();
            Disconnect();
        } else {
            /// Close connection, to not stay in unsynchronised state.
            Disconnect();
        }
    }

private:
    void Disconnect() {
        if (socket_ != -1) {
#if defined(_win_)
            closesocket(socket_);
#else
            close(socket_);
#endif
            socket_ = -1;
        }
    }

    bool Connect(const struct addrinfo* res) {
        const struct addrinfo* addr0 = res;

        while (res) {
            SOCKET s(socket(res->ai_family, res->ai_socktype, res->ai_protocol));

            if (s == -1) {
                continue;
            }

            if (connect(s, res->ai_addr, (int)res->ai_addrlen)) {
                if (errno == EINPROGRESS ||
                    errno == EAGAIN ||
                    errno == EWOULDBLOCK)
                {
                    pollfd fd;
                    fd.fd = s;
                    fd.events = POLLOUT;
                    int rval = net::Poll(&fd, 1, 1000);

                    if (rval > 0) {
                        int opt;
                        socklen_t len = sizeof(opt);
                        getsockopt(s, SOL_SOCKET, SO_ERROR, (char*)&opt, &len);

                        socket_ = opt;
                        return true;
                    } else {
                        continue;
                    }
                }
            }

            socket_ = s;
            return true;
        }

        return false;
    }

private:
    SOCKET socket_;

    std::string server_name_;
    std::string server_timezone_;
    uint64_t server_version_major_;
    uint64_t server_version_minor_;
    uint64_t server_revision_;
};

Client::Client()
    : host_()
    , port_(0)
{
}

Client::Client(const std::string& host, int port)
    : host_(host)
    , port_(port)
{
}

Client::~Client()
{ }

void Client::Connect() {
    // TODO check initialization
    impl_.reset(new Impl(host_, std::to_string(port_)));
    impl_->Handshake();
}

void Client::SendQuery(const std::string& query) {
    if (impl_) {
        impl_->SendQuery(query);
    }
}

}

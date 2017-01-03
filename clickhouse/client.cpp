#include "client.h"
#include "protocol.h"
#include "varint.h"

#include <system_error>
#include <vector>
#include <iostream>

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <fcntl.h>
#include <memory.h>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>

#define DBMS_NAME               "ClickHouse"
#define DBMS_VERSION_MAJOR      1
#define DBMS_VERSION_MINOR      1
#define REVISION                54126

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES 50264
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO       51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO      54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE  54058

namespace clickhouse {


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
        #if 0
            ClientInfo client_info_to_send;

            if (!client_info)
            {
                /// No client info passed - means this query initiated by me.
                client_info_to_send.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
                client_info_to_send.fillOSUserHostNameAndVersionInfo();
                client_info_to_send.client_name = (DBMS_NAME " ") + client_name;
            }
            else
            {
                /// This query is initiated by another query.
                client_info_to_send = *client_info;
                client_info_to_send.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
            }

            client_info_to_send.write(*out, server_revision);
        #endif
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


        ReceiveQuery();
    }

private:
    void ReceiveQuery() {
        uint64_t packet_type;
        readVarUInt(socket_, packet_type);

        std::cerr << "receive packet " << packet_type << std::endl;
        switch (packet_type) {
            case ServerCodes::Data: {
                std::string external_table_name;

                if (server_revision_ >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
                    readStringBinary(socket_, external_table_name);

                    std::cerr << "external_table_name : " << external_table_name << std::endl;
                }
                /// Дополнительная информация о блоке.
                if (server_revision_ >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
                    uint64_t rows;
                    uint64_t blocks;
                    uint64_t bytes;
                    bool applied_limit;
                    size_t rows_before_limit;
                    bool calculated_rows_before_limit;

                    //res.info.read(istr);
                    readVarUInt(socket_, rows);
                    readVarUInt(socket_, blocks);
                    readVarUInt(socket_, bytes);
                    readBinary(socket_, applied_limit);
                    readVarUInt(socket_, rows_before_limit);
                    readBinary(socket_, calculated_rows_before_limit);

                    std::cerr << "rows : " << rows << std::endl;
                    std::cerr << "blocks : " << blocks << std::endl;
                    std::cerr << "bytes : " << bytes << std::endl;
                    std::cerr << "applied_limit : " << applied_limit << std::endl;
                    std::cerr << "rows_before_limit : " << rows_before_limit << std::endl;
                    std::cerr << "calculated_rows_before_limit : " << calculated_rows_before_limit << std::endl;
                }

                uint64_t num_columns;
                uint64_t num_rows;

                readVarUInt(socket_, num_columns);
                readVarUInt(socket_, num_rows);

                std::cerr << "num_columns : " << num_columns << std::endl;
                std::cerr << "num_rows : " << num_rows << std::endl;
                break;
            }
        }
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
            close(socket_);
            socket_ = -1;
        }
    }

    bool Connect(const struct addrinfo* res) {
        const struct addrinfo* addr0 = res;

        while (res) {
            int s(socket(res->ai_family, res->ai_socktype, res->ai_protocol));

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
                    int rval = poll(&fd, 1, 1000);

                    if (rval > 0) {
                        int opt;
                        socklen_t len = sizeof(opt);
                        getsockopt(s, SOL_SOCKET, SO_ERROR, &opt, &len);

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
    int socket_;

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

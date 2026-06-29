#include "async_client.h"

#include "base/byte_ring.h"
#include "base/output.h"
#include "base/socket.h"
#include "base/wire_format.h"
#include "exceptions.h"
#include "protocol.h"
#include "version.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(_win_)
#   include <winsock2.h>
#   include <ws2tcpip.h>
#else
#   include <errno.h>
#   include <fcntl.h>
#   include <netdb.h>
#   include <sys/socket.h>
#   include <unistd.h>
#endif

namespace clickhouse {

namespace {

constexpr std::uint64_t DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES = 50264;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS = 51554;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_BLOCK_INFO = 51903;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_CLIENT_INFO = 54032;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET = 54441;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_OPENTELEMETRY = 54442;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH = 54448;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME = 54449;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS = 54453;
constexpr std::uint64_t DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION = 54454;
constexpr std::uint64_t DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM = 54458;
constexpr std::uint64_t DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS = 54459;

constexpr std::uint64_t CLIENT_PROTOCOL_REVISION = DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS;

struct ServerInfoLite {
    std::string name;
    std::string timezone;
    std::string display_name;
    std::uint64_t version_major{0};
    std::uint64_t version_minor{0};
    std::uint64_t version_patch{0};
    std::uint64_t revision{0};
};

struct ClientInfoLite {
    std::uint8_t iface_type{1};  // TCP
    std::uint8_t query_kind{1};
    std::string initial_user;
    std::string initial_query_id;
    std::string quota_key;
    std::string os_user;
    std::string client_hostname;
    std::string client_name{"clickhouse-cpp"};
    std::string initial_address{"[::ffff:127.0.0.1]:0"};
    std::uint64_t client_version_major{CLICKHOUSE_CPP_VERSION_MAJOR};
    std::uint64_t client_version_minor{CLICKHOUSE_CPP_VERSION_MINOR};
    std::uint64_t client_version_patch{CLICKHOUSE_CPP_VERSION_PATCH};
    std::uint32_t client_revision{static_cast<std::uint32_t>(CLIENT_PROTOCOL_REVISION)};
};

std::string QuoteIdentifier(std::string_view input) {
    std::string output;
    output.reserve(input.size() + 2);
    output.push_back('`');
    for (const char c : input) {
        if (c == '`') {
            output.append("``");
        } else {
            output.push_back(c);
        }
    }
    output.push_back('`');
    return output;
}

inline int get_socket_error_code() noexcept {
#if defined(_win_)
    return WSAGetLastError();
#else
    return errno;
#endif
}

inline bool is_would_block_error(int err) noexcept {
#if defined(_win_)
    return err == WSAEWOULDBLOCK;
#else
    return err == EAGAIN || err == EWOULDBLOCK;
#endif
}

inline bool is_connect_in_progress(int err) noexcept {
#if defined(_win_)
    return err == WSAEWOULDBLOCK || err == WSAEINPROGRESS;
#else
    return err == EINPROGRESS || err == EAGAIN || err == EWOULDBLOCK;
#endif
}

void set_nonblocking(SOCKET socket_handle, bool value) {
#if defined(_win_)
    u_long mode = value ? 1UL : 0UL;
    if (ioctlsocket(socket_handle, FIONBIO, &mode) != 0) {
        throw std::system_error(get_socket_error_code(), std::system_category(), "failed to set nonblocking");
    }
#else
    int flags = fcntl(socket_handle, F_GETFL, 0);
    if (flags == -1) {
        flags = 0;
    }
    if (value) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    if (fcntl(socket_handle, F_SETFL, flags) == -1) {
        throw std::system_error(get_socket_error_code(), std::system_category(), "failed to set nonblocking");
    }
#endif
}

int poll_socket(SOCKET socket_handle, short events) noexcept {
    pollfd fd;
    fd.fd = socket_handle;
    fd.events = events;
    fd.revents = 0;
#if defined(_win_)
    return WSAPoll(&fd, 1, 0);
#else
    return poll(&fd, 1, 0);
#endif
}

void close_socket(SOCKET socket_handle) noexcept {
#if defined(_win_)
    closesocket(socket_handle);
#else
    close(socket_handle);
#endif
}

class NonBlockingSocket {
public:
    NonBlockingSocket() = default;
    ~NonBlockingSocket() { close(); }

    NonBlockingSocket(NonBlockingSocket&& other) noexcept
        : socket_(other.socket_)
    {
        other.socket_ = invalid_socket();
    }

    NonBlockingSocket& operator=(NonBlockingSocket&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        close();
        socket_ = other.socket_;
        other.socket_ = invalid_socket();
        return *this;
    }

    NonBlockingSocket(const NonBlockingSocket&) = delete;
    NonBlockingSocket& operator=(const NonBlockingSocket&) = delete;

    static constexpr SOCKET invalid_socket() noexcept {
#if defined(_win_)
        return INVALID_SOCKET;
#else
        return -1;
#endif
    }

    bool is_open() const noexcept { return socket_ != invalid_socket(); }
    SOCKET native_handle() const noexcept { return socket_; }

    enum class ConnectStartResult {
        started,
        connected,
    };

    ConnectStartResult start_connect(const NetworkAddress& address) {
        close();

        for (auto res = address.Info(); res != nullptr; res = res->ai_next) {
            const SOCKET s = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
            if (s == invalid_socket()) {
                continue;
            }

            try {
                set_nonblocking(s, true);
            } catch (...) {
                close_socket(s);
                continue;
            }

            if (::connect(s, res->ai_addr, static_cast<int>(res->ai_addrlen)) == 0) {
                socket_ = s;
                return ConnectStartResult::connected;
            }

            const int err = get_socket_error_code();
            if (is_connect_in_progress(err)) {
                socket_ = s;
                return ConnectStartResult::started;
            }

            close_socket(s);
        }

        throw std::system_error(get_socket_error_code(), std::system_category(), "failed to connect");
    }

    bool poll_connected() {
        if (!is_open()) {
            return false;
        }
        const int rc = poll_socket(socket_, POLLOUT);
        if (rc <= 0) {
            return false;
        }

        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(socket_, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err), &len) != 0) {
            throw std::system_error(get_socket_error_code(), std::system_category(), "getsockopt(SO_ERROR) failed");
        }
        if (err != 0) {
            throw std::system_error(err, std::system_category(), "connect failed");
        }
        return true;
    }

    std::size_t send_some(const std::uint8_t* data, std::size_t len, bool& would_block) {
        would_block = false;
        if (!is_open() || len == 0) {
            return 0;
        }
#if defined(_linux_)
        static constexpr int flags = MSG_NOSIGNAL;
#else
        static constexpr int flags = 0;
#endif
        const ssize_t rc = ::send(socket_, reinterpret_cast<const char*>(data), static_cast<int>(len), flags);
        if (rc > 0) {
            return static_cast<std::size_t>(rc);
        }
        if (rc == 0) {
            return 0;
        }

        const int err = get_socket_error_code();
        if (is_would_block_error(err)) {
            would_block = true;
            return 0;
        }
        throw std::system_error(err, std::system_category(), "send failed");
    }

    std::size_t recv_some(std::uint8_t* data, std::size_t len, bool& would_block) {
        would_block = false;
        if (!is_open() || len == 0) {
            return 0;
        }
        const ssize_t rc = ::recv(socket_, reinterpret_cast<char*>(data), static_cast<int>(len), 0);
        if (rc > 0) {
            return static_cast<std::size_t>(rc);
        }
        if (rc == 0) {
            return 0;
        }

        const int err = get_socket_error_code();
        if (is_would_block_error(err)) {
            would_block = true;
            return 0;
        }
        throw std::system_error(err, std::system_category(), "recv failed");
    }

    void close() noexcept {
        if (is_open()) {
            close_socket(socket_);
            socket_ = invalid_socket();
        }
    }

private:
    SOCKET socket_{invalid_socket()};
};

struct VarintState {
    std::uint64_t value{0};
    std::uint8_t shift{0};
    std::uint8_t bytes{0};

    void reset() noexcept {
        value = 0;
        shift = 0;
        bytes = 0;
    }
};

bool try_read_fixed(internal::ByteRing& ring, void* out, std::size_t len) {
    if (ring.size() < len) {
        return false;
    }
    ring.read(out, len);
    return true;
}

bool try_read_varint64(internal::ByteRing& ring, VarintState& state, std::uint64_t& out) {
    constexpr std::size_t kMaxVarintBytes = 10;

    while (ring.size() > 0) {
        std::uint8_t byte = 0;
        ring.read(&byte, 1);

        state.value |= static_cast<std::uint64_t>(byte & 0x7FU) << state.shift;
        state.shift = static_cast<std::uint8_t>(state.shift + 7);
        state.bytes = static_cast<std::uint8_t>(state.bytes + 1);

        if ((byte & 0x80U) == 0) {
            out = state.value;
            state.reset();
            return true;
        }
        if (state.bytes >= kMaxVarintBytes) {
            throw ProtocolError("invalid varint");
        }
    }
    return false;
}

struct StringState {
    VarintState len_state{};
    std::uint64_t remaining{0};
    bool has_len{false};
    bool skip{false};
    std::string value{};

    void reset(bool skip_mode) {
        len_state.reset();
        remaining = 0;
        has_len = false;
        skip = skip_mode;
        value.clear();
    }
};

bool advance_string(internal::ByteRing& ring, StringState& state) {
    if (!state.has_len) {
        std::uint64_t len = 0;
        if (!try_read_varint64(ring, state.len_state, len)) {
            return false;
        }
        state.remaining = len;
        state.has_len = true;
        if (!state.skip) {
            if (len > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
                throw ProtocolError("string too long");
            }
            state.value.reserve(static_cast<std::size_t>(len));
        }
    }

    while (state.remaining > 0) {
        const auto span = ring.read_span();
        if (span.size == 0) {
            return false;
        }
        const std::size_t n = std::min<std::size_t>(span.size, static_cast<std::size_t>(state.remaining));
        if (!state.skip) {
            state.value.append(reinterpret_cast<const char*>(span.data), n);
        }
        ring.consume_read(n);
        state.remaining -= n;
    }

    return true;
}

struct SkipBytesState {
    std::uint64_t remaining{0};

    void reset(std::uint64_t len) noexcept { remaining = len; }
};

bool advance_skip_bytes(internal::ByteRing& ring, SkipBytesState& state) {
    while (state.remaining > 0) {
        const auto span = ring.read_span();
        if (span.size == 0) {
            return false;
        }
        const std::size_t n = std::min<std::size_t>(span.size, static_cast<std::size_t>(state.remaining));
        ring.consume_read(n);
        state.remaining -= n;
    }
    return true;
}

struct BlockSkipPlan {
    enum class Kind { Fixed, String };
    Kind kind{Kind::Fixed};
    std::size_t bytes_per_row{0};
};

std::optional<BlockSkipPlan> build_column_skip_plan(std::string_view type_name) {
    const auto fixed = [&](std::size_t bytes) -> std::optional<BlockSkipPlan> {
        return BlockSkipPlan{BlockSkipPlan::Kind::Fixed, bytes};
    };

    if (type_name == "UInt8" || type_name == "Int8" || type_name == "Enum8") {
        return fixed(1);
    }
    if (type_name == "UInt16" || type_name == "Int16" || type_name == "Enum16" || type_name == "Date") {
        return fixed(2);
    }
    if (type_name == "UInt32" || type_name == "Int32" || type_name == "Float32" || type_name == "IPv4" || type_name == "Date32") {
        return fixed(4);
    }
    if (type_name == "DateTime") {
        return fixed(4);
    }
    if (type_name == "UInt64" || type_name == "Int64" || type_name == "Float64") {
        return fixed(8);
    }
    if (type_name == "UUID" || type_name == "IPv6") {
        return fixed(16);
    }
    if (type_name == "String") {
        return BlockSkipPlan{BlockSkipPlan::Kind::String, 0};
    }

    if (type_name.rfind("DateTime(", 0) == 0) {
        // DateTime('Europe/Moscow') etc.
        return fixed(4);
    }
    if (type_name.rfind("DateTime64(", 0) == 0) {
        // DateTime64(6) and DateTime64(6, 'UTC') etc.
        return fixed(8);
    }

    if (type_name.rfind("FixedString(", 0) == 0) {
        const auto close = type_name.find(')');
        if (close == std::string_view::npos) {
            return std::nullopt;
        }
        const auto inner = type_name.substr(std::string_view("FixedString(").size(), close - std::string_view("FixedString(").size());
        try {
            const std::size_t n = static_cast<std::size_t>(std::stoul(std::string(inner)));
            return fixed(n);
        } catch (...) {
            return std::nullopt;
        }
    }

    if (type_name.rfind("Decimal32(", 0) == 0) {
        return fixed(4);
    }
    if (type_name.rfind("Decimal64(", 0) == 0) {
        return fixed(8);
    }
    if (type_name.rfind("Decimal128(", 0) == 0) {
        return fixed(16);
    }

    return std::nullopt;
}

struct BlockSkipState {
    enum class Step {
        BlockInfoNum1,
        BlockInfoOverflow,
        BlockInfoNum2,
        BlockInfoBucketNum,
        BlockInfoNum0,
        NumColumns,
        NumRows,
        ColumnName,
        ColumnType,
        CustomFormatLen,
        CustomFormatBody,
        ColumnData,
        Done,
    };

    Step step{Step::NumColumns};
    VarintState varint{};
    StringState str{};
    StringState row_str{};
    SkipBytesState skip{};

    std::uint64_t num_columns{0};
    std::uint64_t num_rows{0};
    std::uint64_t col_index{0};
    std::uint8_t custom_len{0};

    std::vector<BlockSkipPlan> plans{};
    std::size_t data_plan_index{0};
    std::uint64_t row_index{0};

    void reset_for_new_block(bool has_block_info, bool has_custom_serialization) {
        step = has_block_info ? Step::BlockInfoNum1 : Step::NumColumns;
        varint.reset();
        str.reset(true);
        row_str.reset(true);
        skip.reset(0);
        num_columns = 0;
        num_rows = 0;
        col_index = 0;
        custom_len = 0;
        plans.clear();
        data_plan_index = 0;
        row_index = 0;

        (void)has_custom_serialization;
    }
};

bool advance_skip_block(
    internal::ByteRing& ring,
    BlockSkipState& state,
    std::uint64_t server_revision) {
    const bool has_custom_serialization = server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION;

    while (true) {
        switch (state.step) {
        case BlockSkipState::Step::BlockInfoNum1: {
            std::uint64_t tmp = 0;
            if (!try_read_varint64(ring, state.varint, tmp)) {
                return false;
            }
            state.step = BlockSkipState::Step::BlockInfoOverflow;
            break;
        }
        case BlockSkipState::Step::BlockInfoOverflow: {
            std::uint8_t tmp = 0;
            if (!try_read_fixed(ring, &tmp, sizeof(tmp))) {
                return false;
            }
            state.step = BlockSkipState::Step::BlockInfoNum2;
            break;
        }
        case BlockSkipState::Step::BlockInfoNum2: {
            std::uint64_t tmp = 0;
            if (!try_read_varint64(ring, state.varint, tmp)) {
                return false;
            }
            state.step = BlockSkipState::Step::BlockInfoBucketNum;
            break;
        }
        case BlockSkipState::Step::BlockInfoBucketNum: {
            std::int32_t tmp = 0;
            if (!try_read_fixed(ring, &tmp, sizeof(tmp))) {
                return false;
            }
            state.step = BlockSkipState::Step::BlockInfoNum0;
            break;
        }
        case BlockSkipState::Step::BlockInfoNum0: {
            std::uint64_t tmp = 0;
            if (!try_read_varint64(ring, state.varint, tmp)) {
                return false;
            }
            state.step = BlockSkipState::Step::NumColumns;
            break;
        }
        case BlockSkipState::Step::NumColumns: {
            if (!try_read_varint64(ring, state.varint, state.num_columns)) {
                return false;
            }
            state.plans.clear();
            state.plans.reserve(static_cast<std::size_t>(state.num_columns));
            state.step = BlockSkipState::Step::NumRows;
            break;
        }
        case BlockSkipState::Step::NumRows: {
            if (!try_read_varint64(ring, state.varint, state.num_rows)) {
                return false;
            }
            state.col_index = 0;
            state.str.reset(true);
            state.step = (state.num_columns == 0) ? BlockSkipState::Step::Done : BlockSkipState::Step::ColumnName;
            break;
        }
        case BlockSkipState::Step::ColumnName: {
            state.str.skip = true;
            if (!advance_string(ring, state.str)) {
                return false;
            }
            state.str.reset(true);
            state.step = BlockSkipState::Step::ColumnType;
            break;
        }
        case BlockSkipState::Step::ColumnType: {
            state.str.skip = false;
            if (!advance_string(ring, state.str)) {
                return false;
            }
            const std::string type_name = std::move(state.str.value);
            state.str.reset(true);

            const auto plan = build_column_skip_plan(type_name);
            if (!plan.has_value()) {
                throw UnimplementedError("unsupported column type in server block: " + type_name);
            }
            state.plans.push_back(*plan);

            if (has_custom_serialization) {
                state.step = BlockSkipState::Step::CustomFormatLen;
            } else {
                state.col_index++;
                state.step = (state.col_index >= state.num_columns) ? BlockSkipState::Step::ColumnData : BlockSkipState::Step::ColumnName;
            }
            break;
        }
        case BlockSkipState::Step::CustomFormatLen: {
            if (!try_read_fixed(ring, &state.custom_len, sizeof(state.custom_len))) {
                return false;
            }
            if (state.custom_len > 0) {
                state.skip.reset(state.custom_len);
                state.step = BlockSkipState::Step::CustomFormatBody;
            } else {
                state.col_index++;
                state.step = (state.col_index >= state.num_columns) ? BlockSkipState::Step::ColumnData : BlockSkipState::Step::ColumnName;
            }
            break;
        }
        case BlockSkipState::Step::CustomFormatBody: {
            if (!advance_skip_bytes(ring, state.skip)) {
                return false;
            }
            state.custom_len = 0;
            state.col_index++;
            state.step = (state.col_index >= state.num_columns) ? BlockSkipState::Step::ColumnData : BlockSkipState::Step::ColumnName;
            break;
        }
        case BlockSkipState::Step::ColumnData: {
            if (state.num_rows == 0 || state.plans.empty()) {
                state.step = BlockSkipState::Step::Done;
                break;
            }
            while (state.data_plan_index < state.plans.size()) {
                const auto& plan = state.plans[state.data_plan_index];
                if (plan.kind == BlockSkipPlan::Kind::Fixed) {
                    const std::uint64_t bytes = state.num_rows * static_cast<std::uint64_t>(plan.bytes_per_row);
                    if (state.skip.remaining == 0) {
                        state.skip.reset(bytes);
                    }
                    if (!advance_skip_bytes(ring, state.skip)) {
                        return false;
                    }
                } else if (plan.kind == BlockSkipPlan::Kind::String) {
                    // Each row is WireFormat::WriteString.
                    while (state.row_index < state.num_rows) {
                        state.row_str.skip = true;
                        if (!advance_string(ring, state.row_str)) {
                            return false;
                        }
                        state.row_str.reset(true);
                        state.row_index++;
                    }
                    state.row_index = 0;
                }

                state.data_plan_index++;
            }

            state.step = BlockSkipState::Step::Done;
            break;
        }
        case BlockSkipState::Step::Done: {
            state.step = BlockSkipState::Step::Done;
            return true;
        }
        }

    }
}

struct ExceptionParseState {
    enum class Step {
        Code,
        Name,
        DisplayText,
        StackTrace,
        HasNested,
        Done,
    };

    Step step{Step::Code};
    StringState str{};
    std::int32_t code{0};
    bool has_nested{false};
    std::string display_text{};

    void reset() {
        step = Step::Code;
        str.reset(false);
        code = 0;
        has_nested = false;
        display_text.clear();
    }
};

bool advance_exception(internal::ByteRing& ring, ExceptionParseState& state) {
    while (true) {
        switch (state.step) {
        case ExceptionParseState::Step::Code: {
            if (!try_read_fixed(ring, &state.code, sizeof(state.code))) {
                return false;
            }
            state.step = ExceptionParseState::Step::Name;
            state.str.reset(true);
            break;
        }
        case ExceptionParseState::Step::Name: {
            state.str.skip = true;
            if (!advance_string(ring, state.str)) {
                return false;
            }
            state.str.reset(false);
            state.step = ExceptionParseState::Step::DisplayText;
            break;
        }
        case ExceptionParseState::Step::DisplayText: {
            state.str.skip = false;
            if (!advance_string(ring, state.str)) {
                return false;
            }
            if (state.display_text.empty()) {
                state.display_text = std::move(state.str.value);
            }
            state.str.reset(true);
            state.step = ExceptionParseState::Step::StackTrace;
            break;
        }
        case ExceptionParseState::Step::StackTrace: {
            state.str.skip = true;
            if (!advance_string(ring, state.str)) {
                return false;
            }
            state.str.reset(true);
            state.step = ExceptionParseState::Step::HasNested;
            break;
        }
        case ExceptionParseState::Step::HasNested: {
            if (!try_read_fixed(ring, &state.has_nested, sizeof(state.has_nested))) {
                return false;
            }
            if (state.has_nested) {
                // Parse nested exception but keep only outer display_text.
                state.step = ExceptionParseState::Step::Code;
                state.has_nested = false;
            } else {
                state.step = ExceptionParseState::Step::Done;
            }
            break;
        }
        case ExceptionParseState::Step::Done:
            return true;
        }
    }
}

struct PacketEvent {
    enum class Type { Data, EndOfStream, Exception, Other };
    Type type{Type::Other};
    std::string exception_message{};
};

struct PacketParseState {
    enum class State {
        PacketType,
        Hello,
        Progress,
        Data,
        Exception,
        Log,
        ProfileInfo,
        TableColumns,
        ProfileEvents,
        Done,
    };

    State state{State::PacketType};
    VarintState varint{};
    std::uint64_t packet_type{0};

    // Generic helpers
    StringState string{};
    BlockSkipState block{};
    ExceptionParseState exception{};

    // Progress
    std::uint64_t progress_rows{0};
    std::uint64_t progress_bytes{0};
    std::uint64_t progress_total_rows{0};
    std::uint64_t progress_written_rows{0};
    std::uint64_t progress_written_bytes{0};
    std::uint8_t progress_step{0};

    // ProfileInfo
    std::uint8_t profile_step{0};
    std::uint64_t profile_u64{0};
    bool profile_bool{false};

    void reset_for_next_packet() {
        state = State::PacketType;
        varint.reset();
        packet_type = 0;
        string.reset(true);
        block.reset_for_new_block(false, false);
        exception.reset();
        progress_step = 0;
        profile_step = 0;
        profile_u64 = 0;
        profile_bool = false;
    }
};

bool advance_packet(
    internal::ByteRing& ring,
    PacketParseState& state,
    std::uint64_t server_revision,
    PacketEvent& out_event) {
    while (true) {
        switch (state.state) {
        case PacketParseState::State::PacketType: {
            if (!try_read_varint64(ring, state.varint, state.packet_type)) {
                return false;
            }
            switch (state.packet_type) {
            case ServerCodes::Data:
                state.state = PacketParseState::State::Data;
                state.string.reset(true);
                state.block.reset_for_new_block(server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO,
                                                server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION);
                break;
            case ServerCodes::Progress:
                state.state = PacketParseState::State::Progress;
                state.progress_step = 0;
                break;
            case ServerCodes::Exception:
                state.state = PacketParseState::State::Exception;
                state.exception.reset();
                break;
            case ServerCodes::EndOfStream:
                out_event.type = PacketEvent::Type::EndOfStream;
                state.reset_for_next_packet();
                return true;
            case ServerCodes::Log:
                state.state = PacketParseState::State::Log;
                state.string.reset(true);
                state.block.reset_for_new_block(server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO,
                                                server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION);
                break;
            case ServerCodes::ProfileInfo:
                state.state = PacketParseState::State::ProfileInfo;
                state.profile_step = 0;
                break;
            case ServerCodes::TableColumns:
                state.state = PacketParseState::State::TableColumns;
                state.string.reset(true);
                break;
            case ServerCodes::ProfileEvents:
                state.state = PacketParseState::State::ProfileEvents;
                state.string.reset(true);
                state.block.reset_for_new_block(server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO,
                                                server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION);
                break;
            case ServerCodes::Hello:
            case ServerCodes::Pong:
                // Ignore.
                out_event.type = PacketEvent::Type::Other;
                state.reset_for_next_packet();
                return true;
            default:
                throw UnimplementedError("unimplemented server packet " + std::to_string(static_cast<int>(state.packet_type)));
            }
            break;
        }
        case PacketParseState::State::Progress: {
            // rows, bytes, total_rows always for our protocol revision; written_* gated by server revision.
            if (state.progress_step == 0) {
                if (!try_read_varint64(ring, state.varint, state.progress_rows)) {
                    return false;
                }
                state.progress_step = 1;
            }
            if (state.progress_step == 1) {
                if (!try_read_varint64(ring, state.varint, state.progress_bytes)) {
                    return false;
                }
                state.progress_step = 2;
            }
            if constexpr (CLIENT_PROTOCOL_REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) {
                if (state.progress_step == 2) {
                    if (!try_read_varint64(ring, state.varint, state.progress_total_rows)) {
                        return false;
                    }
                    state.progress_step = 3;
                }
            }
            if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO) {
                if (state.progress_step == 3) {
                    if (!try_read_varint64(ring, state.varint, state.progress_written_rows)) {
                        return false;
                    }
                    state.progress_step = 4;
                }
                if (state.progress_step == 4) {
                    if (!try_read_varint64(ring, state.varint, state.progress_written_bytes)) {
                        return false;
                    }
                }
            }
            out_event.type = PacketEvent::Type::Other;
            state.reset_for_next_packet();
            return true;
        }
        case PacketParseState::State::Data: {
            if (server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
                state.string.skip = true;
                if (!advance_string(ring, state.string)) {
                    return false;
                }
                state.string.reset(true);
            }
            if (!advance_skip_block(ring, state.block, server_revision)) {
                return false;
            }
            out_event.type = PacketEvent::Type::Data;
            state.reset_for_next_packet();
            return true;
        }
        case PacketParseState::State::Exception: {
            if (!advance_exception(ring, state.exception)) {
                return false;
            }
            out_event.type = PacketEvent::Type::Exception;
            out_event.exception_message = state.exception.display_text.empty() ? "server exception" : state.exception.display_text;
            state.reset_for_next_packet();
            return true;
        }
        case PacketParseState::State::Log: {
            state.string.skip = true;
            if (!advance_string(ring, state.string)) {
                return false;
            }
            state.string.reset(true);
            if (!advance_skip_block(ring, state.block, server_revision)) {
                return false;
            }
            out_event.type = PacketEvent::Type::Other;
            state.reset_for_next_packet();
            return true;
        }
        case PacketParseState::State::ProfileInfo: {
            // rows(u64), blocks(u64), bytes(u64), applied_limit(bool), rows_before_limit(u64), calculated(bool)
            if (state.profile_step == 0) {
                if (!try_read_varint64(ring, state.varint, state.profile_u64)) {
                    return false;
                }
                state.profile_step = 1;
            }
            if (state.profile_step == 1) {
                if (!try_read_varint64(ring, state.varint, state.profile_u64)) {
                    return false;
                }
                state.profile_step = 2;
            }
            if (state.profile_step == 2) {
                if (!try_read_varint64(ring, state.varint, state.profile_u64)) {
                    return false;
                }
                state.profile_step = 3;
            }
            if (state.profile_step == 3) {
                if (!try_read_fixed(ring, &state.profile_bool, sizeof(state.profile_bool))) {
                    return false;
                }
                state.profile_step = 4;
            }
            if (state.profile_step == 4) {
                if (!try_read_varint64(ring, state.varint, state.profile_u64)) {
                    return false;
                }
                state.profile_step = 5;
            }
            if (state.profile_step == 5) {
                if (!try_read_fixed(ring, &state.profile_bool, sizeof(state.profile_bool))) {
                    return false;
                }
            }
            out_event.type = PacketEvent::Type::Other;
            state.reset_for_next_packet();
            return true;
        }
        case PacketParseState::State::TableColumns: {
            // external table name
            state.string.skip = true;
            if (!advance_string(ring, state.string)) {
                return false;
            }
            state.string.reset(true);
            // columns metadata
            state.string.skip = true;
            if (!advance_string(ring, state.string)) {
                return false;
            }
            out_event.type = PacketEvent::Type::Other;
            state.reset_for_next_packet();
            return true;
        }
        case PacketParseState::State::ProfileEvents: {
            state.string.skip = true;
            if (!advance_string(ring, state.string)) {
                return false;
            }
            state.string.reset(true);
            if (!advance_skip_block(ring, state.block, server_revision)) {
                return false;
            }
            out_event.type = PacketEvent::Type::Other;
            state.reset_for_next_packet();
            return true;
        }
        case PacketParseState::State::Done:
            return true;
        case PacketParseState::State::Hello:
            throw ProtocolError("unexpected hello packet in generic parser");
        }
    }
}

void write_block(
    const Block& block,
    OutputStream& output,
    std::uint64_t server_revision) {
    if (server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
        WireFormat::WriteUInt64(output, 1);
        WireFormat::WriteFixed<std::uint8_t>(output, block.Info().is_overflows);
        WireFormat::WriteUInt64(output, 2);
        WireFormat::WriteFixed<std::int32_t>(output, block.Info().bucket_num);
        WireFormat::WriteUInt64(output, 0);
    }

    WireFormat::WriteUInt64(output, block.GetColumnCount());
    WireFormat::WriteUInt64(output, block.GetRowCount());

    for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
        WireFormat::WriteString(output, bi.Name());
        WireFormat::WriteString(output, bi.Type()->GetName());

        if (server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION) {
            WireFormat::WriteFixed<std::uint8_t>(output, 0);
        }

        const bool contains_data = block.GetRowCount() > 0;
        if (contains_data) {
            bi.Column()->Save(&output);
        }
    }

    output.Flush();
}

void encode_data_packet(
    Buffer& out,
    const Block& block,
    std::uint64_t server_revision) {
    out.clear();
    BufferOutput output(&out);
    WireFormat::WriteUInt64(output, ClientCodes::Data);
    if (server_revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
        WireFormat::WriteString(output, std::string());
    }
    write_block(block, output, server_revision);
    output.Flush();
}

void encode_query_packet(
    Buffer& out,
    const std::string& query_text,
    std::string_view query_id,
    std::uint64_t server_revision) {
    out.clear();
    BufferOutput output(&out);

    WireFormat::WriteUInt64(output, ClientCodes::Query);
    WireFormat::WriteString(output, query_id);

    if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO) {
        ClientInfoLite info;

        WireFormat::WriteFixed(output, info.query_kind);
        WireFormat::WriteString(output, info.initial_user);
        WireFormat::WriteString(output, info.initial_query_id);
        WireFormat::WriteString(output, info.initial_address);
        if (server_revision >= DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME) {
            WireFormat::WriteFixed<std::int64_t>(output, 0);
        }
        WireFormat::WriteFixed(output, info.iface_type);

        WireFormat::WriteString(output, info.os_user);
        WireFormat::WriteString(output, info.client_hostname);
        WireFormat::WriteString(output, info.client_name);
        WireFormat::WriteUInt64(output, info.client_version_major);
        WireFormat::WriteUInt64(output, info.client_version_minor);
        WireFormat::WriteUInt64(output, info.client_revision);

        if (server_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO) {
            WireFormat::WriteString(output, info.quota_key);
        }
        if (server_revision >= DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH) {
            WireFormat::WriteUInt64(output, 0u);
        }
        if (server_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
            WireFormat::WriteUInt64(output, info.client_version_patch);
        }
        if (server_revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY) {
            WireFormat::WriteFixed<std::uint8_t>(output, 0);
        }
        if (server_revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS) {
            WireFormat::WriteUInt64(output, 0);
            WireFormat::WriteUInt64(output, 0);
            WireFormat::WriteUInt64(output, 0);
        }
    }

    // Per-query settings (none).
    if (server_revision < DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS) {
        // Older servers require binary settings serialization, which isn't implemented here.
        throw UnimplementedError("async client requires a newer ClickHouse server revision");
    }
    WireFormat::WriteString(output, std::string());

    if (server_revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET) {
        WireFormat::WriteString(output, "");
    }

    WireFormat::WriteUInt64(output, Stages::Complete);
    WireFormat::WriteUInt64(output, CompressionState::Disable);
    WireFormat::WriteString(output, query_text);

    if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS) {
        WireFormat::WriteString(output, std::string());
    }

    output.Flush();
}

void encode_hello_packet(Buffer& out, const AsyncClientOptions& options) {
    out.clear();
    BufferOutput output(&out);

    WireFormat::WriteUInt64(output, ClientCodes::Hello);
    WireFormat::WriteString(output, std::string("clickhouse-cpp"));
    WireFormat::WriteUInt64(output, CLICKHOUSE_CPP_VERSION_MAJOR);
    WireFormat::WriteUInt64(output, CLICKHOUSE_CPP_VERSION_MINOR);
    WireFormat::WriteUInt64(output, CLIENT_PROTOCOL_REVISION);
    WireFormat::WriteString(output, options.database);
    WireFormat::WriteString(output, options.user);
    WireFormat::WriteString(output, options.password);
    output.Flush();
}

struct HelloParseState {
    enum class Step {
        PacketType,
        Name,
        VersionMajor,
        VersionMinor,
        Revision,
        Timezone,
        DisplayName,
        VersionPatch,
        Exception,
        Done,
    };

    Step step{Step::PacketType};
    VarintState varint{};
    StringState str{};
    ExceptionParseState exception{};
    std::uint64_t packet_type{0};

    void reset() {
        step = Step::PacketType;
        varint.reset();
        str.reset(true);
        exception.reset();
        packet_type = 0;
    }
};

enum class HelloParseResult {
    NeedMoreData,
    Success,
    Exception,
};

HelloParseResult advance_server_hello(
    internal::ByteRing& ring,
    HelloParseState& state,
    ServerInfoLite& out,
    std::string& out_exception_message) {
    while (true) {
        switch (state.step) {
        case HelloParseState::Step::PacketType: {
            if (!try_read_varint64(ring, state.varint, state.packet_type)) {
                return HelloParseResult::NeedMoreData;
            }
            if (state.packet_type == ServerCodes::Hello) {
                state.step = HelloParseState::Step::Name;
                state.str.reset(false);
            } else if (state.packet_type == ServerCodes::Exception) {
                state.step = HelloParseState::Step::Exception;
                state.exception.reset();
            } else {
                throw ProtocolError("unexpected packet during handshake");
            }
            break;
        }
        case HelloParseState::Step::Exception: {
            if (!advance_exception(ring, state.exception)) {
                return HelloParseResult::NeedMoreData;
            }
            out_exception_message = state.exception.display_text.empty() ? "server exception" : state.exception.display_text;
            state.reset();
            return HelloParseResult::Exception;
        }
        case HelloParseState::Step::Name: {
            state.str.skip = false;
            if (!advance_string(ring, state.str)) {
                return HelloParseResult::NeedMoreData;
            }
            out.name = std::move(state.str.value);
            state.str.reset(true);
            state.step = HelloParseState::Step::VersionMajor;
            break;
        }
        case HelloParseState::Step::VersionMajor: {
            if (!try_read_varint64(ring, state.varint, out.version_major)) {
                return HelloParseResult::NeedMoreData;
            }
            state.step = HelloParseState::Step::VersionMinor;
            break;
        }
        case HelloParseState::Step::VersionMinor: {
            if (!try_read_varint64(ring, state.varint, out.version_minor)) {
                return HelloParseResult::NeedMoreData;
            }
            state.step = HelloParseState::Step::Revision;
            break;
        }
        case HelloParseState::Step::Revision: {
            if (!try_read_varint64(ring, state.varint, out.revision)) {
                return HelloParseResult::NeedMoreData;
            }
            if (out.revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
                state.step = HelloParseState::Step::Timezone;
                state.str.reset(false);
            } else if (out.revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME) {
                state.step = HelloParseState::Step::DisplayName;
                state.str.reset(false);
            } else if (out.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
                state.step = HelloParseState::Step::VersionPatch;
            } else {
                state.step = HelloParseState::Step::Done;
            }
            break;
        }
        case HelloParseState::Step::Timezone: {
            state.str.skip = false;
            if (!advance_string(ring, state.str)) {
                return HelloParseResult::NeedMoreData;
            }
            out.timezone = std::move(state.str.value);
            state.str.reset(true);
            if (out.revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME) {
                state.step = HelloParseState::Step::DisplayName;
                state.str.reset(false);
            } else if (out.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
                state.step = HelloParseState::Step::VersionPatch;
            } else {
                state.step = HelloParseState::Step::Done;
            }
            break;
        }
        case HelloParseState::Step::DisplayName: {
            state.str.skip = false;
            if (!advance_string(ring, state.str)) {
                return HelloParseResult::NeedMoreData;
            }
            out.display_name = std::move(state.str.value);
            state.str.reset(true);
            if (out.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
                state.step = HelloParseState::Step::VersionPatch;
            } else {
                state.step = HelloParseState::Step::Done;
            }
            break;
        }
        case HelloParseState::Step::VersionPatch: {
            if (!try_read_varint64(ring, state.varint, out.version_patch)) {
                return HelloParseResult::NeedMoreData;
            }
            state.step = HelloParseState::Step::Done;
            break;
        }
        case HelloParseState::Step::Done: {
            state.reset();
            return HelloParseResult::Success;
        }
        }
    }
}

}  // namespace

class AsyncClient::Impl {
public:
    explicit Impl(AsyncClientOptions options)
        : options_(std::move(options))
        , inbox_(std::max<std::size_t>(options_.inbox_ring_bytes, 1u))
    {
        last_progress_at_ = std::chrono::steady_clock::now();
    }

    void start_connect() {
        clear_disabled_if_expired(std::chrono::steady_clock::now());
        if (is_disabled()) {
            return;
        }

        close();
        connect_started_at_ = std::chrono::steady_clock::now();

        encode_hello_packet(hello_bytes_, options_);
        hello_offset_ = 0;

        const NetworkAddress address(options_.host, std::to_string(static_cast<unsigned long long>(options_.port)));
        const auto connect_res = socket_.start_connect(address);

        if (connect_res == NonBlockingSocket::ConnectStartResult::connected) {
            state_ = State::HandshakingSendHello;
        } else {
            state_ = State::Connecting;
        }

        last_progress_at_ = connect_started_at_;
    }

    void close() {
        socket_.close();
        inbox_.clear();
        packet_state_.reset_for_next_packet();
        hello_state_.reset();
        server_info_ = {};
        state_ = State::Disconnected;
        hello_offset_ = 0;
        addendum_offset_ = 0;
        current_tx_.reset();
        current_tx_offset_ = 0;
        current_request_phase_.reset();
    }

    bool connected() const noexcept {
        return state_ == State::Ready || state_ == State::RequestSendQuery || state_ == State::RequestWaitForData
            || state_ == State::RequestSendBlock || state_ == State::RequestSendEnd || state_ == State::RequestWaitForEOS;
    }

    bool disabled() const noexcept { return is_disabled(); }

    std::size_t inflight_requests() const noexcept { return requests_.size(); }
    std::size_t inflight_bytes() const noexcept { return inflight_bytes_; }

    EnqueueResult enqueue_insert(std::string_view table, const Block& block, std::string_view query_id) {
        clear_disabled_if_expired(std::chrono::steady_clock::now());
        if (is_disabled()) {
            return EnqueueResult::disabled;
        }
        if (!connected() || state_ == State::Connecting || state_ == State::HandshakingSendHello || state_ == State::HandshakingRecvHello
            || state_ == State::HandshakingSendAddendum) {
            return EnqueueResult::not_connected;
        }
        if (state_ != State::Ready && state_ != State::RequestSendQuery && state_ != State::RequestWaitForData
            && state_ != State::RequestSendBlock && state_ != State::RequestSendEnd && state_ != State::RequestWaitForEOS) {
            return EnqueueResult::not_connected;
        }
        if (requests_.size() >= options_.max_inflight_requests) {
            return EnqueueResult::dropped;
        }

        // Encode INSERT query + terminator empty block.
        std::stringstream fields;
        const auto num_columns = block.GetColumnCount();
        for (unsigned int i = 0; i < num_columns; ++i) {
            if (i != 0) {
                fields << ",";
            }
            fields << QuoteIdentifier(block.GetColumnName(i));
        }

        const std::string query_text = "INSERT INTO " + std::string(table) + " ( " + fields.str() + " ) VALUES";

        Request req;
        encode_query_packet(req.query_bytes, query_text, query_id, server_info_.revision);
        encode_data_packet(req.query_terminator_bytes, Block(), server_info_.revision);
        encode_data_packet(req.data_bytes, block, server_info_.revision);
        encode_data_packet(req.end_bytes, Block(), server_info_.revision);

        req.total_bytes = req.query_bytes.size() + req.query_terminator_bytes.size() + req.data_bytes.size() + req.end_bytes.size();
        if (inflight_bytes_ + req.total_bytes > options_.max_inflight_bytes) {
            return EnqueueResult::dropped;
        }

        inflight_bytes_ += req.total_bytes;
        requests_.push_back(std::move(req));
        if (state_ == State::Ready) {
            begin_next_request_if_needed(std::chrono::steady_clock::now());
        }
        return EnqueueResult::queued;
    }

    PollResult poll(std::chrono::steady_clock::time_point now, std::chrono::nanoseconds budget) {
        clear_disabled_if_expired(now);

        PollResult result;
        result.connected = connected();

        if (is_disabled()) {
            return result;
        }

        if (budget <= std::chrono::nanoseconds::zero()) {
            return result;
        }

        const auto deadline = now + budget;

        try {
            for (;;) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    break;
                }

                if (options_.stall_timeout.count() > 0 && state_ != State::Disconnected && state_ != State::Connecting && state_ != State::Ready
                    && (now - last_progress_at_) > options_.stall_timeout) {
                    const auto dropped = requests_.size();
                    trip_breaker(now, "stall timeout");
                    result.requests_failed += dropped;
                    result.progressed = true;
                    result.connected = connected();
                    break;
                }

                bool progressed = false;
                progressed |= advance_io(now, result);
                progressed |= advance_state(now, result);

                if (!progressed) {
                    break;
                }
                result.progressed = true;
            }
        } catch (const std::exception& e) {
            const auto dropped = requests_.size();
            trip_breaker(now, e.what());
            result.requests_failed += dropped;
            result.progressed = true;
        }

        result.connected = connected();
        return result;
    }

private:
    static constexpr std::size_t kMaxIoChunkBytes = 64 * 1024;

    enum class State {
        Disconnected,
        Connecting,
        HandshakingSendHello,
        HandshakingRecvHello,
        HandshakingSendAddendum,
        Ready,
        RequestSendQuery,
        RequestWaitForData,
        RequestSendBlock,
        RequestSendEnd,
        RequestWaitForEOS,
    };

    struct Request {
        Buffer query_bytes;
        Buffer query_terminator_bytes;
        Buffer data_bytes;
        Buffer end_bytes;
        std::size_t total_bytes{0};
    };

    bool is_disabled() const noexcept {
        if (!disabled_until_.has_value()) {
            return false;
        }
        return std::chrono::steady_clock::now() < *disabled_until_;
    }

    void clear_disabled_if_expired(std::chrono::steady_clock::time_point now) {
        if (disabled_until_.has_value() && now >= *disabled_until_) {
            disabled_until_.reset();
        }
    }

    void trip_breaker(std::chrono::steady_clock::time_point now, const char* /*reason*/) {
        socket_.close();
        inbox_.clear();
        packet_state_.reset_for_next_packet();
        hello_state_.reset();
        server_info_ = {};

        // Drop queued requests.
        requests_.clear();
        inflight_bytes_ = 0;
        current_tx_.reset();
        current_tx_offset_ = 0;
        current_request_phase_.reset();
        state_ = State::Disconnected;

        disabled_until_ = now + options_.cooldown;
    }

    bool begin_next_request_if_needed(std::chrono::steady_clock::time_point now) {
        if (state_ != State::Ready) {
            return false;
        }
        if (requests_.empty()) {
            return false;
        }
        last_progress_at_ = now;
        current_request_phase_ = RequestPhase::SendingQuery;
        current_tx_ = &requests_.front().query_bytes;
        current_tx_offset_ = 0;
        state_ = State::RequestSendQuery;
        return true;
    }

    enum class RequestPhase { SendingQuery, SendingQueryTerminator, WaitingForData, SendingBlock, SendingEnd, WaitingForEOS };

    bool advance_io(std::chrono::steady_clock::time_point now, PollResult& out) {
        if (!socket_.is_open()) {
            return false;
        }

        bool progressed = false;

        // Try sending current bytes if any.
        if (current_tx_ && current_tx_offset_ < (*current_tx_)->size()) {
            bool would_block = false;
            const auto* data = (*current_tx_)->data() + current_tx_offset_;
            const std::size_t remaining = (*current_tx_)->size() - current_tx_offset_;
            const std::size_t to_send = std::min(remaining, kMaxIoChunkBytes);
            const std::size_t sent = socket_.send_some(data, to_send, would_block);
            if (sent > 0) {
                current_tx_offset_ += sent;
                out.bytes_sent += sent;
                progressed = true;
                last_progress_at_ = now;
            }
        }

        // Try receiving if we have room.
        // Avoid recv() while a nonblocking connect() is still in progress.
        if (state_ != State::Connecting && inbox_.available() > 0) {
            auto span = inbox_.write_span();
            if (span.size > 0) {
                bool would_block = false;
                const std::size_t to_recv = std::min(span.size, kMaxIoChunkBytes);
                const std::size_t received = socket_.recv_some(span.data, to_recv, would_block);
                if (received > 0) {
                    inbox_.commit_write(received);
                    out.bytes_recv += received;
                    progressed = true;
                    last_progress_at_ = now;
                } else if (received == 0 && !would_block) {
                    // Remote closed.
                    const auto dropped = requests_.size();
                    trip_breaker(now, "connection closed");
                    out.requests_failed += dropped;
                    progressed = true;
                }
            }
        }

        return progressed;
    }

    bool advance_state(std::chrono::steady_clock::time_point now, PollResult& out) {
        bool progressed = false;

        switch (state_) {
        case State::Disconnected:
            return false;
        case State::Connecting: {
            if (options_.connect_timeout.count() > 0 && (now - connect_started_at_) > options_.connect_timeout) {
                const auto dropped = requests_.size();
                trip_breaker(now, "connect timeout");
                out.requests_failed += dropped;
                return true;
            }
            if (socket_.poll_connected()) {
                last_progress_at_ = now;
                state_ = State::HandshakingSendHello;
                progressed = true;
            }
            break;
        }
        case State::HandshakingSendHello: {
            if (!socket_.is_open()) {
                return false;
            }
            // Reuse transmit slot for hello.
            if (hello_offset_ < hello_bytes_.size()) {
                bool would_block = false;
                const std::size_t sent = socket_.send_some(hello_bytes_.data() + hello_offset_, hello_bytes_.size() - hello_offset_, would_block);
                if (sent > 0) {
                    hello_offset_ += sent;
                    out.bytes_sent += sent;
                    last_progress_at_ = now;
                    progressed = true;
                }
                if (hello_offset_ < hello_bytes_.size()) {
                    break;
                }
            }
            state_ = State::HandshakingRecvHello;
            progressed = true;
            break;
        }
        case State::HandshakingRecvHello: {
            std::string handshake_exception;
            const auto hello_res = advance_server_hello(inbox_, hello_state_, server_info_, handshake_exception);
            if (hello_res == HelloParseResult::NeedMoreData) {
                break;
            }
            if (hello_res == HelloParseResult::Exception) {
                const auto dropped = requests_.size();
                trip_breaker(now, handshake_exception.c_str());
                out.requests_failed += dropped;
                progressed = true;
                break;
            }

            if (server_info_.revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM) {
                addendum_bytes_.clear();
                addendum_bytes_.push_back(0);  // empty string
                addendum_offset_ = 0;
                state_ = State::HandshakingSendAddendum;
            } else {
                state_ = State::Ready;
                begin_next_request_if_needed(now);
            }
            progressed = true;
            break;
        }
        case State::HandshakingSendAddendum: {
            if (addendum_offset_ < addendum_bytes_.size()) {
                bool would_block = false;
                const std::size_t sent = socket_.send_some(addendum_bytes_.data() + addendum_offset_,
                                                          addendum_bytes_.size() - addendum_offset_,
                                                          would_block);
                if (sent > 0) {
                    addendum_offset_ += sent;
                    out.bytes_sent += sent;
                    last_progress_at_ = now;
                    progressed = true;
                }
                if (addendum_offset_ < addendum_bytes_.size()) {
                    break;
                }
            }
            state_ = State::Ready;
            begin_next_request_if_needed(now);
            progressed = true;
            break;
        }
        case State::Ready:
            return begin_next_request_if_needed(now);
        case State::RequestSendQuery:
        case State::RequestSendBlock:
        case State::RequestSendEnd: {
            progressed |= advance_send_phases(now, out);
            break;
        }
        case State::RequestWaitForData:
        case State::RequestWaitForEOS: {
            progressed |= advance_wait_phases(now, out);
            break;
        }
        }

        return progressed;
    }

    bool advance_send_phases(std::chrono::steady_clock::time_point now, PollResult& out) {
        if (!current_request_phase_.has_value() || requests_.empty()) {
            state_ = State::Ready;
            return false;
        }
        if (!current_tx_ || current_tx_offset_ < (*current_tx_)->size()) {
            return false;
        }

        // Segment fully sent.
        switch (*current_request_phase_) {
        case RequestPhase::SendingQuery:
            current_request_phase_ = RequestPhase::SendingQueryTerminator;
            current_tx_ = &requests_.front().query_terminator_bytes;
            current_tx_offset_ = 0;
            state_ = State::RequestSendQuery;
            return true;
        case RequestPhase::SendingQueryTerminator:
            current_request_phase_ = RequestPhase::WaitingForData;
            current_tx_.reset();
            current_tx_offset_ = 0;
            state_ = State::RequestWaitForData;
            return true;
        case RequestPhase::SendingBlock:
            current_request_phase_ = RequestPhase::SendingEnd;
            current_tx_ = &requests_.front().end_bytes;
            current_tx_offset_ = 0;
            state_ = State::RequestSendEnd;
            return true;
        case RequestPhase::SendingEnd:
            current_request_phase_ = RequestPhase::WaitingForEOS;
            current_tx_.reset();
            current_tx_offset_ = 0;
            state_ = State::RequestWaitForEOS;
            return true;
        case RequestPhase::WaitingForData:
        case RequestPhase::WaitingForEOS:
            break;
        }

        (void)now;
        (void)out;
        return false;
    }

    bool advance_wait_phases(std::chrono::steady_clock::time_point now, PollResult& out) {
        if (!current_request_phase_.has_value() || requests_.empty()) {
            state_ = State::Ready;
            return false;
        }

        bool progressed = false;
        for (;;) {
            PacketEvent ev;
            if (!advance_packet(inbox_, packet_state_, server_info_.revision, ev)) {
                break;
            }
            progressed = true;
            last_progress_at_ = now;

            if (*current_request_phase_ == RequestPhase::WaitingForData) {
                if (ev.type == PacketEvent::Type::Data) {
                    current_request_phase_ = RequestPhase::SendingBlock;
                    current_tx_ = &requests_.front().data_bytes;
                    current_tx_offset_ = 0;
                    state_ = State::RequestSendBlock;
                    break;
                }
                if (ev.type == PacketEvent::Type::Exception) {
                    const auto dropped = requests_.size();
                    trip_breaker(now, "server exception");
                    out.requests_failed += dropped;
                    break;
                }
            } else if (*current_request_phase_ == RequestPhase::WaitingForEOS) {
                if (ev.type == PacketEvent::Type::EndOfStream) {
                    out.requests_completed += 1;
                    finish_current_request(now);
                    break;
                }
                if (ev.type == PacketEvent::Type::Exception) {
                    out.requests_failed += 1;
                    finish_current_request(now);
                    break;
                }
            }
        }

        return progressed;
    }

    void finish_current_request(std::chrono::steady_clock::time_point now) {
        if (requests_.empty()) {
            current_request_phase_.reset();
            state_ = State::Ready;
            return;
        }

        inflight_bytes_ -= requests_.front().total_bytes;
        requests_.pop_front();

        current_tx_.reset();
        current_tx_offset_ = 0;
        current_request_phase_.reset();
        state_ = State::Ready;
        begin_next_request_if_needed(now);
    }

private:
    AsyncClientOptions options_;

    State state_{State::Disconnected};
    NonBlockingSocket socket_{};
    internal::ByteRing inbox_;

    PacketParseState packet_state_{};
    HelloParseState hello_state_{};
    ServerInfoLite server_info_{};

    Buffer hello_bytes_{};
    std::size_t hello_offset_{0};
    Buffer addendum_bytes_{};
    std::size_t addendum_offset_{0};

    std::deque<Request> requests_{};
    std::size_t inflight_bytes_{0};

    std::optional<std::chrono::steady_clock::time_point> disabled_until_{};

    std::chrono::steady_clock::time_point connect_started_at_{};
    std::chrono::steady_clock::time_point last_progress_at_{};

    std::optional<RequestPhase> current_request_phase_{};
    std::optional<Buffer*> current_tx_{};
    std::size_t current_tx_offset_{0};
};

AsyncClient::AsyncClient(AsyncClientOptions options)
    : impl_(std::make_unique<Impl>(std::move(options)))
{}

AsyncClient::~AsyncClient() = default;

void AsyncClient::start_connect() { impl_->start_connect(); }
void AsyncClient::close() { impl_->close(); }
bool AsyncClient::connected() const noexcept { return impl_->connected(); }
bool AsyncClient::disabled() const noexcept { return impl_->disabled(); }

EnqueueResult AsyncClient::enqueue_insert(std::string_view table, const Block& block, std::string_view query_id) {
    return impl_->enqueue_insert(table, block, query_id);
}

PollResult AsyncClient::poll(std::chrono::steady_clock::time_point now, std::chrono::nanoseconds budget) {
    return impl_->poll(now, budget);
}

std::size_t AsyncClient::inflight_requests() const noexcept { return impl_->inflight_requests(); }
std::size_t AsyncClient::inflight_bytes() const noexcept { return impl_->inflight_bytes(); }

}  // namespace clickhouse

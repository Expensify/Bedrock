#pragma once

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace BedrockTestPorts {

constexpr uint32_t VERSION = 1;
constexpr uint16_t START_PORT = 10000;
constexpr uint16_t MAX_PORT = 20000;
constexpr int TIMEOUT_MS = 10000;

enum class Operation : uint32_t { REGISTER, GET_PORT, RETURN_PORT, DISCONNECT };

// Local IPC only. Responses repeat the operation and use negative errno values for errors.
struct Message {
    uint32_t version = VERSION;
    Operation operation = Operation::REGISTER;
    int32_t value = 0;
};
static_assert(sizeof(Message) == 12);

class FD {
public:
    explicit FD(int value = -1) : value(value) {}
    ~FD() { if (value >= 0) { close(value); } }
    FD(const FD&) = delete;
    FD& operator=(const FD&) = delete;
    int release() { const int result = value; value = -1; return result; }
    int value;
};

inline std::system_error error(const std::string& operation, int code = errno)
{
    return std::system_error(code, std::generic_category(), "Test port server: " + operation);
}

inline void configureSocket(int fd, bool nonblocking = false)
{
    if (fd < 0 || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0 ||
        (nonblocking && fcntl(fd, F_SETFL, O_NONBLOCK) < 0)) {
        throw error("configure socket");
    }
#ifdef __APPLE__
    int enabled = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) < 0) {
        throw error("disable SIGPIPE");
    }
#endif
}

inline ssize_t sendBytes(int fd, const void* bytes, size_t size)
{
#ifdef MSG_NOSIGNAL
    return send(fd, bytes, size, MSG_NOSIGNAL);
#else
    return send(fd, bytes, size, 0);
#endif
}

inline std::string directory()
{
    return "/tmp/bedrock-test-ports-" + std::to_string(getuid());
}

inline void ensureDirectory(const std::string& path = directory())
{
    if (mkdir(path.c_str(), 0700) < 0 && errno != EEXIST) {
        throw error("create runtime directory");
    }
    struct stat info;
    if (lstat(path.c_str(), &info) < 0 || !S_ISDIR(info.st_mode) ||
        info.st_uid != getuid() || (info.st_mode & 0777) != 0700) {
        throw error("runtime directory must be owned by this user with mode 0700", EACCES);
    }
}

inline sockaddr_un address(const std::string& runtimeDirectory = directory())
{
    sockaddr_un result = {};
    result.sun_family = AF_UNIX;
    const std::string path = runtimeDirectory + "/server.sock";
    if (path.size() >= sizeof(result.sun_path)) {
        throw error("socket path too long", ENAMETOOLONG);
    }
    memcpy(result.sun_path, path.c_str(), path.size() + 1);
    return result;
}

// Used by the client only. Server IO is nonblocking and handled by its event loop.
inline void transfer(int fd, void* bytes, size_t size, bool sending)
{
    auto* position = static_cast<char*>(bytes);
    while (size) {
        const ssize_t count = sending ? sendBytes(fd, position, size) : recv(fd, position, size, 0);
        if (count > 0) {
            position += count;
            size -= count;
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            throw error(sending ? "send request" : "receive response", count == 0 ? ECONNRESET : errno);
        }
    }
}

inline Message exchange(int fd, Operation operation, int32_t value = 0)
{
    Message message{VERSION, operation, value};
    transfer(fd, &message, sizeof(message), true);
    transfer(fd, &message, sizeof(message), false);
    if (message.version != VERSION || message.operation != operation) {
        throw error("invalid protocol response", EPROTO);
    }
    return message;
}

inline Message request(int fd, Operation operation, int32_t value = 0)
{
    const Message message = exchange(fd, operation, value);
    if (message.value < 0) {
        throw error("request rejected", -message.value);
    }
    return message;
}

inline void clientTimeout(int fd)
{
    timeval timeout{TIMEOUT_MS / 1000, 0};
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
        throw error("configure client timeout");
    }
}

} // namespace BedrockTestPorts

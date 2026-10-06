#include <test/lib/PortServerClient.h>
#include <test/lib/PortServerProtocol.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <netinet/in.h>
#include <pthread.h>
#include <set>
#include <signal.h>
#include <spawn.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <thread>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

extern char** environ;

using namespace std;
using namespace BedrockTestPorts;

namespace {

struct Registry {
    mutex lock;
    set<PortServerClient*> clients;
    once_flag forkHandlers;
};

Registry& registry()
{
    // Fork handlers and static clients can outlive other static objects at shutdown.
    static Registry* instance = new Registry;
    return *instance;
}

int tryConnect(const string& runtimeDirectory)
{
    FD fd(socket(AF_UNIX, SOCK_STREAM, 0));
    configureSocket(fd.value);
    clientTimeout(fd.value);
    const auto addr = address(runtimeDirectory);
    if (::connect(fd.value, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        if (errno == ENOENT || errno == ECONNREFUSED || errno == EAGAIN) {
            return -1;
        }
        throw error("connect");
    }
    return fd.release();
}

string helperPath()
{
    namespace fs = filesystem;
    constexpr const char* binary = "bedrock-test-port-server";
    if (const char* directory = getenv("BEDROCK_DIR")) {
        const fs::path path = fs::path(directory) / binary;
        if (access(path.c_str(), X_OK) == 0) {
            return path.string();
        }
    }

    // Test binaries live below the checkout root; this also handles callers without PATH setup.
    char executable[4096];
#ifdef __APPLE__
    uint32_t size = sizeof(executable);
    const bool found = _NSGetExecutablePath(executable, &size) == 0;
#else
    const ssize_t size = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    const bool found = size > 0;
    if (found) {
        executable[size] = '\0';
    }
#endif
    if (found) {
        for (fs::path parent = fs::absolute(executable).parent_path(); !parent.empty();) {
            const fs::path path = parent / binary;
            if (access(path.c_str(), X_OK) == 0) {
                return path.string();
            }
            if (parent == parent.root_path()) {
                break;
            }
            parent = parent.parent_path();
        }
    }
    return binary;
}

int startServer(int lockFD, const string& runtimeDirectory)
{
    int sockets[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
        throw error("create bootstrap connection");
    }
    FD client(sockets[0]);
    FD server(sockets[1]);
    configureSocket(client.value);
    configureSocket(server.value);
    clientTimeout(client.value);

    // Keep sources above the helper's reserved descriptors to avoid dup2 source/target collisions.
    FD bootstrap(fcntl(server.value, F_DUPFD_CLOEXEC, 5));
    FD lock(fcntl(lockFD, F_DUPFD_CLOEXEC, 5));
    if (bootstrap.value < 0 || lock.value < 0) {
        throw error("duplicate helper descriptors");
    }
    posix_spawn_file_actions_t actions;
    posix_spawnattr_t attributes;
    int result = posix_spawn_file_actions_init(&actions);
    if (result) {
        throw error("initialize spawn actions", result);
    }
    result = posix_spawnattr_init(&attributes);
    if (result) {
        posix_spawn_file_actions_destroy(&actions);
        throw error("initialize spawn attributes", result);
    }
    auto check = [&](int status) {
        if (status) {
            posix_spawnattr_destroy(&attributes);
            posix_spawn_file_actions_destroy(&actions);
            throw error("configure helper launch", status);
        }
    };
    check(posix_spawn_file_actions_adddup2(&actions, bootstrap.value, 3));
    check(posix_spawn_file_actions_adddup2(&actions, lock.value, 4));
    check(posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null", O_RDONLY, 0));
    check(posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, "/dev/null", O_WRONLY, 0));
    check(posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, "/dev/null", O_WRONLY, 0));
    sigset_t mask;
    sigemptyset(&mask);
    check(posix_spawnattr_setsigmask(&attributes, &mask));
    sigset_t defaults;
    sigemptyset(&defaults);
    sigaddset(&defaults, SIGTERM);
    sigaddset(&defaults, SIGINT);
    check(posix_spawnattr_setsigdefault(&attributes, &defaults));
    check(posix_spawnattr_setpgroup(&attributes, 0));
    check(posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF));

    const string path = helperPath();
    char* args[] = {const_cast<char*>(path.c_str()), const_cast<char*>(runtimeDirectory.c_str()), nullptr};
    pid_t pid;
    result = posix_spawnp(&pid, path.c_str(), &actions, &attributes, args, environ);
    posix_spawnattr_destroy(&attributes);
    posix_spawn_file_actions_destroy(&actions);
    if (result) {
        throw error("launch " + path + " (build bedrock-test-port-server first)", result);
    }
    // The helper can outlive its launching suite's connection. Reap it independently of clients.
    thread([pid]() {
        while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {}
    }).detach();
    return client.release();
}

} // namespace

PortServerClient::PortServerClient(uint16_t from) : PortServerClient(from, directory()) {}

PortServerClient::PortServerClient(uint16_t from, const string& runtimeDirectory) :
    _from(from), _directory(runtimeDirectory)
{
    if (from < START_PORT || from > MAX_PORT) {
        throw error("invalid starting port", EINVAL);
    }
    auto& state = registry();
    call_once(state.forkHandlers, []() {
        const int result = pthread_atfork(beforeFork, afterForkParent, afterForkChild);
        if (result) {
            throw error("register fork handlers", result);
        }
    });
    lock_guard<mutex> guard(state.lock);
    state.clients.insert(this);
}

PortServerClient::~PortServerClient()
{
    disconnect();
    auto& state = registry();
    lock_guard<mutex> guard(state.lock);
    state.clients.erase(this);
}

void PortServerClient::beforeFork()
{
    auto& state = registry();
    state.lock.lock();
    for (auto* client : state.clients) {
        client->_mutex.lock();
    }
}

void PortServerClient::afterForkParent()
{
    auto& state = registry();
    for (auto* client : state.clients) {
        client->_mutex.unlock();
    }
    state.lock.unlock();
}

void PortServerClient::afterForkChild()
{
    auto& state = registry();
    for (auto* client : state.clients) {
        if (client->_socket >= 0) {
            close(client->_socket);
            client->_socket = -1;
        }
        client->_failed = false;
        client->_mutex.unlock();
    }
    state.lock.unlock();
}

void PortServerClient::connect()
{
    if (_failed) {
        throw error("client session was lost; outstanding reservations cannot be recovered", ECONNRESET);
    }
    if (_socket >= 0) {
        return;
    }
    ensureDirectory(_directory);
    const auto deadline = chrono::steady_clock::now() + chrono::milliseconds(TIMEOUT_MS);
    while (chrono::steady_clock::now() < deadline) {
        FD connection(tryConnect(_directory));
        if (connection.value < 0) {
            FD lock(open((_directory + "/server.lock").c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600));
            if (lock.value < 0) {
                throw error("open singleton lock");
            }
            if (flock(lock.value, LOCK_EX | LOCK_NB) == 0) {
                connection.value = tryConnect(_directory);
                if (connection.value < 0) {
                    connection.value = startServer(lock.value, _directory);
                }
                // Closing our copy must not explicitly unlock the helper's inherited lock.
            } else if (errno != EWOULDBLOCK && errno != EINTR) {
                throw error("lock singleton");
            }
        }
        if (connection.value >= 0) {
            try {
                BedrockTestPorts::request(connection.value, Operation::REGISTER, getpid());
                _socket = connection.release();
                return;
            } catch (const system_error& exception) {
                // No ports belong to this connection yet. A last-client shutdown can race registration.
                if (exception.code().value() != ECONNRESET && exception.code().value() != EPIPE) {
                    throw;
                }
            }
        }
        this_thread::sleep_for(chrono::milliseconds(10));
    }
    throw error("timed out connecting to singleton", ETIMEDOUT);
}

int32_t PortServerClient::request(uint32_t operation, int32_t value)
{
    Message response;
    try {
        response = exchange(_socket, static_cast<Operation>(operation), value);
    } catch (...) {
        // An uncertain request may have allocated a port. Never reconnect this session silently.
        _failed = true;
        close(_socket);
        _socket = -1;
        throw;
    }
    if (response.value < 0) {
        throw error("request rejected", -response.value);
    }
    return response.value;
}

uint16_t PortServerClient::getPort()
{
    lock_guard<mutex> guard(_mutex);
    connect();
    return request(static_cast<uint32_t>(Operation::GET_PORT), _from);
}

void PortServerClient::returnPort(uint16_t port)
{
    lock_guard<mutex> guard(_mutex);
    if (_socket < 0) {
        throw error("return port without a live client session", ENOTCONN);
    }
    request(static_cast<uint32_t>(Operation::RETURN_PORT), port);
}

void PortServerClient::disconnectLocked()
{
    if (_socket >= 0) {
        // Disconnect is also used by destructors. Socket closure remains sufficient if the RPC fails.
        try {
            BedrockTestPorts::request(_socket, Operation::DISCONNECT);
        } catch (...) {}
        close(_socket);
        _socket = -1;
    }
    _failed = false;
}

void PortServerClient::disconnect()
{
    lock_guard<mutex> guard(_mutex);
    disconnectLocked();
}

int PortServerClient::waitForPort(uint16_t port)
{
    FD fd(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (fd.value < 0) {
        return 1;
    }
    int reuse = 1;
    setsockopt(fd.value, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    const auto deadline = chrono::steady_clock::now() + chrono::seconds(5);
    do {
        if (::bind(fd.value, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
            return 0;
        }
        this_thread::sleep_for(chrono::milliseconds(100));
    } while (chrono::steady_clock::now() < deadline);
    return 1;
}

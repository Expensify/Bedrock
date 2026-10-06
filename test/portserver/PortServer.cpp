#include <test/lib/PortServer.h>
#include <test/lib/PortServerProtocol.h>

#include <chrono>
#include <map>
#include <netinet/in.h>
#include <set>
#include <vector>

#ifdef __APPLE__
#include <sys/event.h>
#else
#include <sys/syscall.h>
#endif

using namespace std;
using namespace BedrockTestPorts;

namespace {
using Clock = chrono::steady_clock;

struct Client
{
    pid_t pid = 0;
    Message message;
    size_t received = 0;
    size_t sent = 0;
    bool responding = false;
    bool disconnecting = false;
    Clock::time_point deadline = Clock::now() + chrono::milliseconds(TIMEOUT_MS);
    set<uint16_t> ports;
};

int watchProcess(pid_t pid)
{
#ifdef __APPLE__
    FD watch(kqueue());
    if (watch.value < 0) {
        return -1;
    }
    struct kevent event;
    EV_SET(&event, pid, EVFILT_PROC, EV_ADD | EV_ONESHOT, NOTE_EXIT, 0, nullptr);
    if (kevent(watch.value, &event, 1, nullptr, 0, nullptr) < 0) {
        return -1;
    }
    fcntl(watch.value, F_SETFD, FD_CLOEXEC);
    return watch.release();
#else
    // pidfds become readable on exit, including while the process is still a zombie.
    return syscall(SYS_pidfd_open, pid, 0);
#endif
}

bool canBindAddress(uint16_t port, uint32_t host)
{
    FD socketFD(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (socketFD.value < 0) {
        throw error("create bind-check socket");
    }
    int reuse = 1;
    setsockopt(socketFD.value, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(host);
    address.sin_port = htons(port);
    return ::bind(socketFD.value, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
}

bool canBind(uint16_t port)
{
    // Testers use both loopback and wildcard listeners. Some systems permit a wildcard bind
    // alongside an existing specific-address listener, so check both addresses separately.
    return canBindAddress(port, INADDR_LOOPBACK) && canBindAddress(port, INADDR_ANY);
}

class Server {
public:
    Server(int bootstrapFD, const string& runtimeDirectory) : bootstrap(bootstrapFD), socketAddress(address(runtimeDirectory))
    {
        configureSocket(listener.value, true);
        configureSocket(bootstrap.value, true);
        const auto& addr = socketAddress;
        // The singleton lock is held throughout socket creation and removal.
        if (unlink(addr.sun_path) < 0 && errno != ENOENT) {
            throw error("remove stale socket");
        }
        if (::bind(listener.value, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw error("bind listener");
        }
        ownsSocket = true;
        if (listen(listener.value, 128) < 0) {
            throw error("listen");
        }
        clients.emplace(bootstrap.value, Client{});
        bootstrap.release();
    }

    ~Server()
    {
        for (const auto& [fd, client] : clients) {
            close(fd);
        }
        for (const auto& [pid, fd] : processes) {
            close(fd);
        }
        if (ownsSocket) {
            unlink(socketAddress.sun_path);
        }
    }

    void run()
    {
        while (!clients.empty()) {
            vector<pollfd> descriptors{{listener.value, POLLIN, 0}};
            for (const auto& [fd, client] : clients) {
                descriptors.push_back({fd, static_cast<short>(client.responding ? POLLOUT : POLLIN), 0});
            }
            const size_t processStart = descriptors.size();
            for (const auto& [pid, fd] : processes) {
                descriptors.push_back({fd, POLLIN, 0});
            }
            if (poll(descriptors.data(), descriptors.size(), 100) < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw error("poll");
            }

            // Reclaim every connection for an exited PID, even if descendants inherited sockets.
            set<pid_t> exited;
            for (size_t i = processStart; i < descriptors.size(); ++i) {
                if (descriptors[i].revents) {
                    for (const auto& [pid, fd] : processes) {
                        if (fd == descriptors[i].fd) {
                            exited.insert(pid);
                        }
                    }
                }
            }
            for (auto it = clients.begin(); it != clients.end();) {
                const int fd = it->first;
                ++it;
                if (exited.count(clients.at(fd).pid)) {
                    removeClient(fd);
                }
            }

            for (size_t i = 1; i < processStart; ++i) {
                const auto& descriptor = descriptors[i];
                if (!clients.count(descriptor.fd)) {
                    continue;
                }
                // Do not retain partial requests or stalled responses indefinitely.
                const auto& client = clients.at(descriptor.fd);
                if ((descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) ||
                    ((!client.pid || client.received || client.responding) && Clock::now() >= client.deadline)) {
                    removeClient(descriptor.fd);
                } else if (descriptor.revents && !service(descriptor.fd)) {
                    removeClient(descriptor.fd);
                }
            }
            // Accept after processing the snapshot, so reused descriptors cannot inherit old events.
            // Queued clients can keep the server alive even when the last old client just disconnected.
            if (descriptors[0].revents & POLLIN) {
                for (int count = 0; count < 128; ++count) {
                    FD fd(accept(listener.value, nullptr, nullptr));
                    if (fd.value < 0) {
                        if (errno == EINTR) {
                            continue;
                        }
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        throw error("accept");
                    }
                    configureSocket(fd.value, true);
                    clients.emplace(fd.value, Client{});
                    fd.release();
                }
            }
        }
    }

private:
    FD listener{socket(AF_UNIX, SOCK_STREAM, 0)};
    FD bootstrap;
    const sockaddr_un socketAddress;
    bool ownsSocket = false;
    map<int, Client> clients;
    map<pid_t, int> processes;
    set<uint16_t> allocated;

    void removeClient(int fd)
    {
        const auto& client = clients.at(fd);
        const pid_t pid = client.pid;
        for (uint16_t port : client.ports) {
            allocated.erase(port);
        }
        close(fd);
        clients.erase(fd);
        if (pid && processes.count(pid)) {
            for (const auto& [otherFD, other] : clients) {
                if (other.pid == pid) {
                    return;
                }
            }
            close(processes.at(pid));
            processes.erase(pid);
        }
    }

    int32_t execute(Client& client)
    {
        const Message& message = client.message;
        if (message.version != VERSION) {
            return -EPROTO;
        }
        if (message.operation == Operation::REGISTER) {
            if (client.pid || message.value <= 0) {
                return -EINVAL;
            }
            if (!processes.count(message.value)) {
                const int fd = watchProcess(message.value);
                if (fd < 0) {
                    return -errno;
                }
                processes.emplace(message.value, fd);
            }
            client.pid = message.value;
            return getpid();
        }
        if (!client.pid) {
            return -EPROTO;
        }
        switch (message.operation) {
            case Operation::GET_PORT:
                if (message.value < START_PORT || message.value > MAX_PORT) {
                    return -EINVAL;
                }
                for (int port = message.value; port <= MAX_PORT; ++port) {
                    if (!allocated.count(port) && canBind(port)) {
                        allocated.insert(port);
                        client.ports.insert(port);
                        return port;
                    }
                }
                return -EADDRNOTAVAIL;

            case Operation::RETURN_PORT:
                if (message.value < START_PORT || message.value > MAX_PORT || !client.ports.erase(message.value)) {
                    return -EINVAL;
                }
                allocated.erase(message.value);
                return 0;

            case Operation::DISCONNECT:
                client.disconnecting = true;
                return 0;

            default:
                return -EINVAL;
        }
    }

    bool service(int fd)
    {
        auto& client = clients.at(fd);
        if (!client.responding) {
            const ssize_t count = recv(fd, reinterpret_cast<char*>(&client.message) + client.received,
                                       sizeof(Message) - client.received, 0);
            if (count <= 0) {
                return count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
            }
            if (!client.received) {
                client.deadline = Clock::now() + chrono::milliseconds(TIMEOUT_MS);
            }
            client.received += count;
            if (client.received == sizeof(Message)) {
                client.message.value = execute(client);
                client.message.version = VERSION;
                client.responding = true;
            }
        }
        if (client.responding) {
            const ssize_t count = sendBytes(fd, reinterpret_cast<char*>(&client.message) + client.sent,
                                            sizeof(Message) - client.sent);
            if (count <= 0) {
                return count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
            }
            client.sent += count;
            if (client.sent == sizeof(Message)) {
                if (client.disconnecting) {
                    return false;
                }
                client.received = client.sent = 0;
                client.responding = false;
            }
        }
        return true;
    }
};
} // namespace

int runTestPortServer(int lockFD, int bootstrapFD, const string& runtimeDirectory)
{
    FD lock(lockFD);
    Server server(bootstrapFD, runtimeDirectory);
    server.run();
    return 0;
}

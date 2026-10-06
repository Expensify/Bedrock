#include <test/lib/PortMap.h>
#include <test/lib/PortServerProtocol.h>
#include <test/lib/tpunit++.hpp>

#include <array>
#include <chrono>
#include <future>
#include <memory>
#include <signal.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <thread>

using namespace BedrockTestPorts;

struct PortServerTest : tpunit::TestFixture {
    PortServerTest() : tpunit::TestFixture("PortServer",
        BEFORE(PortServerTest::setUp), AFTER(PortServerTest::tearDown),
        TEST(PortServerTest::clientOwnership),
        TEST(PortServerTest::occupiedAndExhausted),
        TEST(PortServerTest::threadedAllocation),
        TEST(PortServerTest::concurrentStartup),
        TEST(PortServerTest::forkIsolation),
        TEST(PortServerTest::launcherCanExit),
        TEST(PortServerTest::pidExitWithInheritedSockets),
        TEST(PortServerTest::shutdownRace),
        TEST(PortServerTest::serverCrash),
        TEST(PortServerTest::partialProtocol)) {}

    string runtimeDirectory;
    vector<pid_t> children;

    static bool waitUntil(const function<bool()>& condition)
    {
        const auto deadline = chrono::steady_clock::now() + chrono::seconds(5);
        do {
            if (condition()) {
                return true;
            }
            this_thread::sleep_for(chrono::milliseconds(10));
        } while (chrono::steady_clock::now() < deadline);
        return false;
    }

    void setUp()
    {
        char path[] = "/tmp/bedrock-port-server-tests-XXXXXX";
        if (!mkdtemp(path)) {
            throw error("create test directory");
        }
        runtimeDirectory = path;
    }

    bool unlocked()
    {
        FD lock(open((runtimeDirectory + "/server.lock").c_str(), O_RDWR));
        return lock.value < 0 || flock(lock.value, LOCK_EX | LOCK_NB) == 0;
    }

    void tearDown()
    {
        for (pid_t pid : children) {
            kill(pid, SIGKILL);
            while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {}
        }
        children.clear();
        EXPECT_TRUE(waitUntil([&]() { return unlocked(); }));
        unlink(address(runtimeDirectory).sun_path);
        unlink((runtimeDirectory + "/server.lock").c_str());
        rmdir(runtimeDirectory.c_str());
    }

    pid_t forkChild()
    {
        const pid_t pid = fork();
        if (pid < 0) {
            throw error("fork test child");
        }
        if (pid) {
            children.push_back(pid);
        }
        return pid;
    }

    int reap(pid_t pid)
    {
        int status = 0;
        while (waitpid(pid, &status, 0) < 0) {
            if (errno != EINTR) {
                throw error("reap test child");
            }
        }
        erase(children, pid);
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }

    int rawClient()
    {
        FD fd(socket(AF_UNIX, SOCK_STREAM, 0));
        configureSocket(fd.value);
        clientTimeout(fd.value);
        const auto addr = address(runtimeDirectory);
        if (::connect(fd.value, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw error("connect test client");
        }
        return fd.release();
    }

    pid_t serverPID()
    {
        FD fd(rawClient());
        const pid_t pid = BedrockTestPorts::request(fd.value, Operation::REGISTER, getpid()).value;
        BedrockTestPorts::request(fd.value, Operation::DISCONNECT);
        return pid;
    }

    void clientOwnership()
    {
        PortMap first(PortMap::START_PORT, runtimeDirectory);
        PortMap second(PortMap::START_PORT, runtimeDirectory);
        const uint16_t a = first.getPort();
        const uint16_t b = second.getPort();
        ASSERT_NOT_EQUAL(a, b);
        ASSERT_EQUAL(first.waitForPort(a), 0);
        ASSERT_THROW(second.returnPort(a), system_error);
        const uint16_t c = second.getPort();
        ASSERT_NOT_EQUAL(a, c);
        first.returnPort(a);
        ASSERT_EQUAL(first.getPort(), a);
        first.disconnect();
        ASSERT_EQUAL(second.getPort(), a);
        ASSERT_THROW(first.returnPort(a), system_error);
        // An explicit disconnect allows a fresh session, and preserves other clients' reservations.
        const uint16_t d = first.getPort();
        ASSERT_NOT_EQUAL(d, a);
        ASSERT_NOT_EQUAL(d, b);
        ASSERT_NOT_EQUAL(d, c);
        first.disconnect();
        second.disconnect();
        ASSERT_TRUE(waitUntil([&]() { return access(address(runtimeDirectory).sun_path, F_OK) != 0 && unlocked(); }));
    }

    void occupiedAndExhausted()
    {
        // Coordinate real listeners with other suites, even though this server's lifecycle is isolated.
        PortMap machinePorts;
        const uint16_t reserved = machinePorts.getPort();
        PortMap first(reserved, runtimeDirectory);
        const uint16_t port = first.getPort();
        ASSERT_EQUAL(port, reserved);
        first.returnPort(port);
        FD blocker(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);
        ASSERT_EQUAL(::bind(blocker.value, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        ASSERT_EQUAL(listen(blocker.value, 1), 0);
        PortMap next(port, runtimeDirectory);
        ASSERT_GREATER_THAN(next.getPort(), port);

        PortMap machineLast(PortMap::MAX_PORT);
        try {
            machineLast.getPort();
        } catch (const system_error& exception) {
            if (exception.code().value() == EADDRNOTAVAIL) {
                // Another suite or program already owns the only port in this subtest's range.
                return;
            }
            throw;
        }
        FD lastBlocker(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
        addr.sin_port = htons(PortMap::MAX_PORT);
        const int bound = ::bind(lastBlocker.value, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ASSERT_TRUE(bound == 0 || errno == EADDRINUSE);
        PortMap last(PortMap::MAX_PORT, runtimeDirectory);
        ASSERT_THROW(last.getPort(), system_error);
        if (bound == 0) {
            close(lastBlocker.release());
            ASSERT_EQUAL(last.getPort(), PortMap::MAX_PORT);
            PortMap competitor(PortMap::MAX_PORT, runtimeDirectory);
            ASSERT_THROW(competitor.getPort(), system_error);
            // Exhaustion is a rejected request, not loss of the client's existing reservations.
            last.returnPort(PortMap::MAX_PORT);
            ASSERT_EQUAL(competitor.getPort(), PortMap::MAX_PORT);
        }
        ASSERT_THROW(PortMap(PortMap::MAX_PORT + 1), system_error);
    }

    void threadedAllocation()
    {
        PortMap client(PortMap::START_PORT, runtimeDirectory);
        vector<future<vector<uint16_t>>> workers;
        for (int i = 0; i < 8; ++i) {
            workers.push_back(async(launch::async, [&]() {
                vector<uint16_t> ports;
                for (int j = 0; j < 32; ++j) {
                    ports.push_back(client.getPort());
                }
                return ports;
            }));
        }
        set<uint16_t> ports;
        for (auto& worker : workers) {
            for (uint16_t port : worker.get()) {
                ASSERT_TRUE(ports.insert(port).second);
            }
        }
        ASSERT_EQUAL(ports.size(), 256U);
        for (uint16_t port : ports) {
            client.returnPort(port);
        }
    }

    struct Report {
        pid_t server = 0;
        array<uint16_t, 16> ports = {};
    };

    void concurrentStartup()
    {
        // A leftover socket must not prevent the lock holder from starting a new server.
        {
            FD stale(socket(AF_UNIX, SOCK_STREAM, 0));
            const auto addr = address(runtimeDirectory);
            ASSERT_EQUAL(::bind(stale.value, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)), 0);
        }
        vector<unique_ptr<FD>> channels;
        vector<pid_t> pids;
        for (int i = 0; i < 12; ++i) {
            int pair[2];
            ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, pair), 0);
            FD parent(pair[0]);
            FD child(pair[1]);
            const pid_t pid = forkChild();
            if (!pid) {
                close(parent.release());
                try {
                    char ready;
                    transfer(child.value, &ready, 1, false);
                    PortMap first(PortMap::START_PORT, runtimeDirectory);
                    PortMap second(PortMap::START_PORT, runtimeDirectory);
                    Report report;
                    for (size_t j = 0; j < report.ports.size(); ++j) {
                        report.ports[j] = j % 2 ? first.getPort() : second.getPort();
                    }
                    report.server = serverPID();
                    transfer(child.value, &report, sizeof(report), true);
                    transfer(child.value, &ready, 1, false);
                    _exit(0);
                } catch (...) {
                    _exit(1);
                }
            }
            configureSocket(parent.value);
            clientTimeout(parent.value);
            channels.emplace_back(new FD(parent.release()));
            pids.push_back(pid);
        }
        char ready = 1;
        for (auto& channel : channels) {
            transfer(channel->value, &ready, 1, true);
        }
        set<uint16_t> ports;
        pid_t server = 0;
        for (auto& channel : channels) {
            Report report;
            transfer(channel->value, &report, sizeof(report), false);
            if (!server) {
                server = report.server;
            }
            ASSERT_EQUAL(report.server, server);
            for (uint16_t port : report.ports) {
                ASSERT_TRUE(ports.insert(port).second);
            }
        }
        ASSERT_EQUAL(ports.size(), 192U);
        for (auto& channel : channels) {
            transfer(channel->value, &ready, 1, true);
        }
        for (pid_t pid : pids) {
            ASSERT_EQUAL(reap(pid), 0);
        }
        ASSERT_TRUE(waitUntil([&]() { return access(address(runtimeDirectory).sun_path, F_OK) != 0 && unlocked(); }));
    }

    void forkIsolation()
    {
        PortMap client(PortMap::START_PORT, runtimeDirectory);
        const uint16_t parentPort = client.getPort();
        int pair[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, pair), 0);
        FD parent(pair[0]);
        FD child(pair[1]);
        const pid_t pid = forkChild();
        if (!pid) {
            close(parent.release());
            try {
                const uint16_t childPort = client.getPort();
                Report report;
                report.ports[0] = childPort;
                transfer(child.value, &report, sizeof(report), true);
                try {
                    client.returnPort(parentPort);
                    _exit(2);
                } catch (const system_error&) {}
                client.returnPort(childPort);
                client.disconnect();
                _exit(0);
            } catch (...) {
                _exit(1);
            }
        }
        close(child.release());
        configureSocket(parent.value);
        clientTimeout(parent.value);
        Report report;
        transfer(parent.value, &report, sizeof(report), false);
        ASSERT_NOT_EQUAL(report.ports[0], parentPort);
        ASSERT_EQUAL(reap(pid), 0);
        client.returnPort(parentPort);
        ASSERT_EQUAL(client.getPort(), parentPort);
    }

    void launcherCanExit()
    {
        int pair[2];
        int output[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, pair), 0);
        ASSERT_EQUAL(pipe(output), 0);
        FD parent(pair[0]);
        FD child(pair[1]);
        FD outputRead(output[0]);
        FD outputWrite(output[1]);
        const pid_t launcher = forkChild();
        if (!launcher) {
            close(parent.release());
            close(outputRead.release());
            if (dup2(outputWrite.value, STDERR_FILENO) < 0) {
                _exit(2);
            }
            close(outputWrite.release());
            try {
                PortMap client(PortMap::START_PORT, runtimeDirectory);
                client.getPort();
                Report report;
                report.server = serverPID();
                transfer(child.value, &report, sizeof(report), true);
                char done;
                transfer(child.value, &done, 1, false);
                _exit(0);
            } catch (...) {
                _exit(1);
            }
        }
        close(child.release());
        close(outputWrite.release());
        configureSocket(parent.value);
        clientTimeout(parent.value);
        Report report;
        transfer(parent.value, &report, sizeof(report), false);
        PortMap keeper(PortMap::START_PORT, runtimeDirectory);
        keeper.getPort();
        char done = 1;
        transfer(parent.value, &done, 1, true);
        ASSERT_EQUAL(reap(launcher), 0);
        ASSERT_EQUAL(serverPID(), report.server);
        // A shared helper must not keep its launching suite's captured output open after it exits.
        pollfd descriptor{outputRead.value, POLLIN | POLLHUP, 0};
        ASSERT_EQUAL(poll(&descriptor, 1, 1000), 1);
        ASSERT_EQUAL(read(outputRead.value, &done, 1), 0);
        keeper.getPort();
    }

    void pidExitWithInheritedSockets()
    {
        PortMap keeper(PortMap::START_PORT, runtimeDirectory);
        keeper.getPort();
        int pair[2];
        ASSERT_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, pair), 0);
        FD parent(pair[0]);
        FD child(pair[1]);
        const pid_t owner = forkChild();
        if (!owner) {
            close(parent.release());
            try {
                // Raw sockets deliberately bypass the client's atfork cleanup.
                FD first(rawClient());
                FD second(rawClient());
                BedrockTestPorts::request(first.value, Operation::REGISTER, getpid());
                BedrockTestPorts::request(second.value, Operation::REGISTER, getpid());
                Report report;
                report.ports[0] = BedrockTestPorts::request(first.value, Operation::GET_PORT, PortMap::START_PORT).value;
                report.ports[1] = BedrockTestPorts::request(second.value, Operation::GET_PORT, PortMap::START_PORT).value;
                const pid_t descendant = fork();
                if (descendant < 0) {
                    _exit(2);
                }
                if (!descendant) {
                    char done;
                    transfer(child.value, &done, 1, false);
                    _exit(0);
                }
                report.server = descendant;
                transfer(child.value, &report, sizeof(report), true);
                while (true) {
                    pause();
                }
            } catch (...) {
                _exit(1);
            }
        }
        close(child.release());
        configureSocket(parent.value);
        clientTimeout(parent.value);
        Report report;
        transfer(parent.value, &report, sizeof(report), false);
        children.push_back(report.server);
        ASSERT_EQUAL(kill(owner, SIGKILL), 0);
        // Leave the owner unreaped. Process monitoring must also detect zombies, while a live
        // descendant keeps both connections open and would prevent socket-closure cleanup.
        ASSERT_EQUAL(kill(report.server, 0), 0);
        PortMap reclaimed(report.ports[0], runtimeDirectory);
        ASSERT_TRUE(waitUntil([&]() {
            const uint16_t port = reclaimed.getPort();
            reclaimed.returnPort(port);
            return port == report.ports[0];
        }));
        PortMap other(report.ports[1], runtimeDirectory);
        ASSERT_EQUAL(other.getPort(), report.ports[1]);
        char done = 1;
        transfer(parent.value, &done, 1, true);
        ASSERT_EQUAL(reap(owner), -1);
    }

    void shutdownRace()
    {
        ino_t lockInode = 0;
        for (int i = 0; i < 32; ++i) {
            PortMap first(PortMap::START_PORT, runtimeDirectory);
            first.getPort();
            struct stat info;
            ASSERT_EQUAL(stat((runtimeDirectory + "/server.lock").c_str(), &info), 0);
            if (!lockInode) {
                lockInode = info.st_ino;
            }
            ASSERT_EQUAL(info.st_ino, lockInode);
            auto newcomer = async(launch::async, [&]() {
                PortMap second(PortMap::START_PORT, runtimeDirectory);
                const uint16_t port = second.getPort();
                second.returnPort(port);
            });
            first.disconnect();
            newcomer.get();
        }
        ASSERT_TRUE(waitUntil([&]() { return access(address(runtimeDirectory).sun_path, F_OK) != 0 && unlocked(); }));
    }

    void serverCrash()
    {
        PortMap first(PortMap::START_PORT, runtimeDirectory);
        first.getPort();
        ASSERT_EQUAL(kill(serverPID(), SIGKILL), 0);
        ASSERT_TRUE(waitUntil([&]() { return unlocked(); }));
        ASSERT_THROW(first.getPort(), system_error);
        ASSERT_THROW(first.getPort(), system_error);
        PortMap fresh(PortMap::START_PORT, runtimeDirectory);
        ASSERT_GREATER_THAN_EQUAL(fresh.getPort(), PortMap::START_PORT);
    }

    void partialProtocol()
    {
        PortMap keeper(PortMap::START_PORT, runtimeDirectory);
        keeper.getPort();
        FD client(rawClient());
        ASSERT_EQUAL(exchange(client.value, Operation::GET_PORT, PortMap::START_PORT).value, -EPROTO);
        Message registration{VERSION, Operation::REGISTER, getpid()};
        transfer(client.value, &registration, 3, true);
        // Other clients continue to make progress while this request is incomplete.
        keeper.getPort();
        transfer(client.value, reinterpret_cast<char*>(&registration) + 3, sizeof(registration) - 3, true);
        transfer(client.value, &registration, sizeof(registration), false);
        ASSERT_EQUAL(registration.value, serverPID());
        const int32_t port = BedrockTestPorts::request(client.value, Operation::GET_PORT, PortMap::START_PORT).value;
        ASSERT_GREATER_THAN_EQUAL(port, PortMap::START_PORT);
        BedrockTestPorts::request(client.value, Operation::RETURN_PORT, port);
        BedrockTestPorts::request(client.value, Operation::DISCONNECT);
    }
} __PortServerTest;

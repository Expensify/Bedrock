#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <sys/types.h>

// Each instance is an independent client, even when several instances share a PID.
// Construction is passive; the first allocation connects to the per-user server.
class PortServerClient {
public:
    static constexpr int64_t START_PORT = 10000;
    static constexpr int64_t MAX_PORT = 20000;

    explicit PortServerClient(uint16_t from = START_PORT);
    // Explicit endpoints allow lifecycle tests to run without touching other suites' reservations.
    PortServerClient(uint16_t from, const std::string& runtimeDirectory);
    ~PortServerClient();

    // Reserve a bindable TCP port until it is returned, this client disconnects, or its PID exits.
    uint16_t getPort();
    // Only the client that allocated a port can return it.
    void returnPort(uint16_t port);
    // Local bind check, with a five-second timeout. Does not contact the server or change ownership.
    int waitForPort(uint16_t port);
    // Release every reservation and close the session. Subsequent allocations create a new session.
    void disconnect();

private:
    const uint16_t _from;
    const std::string _directory;
    int _socket = -1;
    bool _failed = false;
    std::mutex _mutex;

    void connect();
    int32_t request(uint32_t operation, int32_t value);
    void disconnectLocked();

    // Fork handlers prevent a child from inheriting a locked mutex or a parent's client session.
    static void beforeFork();
    static void afterForkParent();
    static void afterForkChild();
};

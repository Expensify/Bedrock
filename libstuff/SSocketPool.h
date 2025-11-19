#pragma once
#include <libstuff/STCPManager.h>
#include <chrono>
#include <condition_variable>

class SSocketPool {
public:
    SSocketPool(const string& host);
    ~SSocketPool();

    // Returns an existing or new socket.
    unique_ptr<STCPManager::Socket> getSocket();

    // Makes an existing socket available to be used again.
    void returnSocket(unique_ptr<STCPManager::Socket>&& s);

    // The hostname for the socket.
    const string host;

    // The timeout after which a socket is closed if not used.
    const chrono::steady_clock::duration timeout = 10s;
private:
    void _timeoutThreadFunc();

    bool _exit = false;
    mutex _poolMutex;
    condition_variable _poolCV;
    list<pair<chrono::steady_clock::time_point, unique_ptr<STCPManager::Socket>>> _sockets;
    thread _timeoutThread;
};

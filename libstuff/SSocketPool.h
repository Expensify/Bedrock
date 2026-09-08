/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SSocketPool.h
 * Path:    libstuff/SSocketPool.h
 * Pair:    SSocketPool.cpp
 *
 * INTENT
 *   Keep-alive connection pool for one host: hands out an existing idle
 *   STCPManager::Socket or opens a new one, and closes sockets that sit idle past a
 *   timeout via a dedicated background thread.
 *
 * OBJECTS
 *   SSocketPool - getSocket()/returnSocket() public API; _timeoutThreadFunc() (runs on
 *     its own thread, prunes expired sockets) and _getAddress() (caches the resolved
 *     address for `host`, re-resolving after addressTimeout) private helpers.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; a thin pooling layer over STCPManager::Socket, so it belongs beside it.
 *
 * NAMING QUALITY
 *   Consistent: `_`-prefixed private members/methods, bare public/const members.
 * ─────────────────────────────────────────────────────────────────────*/

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

    // How long a resolved address is reused before the host is looked up again.
    const chrono::steady_clock::duration addressTimeout = 10s;
private:
    void _timeoutThreadFunc();

    // Returns the cached address for `host`, resolving it again if there isn't one or it's older
    // than `addressTimeout`. Throws if the lookup fails.
    sockaddr_in _getAddress();

    bool _exit = false;
    mutex _poolMutex;
    condition_variable _poolCV;
    list<pair<chrono::steady_clock::time_point, unique_ptr<STCPManager::Socket>>> _sockets;
    thread _timeoutThread;

    // The last address `host` resolved to, and when. Held under its own mutex so a lookup doesn't
    // block threads returning sockets to the pool, and so only one thread looks up a stale address.
    mutex _addressMutex;
    sockaddr_in _address{};
    chrono::steady_clock::time_point _addressTime;
};

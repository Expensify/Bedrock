/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    STCPManager.h
 * Path:    libstuff/STCPManager.h
 * Pair:    STCPManager.cpp
 *
 * INTENT
 *   Convenience base providing the poll()-loop machinery (prePoll/postPoll) shared by
 *   every class that manages a set of raw or SSL TCP sockets, plus the Socket and Port
 *   value types those managers operate on. Deliberately holds no data members of its
 *   own so that all synchronization stays localized to the derived class (see
 *   class_hierarchy.md) or to Socket itself.
 *
 * OBJECTS
 *   STCPManager               - namespace-like struct: static prePoll/postPoll/openPort only.
 *   STCPManager::Socket       - per-connection state machine (RESOLVING/CONNECTING/CONNECTED/
 *     SHUTTINGDOWN/CLOSED); owns the fd, send/recv buffers, optional SSL state, and the
 *     shared_ptr<SResolution> used for async DNS.
 *   STCPManager::Port         - RAII wrapper for a listening socket fd + its host string.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; the base class for the STCPNode/SQLiteServer hierarchy described in class_hierarchy.md.
 *
 * NAMING QUALITY
 *   Split within Socket's protected section: methods (_connectAfterDNSResolution,
 *   _openSocket, _startResolution) are `_`-prefixed but sibling protected data members
 *   (sendBuffer, sendRecvMutex, https, dnsResolution, hostToResolve, socketCount) are not,
 *   unlike the repo's usual `_`-on-private-members convention applied uniformly.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <poll.h>
#include <string>

#include <libstuff/libstuff.h>
#include <libstuff/SFastBuffer.h>
#include <libstuff/SResolver.h>

class SSSLState;

using namespace std;

// Convenience base class for managing a series of TCP sockets. This includes filling receive buffers, emptying send
// buffers, completing connections, performing graceful shutdowns, etc.
struct STCPManager
{
    // Captures all the state for a single socket
    class Socket {
public:
        enum State { RESOLVING, CONNECTING, CONNECTED, SHUTTINGDOWN, CLOSED };

        // How long the constructor waits for its lookup before giving up and deferring.
        static const int DEFAULT_RESOLVE_GRACE_MS = 5;

        // Resolves `host` off-thread, which may leave the socket in the RESOLVING state.
        Socket(const string& host, bool https = false, int resolveGraceMS = DEFAULT_RESOLVE_GRACE_MS);

        // Connects to an already-resolved address, so no DNS resolution is required.
        Socket(const sockaddr_in& addr, bool https = false, const string& hostname = "");
        Socket(int sock = 0, State state_ = CONNECTING, bool https = false);
        Socket(Socket&& from);
        virtual ~Socket();
        // Attributes
        int s;
        sockaddr_in addr;
        SFastBuffer recvBuffer;
        atomic<State> state;
        bool connectFailure;
        uint64_t openTime;
        uint64_t lastSendTime;
        uint64_t lastRecvTime;
        SSSLState* ssl;
        void* data;
        virtual bool send(size_t* bytesSentCount = nullptr);
        virtual bool send(const string& buffer, size_t* bytesSentCount = nullptr);
        virtual bool recv();
        void shutdown(State toState = SHUTTINGDOWN);

        uint64_t id;
        string logString;

        bool sendBufferEmpty();
        string sendBufferCopy();
        void setSendBuffer(const string& buffer);

protected:
        friend struct STCPManager;

        // Run after DNS resolution completes to create the socket (or fail, as appropriate).
        void _connectAfterDNSResolution();

        // Opens the fd for `addr` and, for HTTPS sockets, its SSL state. Returns false and marks the socket
        // closed and failed if the fd can't be opened or the SSL state can't be set up.
        bool _openSocket();

        // Validates `host` and starts its lookup. `dnsResolution` is const, so it has to be built in
        // the member initializer list, and this is what lets the assertion still run first.
        static shared_ptr<SResolution> _startResolution(const string& host);

        static atomic<uint64_t> socketCount;
        recursive_mutex sendRecvMutex;

        // This is private because it's used by our synchronized send() functions. This requires it to only
        // be accessed through the (also synchronized) wrapper functions above.
        // NOTE: Currently there's no synchronization around `recvBuffer`. It can only be accessed by one thread.
        SFastBuffer sendBuffer;

        bool https;

        // Will be null only if this socket was initialized from an address or an existing fd. A literal
        // IP string still gets one, already resolved.
        // Shared ownership allows the resolver thread to continue even if the socket is destroyed during
        // a slow DNS lookup.
        const shared_ptr<SResolution> dnsResolution;
        string hostToResolve;
    };

    class Port {
public:
        Port(int _s, const string& _host);
        ~Port();

        // Attributes
        const int s;
        const string host;
    };

    // Updates all managed sockets
    // TODO: Actually explain what these do.
    static void prePoll(fd_map& fdm, Socket& socket);
    static void postPoll(fd_map& fdm, Socket& socket);

    static unique_ptr<Port> openPort(const string& host);
};

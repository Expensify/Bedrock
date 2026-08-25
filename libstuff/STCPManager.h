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

        // How long the constructor waits for its lookup before giving up and deferring. The local
        // caching resolver answers a warm host well inside this, so in practice most sockets
        // connect in the constructor and never reach RESOLVING at all. Tests pass 0 to force the
        // deferred path.
        static const int DEFAULT_RESOLVE_GRACE_MS = 5;

        // Resolves `host` off-thread, which may leave the socket in the RESOLVING state; the
        // connection is finished later by STCPManager::postPoll, so only owners that drive the
        // socket through prePoll/postPoll can use it.
        Socket(const string& host, bool https = false, int resolveGraceMS = DEFAULT_RESOLVE_GRACE_MS);

        // Connects to an already-resolved address, so there's nothing to wait for and the socket is
        // CONNECTING when the constructor returns. `hostname` is only used for SSL. Throws if the
        // socket can't be opened.
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

        // Opens the fd and sets up SSL now that `dnsResolution` has an answer, moving the socket
        // from RESOLVING to CONNECTING (or to CLOSED if the lookup failed). Called by
        // STCPManager::postPoll.
        void _connectAfterDNSResolution();

        static atomic<uint64_t> socketCount;
        recursive_mutex sendRecvMutex;

        // This is private because it's used by our synchronized send() functions. This requires it to only
        // be accessed through the (also synchronized) wrapper functions above.
        // NOTE: Currently there's no synchronization around `recvBuffer`. It can only be accessed by one thread.
        SFastBuffer sendBuffer;

        bool https;

        // Set for the lifetime of any socket built from a hostname, and null for one built from an
        // address or an existing fd. It's co-owned with the resolver worker, so destroying the
        // socket while a lookup is still running is safe. Because it's never cleared, a RESOLVING
        // socket always has one to poll on.
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

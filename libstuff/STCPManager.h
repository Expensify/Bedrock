#pragma once
#include <atomic>
#include <list>
#include <mutex>
#include <netinet/in.h>
#include <poll.h>
#include <string>

#include <libstuff/libstuff.h>
#include <libstuff/SFastBuffer.h>

class SSSLState;
class SX509;

using namespace std;

// Convenience base class for managing a series of TCP sockets. This includes filling receive buffers, emptying send
// buffers, completing connections, performing graceful shutdowns, etc.
struct STCPManager {
    // Captures all the state for a single socket
    class Socket {
      public:
        enum State { CONNECTING, CONNECTED, SHUTTINGDOWN, CLOSED };
        Socket(const string& host, SX509* x509 = nullptr);
        Socket(int sock = 0, State state_ = CONNECTING, SX509* x509 = nullptr);
        Socket(Socket&& from);
        ~Socket();
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
        bool send(size_t* bytesSentCount = nullptr);
        bool send(const string& buffer, size_t* bytesSentCount = nullptr);
        bool recv();
        void shutdown(State toState = SHUTTINGDOWN);
        uint64_t id;
        string logString;

        bool sendBufferEmpty();
        string sendBufferCopy();
        void setSendBuffer(const string& buffer);

      private:
        static atomic<uint64_t> socketCount;
        recursive_mutex sendRecvMutex;

        // This is private because it's used by our synchronized send() functions. This requires it to only
        // be accessed through the (also synchronized) wrapper functions above.
        // NOTE: Currently there's no synchronization around `recvBuffer`. It can only be accessed by one thread.
        SFastBuffer sendBuffer;

        // Each socket owns it's own SX509 object to avoid thread-safety issues reading/writing the same certificate in
        // the underlying ssl code. Once assigned, the socket owns this object for it's lifetime and will delete it
        // upon destruction.
        SX509* _x509;
    };

    class Port {
      public:
        Port(int _s, string _host);
        ~Port();

        // Attributes
        const int s;
        const string host;
    };

    // Updates all managed sockets
    // TODO: Actually explain what these do.
    static void prePoll(fd_map& fdm, Socket& socket);
    static void postPoll(fd_map& fdm, Socket& socket);

    static unique_ptr<Port> openPort(const string& host, int remainingTries = 1);
};

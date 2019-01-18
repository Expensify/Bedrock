#pragma once

// Convenience base class for managing a series of TCP sockets. This includes filling receive buffers, emptying send
// buffers, completing connections, performing graceful shutdowns, etc.
struct STCPManager {
    // Captures all the state for a single socket
    class Socket {
      public:
        enum State { CONNECTING, CONNECTED, SHUTTINGDOWN, CLOSED };
        Socket(int sock = 0, State state_ = CONNECTING, SX509* x509 = nullptr);
        ~Socket();
        // Attributes
        int s;
        sockaddr_in addr;
        string recvBuffer;
        atomic<State> state;
        bool connectFailure;
        uint64_t openTime;
        uint64_t lastSendTime;
        uint64_t lastRecvTime;
        SSSLState* ssl;
        void* data;
        bool send();
        bool send(const string& buffer);
        bool recv();
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
        string sendBuffer;

        // Each socket owns it's own SX509 object to avoid thread-safety issues reading/writing the same certificate in
        // the underlying ssl code. Once assigned, the socket owns this object for it's lifetime and will delete it
        // upon destruction.
        SX509* _x509;
    };

    // Cleans up outstanding sockets
    virtual ~STCPManager();

    // Updates all managed sockets
    void prePoll(fd_map& fdm);
    void postPoll(fd_map& fdm);

    // Opens outgoing socket
    Socket* openSocket(const string& host, SX509* x509 = nullptr, recursive_mutex* listMutexPtr = nullptr);

    // Gracefully shuts down a socket
    void shutdownSocket(Socket* socket, int how = SHUT_RDWR);

    // Hard terminate a socket
    void closeSocket(Socket* socket);

    // Attributes
    list<Socket*> socketList;
};

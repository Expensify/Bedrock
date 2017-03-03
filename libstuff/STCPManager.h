#pragma once

// Convenience base class for managing a series of TCP sockets.  This
// includes filling receive buffers, emptying send buffers, completing
// connections, performing graceful shutdowns, etc.
struct STCPManager {
    // Captures all the state for a single socket
    class Socket {
      public:
        enum State { CONNECTING, CONNECTED, SHUTTINGDOWN, CLOSED };
        Socket(int sock = 0, State state_ = CONNECTING);
        // Attributes
        int s;
        sockaddr_in addr;
        string sendBuffer;
        string recvBuffer;
        State state;
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

      private:
        static atomic<uint64_t> socketCount;
    };

    // Cleans up outstanding sockets
    virtual ~STCPManager();

    // Updates all managed sockets
    int preSelect(fd_map& fdm);
    void postSelect(fd_map& fdm);

    // Opens outgoing socket
    Socket* openSocket(const string& host, SX509* x509 = nullptr);

    // Gracefully shuts down a socket
    void shutdownSocket(Socket* socket, int how = SHUT_RDWR);

    // Hard terminate a socket
    void closeSocket(Socket* socket);

    // Attributes
    list<Socket*> socketList;
};

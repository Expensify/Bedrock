#pragma once
#include <libstuff/STCPManager.h>

// Convenience class for listening on a port and accepting sockets.
struct STCPServer : public STCPManager {

    // Stores information about a port
    class Port {
      public:
        Port(int _s, string _host);
        ~Port();

        // Attributes
        const int s;
        const string host;
    };

    // Listens on a given port and accepts sockets, and shuts them down
    STCPServer(const string& host);

    // Destructor
    virtual ~STCPServer();

    // Begins listening on a new port
    unique_ptr<Port> openPort(const string& host);

    // Tries to accept a new incoming socket
    Socket* acceptSocket(unique_ptr<Port>& port);

    // Updates all managed ports and sockets
    void prePoll(fd_map& fdm);
    void prePollPort(fd_map& fdm, const unique_ptr<Port>& port) const;
    void postPoll(fd_map& fdm);

    // Attributes
    unique_ptr<Port> port;

    // Do we need a mutex protecting this? Depends.
    list<STCPManager::Socket*> socketList;
};

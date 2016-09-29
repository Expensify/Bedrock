#pragma once

// Convenience class for listening on a port and accepting sockets.
struct STCPServer : public STCPManager {
    // Listens on a given port and accepts sockets, and shuts them down
    STCPServer(const string& host);
    virtual ~STCPServer();

    // Opens/closes the port
    void openPort(const string& host);
    void closePort();

    // Tries to accept a new incoming socket
    Socket* acceptSocket();

    // Updates all managed ports and sockets
    int preSelect(fd_map& fdm);
    void postSelect(fd_map& fdm);

    // Attributes
    int port;
};

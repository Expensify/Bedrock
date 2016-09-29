#pragma once

// Convenience class for sending requests and processing responses
struct SDataClient : public STCPManager {
    // Connection
    struct Connection {
        // Attributes
        string host;
        Socket* s;
        SData request;
    };

    // Attributes
    list<Connection *> activeConnectionList, idleConnectionList;

    // Maintains a series of outbound connections
    virtual ~SDataClient();

    // Send a new request to a given host
    void sendRequest(const string& host, const SData& request);

    // Override this to process responses
    virtual void onResponse(const string& host, const SData& request, const SData& response) = 0;

    // Processes the sockets and handles responses
    void postSelect(fd_map& fdm);
};

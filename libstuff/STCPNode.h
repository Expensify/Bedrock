#pragma once

struct STCPNode : public STCPServer {
    // Begins listening for connections on a given port
    STCPNode(const string& name, const string& host, const uint64_t recvTimeout_ = STIME_US_PER_M);
    virtual ~STCPNode();

    // Connects to a peer in the database cluster
    void addPeer(const string& name, const string& host, const STable& params);

    // Updates all peers
    int preSelect(fd_map& fdm);
    void postSelect(fd_map& fdm, uint64_t& nextActivity);

    // Represents a single peer in the database cluster
    struct Peer : public SData {
        // Attributes
        string name;
        string host;
        STable params;
        Socket* s;
        uint64_t latency;
        uint64_t nextReconnect;
        int failedConnections;

        // Helper methods
        Peer(const string& name_, const string& host_, const STable& params_)
            : name(name_), host(host_), params(params_), s(nullptr), latency(0), nextReconnect(0), failedConnections(0) {
        }
        bool connected() { return (s && s->state == STCP_CONNECTED); }
        void reset() {
            clear();
            s = nullptr;
            latency = 0;
        }
    };

    // Attributes
    string name;
    uint64_t recvTimeout;
    list<Peer*> peerList;
    list<Socket*> acceptedSocketList;

    // Called when we first establish a connection with a new peer
    virtual void _onConnect(Peer* peer) = 0;

    // Called when we lose connection with a peer
    virtual void _onDisconnect(Peer* peer) = 0;

    // Called when the peer sends us a message; throw a (const char*) to reconnect.
    virtual void _onMESSAGE(Peer* peer, const SData& message) = 0;

  private:
    // Override dead function
    void postSelect(fd_map& ignore) { SERROR("Don't call."); }

    // Helper functions
    void _sendPING(Peer* peer);
};

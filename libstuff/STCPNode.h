#pragma once

struct STCPNode : public STCPServer {
    // Begins listening for connections on a given port
    STCPNode(const string& name, const string& host, const uint64_t recvTimeout_ = STIME_US_PER_M);
    virtual ~STCPNode();

    // Updates all peers
    void prePoll(fd_map& fdm);
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Represents a single peer in the database cluster
    struct Peer : public SData {
        // Attributes
        string name;
        string host;
        STable params;
        Socket* s;
        uint64_t latency;
        uint64_t nextReconnect;
        uint64_t id;
        int failedConnections;

        // Helper methods
        Peer(const string& name_, const string& host_, const STable& params_, uint64_t id_)
          : name(name_), host(host_), params(params_), s(nullptr), latency(0), nextReconnect(0), id(id_),
            failedConnections(0)
        { }
        bool connected() { return (s && s->state == STCPManager::Socket::CONNECTED); }
        void reset() {
            clear();
            s = nullptr;
            latency = 0;
        }
    };
    
    // Connects to a peer in the database cluster
    void addPeer(const string& name, const string& host, const STable& params);

    // Returns a peer by it's ID. If the ID is invalid, returns nullptr.
    Peer* getPeerByID(uint64_t id);

    // Attributes
    string name;
    uint64_t recvTimeout;
    vector<Peer*> peerList;
    list<Socket*> acceptedSocketList;

    // Called when we first establish a connection with a new peer
    virtual void _onConnect(Peer* peer) = 0;

    // Called when we lose connection with a peer
    virtual void _onDisconnect(Peer* peer) = 0;

    // Called when the peer sends us a message; throw an SException to reconnect.
    virtual void _onMESSAGE(Peer* peer, const SData& message) = 0;

  private:
    // Override dead function
    void postPoll(fd_map& ignore) { SERROR("Don't call."); }

    // Helper functions
    void _sendPING(Peer* peer);
};

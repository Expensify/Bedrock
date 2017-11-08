#pragma once

struct STCPNode : public STCPServer {
    // Begins listening for connections on a given port
    STCPNode(const string& name, const string& host, const uint64_t recvTimeout_ = STIME_US_PER_M);
    virtual ~STCPNode();

    // Updates all peers
    void prePoll(fd_map& fdm);
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Represents a single peer in the database cluster
    class ExternalPeer;
    struct Peer : public SData {
        friend class ExternalPeer;

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
            failedConnections(0), externalPeerCount(0)
        { }
        bool connected() { return (s && s->state == STCPManager::Socket::CONNECTED); }
        void reset() {
            clear();
            s = nullptr;
            latency = 0;
        }

        // Get an externally accessible object that can thread-safely write responses to peers.
        static ExternalPeer getExternalPeer(Peer* peer);

        // Close the peer's socket
        void closeSocket(STCPManager& manager);

      private:
        recursive_mutex socketMutex;
        atomic<int> externalPeerCount;
    };

    class ExternalPeer {
        friend class Peer;

      public:
        // Move constructor.
        ExternalPeer(ExternalPeer && other);

        // Send a request to this peer.
        void sendRequest(const SData& request);

        // Destructor
        ~ExternalPeer();

      private:
        // Only instantiated by a Peer object.
        ExternalPeer(Peer* peer);

        // The peer object that instantiated this object.
        Peer* _peer;

        // A name for this object, since SWARN requires it.
        string name;
    };
    
    // Connects to a peer in the database cluster
    void addPeer(const string& name, const string& host, const STable& params);

    // Returns a peer by it's ID. If the ID is invalid, returns nullptr.
    Peer* getPeerByID(uint64_t id);

    // Get an externally accessible peer object by its ID.
    ExternalPeer getExternalPeerByID(uint64_t id);

    // Inverse of the above function. If the peer is not found, returns 0.
    uint64_t getIDByPeer(Peer* peer);

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

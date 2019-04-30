#pragma once

struct STCPNode : public STCPServer {
    // Begins listening for connections on a given port
    STCPNode(const string& name, const string& host, const uint64_t recvTimeout_ = STIME_US_PER_M);
    virtual ~STCPNode();

    // Possible states of a node in a DB cluster
    enum State {
        UNKNOWN,
        SEARCHING,     // Searching for peers
        SYNCHRONIZING, // Synchronizing with highest priority peer
        WAITING,       // Waiting for an opportunity to leader or follower
        STANDINGUP,    // Taking over leadership
        LEADING,       // Acting as leader node
        STANDINGDOWN,  // Giving up leader role
        SUBSCRIBING,   // Preparing to follow the leader
        FOLLOWING      // Following the leader node
    };
    static const string& stateName(State state);
    static State stateFromName(const string& name);

    // Updates all peers
    void prePoll(fd_map& fdm);
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Represents a single peer in the database cluster
    struct Peer : public SData {
        friend class STCPNode;
        friend class SQLiteNode;

        // Attributes
        string name;
        string host;
        STable params;
        State state;
        uint64_t latency;
        uint64_t nextReconnect;
        uint64_t id;
        int failedConnections;

        // Helper methods
        Peer(const string& name_, const string& host_, const STable& params_, uint64_t id_)
          : name(name_), host(host_), params(params_), state(SEARCHING), latency(0), nextReconnect(0), id(id_),
            failedConnections(0), s(nullptr)
        { }
        bool connected() { return (s && s->state.load() == STCPManager::Socket::CONNECTED); }
        void reset() {
            clear();
            s = nullptr;
            latency = 0;
        }

        // Close the peer's socket. This is synchronized so that you can safely call closeSocket and sendMessage on
        // different threads.
        void closeSocket(STCPManager* manager);

        // Send a message to this peer.
        void sendMessage(const SData& message);

      private:
        Socket* s;
        recursive_mutex socketMutex;
    };

    // Connects to a peer in the database cluster
    void addPeer(const string& name, const string& host, const STable& params);

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

  protected:
    // Returns a peer by it's ID. If the ID is invalid, returns nullptr.
    Peer* getPeerByID(uint64_t id);

    // Inverse of the above function. If the peer is not found, returns 0.
    uint64_t getIDByPeer(Peer* peer);

  private:
    // Override dead function
    void postPoll(fd_map& ignore) { SERROR("Don't call."); }

    // Helper functions
    void _sendPING(Peer* peer);
};

#pragma once

// Convenience class for maintaining connections with a mesh of peers
#define PDEBUG(_MSG_) SDEBUG("->{" << peer->name << "} " << _MSG_)
#define PINFO(_MSG_) SINFO("->{" << peer->name << "} " << _MSG_)
#define PHMMM(_MSG_) SHMMM("->{" << peer->name << "} " << _MSG_)
#define PWARN(_MSG_) SWARN("->{" << peer->name << "} " << _MSG_)

// Diagnostic class for timing what fraction of time happens in certain blocks.
class AutoTimer {
  public:
    AutoTimer(string name) : _name(name), _intervalStart(chrono::steady_clock::now()), _countedTime(0) { }
    void start() { _instanceStart = chrono::steady_clock::now(); };
    void stop() {
        auto stopped = chrono::steady_clock::now();
        _countedTime += stopped - _instanceStart;
        if (stopped > (_intervalStart + 10s)) {
            auto counted = chrono::duration_cast<chrono::milliseconds>(_countedTime).count();
            auto elapsed = chrono::duration_cast<chrono::milliseconds>(stopped - _intervalStart).count();
            static char percent[10] = {0};
            snprintf(percent, 10, "%.2f", static_cast<double>(counted) / static_cast<double>(elapsed) * 100.0);
            SINFO("[performance] AutoTimer (" << _name << "): " << counted << "/" << elapsed << " ms timed, " << percent << "%");
            _intervalStart = stopped;
            _countedTime = chrono::microseconds::zero();
        }
    };

  private:
    string _name;
    chrono::steady_clock::time_point _intervalStart;
    chrono::steady_clock::time_point _instanceStart;
    chrono::steady_clock::duration _countedTime;
};

class AutoTimerTime {
  public:
    AutoTimerTime(AutoTimer& t) : _t(t) { _t.start(); }
    ~AutoTimerTime() { _t.stop(); }

  private:
    AutoTimer& _t;
};

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
            state = SEARCHING;
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

    // A PeerList is just a vector<Peer*> that exposes certain methods behind a mutex such that the entire data
    // structure is synchronized. It has special handing for `begin()` and `end()` so that we can iterate over the list
    // and guarantee it's not changed in the process. See LockedPeerList below.
    class LockedPeerList;
    class PeerList {
        friend class LockedPeerList;
      public:
        template <typename T>
        auto push_back(const T& i) {
            lock_guard<decltype(_mutex)> l(_mutex);
            return _peerList.push_back(i); }
        template <typename T>
        auto operator[](T i) {
            lock_guard<decltype(_mutex)> l(_mutex);
            if (i >= _peerList.size()) {
                throw out_of_range("Attempted to access out-of-range Peer in PeerList");
            }
            return _peerList[i];
        }
        auto size() {
            lock_guard<decltype(_mutex)> l(_mutex);
            return _peerList.size();
        }
        auto empty() {
            lock_guard<decltype(_mutex)> l(_mutex);
            return _peerList.empty();
        }
        auto clear() {
            lock_guard<decltype(_mutex)> l(_mutex);
            return _peerList.clear();
        }
        auto atomic() {
            return LockedPeerList(*this);
        }
      private:
        auto begin() {
            lock_guard<decltype(_mutex)> l(_mutex);
            return _peerList.begin();
        }
        auto end() {
            lock_guard<decltype(_mutex)> l(_mutex);
            return _peerList.end();
        }

        vector<Peer*> _peerList;
        recursive_mutex _mutex;
    };

    // Because range-based for loops extend the life of the range expression to last the entire loop, we can use a
    // RAII-style wrapper around a peer list to lock the object for the life of the loop.
    // Instead of doing: for (auto& item : myPeerList) {
    // We can do: for (auto& item : myPeerList.atomic()) {
    // And instead of operating on a bare PeerList, we'll create a temporary LockedPeerList that calls `lock` in the
    // constructor and `unlock` in the destructor, and lasts the lifetime of the for loop.
    // Because the constructor is private, the only way to get a LockedPeerList is to call `atomic()` on a PeerList,
    // and because `begin()` and `end()` are private in `PeerList`, the only way to get those iterators is on a
    // LockedPeerList.
    // This prevents most synchronization problems, but doesn't prevent someone from using indexes into the array
    // directly.
    class LockedPeerList {
        friend class PeerList;
      public:
        auto begin() { return _peerList.begin(); }
        auto end() { return _peerList.end(); }
        ~LockedPeerList() {
            _peerList._mutex.unlock();
        }
      private:
        LockedPeerList(PeerList& peerList) : _peerList(peerList) {
            _peerList._mutex.lock();
        }
        PeerList& _peerList;
    };

    // Attributes
    string name;
    uint64_t recvTimeout;
    PeerList peerList;
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

    AutoTimer _deserializeTimer;
    AutoTimer _sConsumeFrontTimer;
    AutoTimer _sAppendTimer;
};

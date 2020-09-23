#pragma once
#include <libstuff/libstuff.h>

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
    class Peer {
      public:
        // Constructor.
        Peer(const string& name_, const string& host_, const STable& params_, uint64_t id_)
          : name(name_), host(host_), id(id_), params(params_),
            commitCount(0),
            failedConnections(0),
            hash(),
            latency(0),
            loggedIn(false),
            nextReconnect(0),
            priority(0),
            state(SEARCHING),
            standupResponse(Response::NONE),
            subscribed(false),
            transactionResponse(Response::NONE),
            version()
        { }

        bool isPermafollower() const {
            auto it = params.find("Permafollower");
            if (it != params.end() && it->second == "true") {
                return true;
            }
            return false;
        }

        bool connected() const {
            lock_guard<decltype(_stateMutex)> l(_stateMutex);
            return (s && s->state.load() == STCPManager::Socket::CONNECTED);
        }

        void reset() {
            lock_guard<decltype(_stateMutex)> l(_stateMutex);
            //clear();
            state = SEARCHING;
            s = nullptr;
            latency = 0;
        }

        // Close the peer's socket. This is synchronized so that you can safely call closeSocket and sendMessage on
        // different threads.
        void closeSocket(STCPManager* manager);

        // Send a message to this peer.
        void sendMessage(const SData& message);

        enum class Response {
            NONE,
            APPROVE,
            DENY
        };

        static string responseName(Response response) {
            switch (response) {
                case STCPNode::Peer::Response::NONE:
                return "NONE";
                break;
                case STCPNode::Peer::Response::APPROVE:
                return "APPROVE";
                break;
                case STCPNode::Peer::Response::DENY:
                return "DENY";
                break;
            }
            return "";
        }

        // Attributes
        const string name;
        const string host;
        const uint64_t id;
        const STable params;
        atomic<uint64_t> commitCount;
        atomic<int> failedConnections;
        atomic<string> hash;
        atomic<uint64_t> latency;
        atomic<bool> loggedIn;
        atomic<uint64_t> nextReconnect;
        atomic<int> priority;
        atomic<State> state;
        atomic<Response> standupResponse;
        atomic<bool> subscribed;
        atomic<Response> transactionResponse;
        atomic<string> version;

        STable getData() const {
            // Add all of our standard stuff.
            STable result({
                {"name", name},
                {"host", host},
                {"state", (stateName(state) + (connected() ? "" : " (DISCONNECTED)"))},
                {"latency", to_string(latency)},
                {"nextReconnect", to_string(nextReconnect)},
                {"id", to_string(id)},
                {"failedConnections", to_string(failedConnections)},
                {"loggedIn", (loggedIn ? "true" : "false")},
                {"priority", to_string(priority)},
                {"version", version},
                {"hash", hash},
                {"commitCount", to_string(commitCount)},
                {"standupResponse", responseName(standupResponse)},
                {"transactionResponse", responseName(transactionResponse)},
                {"subscribed", (subscribed ? "true" : "false")},
            });

            // And anything from the params (note: doesn't overwrite our standard stuff).
            for (auto& p : params) {
                result.emplace(p);
            }
            return result;
        }

      private:
        // This allows direct access to the socket from the node object that should actually be managing peer
        // connections, which should always be handled by a single thread, and thus safe. Ideally, this isn't required,
        // but for the time being, the amount of refactoring required to fix that is too high.
        friend class STCPNode;
        friend class SQLiteNode;
        Socket* s = nullptr;

        // This is not meant to be accessible from STCPNode (but has to be with the way `friend` works).
        mutable recursive_mutex _stateMutex;
    };

    // Begins listening for connections on a given port
    STCPNode(const string& name, const string& host, const vector<Peer*> _peerList, const uint64_t recvTimeout_ = STIME_US_PER_M);
    virtual ~STCPNode();

    // Attributes
    string name;
    uint64_t recvTimeout;
    //PeerList peerList;
    const vector<Peer*> peerList;
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

// serialization for Responses.
ostream& operator<<(ostream& os, const atomic<STCPNode::Peer::Response>& response);

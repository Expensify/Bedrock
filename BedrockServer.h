// Manages connections to a single instance of the bedrock server.
#pragma once
#include <libstuff/libstuff.h>
#include "BedrockNode.h"
#include "BedrockPlugin.h"
#include <thread>
#include <condition_variable>

// Template specialization to make SSynchronizedQueue work with SQLiteNode::Command*
template<>
template<typename KeyType, typename ValueType>
bool SSynchronizedQueue<SQLiteNode::Command*>::cancel(const KeyType& name, const ValueType& value) {
    SAUTOLOCK(_queueMutex);
    // Loop across and see if we can find it; if so, cancel
    for (auto queueIt = _queue.begin(); queueIt != _queue.end(); ++queueIt) {
        if ((*queueIt)->request[name] == value) {
            // Found it
            _queue.erase(queueIt);
            return true;
        }
    }

    // Didn't find it
    return false;
}

class BedrockServer : public STCPServer {
  public: // External Bedrock

    // A MessageQueue is a synchronized queue of SDatas.
    typedef SSynchronizedQueue<SData> MessageQueue;

    typedef SSynchronizedQueue<SQLiteNode::Command*> CommandQueue;

    // A synchronized queue of messages for enabling the main, read, and write
    // threads to communicate safely.
    //
    class ThreadData {
      public:
        ThreadData(string name_, SData args_, SSynchronized<SQLCState>& replicationState_,
                   SSynchronized<uint64_t>& replicationCommitCount_, SSynchronized<bool>& gracefulShutdown_,
                   SSynchronized<string>& masterVersion_, MessageQueue& queuedRequests_,
                   MessageQueue& processedResponses_, CommandQueue& escalatedCommands_, CommandQueue& peekedCommands_,
                   BedrockServer* server_) :
            name(name_),
            args(args_),
            replicationState(replicationState_),
            replicationCommitCount(replicationCommitCount_),
            gracefulShutdown(gracefulShutdown_),
            masterVersion(masterVersion_),
            queuedRequests(queuedRequests_),
            processedResponses(processedResponses_),
            escalatedCommands(escalatedCommands_),
            peekedCommands(peekedCommands_),
            server(server_),
            threadObject() {}

        MessageQueue directMessages;

        // Thread's name.
        string name;

        // Command line args passed in.
        SData args;

        // Shared var for communicating replication thread's status.
        SSynchronized<SQLCState>& replicationState;

        // Shared var for communicating replication thread's commit count (for sticky connections)
        SSynchronized<uint64_t>& replicationCommitCount;

        // Shared var for communicating shutdown status between threads.
        SSynchronized<bool>& gracefulShutdown;

        // Shared var for communicating the master version (for knowing if we should skip the slave peek).
        SSynchronized<string>& masterVersion;

        // Shared external queue between threads. Queued for read-only thread(s)
        MessageQueue& queuedRequests;

        // Shared external queue between threads. Finished commands ready to return to client.
        MessageQueue& processedResponses;

        CommandQueue& escalatedCommands;
        CommandQueue& peekedCommands;

        // The server this thread is running in.
        BedrockServer* server;

        // The actual thread object associated with this data object. This is set after initialization.
        thread threadObject;
    };

    // Constructor / Destructor
    BedrockServer(const SData& args);
    virtual ~BedrockServer();

    // Accessors
    SQLCState getState() { return _replicationState.get(); }

    // Ready to gracefully shut down
    bool shutdownComplete();

    // Flush the send buffers
    int preSelect(fd_map& fdm);

    // Accept connections and dispatch requests
    void postSelect(fd_map& fdm, uint64_t& nextActivity);

    // Control the command port. The server will toggle this as necessary, unless manualOverride is set,
    // in which case that setting trumps the `suppress` setting.
    void suppressCommandPort(bool suppress, bool manualOverride = false);

    // Add a new request to our message queue.
    void queueRequest(const SData& request);

    // Returns the version string of the server.
    const string& getVersion();

    // Each plugin can register as many httpsManagers as it likes. They'll all get checked for activity in the
    // read loop on the sync thread.
    list<list<SHTTPSManager*>> httpsManagers;

    // Keeps track of the time we spend idle.
    SPerformanceTimer pollTimer;

    // Called by a bedrockNode when it needs to make an escalated request available externally.
    void enqueueCommand(SQLiteNode::Command* command);

  private: // Internal Bedrock
    // Attributes
    SData _args;
    uint64_t _requestCount;
    map<uint64_t, Socket*> _requestCountSocketMap;
    list<ThreadData> _workerThreadList;
    SSynchronized<SQLCState> _replicationState;
    SSynchronized<uint64_t> _replicationCommitCount;
    SSynchronized<bool> _nodeGracefulShutdown;
    SSynchronized<string> _masterVersion;
    MessageQueue _queuedRequests;
    MessageQueue _processedResponses;

    // Two new queues for communicating escalated requests out from the sync thread to workers, and then when
    // completed, communicating those responses back to the sync thread.
    CommandQueue _escalatedCommands;
    CommandQueue _peekedCommands;

    bool _suppressCommandPort;
    bool _suppressCommandPortManualOverride;
    map<Port*, BedrockPlugin*> _portPluginMap;
    string _version;
    ThreadData _syncThread;

    static void worker(ThreadData& data, int threadId, int threadCount);
    static void syncWorker(ThreadData& data);

    static condition_variable _threadInitVar;
    static mutex _threadInitMutex;
    static bool _threadReady;

    static BedrockNode* _syncNode;
};

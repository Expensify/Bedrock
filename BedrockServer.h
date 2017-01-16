// Manages connections to a single instance of the bedrock server.
#pragma once
#include <libstuff/libstuff.h>
#include "BedrockNode.h"
#include "BedrockPlugin.h"

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/// BedrockServer
/// ---------
class BedrockServer : public STCPServer {
  public: // External Bedrock
    // A synchronized queue of messages for enabling the main, read, and write
    // threads to communicate safely.
    //
    class MessageQueue {
      public:
        // Constructor / Destructor
        MessageQueue();
        ~MessageQueue();

        // Explicitly delete copy constructor so it can't accidentally get called.
        MessageQueue(const MessageQueue& other) = delete;

        // Wait for something to be put onto the queue
        int preSelect(fd_map& fdm);
        void postSelect(fd_map& fdm, int bytesToRead = 1);

        // Synchronized interface to add/remove work
        void push(const SData& rhs);
        SData pop();
        bool empty();
        bool cancel(const string& name, const string& value);

      private:
        // Private state
        list<SData> _queue;
        recursive_mutex _queueMutex;
        int _pipeFD[2] = {-1, -1};
    };

    // All the data required for a thread to create an BedrockNode
    // and coordinate with other threads.
    struct Thread {
        Thread(const string& name_,                         // Thread name
               SData args_,                                 // Command line args passed in.
               SSynchronized<SQLCState>& replicationState_, // Shared var for communicating replication thread's status.
               SSynchronized<uint64_t>& replicationCommitCount_, // Shared var for communicating replication thread's
                                                                 // commit count (for sticky connections)
               SSynchronized<bool>& gracefulShutdown_, // Shared var for communicating shutdown status between threads.
               SSynchronized<string>& masterVersion_, // Shared var for communicating the master version (for knowing if
                                                      // we should skip the slave peek).
               MessageQueue& queuedRequests_, // Shared external queue between threads. Queued for read-only thread(s)
               MessageQueue&
                   queuedEscalatedRequests_, // Shared external queue between threads. Queued for replication thread
               MessageQueue& processedResponses_, // Shared external queue between threads. Finished commands ready to
                                                  // return to client.
               BedrockServer* server_)            // The server spawning the thread.
            : name(name_),
              args(args_),
              replicationState(replicationState_),
              replicationCommitCount(replicationCommitCount_),
              gracefulShutdown(gracefulShutdown_),
              masterVersion(masterVersion_),
              queuedRequests(queuedRequests_),
              queuedEscalatedRequests(queuedEscalatedRequests_),
              processedResponses(processedResponses_),
              ready(false),
              server(server_) {
            // Initialized above
        }

        // Public attributes
        string name;
        SData args;
        SSynchronized<SQLCState>& replicationState;
        SSynchronized<uint64_t>& replicationCommitCount;
        SSynchronized<bool>& gracefulShutdown;
        SSynchronized<string>& masterVersion;
        MessageQueue& queuedRequests;
        MessageQueue& queuedEscalatedRequests;
        MessageQueue& processedResponses;
        MessageQueue directMessages;
        void* thread;
        SSynchronized<bool> ready;
        BedrockServer* server;
        bool finished = false;
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
    // read loop on the write thread.
    list<list<SHTTPSManager*>> httpsManagers;

    // Keeps track of the time we spend idle.
    SPerformanceTimer pollTimer;

  private: // Internal Bedrock
    // Attributes
    SData _args;
    uint64_t _requestCount;
    map<uint64_t, Socket*> _requestCountSocketMap;
    Thread* _writeThread;
    list<Thread*> _readThreadList;
    SSynchronized<SQLCState> _replicationState;
    SSynchronized<uint64_t> _replicationCommitCount;
    SSynchronized<bool> _nodeGracefulShutdown;
    SSynchronized<string> _masterVersion;
    MessageQueue _queuedRequests;
    MessageQueue _queuedEscalatedRequests;
    MessageQueue _processedResponses;
    bool _suppressCommandPort;
    bool _suppressCommandPortManualOverride;
    map<Port*, BedrockPlugin*> _portPluginMap;
    string _version;
};

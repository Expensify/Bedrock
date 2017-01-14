// Manages connections to a single instance of the bedrock server.
#pragma once
#include <libstuff/libstuff.h>
#include "BedrockNode.h"
#include "BedrockPlugin.h"
#include "PollTimer.h"
#include <thread>
#include <condition_variable>

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/// BedrockServer
/// ---------
class BedrockServer : public STCPServer {
  public: // External Bedrock

    // This lets an SHTTPSManager notify us when it's finished a request, so we can interrupt our write threads'
    // poll loop so they'll pick up on the new activity.
    class Notification : public SHTTPSManager::Notifiable {
      public:
        Notification(BedrockServer& server) : _server(server) {}
        ~Notification() {}
        void notifyActivity() {
            _server.dummyQueue.push(SData("DUMMY"));
        }

      private:
        BedrockServer& _server;
    };

    // A synchronized queue of messages for enabling the main, read, and write
    // threads to communicate safely.
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

    class ThreadData {
      public:
        ThreadData(string name_, SData args_, SSynchronized<SQLCState>& replicationState_,
                   SSynchronized<uint64_t>& replicationCommitCount_, SSynchronized<bool>& gracefulShutdown_,
                   SSynchronized<string>& masterVersion_, MessageQueue& queuedRequests_,
                   MessageQueue& queuedEscalatedRequests_, MessageQueue& processedResponses_, BedrockServer* server_) :
            name(name_),
            args(args_),
            replicationState(replicationState_),
            replicationCommitCount(replicationCommitCount_),
            gracefulShutdown(gracefulShutdown_),
            masterVersion(masterVersion_),
            queuedRequests(queuedRequests_),
            queuedEscalatedRequests(queuedEscalatedRequests_),
            processedResponses(processedResponses_),
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

        // Shared external queue between threads. Queued for replication thread
        MessageQueue& queuedEscalatedRequests;

        // Shared external queue between threads. Finished commands ready to return to client.
        MessageQueue& processedResponses;

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
    // read loop on the write thread.
    list<list<SHTTPSManager*>> httpsManagers;

    // Keeps track of the time we spend idle.
    PollTimer pollTimer;

    // Used only to interrupt poll because other work is available.
    MessageQueue dummyQueue;

  private: // Internal Bedrock
    // Attributes
    SData _args;
    uint64_t _requestCount;
    map<uint64_t, Socket*> _requestCountSocketMap;
    list<ThreadData> _writeThreadList;
    list<ThreadData> _readThreadList;
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

    static void readWorker(ThreadData& data);
    static void writeWorker(ThreadData& data, MessageQueue& dummy);

    static condition_variable _threadInitVar;
    static mutex _threadInitMutex;
    static int _threadsReady;

    Notification _notification;
};

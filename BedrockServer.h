// Manages connections to a single instance of the bedrock server.
#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>
#include "BedrockPlugin.h"
#include "BedrockCommandQueue.h"

class BedrockServer : public SQLiteServer {
  public:

#if 0
    class MessageQueue : public SSynchronizedQueue<SData> {
      public:
        bool cancel(const string& name, const string& value) {
            SAUTOLOCK(_queueMutex);
            // Loop across and see if we can find it; if so, cancel
            for (auto queueIt = _queue.begin(); queueIt != _queue.end(); ++queueIt) {
                if ((*queueIt)[name] == value) {
                    // Found it
                    _queue.erase(queueIt);
                    return true;
                }
            }

            // Didn't find it
            return false;
        }
    };
#endif

    class CommandQueue : public SSynchronizedQueue<BedrockCommand> {
      public:
        bool cancel(const string& name, const string& value) {
            SAUTOLOCK(_queueMutex);
            // Loop across and see if we can find it; if so, cancel
            for (auto queueIt = _queue.begin(); queueIt != _queue.end(); ++queueIt) {
                if ((*queueIt).request[name] == value) {
                    // Found it
                    _queue.erase(queueIt);
                    return true;
                }
            }

            // Didn't find it
            return false;
        }
    };

    BedrockServer(const SData& args);
    virtual ~BedrockServer();

    // Accept an incoming command from an SQLiteNode.
    void acceptCommand(SQLiteCommand&& command);

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

  private:
    // Attributes
    SData _args;

    BedrockCommandQueue _commandQueue;

    // The queues in which we store commands.
    // A map of priorities to commands ordered by timestamp.

    // Each time we read a new request from a client, we give it a unique ID.
    uint64_t _requestCount;

    // We keep a map of requests to socket. We should never have more than one request per socket at a given time, or
    // we could deliver responses in the wrong order.
    map<uint64_t, Socket*> _requestCountSocketMap;

    // This is the replication state of the sync node. It's updated after every SQLiteNode::update() iteration. A
    // reference to this object is passed to the sync thread.
    atomic<SQLiteNode::State> _replicationState;

    //list<ThreadData> _workerThreadDataList;
    //atomic<uint64_t> _replicationCommitCount;

    // This flag will be raised when we want to start shutting down. A reference is passed to the sync thread to allow
    // it to shut down its SQLiteNode.
    atomic<bool> _nodeGracefulShutdown;

    // This exists to provide data to plugins. Refactor.
    // atomic<string> _masterVersion;
    // MessageQueue _queuedRequests;
    // MessageQueue _processedResponses;

    // Two queues for communicating escalated requests out from the sync thread to workers, and then when
    // completed, communicating those responses back to the sync thread.
    // CommandQueue _escalatedCommands;
    // CommandQueue _peekedCommands;

    // This is a synchronized queued that can wake up a `poll()` call if something is added to it. This contains the
    // list of commands that worker threads were unable to complete on their own that needed to be passed back to the
    // sync thread. A reference is passed to the sync thread.
    CommandQueue _syncNodeQueuedCommands;

    bool _suppressCommandPort;
    bool _suppressCommandPortManualOverride;
    map<Port*, BedrockPlugin*> _portPluginMap;

    // The server version. This may be fake if the args contain a `versionOverride` value.
    string _version;

    // This is the function that launches the sync thread, which will bring up the SQLiteNode for this server, and then
    // start the worker threads.
    static void sync(SData& args,
                     atomic<SQLiteNode::State>& replicationState,
                     atomic<bool>& nodeGracefulShutdown,
                     CommandQueue& syncNodeQueuedCommands,
                     BedrockServer& server);

    // Each worker thread runs this function. It receives the same data as the sync thread, plus its individual thread
    // ID.
    static void worker(SData& args,
                       atomic<SQLiteNode::State>& _replicationState,
                       atomic<bool>& nodeGracefulShutdown,
                       CommandQueue& syncNodeQueuedCommands,
                       BedrockServer& server,
                       int threadId,
                       int threadCount);

    static constexpr auto syncThreadName = "sync";
    thread syncThread;
};

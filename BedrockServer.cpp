// Manages connections to a single instance of the bedrock server.
#include <libstuff/libstuff.h>
#include "BedrockServer.h"
#include "BedrockPlugin.h"
#include "BedrockCore.h"

// Status is special - it has commands that need to be handled by the sync node.
#include <plugins/Status.h>

void BedrockServer::acceptCommand(SQLiteCommand&& command) {
    _commandQueue.push(BedrockCommand(move(command)));
}

void BedrockServer::cancelCommand(const string& commandID) {
    _commandQueue.removeByID(commandID);
}

void BedrockServer::sync(SData& args,
                         atomic<SQLiteNode::State>& replicationState,
                         atomic<bool>& nodeGracefulShutdown,
                         CommandQueue& syncNodeQueuedCommands,
                         BedrockServer& server)
{
    // Initialize the thread.
    SInitialize(syncThreadName);

    // Parse out the number of worker threads we'll use. The DB needs to know this because it will expect a
    // corresponding number of journal tables. "-readThreads" exists only for backwards compatibility.
    // TODO: remove when nothing uses readThreads.
    int workerThreads = args.calc("-workerThreads");
    workerThreads = workerThreads ? workerThreads : args.calc("-readThreads");
    // If still no value, use the number of cores on the machine, if available.
    workerThreads = workerThreads ? workerThreads : max(1u, thread::hardware_concurrency());

    // Initialize the DB.
    SQLite db(args["-db"], args.calc("-cacheSize"), 1024, args.calc("-maxJournalSize"), -1, workerThreads - 1);

    // And the command processor.
    BedrockCore core(db);

    // And the sync node.
    uint64_t firstTimeout = STIME_US_PER_M * 2 + SRandom::rand64() % STIME_US_PER_S * 30;
    SQLiteNode syncNode(server, db, args["-nodeName"], args["-nodeHost"], args["-peerList"], args.calc("-priority"), firstTimeout,
                        server.getVersion(), args.calc("-quorumCheckpoint"));

    // The node is now coming up, and should eventually end up in a `MASTERING` or `SLAVING` state. We can start adding
    // our worker threads now. We don't wait until the node is `MASTERING` or `SLAVING`, as it's state can change while
    // it's running, and our workers will have to maintain awareness of that state anyway.
    SINFO("Starting " << workerThreads << " worker threads.");
    list<thread> workerThreadList;
    for (int threadId = 0; threadId < workerThreads; threadId++) {
        workerThreadList.emplace_back(worker,
                                      ref(args),
                                      ref(replicationState),
                                      ref(nodeGracefulShutdown),
                                      ref(syncNodeQueuedCommands),
                                      ref(server),
                                      threadId,
                                      workerThreads);
    }

    // Now we jump into our main command processing loop.
    uint64_t nextActivity = STimeNow();
    BedrockCommand command;
    while (!syncNode.shutdownComplete()) {
        // The fd_map contains a list of all file descriptors (eg, sockets, Unix pipes) that poll will wait on for
        // activity. Once any of them has activity (or the timeout ends), poll will return.
        fd_map fdm;

        // Pre-process any HTTPS reqeusts that need handling.
        for (list<SHTTPSManager*>& managerList : server.httpsManagers) {
            for (SHTTPSManager* manager : managerList) {
                manager->preSelect(fdm);
            }
        }

        // Pre-process any sockets the sync node is managing.
        syncNode.preSelect(fdm);

        // Add the Unix pipe from the shared queues to the fdm
        // data.peekedCommands.preSelect(fdm);
        // data.directMessages.preSelect(fdm);

        // Ok, so the challenge here is: How can I interrupt poll() if it's sitting waiting with nothing happening, and
        // I get a new command to process?
        // We can add an FD to the poll() set, and kick it when we queue activity for the sync node (which is what
        // command queue already does).

        // Wait for activity on any of those FDs, up to a timeout
        const uint64_t now = STimeNow();
        S_poll(fdm, max(nextActivity, now) - now);
        nextActivity = STimeNow() + STIME_US_PER_S; // 1s max period

        // Process any network traffic that happened in the plugin HTTPS managers.
        for (list<SHTTPSManager*>& managerList : server.httpsManagers) { 
            for (SHTTPSManager* manager : managerList) {
                manager->postSelect(fdm, nextActivity);
            }
        }

        // Process any network traffic that happened in the sync thread.
        syncNode.postSelect(fdm, nextActivity);

        // See if there's any work to process, unless we're already mid-commit, in which case, we'll wait until
        // that one completes.
        bool hasWork = false;
        if (!syncNode.commitInProgress()) {
            try {
                // TODO: We should skip commands with unfinished HTTPS requests.
                command = syncNodeQueuedCommands.pop();
                while (command.complete) {
                    // If there's no initiator, this should be returned to a client instead.
                    SASSERT(command.initiator);
                    // This is complete, we just need to return a response to a peer. Run through these in order
                    // until we find something we need to commit.
                    syncNode.queueResponse(move(command));
                    command = syncNodeQueuedCommands.pop();
                }
                hasWork = true;
            } catch (out_of_range) {
                // No commands to process.
            }
        }

        // We found some work to do, let's start on it.
        if (hasWork) {
            core.peekCommand(command);
            core.processCommand(command);
            // Command is now invalidated, and the sync node owns the copy.
            syncNode.startCommit(command);
        }

        // Let the update loop run as long as it needs.
        SQLiteNode::State state = syncNode.getState();
        while (syncNode.update(nextActivity)) {
            SQLiteNode::State newState = syncNode.getState();
            replicationState.store(newState);
            server._masterVersion.store(syncNode.getMasterVersion());
            // Check if we've become the master. We'll need to let our plugins do their database upgrades.
            if (newState != state && newState == SQLiteNode::MASTERING) {
                // If we changed just changed states to mastering, this should be impossible.
                SASSERT(!syncNode.commitInProgress());
                core.upgradeDatabase();
                syncNode.startCommit();
            }
            state = newState;
        }

        // Update finished, did the command complete?
        // TODO: This is only valid of we even started a command
        if (!syncNode.commitInProgress()) {
            bool commandComplete = true;
            // TODO: this? command = syncNode.completedCommand();
            // Or this whole block gets replaced.
            if (!syncNode.commitSucceeded()) {
                // Try it again in a bit, unless we hit a limit.
                if (command.processCount < 10) {
                    syncNodeQueuedCommands.push(move(command));
                    commandComplete = false;
                } else {
                    SWARN("10 conflicts in a row on the sync thread. Abandoning command.");
                    command.complete = true;
                    command.response.clear();
                    command.response.methodLine = "500 Too Many Conflicts";
                }
            }
            // This should be true unless we conflicted (or even if we did, if we hit the limit).
            if (commandComplete) {
                if (command.initiator) {
                    syncNode.queueResponse(move(command));
                } else {
                    server._reply(command);
                }
            }
        }
    }

#if 0

    // This needs to be set because the constructor for SQLiteNode depends on it.
    data.args["-worker"] = "false";
    SINFO("Starting sync thread for '" << data.name << "'");

    // Create the actual node
    SINFO("Starting SQLiteNode: " << data.args.serialize());

    // Figure out how many threads we should start.
    // "-readThreads" exists only for backwards compatibility. TODO: remove when nothing uses this.
    int readThreads = data.args.calc("-readThreads");
    int workerThreads = data.args.calc("-workerThreads");
    int threads = workerThreads ? workerThreads : readThreads;

    // If no value was specified, default to the number of cores on the machine, unless it's unspecified, in which case
    // we'll default to 1.
    threads = threads ? threads : max(1u, thread::hardware_concurrency());

    // We let the sync thread create our journal tables here so that they exist when we start our workers.
    SQLiteNode syncNode(data.args, -1, threads, data.server);
    _syncNode = &syncNode;
    SINFO("Node created, ready for action.");

    // Notify the parent thread that we're ready to go.
    {
        lock_guard<mutex> lock(_syncThreadInitMutex);
        _syncThreadReady = true;
    }
    _syncThreadReadyCondition.notify_all();

    // Add peers
    list<string> parsedPeerList = SParseList(data.args["-peerList"]);
    for (const string& peer : parsedPeerList) {
        // Get the params from this peer, if any
        string host;
        STable params;
        SASSERT(SParseURIPath(peer, host, params));
        string name = SGetDomain(host);
        if (params.find("nodeName") != params.end()) {
            name = params["nodeName"];
        }
        syncNode.addPeer(name, host, params);
    }

    // Main event loop for replication thread.
    uint64_t nextActivity = STimeNow();
    while (!syncNode.shutdownComplete()) {
        // Update shared var so all threads have awareness of the current replication state
        // and version as determined by the replication node.
        data.replicationState.store(syncNode.getState());
        data.replicationCommitCount.store(syncNode.getCommitCount());
        data.masterVersion.store(syncNode.getMasterVersion());

        // If we've been instructed to shutdown and we haven't yet, do it.
        if (data.gracefulShutdown.load()) {
            syncNode.beginShutdown();
        }

        // The fd_map contains a list of all file descriptors (eg, sockets,
        // Unix pipes) that poll will wait on for activity.  Once any of them
        // has activity (or the timeout ends), poll will return.
        fd_map fdm;

        // Add all HTTPS requests from plugins to the fdm 
        for (list<SHTTPSManager*>& managerList : data.server->httpsManagers) {
            for (SHTTPSManager* manager : managerList) {
                manager->preSelect(fdm);
            }
        }

        // Add the node's sockets to the fdm
        syncNode.preSelect(fdm);

        // Add the Unix pipe from the shared queues to the fdm
        data.peekedCommands.preSelect(fdm);
        data.directMessages.preSelect(fdm);

        // Wait for activity on any of those FDs, up to a timeout
        const uint64_t now = STimeNow();
        S_poll(fdm, max(nextActivity, now) - now);
        nextActivity = STimeNow() + STIME_US_PER_S; // 1s max period

        // Allow plugins to handle any activity
        for (list<SHTTPSManager*>& managerList : data.server->httpsManagers) { 
            for (SHTTPSManager* manager : managerList) {
                manager->postSelect(fdm, nextActivity);
            }
        }

        // Allow the node to handle any activity
        syncNode.postSelect(fdm, nextActivity);

        // Allow the shared queues to handle any activity
        data.peekedCommands.postSelect(fdm);
        data.directMessages.postSelect(fdm);

        // Process any direct messages from the main thread to us
        BedrockServer_WorkerThread_ProcessDirectMessages(syncNode, data.directMessages);

        // Check for available work sent to us from worker threads
        while (true) {
            // Try to get some work
            SQLiteNode::Command* command = data.peekedCommands.pop();
            if (!command) {
                break;
            }

            // Found some work -- let's resume processing it
            SINFO("Re-opening peeked command for processing: " << command->id << ":" << command->request.methodLine);
            syncNode.reopenCommand(command);
        }

        // Let the node process any new commands we've opened or existing commands outstanding.
        while (syncNode.update(nextActivity)) {
        }

        // Did the sync node process any commands?
        SQLiteNode::Command* command = nullptr;
        while ((command = syncNode.getProcessedCommand())) {
            // Finalize the response and add to the output queue
            SAUTOPREFIX(command->request["requestID"]);
            SINFO("Putting escalated command '" << command->id << "' on processed list.");
            BedrockServer_PrepareResponse(command);
            data.processedResponses.push(command->response);

            // Close the command to remove it from any internal queues.
            syncNode.closeCommand(command);
        }
    }
#endif

    // Update the state one last time when the writing replication thread exits.
    SQLiteNode::State state = syncNode.getState();
    if (state > SQLiteNode::WAITING) {
        // This is because the graceful shutdown timer fired and syncNode.shutdownComplete() returned `true` above, but
        // the server still thinks it's in some other state. We can only exit if we're in state <= SQLC_SEARCHING,
        // (per BedrockServer::shutdownComplete()), so we force that state here to allow the shutdown to proceed.
        SWARN("Sync thread exiting in state " << state << ". Setting to SQLC_SEARCHING.");
        state = SQLiteNode::SEARCHING;
    } else {
        SINFO("Sync thread exiting, setting state to: " << state);
    }
    replicationState.store(state);
    //replicationCommitCount.store(syncNode.getCommitCount());

    // Wait for the worker threads to finish.
    int threadId = 0;
    for (auto& workerThread : workerThreadList) {
        SINFO("Closing worker thread '" << "worker" << threadId << "'");
        threadId++;
        workerThread.join();
    }
}

void BedrockServer::worker(SData& args,
                           atomic<SQLiteNode::State>& _replicationState,
                           atomic<bool>& nodeGracefulShutdown,
                           CommandQueue& syncNodeQueuedCommands,
                           BedrockServer& server,
                           int threadId,
                           int threadCount)
{
    SInitialize("worker" + to_string(threadId));
    
    SQLite db(args["-db"], args.calc("-cacheSize"), 1024, args.calc("-maxJournalSize"), threadId, threadCount - 1);
    BedrockCore core(db);

    // Command to work on. This default command is replaced when we find work to do.
    BedrockCommand command;

    while (true) {
        try {
            // If we can't find any work to do, this will throw.
            command = server._commandQueue.get(1000000);

            if (command.complete) {
                // If this command is already complete, we can return it to the caller.
                // If it has an initiator, it should be returned to a peer by a sync node instead.
                SASSERT(!command.initiator);
            }

            // We'll retry on conflict up to this many times.
            int retry = 3;
            bool commandComplete = true;
            while (retry) {
                // Try peeking the command. If this succeeds, then it's finished, and all we need to do is respond to
                // the command at the bottom.
                if (!core.peekCommand(command)) {
                    // If the command opened an HTTPS request, the sync thread will poll for activity on its socket.
                    // Also, only the sync thread can handle quorum commits.
                    if (command.httpsRequest || command.writeConsistency != SQLiteNode::ASYNC) {
                        syncNodeQueuedCommands.push(move(command));
                        // We neither retry nor respond here.
                        break;
                    }  else {
                        // There's nothing blocking us from committing this ourselves. Let's try it. If it returns
                        // true, we need to commit it. Otherwise, there was nothing to commit, and we can jump straight
                        // to responding to it.
                        if (core.processCommand(command)) {
                            // Ok, we need to commit, let's try it. If it succeeds, then we just have to respond.
                            if (!core.commitCommand(command)) {
                                // Commit failed. Conflict. :(
                                commandComplete = false;
                            }
                        }
                    }
                }
                // If we either didn't need to commit, or we committed successfully, we'll get here.
                if (commandComplete) {
                    if (command.initiator) {
                        // Escalated command. Give it back to the sync thread to respond.
                        syncNodeQueuedCommands.push(move(command));
                    } else {
                        server._reply(command);
                    }
                    // Don't need to retry.
                    break;
                }
                // We're about to retry, decrement the retry count.
                retry--;
            }
            // We ran out of retries without finishing! We give it to the sync thread.
            if (!retry) {
                SWARN("Max retries hit, forwarding command to sync node.");
                syncNodeQueuedCommands.push(move(command));
            }
        } catch(...) {
            // Nothing after 1s.
        }

        // Ok, we're done, see if we should exit.
        if (0 /* TODO: exit_condition */) {
            break;
        }
    }
    

#if 0

    // This needs to be set because the constructor for SQLiteNode depends on it.
    data.args["-worker"] = "true";

    // We erase this in a worker thread, because this value gets passed down the constructor stack for an SQLiteNode,
    // eventually becoming the 'host' parameter to STCPServer, which will try to open it unless it's empty. Since our
    // sync node is responsible for that job, we erase erase this in workers so the don't talk to peers.
    data.args.erase("-nodeHost");
    SINFO("Starting worker thread for '" << data.name << "'");

    // Create the actual node
    SINFO("Starting SQLiteNode: " << data.args.serialize());
    SQLiteNode workerNode(data.args, threadId, threadCount, data.server);
    SINFO("Node created, ready for action.");

    while (true) {
        // Set the worker node's state/master status coming from the replication thread.
        // Only worker nodes will allow an external party to set these properties.

        workerNode.setState(data.replicationState.load());
        workerNode.setMasterVersion(data.masterVersion.load());

        // Block until work is available.
        fd_map fdm;
        data.queuedRequests.preSelect(fdm);
        data.escalatedCommands.preSelect(fdm);
        data.directMessages.preSelect(fdm);
        S_poll(fdm, STIME_US_PER_S);
        data.queuedRequests.postSelect(fdm);
        data.escalatedCommands.postSelect(fdm);
        data.directMessages.postSelect(fdm);

        // If we've been instructed to shutdown and there are no more requests waiting
        // to be processed, then exit the loop. Main thread will join us and continue
        // the shutdown process.
        if (data.gracefulShutdown.load() && data.queuedRequests.empty()) {
            break;
        }

        // Process any direct messages from the main thread to us
        BedrockServer_WorkerThread_ProcessDirectMessages(workerNode, data.directMessages);

        // Now try to get a request to work on.  If none available (either
        // select timed out or another thread 'stole' it, go to the top and
        // wait again.  So firt: are there any escalated commands that need
        // processing? If so, let's process those first.  This should always be
        // empty if we're not mastering.
        SQLiteNode::Command* command = data.escalatedCommands.pop();
        bool escalatedCommand = false;
        bool closeCommand = true;
        if (command) {
            // There was an escalated command, we'll use it's request ID as our log prefix.
            SAUTOPREFIX(command->request["requestID"]);
            escalatedCommand = true;
            command = workerNode.reopenCommand(command);
        } else {
            // Otherwise, let's see if we can get a new request.
            SData request = data.queuedRequests.pop();
            if (request.empty()) {
                // We didn't get anything here, there's no work to do. Go back to waiting.
                continue;
            }

            // This *needs* to be after the empty check, because otherwise we'll have added an empty 'requestID' to the
            // request, making it non-empty.
            SAUTOPREFIX(request["requestID"]);

            // If the command is scheduled for the future, we'll forward it to the sync thread, as only the sync thread
            // keeps a long-running queue instead of operating on one command at a time.
            // Also, if the command is a status command, we'll forward it to the sync thread, because status commands
            // are special and require access to information that only the sync thread knows.
            command = workerNode.createCommand(request);
            bool scheduledInFuture = command->creationTimestamp > STimeNow();
            bool isStatusCommand = find(BedrockPlugin_Status::statusCommandNames.begin(),
                                        BedrockPlugin_Status::statusCommandNames.end(),
                                        request.methodLine) != BedrockPlugin_Status::statusCommandNames.end();

            if(scheduledInFuture || isStatusCommand) {
                SINFO("Forwarding command " << command->id << " to sync thread.");
                command->response.clear(); // TODO: These should be clear on creation.
                data.peekedCommands.push(command);
                continue;
            } else {
                // Let this actually execute the peek.
                command = workerNode.reopenCommand(command);
            }
        }

        SDEBUG("Worker thread unblocked!");

        // If opening the command ended up with it sitting in the processed  command queue, then the whole command was
        // finished in `peek`, and we're done. We'll either process the response to send back to the original caller
        // (if we're a slave, or if this was a request that came in on the command port to master), or if it was an
        // ecalated command, we'll stick it back into the `processedCommands` queue so that the sync node can send the
        // response back to the slave that originated it.
        if (workerNode.getProcessedCommand()) {
            if (escalatedCommand) {
                // Send it back to the sync node.
                SINFO("Giving this back to sync thread: " << command->id << ":" << command->request.methodLine);
                data.peekedCommands.push(command);
                closeCommand = false;
            } else {
                // Prepare the final response.
                SINFO("Peek successful. Putting command '" << command->id << "' on processed list.");
                BedrockServer_PrepareResponse(command);
                SINFO("Worker thread responding to (read-only) command: " << command->id << ":" << command->request["debug"]);
                data.processedResponses.push(command->response);
            }
        } else if (workerNode.getQueuedCommand(command->priority)) {
            // If the command is queued, then it wasn't completed in `peek` when we opened the command. There are
            // several possible reasons for this, including that `peek` was never called in open command, or that the
            // command needs to write to the database.
            // In the general case, this just means we will re-queue the command in the sync thread's `escalated`
            // queue. However, there's a special case if we're the master server, and the command is set to ASYNC
            // consistency, and the command has already been peeked. In that case, we'll try and perform the write
            // from the worker thread.

            // dbReady() implies that we're master, and that the initial `upgradeDatabase` command that runs each
            // time we begin mastering has completed.
            bool canWriteInWorker = (_syncNode->dbReady() && command->writeConsistency == SQLC_ASYNC
                                     && !command->httpsRequest && command->peekCount != 0);

            // The standard case, we're not master, the DB isn't ready, or the command isn't ASYNC. Just escalate.
            if (!canWriteInWorker) {
                SINFO("Peek unsuccessful. Signaling replication thread to process command '" << command->id << ":" << (void*)command);

                // TODO: It'd be nice if we didn't have to clear this here before passing back. Maybe we could
                // encapsulate better? (See later invocation as well) Perhaps if `peek` returns `false`, we just do
                // it there?
                command->response.clear();
                data.peekedCommands.push(command);
                closeCommand = false;
            } else {

                // We may want to support this case in the future. For now, these should all have been escalated.
                SASSERT(!command->httpsRequest);

                // And here's our special case, where we can attempt to process a command from a worker thread. We'll
                // try this up to MAX_ASYNC_CONCURRENT_TRIES times, because it's possible to have conflicts doing
                // parallel commits.
                int tries = 0;
                while (++tries < MAX_ASYNC_CONCURRENT_TRIES) {
                    SINFO("Processing ASYNC command " << command->id << " from worker. (try #" << tries << ").");

                    // Try and process.
                    bool needsCommit = workerNode.processCommand(command);

                    // If there was an error processing this, the transaction's been rolled back, but we still need to
                    // send a response to the caller. Otherwise, we can commit now.
                    if (needsCommit) {
                        if (!workerNode.commit()) {
                            // If the commit failed, we just try again.
                            SINFO("ASYNC command " << command->id << " conflicted, retrying.");
                            command->response.clear();
                            continue;
                        } else {
                            // Hey, everything worked!
                            SINFO("ASYNC command " << command->id << " successfully processed.");
                        }
                    }

                    // At this point, we've either received a valid error from the command, (i.e., `401 Unauthorized`),
                    // or we've successfully processed and committed the entire transaction. Now we'll respond back to
                    // either the caller, or the sync thread, if this was escalated by a slave.
                    if (escalatedCommand) {
                        SINFO("Giving this back to sync thread: " << command->id << ":" << command->request.methodLine);
                        data.peekedCommands.push(command);
                        closeCommand = false;
                    } else {
                        SINFO("Preparing response to ASYNC command." << ":" << command->request.methodLine);
                        BedrockServer_PrepareResponse(command);
                        SINFO("Worker thread responding to command: " << command->id << ".");
                        data.processedResponses.push(command->response);
                    }

                    // Done, don't need to try again.
                    break;
                }

                // At this point, either we've already prepared a response, or `tries` has hit our max. If that's the
                // case, we need to give this command back to the sync thread to deal with.
                if (tries == MAX_ASYNC_CONCURRENT_TRIES) {
                    SINFO("Too many conflicts, escalating command." << command->id << ":" << command->request.methodLine);
                    command->response.clear();
                    data.peekedCommands.push(command);
                    closeCommand = false;
                }
            }
        } else {
            SERROR("[dmb] Lost command after worker peek. This should never happen");
        }

        if (closeCommand) {
            // Only close commands we haven't passed to a different node.
            workerNode.closeCommand(command);
        }
    }
#endif
}

BedrockServer::BedrockServer(const SData& args)
  : SQLiteServer(""), _args(args), _requestCount(0), _replicationState(SQLiteNode::SEARCHING),
    /*_replicationCommitCount(0),*/ _nodeGracefulShutdown(false), /*_masterVersion(),*/ _suppressCommandPort(false),
    _suppressCommandPortManualOverride(false) {

    _version = args.isSet("-versionOverride") ? args["-versionOverride"] : args["version"];

    // Output the list of plugins.
    map<string, BedrockPlugin*> registeredPluginMap;
    for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
        // Add one more plugin
        const string& pluginName = SToLower(plugin->getName());
        SINFO("Registering plugin '" << pluginName << "'");
        registeredPluginMap[pluginName] = plugin;
        plugin->enable(false); // Disable in case a previous run enabled it
    }

    // Enable the requested plugins
    list<string> pluginNameList = SParseList(args["-plugins"]);
    for (string& pluginName : pluginNameList) {
        // Enable the named plugin
        BedrockPlugin* plugin = registeredPluginMap[SToLower(pluginName)];
        if (!plugin) {
            SERROR("Cannot find plugin '" << pluginName << "', aborting.");
        }
        SINFO("Enabling plugin '" << pluginName << "'");
        plugin->enable(true);

        // Add the plugin's SHTTPSManagers to our list.
        // As this is a list of lists, push_back will push a *copy* of the list onto our local list, meaning that the
        // plugin's list must be complete and final when `initialize` finishes. There is no facility to add more
        // httpsManagers at a later time.
        httpsManagers.push_back(plugin->httpsManagers);
    }

    SINFO("Launching sync thread '" << syncThreadName << "'");
    syncThread = thread(sync,
                        ref(_args),
                        ref(_replicationState),
                        ref(_nodeGracefulShutdown),
                        ref(_syncNodeQueuedCommands),
                        ref(*this));
}

BedrockServer::~BedrockServer() {
    // Just warn if we have outstanding requests
    SASSERTWARN(_requestCountSocketMap.empty());
    //**NOTE: Threads were cleaned up when the threads were joined earlier.

    // Shut down any outstanding keepalive connections
    for (list<Socket*>::iterator socketIt = socketList.begin(); socketIt != socketList.end();) {
        // Shut it down and go to the next (because closeSocket will
        // invalidate this iterator otherwise)
        Socket* s = *socketIt++;
        closeSocket(s);
    }

    // Shut down the threads
    SINFO("Closing sync thread '" << syncThreadName << "'");
    syncThread.join();
    SINFO("Threads closed.");
}

bool BedrockServer::shutdownComplete() {
    // Shut down if requested and in the right state
    bool gs = _nodeGracefulShutdown.load();
    bool rs = (_replicationState.load() <= SQLiteNode::WAITING);
    bool qr = _commandQueue.empty();
    //bool qe = _escalatedCommands.empty();
    //bool pr = _processedResponses.empty();
    bool retVal = false;

    // If we're *trying* to shutdown, (_nodeGracefulShutdown is set), we'll log what's blocking shutdown,
    // or that nothing is.
    if (gs) {
        if (rs && qr/* && qe && pr*/) {
            retVal = true;
        } else {
            SINFO("Conditions that failed and are blocking shutdown: " <<
                  (rs ? "" : "_replicationState.get() <= SQLC_WAITING, ") <<
                  (qr ? "" : "_queuedRequests.empty(), ") <<
                  //(qe ? "" : "_escalatedCommands.empty(), ") <<
                  //(pr ? "" : "_processedResponses.empty(), ") <<
                  "returning FALSE in shutdownComplete");
        }
    }

    return retVal;
}

int BedrockServer::preSelect(fd_map& fdm) {
    // Do the base class
    STCPServer::preSelect(fdm);

    // The return value here is obsolete.
    return 0;
}

void BedrockServer::postSelect(fd_map& fdm, uint64_t& nextActivity) {
    // Let the base class do its thing
    STCPServer::postSelect(fdm);

    // Open the port the first time we enter a command-processing state
    SQLiteNode::State state = _replicationState.load();

    // If we're a slave, and the master's on a different version than us, we don't open the command port.
    // If we do, we'll escalate all of our commands to the master, which causes undue load on master during upgrades.
    // Instead, we'll simply not respond and let this request get re-directed to another slave.
    string masterVersion = _masterVersion.load();
    if (!_suppressCommandPort && state == SQLiteNode::SLAVING && (masterVersion != _version)) {
        SINFO("Node " << _args["-nodeName"] << " slaving on version " << _version
                      << ", master is version: " /*<< masterVersion <<*/ ", not opening command port.");
        suppressCommandPort(true);

        // If we become master, or if master's version resumes matching ours, open the command port again.
    } else if (_suppressCommandPort && (state == SQLiteNode::MASTERING || (masterVersion == _version))) {
        SINFO("Node " << _args["-nodeName"] << " disabling previously suppressed command port after version check.");
        suppressCommandPort(false);
    }

    if (!_suppressCommandPort && portList.empty() && (state == SQLiteNode::MASTERING || state == SQLiteNode::SLAVING) &&
        !_nodeGracefulShutdown.load()) {
        // Open the port
        SINFO("Ready to process commands, opening command port on '" << _args["-serverHost"] << "'");
        openPort(_args["-serverHost"]);

        // Open any plugin ports on enabled plugins
        for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
            if (plugin->enabled()) {
                string portHost = plugin->getPort();
                if (!portHost.empty()) {
                    // Open the port and associate it with the plugin
                    SINFO("Opening port '" << portHost << "' for plugin '" << plugin->getName() << "'");
                    Port* port = openPort(portHost);
                    _portPluginMap[port] = plugin;
                }
            }
        }
    }

    // **NOTE: We leave the port open between startup and shutdown, even if we enter a state where
    //         we can't process commands -- such as a non master/slave state.  The reason is we
    //         expect any state transitions between startup/shutdown to be due to temporary conditions
    //         that will resolve themselves automatically in a short time.  During this period we
    //         prefer to receive commands and queue them up, even if we can't process them immediately,
    //         on the assumption that we'll be able to process them before the browser times out.

    // Is the OS trying to communicate with us?
    uint64_t sigmask = SGetSignals();
    if (sigmask) {
        // We've received a signal -- what does it mean?
        if (SCatchSignal(SIGTTIN)) {
            // Suppress command port, but only if we haven't already cleared it
            if (!SCatchSignal(SIGTTOU)) {
                SHMMM("Suppressing command port due to SIGTTIN");
                suppressCommandPort(true, true);
                SClearSignals();
            }
        } else if (SCatchSignal(SIGTTOU)) {
            // Clear command port suppression
            SHMMM("Clearing command port supression due to SIGTTOU");
            suppressCommandPort(false, true);
            SClearSignals();
        } else if (SCatchSignal(SIGUSR2)) {
            // Begin logging queries to -queryLog
            if (_args.isSet("-queryLog")) {
                SHMMM("Logging queries to '" << _args["-queryLog"] << "'");
                SQueryLogOpen(_args["-queryLog"]);
            } else {
                SWARN("Can't begin logging queries because -queryLog isn't set, ignoring.");
            }
            SClearSignals();
        } else if (SCatchSignal(SIGQUIT)) {
            // Stop query logging
            SHMMM("Stopping query logging");
            SQueryLogClose();
            SClearSignals();
        } else {
            // For anything else, just shutdown -- but only if we're not already shutting down
            if (!_nodeGracefulShutdown.load()) {
                // Begin a graceful shutdown; close our port
                SINFO("Beginning graceful shutdown due to '"
                      << SGetSignalNames(sigmask) << "', closing command port on '" << _args["-serverHost"] << "'");
                _nodeGracefulShutdown.store(true);
                closePorts();
            }
        }
    }

    // Accept any new connections
    Socket* s = nullptr;
    Port* acceptPort = nullptr;
    while ((s = acceptSocket(acceptPort))) {
        // Accepted a new socket
        // **NOTE: SQLiteNode doesn't need to keep a new list; we'll just
        //         reuse the STCPManager::socketList

        // Look up the plugin that owns this port (if any)
        if (SContains(_portPluginMap, acceptPort)) {
            BedrockPlugin* plugin = _portPluginMap[acceptPort];
            // Allow the plugin to process this
            SINFO("Plugin '" << plugin->getName() << "' accepted a socket from '" << s->addr << "'");
            plugin->onPortAccept(s);

            // Remember that this socket is owned by this plugin
            SASSERT(!s->data);
            s->data = plugin;
        }
    }

    // Process any new activity from incoming sockets
    list<Socket*>::iterator socketIt = socketList.begin();
    while (socketIt != socketList.end()) {
        // Process this socket
        Socket* s = *socketIt++;
        if (s->state == STCP_CLOSED) {
            // The socket has died; close it.  The command will get cleaned up later.
            closeSocket(s);
            map<uint64_t, Socket*>::iterator nextIt = _requestCountSocketMap.begin();
            while (nextIt != _requestCountSocketMap.end()) {
                // Is this the socket that died?
                map<uint64_t, Socket*>::iterator mapIt = nextIt++;
                if (mapIt->second == s) {
                    // This socket has died while we're processing its request.
                    uint64_t requestCount = mapIt->first;
                    SHMMM("Abandoning request #" << requestCount << " to '" << s->addr << "'");
                    _requestCountSocketMap.erase(mapIt);

                    // Remove from the processed queue, if it's in there
                    /*if (_queuedRequests.cancel("requestCount", SToStr(requestCount))) {
                        SINFO("Cancelling abandoned request #"
                              << requestCount << " in queuedRequests; was never processed by read thread.");
                    } else if (_peekedCommands.cancel("requestCount", SToStr(requestCount))) {
                        SINFO("Cancelling abandoned request #"
                              << requestCount << " in queuedEscalatedRequests; was never processed by sync thread.");
                    } else if (_processedResponses.cancel("request.requestCount", SToStr(requestCount))) {
                        SWARN("Can't cancel abandoned request #"
                              << requestCount
                              << " in processedResponses; this *was* processed by the sync thread, but too late now.");
                    } else {
                    */
                        // Doesn't seem to be in any of the queues, meaning it's actively being processed by one of the
                        // threads.
                        // Send a cancel command to all threads.  This will *probably* work, but it's possible that the
                        // thread will
                        // finish processing this command before it gets to processing our cancel request.  But that's
                        // fine -- this
                        // doesn't need to be airtight.  There will always be scenarios where the server processes a
                        // command that
                        // the client has abandoned (eg, if the socket dies while sending the response), so the client
                        // already needs
                        // to handle this scenario.  We just want to minimize it wherever possible.
                        SHMMM("Attempting to cancel abandoned request #"
                              << requestCount << " being processed by some thread; it might slip through the cracks.");
                        SData cancelRequest("CANCEL_REQUEST");
                        cancelRequest["requestCount"] = SToStr(requestCount);
                        //for (auto& threadData : _workerThreadDataList) {
                            // Send it the cancel command
                            // threadData.directMessages.push(cancelRequest);
                        //}
                        // Send it the cancel command
                        // _syncThreadData.directMessages.push(cancelRequest);
                        // TODO: Cancel these locally. Current logs show 575 'abandoned request' where 573 of them are
                        // the 'it might slip through the cracks.' message, and 2 are 'this *was* processed by the sync
                        // thread'. Implement accordinly.
                    //}
                }
            }
        } else if (s->state == STCP_CONNECTED) {
            // Is this socket owned by a plugin?
            BedrockPlugin* plugin = (BedrockPlugin*)s->data;
            if (plugin) {
            #if 0
                // Let the plugin handle it
                SData request;
                bool keepAlive = plugin->onPortRecv(s, request);

                // Did it trigger an internal request?
                if (!request.empty()) {
                    // Queue the request, and note that it came from this plugin
                    // such that we can pass it back to it when done
                    SINFO("Plugin '" << plugin->getName() << "' queuing internal request '" << request.methodLine
                                     << "'");
                    uint64_t requestCount = ++_requestCount;
                    request["plugin"] = plugin->getName();
                    request["requestCount"] = SToStr(requestCount);
                    _queuedRequests.push(request);

                    // Are we keeping this socket alive for the response?
                    if (keepAlive) {
                        // Remember which socket on which to send the response
                        _requestCountSocketMap[requestCount] = s;
                    }
                }

                // Do we keep this connection alive or shut it down?
                if (!keepAlive) {
                    // Begin shutting down the socket
                    SINFO("Plugin '" << plugin->getName() << "' shutting down socket to '" << s->addr << "'");
                    shutdownSocket(s, SHUT_WR);
                }
            #endif
            } else {
                // Get any new requests
                int requestSize = 0;
                SData request;
                while ((requestSize = request.deserialize(s->recvBuffer))) {
                    SConsumeFront(s->recvBuffer, requestSize);

                    // Add requestCount and queue it.
                    uint64_t requestCount = ++_requestCount;
                    request["requestCount"] = to_string(requestCount);

                    // Either shut down the socket or store it so we can eventually sync out the response.
                    uint64_t creationTimestamp = request.calc64("commandExecuteTime");
                    if (SIEquals(request["Connection"], "forget") || creationTimestamp > STimeNow()) {
                        // Respond immediately to make it clear we successfully
                        // queued it, but don't add to the socket map as we don't
                        // care about the answer
                        SINFO("Firing and forgetting '" << request.methodLine << "'");
                        SData response("202 Successfully queued");
                        s->send(response.serialize());
                    } else {
                        // Queue for later response
                        SINFO("Waiting for '" << request.methodLine << "' to complete.");
                        _requestCountSocketMap[requestCount] = s;
                    }

                    // Create a command and queue it.
                    _commandQueue.push(BedrockCommand(request));
                }
            }
        }
    }

    // If any plugin timers are firing, let the plugins know.
    for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
        for (SStopwatch* timer : plugin->timers) {
            if (timer->ding()) {
                plugin->timerFired(timer);
            }
        }
    }
}

void BedrockServer::_reply(BedrockCommand& command)
{

    // The multi-threaded queues work on SDatas of either requests or
    // responses.  The response needs to know a few things about the original
    // request, like the requestCount, connect (so it knows whether to shut
    // down the socket), etc so copy all the request details into the response
    // so when the main thread is ready to write back to the original socket,
    // it has some insight.
    SData& request = command.request;
    SData& response = command.response;
    for (auto& row : request.nameValueMap) {
        response["request." + row.first] = row.second;
    }

    // Add a few others
    response["request.processingTime"] = SToStr(command.processingTime);
    response["request.creationTimestamp"] = SToStr(command.creationTimestamp);
    response["request.methodLine"] = request.methodLine;

    // Calculate how long it took to process this command
    uint64_t totalTime = STimeNow() - response.calc64("request.creationTimestamp");
    uint64_t processingTime = response.calc64("request.processingTime");
    uint64_t waitTime = totalTime - processingTime;

    // See if we still have a socket for this command (assuming we ever did)
    const int64_t requestCount = response.calc64("request.requestCount");
    map<uint64_t, Socket*>::iterator socketIt = _requestCountSocketMap.find(requestCount);
    Socket* s = (socketIt != _requestCountSocketMap.end() ? socketIt->second : 0);

    // **FIXME: Abandon requests are mistaken for being internal; somehow detect this and give plugins
    //          a chance to repair the problem.  Specifically, the Jobs plugin doesn't want to send a
    //          job to a dead socket -- that job will never get done.

    // Log some performance and diagnostic data
    const string& commandStatus = "'" + response["request.methodLine"] + "' "
                                                                         "#" +
                                  SToStr(requestCount) + " "
                                                         "(result '" +
                                  response.methodLine + "') "
                                                        "from '" +
                                  (s ? SToStr(s->addr) : "internal") + "' "
                                                                       "in " +
                                  SToStr(totalTime / STIME_US_PER_MS) + "=" + SToStr(waitTime / STIME_US_PER_MS) +
                                  "+" + SToStr(processingTime / STIME_US_PER_MS) + " ms";
    SINFO("Processed command " << commandStatus);

    // Put the timing data into the response
    // Only slaves need this info.
    /*
    response["totalTime"] = SToStr(totalTime / STIME_US_PER_MS);
    response["waitTime"] = SToStr(waitTime / STIME_US_PER_MS);
    response["processingTime"] = SToStr(processingTime / STIME_US_PER_MS);
    response["nodeName"] = _args["-nodeName"];
    response["commitCount"] = SToStr(_replicationCommitCount.load());
    */
    // Warn on slow commands.
    if (processingTime > 2000 * STIME_US_PER_MS)
        SWARN("Slow command (bedrock blocking) " << commandStatus);

    // Warn on high latency commands.
    // Let's not include ones that needed to send out other requests, or that specifically told us they're slow.
    if (totalTime > 4000 * STIME_US_PER_MS
        && !SIEquals(response["request.Connection"], "wait")
        && !SIEquals(response["latency"],            "high"))
    {
        SWARN("Slow command (high latency) " << commandStatus);
    }

    // Was this command queued by plugin?
    BedrockPlugin* plugin = BedrockPlugin::getPlugin(response["request.plugin"]);
    if (plugin) {
        if (s) {
            // Let the plugin handle it
            SINFO("Plugin '" << plugin->getName() << "' handling response '" << response.methodLine << "' to request '"
                             << response["request.methodLine"] << "'");
            if (!plugin->onPortRequestComplete(response, s)) {
                // Begin shutting down the socket
                SINFO("Plugin '" << plugin->getName() << "' shutting down connection to '" << s->addr << "'");
                shutdownSocket(s, SHUT_RD);
            }
        } else {
            SWARN("Cannot deliver response from plugin" << plugin->getName() << "' for request '"
                                                        << response["request.methodLine"] << "' #" << requestCount);
        }
    } else {
        // No plugin, use default behavior.  If we have a socket, deliver the response
        if (s) {
            // Deliver the response and close the connection if requested.
            // Also scrub the request.* headers in the response. Those were put
            // there so when dealing with the Response SData we has some insight
            // into the original request.
            // **FIXME: This is a bit of a hack; find another way
            bool closeSocket = SIEquals(response["request.Connection"], "close");
            for (map<string, string>::iterator it = response.nameValueMap.begin();
                 it != response.nameValueMap.end();
                 /* no inc. handled in loop body*/)
                if (SStartsWith(it->first, "request."))
                    response.nameValueMap.erase(it++); // Notice post inc.
                else
                    ++it;
            s->send(response.serialize());
            if (closeSocket)
                shutdownSocket(s, SHUT_RD);
        } else {
            // We have no socket.  This is fine if it's "Connection: forget",
            // otherwise it could be a problem -- even a premature
            // disconnect should clean it up before it gets here.
            if (!SIEquals(response["request.Connection"], "forget"))
                SWARN("Cannot deliver response for request '" << response["request.methodLine"] << "' #"
                                                              << requestCount);
        }
    }

    // If there is a socket, it's no longer associated with this request
    if (socketIt != _requestCountSocketMap.end()) {
        _requestCountSocketMap.erase(socketIt);
    }
}

void BedrockServer::suppressCommandPort(bool suppress, bool manualOverride) {
    // If we've set the manual override flag, then we'll only actually make this change if we've specified it again.
    if (_suppressCommandPortManualOverride && !manualOverride) {
        return;
    }

    // Save the state of manual override. Note that it's set to *suppress* on purpose.
    if (manualOverride) {
        _suppressCommandPortManualOverride = suppress;
    }
    // Process accordingly
    _suppressCommandPort = suppress;
    if (suppress) {
        // Close the command port, and all plugin's ports.
        // won't reopen.
        SHMMM("Suppressing command port");
        if (!portList.empty())
            closePorts();
    } else {
        // Clearing past suppression, but don't reopen.  (It's always safe
        // to close, but not always safe to open.)
        SHMMM("Clearing command port suppression");
    }
}

const string& BedrockServer::getVersion() { return _version; }

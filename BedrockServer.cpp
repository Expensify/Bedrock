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
                         atomic<string>& masterVersion,
                         CommandQueue& syncNodeQueuedCommands,
                         BedrockServer& server)
{
    // Initialize the thread.
    SInitialize(_syncThreadName);

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
                        server._version, args.calc("-quorumCheckpoint"));

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
                                      ref(masterVersion),
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
                    SASSERT(command.initiatingPeerID);
                    SASSERT(!command.initiatingClientID);
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
            masterVersion.store(syncNode.getMasterVersion());
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
                if (command.initiatingPeerID) {
                    syncNode.queueResponse(move(command));
                } else {
                    server._reply(command);
                }
            }
        }
    }

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
                           atomic<string>& masterVersion,
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
                SASSERT(!command.initiatingPeerID);
                SASSERT(command.initiatingClientID);
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
                    if (command.initiatingPeerID) {
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

    SINFO("Launching sync thread '" << _syncThreadName << "'");
    _syncThread = thread(sync,
                         ref(_args),
                         ref(_replicationState),
                         ref(_nodeGracefulShutdown),
                         ref(_masterVersion),
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
    SINFO("Closing sync thread '" << _syncThreadName << "'");
    _syncThread.join();
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
        // Currently disabled, and will probably be removed.
        #if 0
        if (SContains(_portPluginMap, acceptPort)) {
            BedrockPlugin* plugin = _portPluginMap[acceptPort];
            // Allow the plugin to process this
            SINFO("Plugin '" << plugin->getName() << "' accepted a socket from '" << s->addr << "'");
            plugin->onPortAccept(s);

            // Remember that this socket is owned by this plugin
            SASSERT(!s->data);
            s->data = plugin;
        }
        #endif
    }

    // Process any new activity from incoming sockets
    for (auto s : socketList) {
        switch (s->state) {
            case STCPManager::Socket::CLOSED:
            {
                _socketIDMap.erase(s->id);
                closeSocket(s);
                // TODO: Cancel any outstanding commands initiated by this socket. This isn't critical, and is an
                // optimization. Otherwise, they'll continue to get processed to completion, and will just never be
                // able to have their responses returned.
            }
            break;
            case STCPManager::Socket::CONNECTED:
            {
                // If nothing's been received, break early.
                if (s->recvBuffer.empty()) {
                    break;
                } else {
                    // Otherwise, we'll see if there's any activity on this socket. Currently, we don't handle clients
                    // pipelining requests well. We process commands in no particular order, so we can't dequeue two
                    // requests off the same socket at one time, or we don't guarantee their return order.
                    auto socketIt = _socketIDMap.find(s->id);
                    if (socketIt != _socketIDMap.end()) {
                        SWARN("Can't dequeue a request while one is pending, or they could end up out-of-order.");
                        break;
                    }
                }

                // If there's a request, we'll dequeue it (but only the first one).
                SData request;
                int requestSize = request.deserialize(s->recvBuffer);
                if (requestSize) {
                    SConsumeFront(s->recvBuffer, requestSize);

                    // Either shut down the socket or store it so we can eventually sync out the response.
                    uint64_t creationTimestamp = request.calc64("commandExecuteTime");
                    if (SIEquals(request["Connection"], "forget") || creationTimestamp > STimeNow()) {
                        // Respond immediately to make it clear we successfully queued it, but don't add to the socket
                        // map as we don't care about the answer.
                        SINFO("Firing and forgetting '" << request.methodLine << "'");
                        SData response("202 Successfully queued");
                        s->send(response.serialize());
                    } else {
                        // Queue for later response
                        SINFO("Waiting for '" << request.methodLine << "' to complete.");
                        _socketIDMap[s->id] = s;
                    }

                    // Create a command and queue it.
                    BedrockCommand command(request);
                    command.initiatingClientID = s->id;
                    _commandQueue.push(move(command));
                }
            }
            break;
            default:
            {
                SWARN("Socket in unhandled state: " << s->state);
            }
            break;
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
    // TODO: This needs to be synchronized if multiple worker threads can call it.

    // Do we have a socket for this command?
    auto socketIt = _socketIDMap.find(command.initiatingClientID);
    if (socketIt != _socketIDMap.end()) {
        socketIt->second->send(command.response.serialize());
        if (SIEquals(command.request["Connection"], "close")) {
            shutdownSocket(socketIt->second, SHUT_RD);
        }

        // We only keep track of sockets with pending commands.
        _socketIDMap.erase(socketIt->second->id);
    }
    else if (!SIEquals(command.request["Connection"], "forget")) {
        SWARN("No socket to reply for: '" << command.request.methodLine << "' #" << command.initiatingClientID);
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

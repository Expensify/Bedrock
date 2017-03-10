// Manages connections to a single instance of the bedrock server.
#include <libstuff/libstuff.h>
#include "BedrockServer.h"
#include "BedrockPlugin.h"
#include "BedrockCore.h"

void BedrockServer::acceptCommand(SQLiteCommand&& command) {
    _commandQueue.push(BedrockCommand(move(command)));
}

void BedrockServer::cancelCommand(const string& commandID) {
    _commandQueue.removeByID(commandID);
}

void BedrockServer::sync(SData& args,
                         atomic<SQLiteNode::State>& replicationState,
                         atomic<bool>& upgradeInProgress,
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

    // We expose the sync node to the server, because it needs it to respond to certain (Status) requests.
    server._syncNode = &syncNode;

    // The node is now coming up, and should eventually end up in a `MASTERING` or `SLAVING` state. We can start adding
    // our worker threads now. We don't wait until the node is `MASTERING` or `SLAVING`, as it's state can change while
    // it's running, and our workers will have to maintain awareness of that state anyway.
    SINFO("Starting " << workerThreads << " worker threads.");
    list<thread> workerThreadList;
    for (int threadId = 0; threadId < workerThreads; threadId++) {
        workerThreadList.emplace_back(worker,
                                      ref(args),
                                      ref(replicationState),
                                      ref(upgradeInProgress),
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
    bool committingCommand = false;
    while (!syncNode.shutdownComplete()) {

        // If we've been instructed to shutdown and we haven't yet, do it.
        if (nodeGracefulShutdown.load()) {
            syncNode.beginShutdown();
        }

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

        // Add our command queue to our fd_map.
        syncNodeQueuedCommands.preSelect(fdm);

        // Wait for activity on any of those FDs, up to a timeout
        const uint64_t now = STimeNow();
        // If we've 
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

        // Ok, let the sync node to it's updating for as many iterations as it requires. We'll update the replication
        // state when it's finished.
        SQLiteNode::State preUpdateState = syncNode.getState();
        while (syncNode.update()) {}
        replicationState.store(syncNode.getState());
        masterVersion.store(syncNode.getMasterVersion());

        // If the node's not in a ready state at this point, we'll probably need to read from the network, so start the
        // main loop over. This can let us wait for logins from peers (for example).
        if (!syncNode.ready()) {
            // TODO: This causes us to stop processing any commands that were in progress as we switched from MASTERING
            // to STANDINGDOWN. We probably want to recognize that case as well.
            continue;
        }

        // If we've just switched to the mastering state, we want to upgrade the DB. We'll set a global flag to let
        // worker threads know that a DB upgrade is in progress, and start the upgrade process, which works basically
        // like a regular distributed commit.
        if (preUpdateState != SQLiteNode::MASTERING && replicationState.load() == SQLiteNode::MASTERING) {
            // TODO: Upgrade the DB.
            if (server._upgradeDB(db)) {
                upgradeInProgress.store(true);
                committingCommand = true;
                syncNode.startCommit(SQLiteNode::QUORUM);

                // As it's a quorum commit, we'll need to read from peers. Let's start the next loop iteration.
                continue;
            }
        }

        // If we started a commit, and one's not in progress, then we've finished it and we'll take that command and
        // stick it back in the appropriate queue.
        if (committingCommand && !syncNode.commitInProgress()) {
            // It should be impossible to get here if we're not mastering.
            SASSERT(replicationState.load() == SQLiteNode::MASTERING);
            if (syncNode.commitSucceeded()) {
                // If we were upgrading, there's no response to send, we're just done.
                if (upgradeInProgress.load()) {
                    committingCommand = false;
                    upgradeInProgress.store(false);
                    continue;
                }
                // Otherwise, mark this command as complete and return.
                command.complete = true;
                if (command.initiatingPeerID) {
                    // This is a command that came from a peer. Have the server send the response back to the peer.
                    syncNode.sendResponse(command);
                } else {
                    // The only other option is this came from a client, so respond via the server.
                    server._reply(command);
                }
            } else {
                // If the commit failed, then it must have conflicted, so we'll requeue it to try again.
                syncNodeQueuedCommands.push(move(command));
            }
            
            // Not committing any more.
            committingCommand = false;
        }

        // We're either mastering, or slaving. There could be a commit in progress on `command`, but there could also
        // be other finished work to handle while we wait for that to complete. Let's see if we can handle any of that
        // work.
        try {
            // Continually look at the front of the queue, and as long as that command is complete, send a response
            // and remove it from the queue. If we find an incomplete command, we'll move on to processing it. If we
            // find no command, `front()` will throw `out_of_range` and we'll start the main loop over again, calling
            // poll().
            // TODO: We should handle completed commands at other places in the queue, too (besides at the front), or we
            // could have a separate queue for completed commands.
            // TODO: We should skip commands with unfinished HTTPS requests.
            while (true) {
                const BedrockCommand& localCommand = syncNodeQueuedCommands.front();
                if (localCommand.complete) {
                    // Make sure this came from a peer rather than a client, if it came from a client it shouldn't be in
                    // this queue completed.
                    SASSERT(localCommand.initiatingPeerID);
                    SASSERT(!localCommand.initiatingClientID);

                    // Now we can pull it off the queue and respond to it.
                    syncNode.sendResponse(syncNodeQueuedCommands.pop());
                } else {
                    // This command isn't complete, so we can't just send a response and be done with it. We'll break
                    // here and move on to the processing stage.
                    break;
                }
            }

            // The next command in the queue is incomplete, so we'll need to process it (if there were no next
            // command, then the above block would have thrown out_of-range), but we don't start processing a new
            // command until we've completed any existing ones.
            if (committingCommand) {
                continue;
            }

            // Now we can pull the next one off the queue and start on it.
            command = syncNodeQueuedCommands.pop();

            // We got a command to work on! Set our log prefix to the request ID.
            // TODO: This is totally wrong, but here as a placeholder.
            // SAUTOPREFIX(command.request["prefix"]);
            SAUTOPREFIX(args["-nodeName"]);

            // And now we'll decide how to handle it.
            if (replicationState.load() == SQLiteNode::MASTERING) {
                // If we're getting it on the sync thread, that means it's already been `peeked` unsuccessfully, and it
                // needed to be processed. If it were `peeked` successfully, then the worker thread wouldn't have given
                // it back to us.
                if (core.processCommand(command)) {
                    // The processor says we need to commit this, so let's start that process.
                    committingCommand = true;
                    syncNode.startCommit(command.writeConsistency);

                    // And we'll start the next main loop.
                    // NOTE: This will cause us to read from the network again. This, in theory, is fine, but we saw
                    // performance problems in the past trying to do something similar on every commit. This may be
                    // alleviated now that we're only doing this on *sync* commits instead of all commits, which should
                    // be a much smaller fraction of all our traffic. We set nextActivity here so that there's no
                    // timeout before we'll give up on poll() if there's nothing to read.
                    nextActivity = STimeNow();
                    continue;
                } else {
                    // Otherwise, the command doesn't need a commit (maybe it was an error, or it didn't have any work
                    // to do. We'll just respond.
                    if (command.initiatingPeerID) {
                        syncNode.sendResponse(command);
                    } else {
                        server._reply(command);
                    }
                }
            } else if (replicationState.load() == SQLiteNode::SLAVING) {
                // If we're slaving, we just escalate directly to master without peeking. We can only get an incomplete
                // command on the slave sync thread if a slave worker thread peeked it unsuccessfully, so we don't
                // bother peeking it again.
                syncNode.escalateCommand(move(command));
            }
        } catch (out_of_range e) {
            // syncNodeQueuedCommands had no commands to work on, we'll need to re-poll for some.
            continue;
        }
    }

    // We just fell out of the loop where we were waiting for shutdown to complete. Update the state one last time when
    // the writing replication thread exits.
    replicationState.store(syncNode.getState());
    if (replicationState.load() > SQLiteNode::WAITING) {
        // This is because the graceful shutdown timer fired and syncNode.shutdownComplete() returned `true` above, but
        // the server still thinks it's in some other state. We can only exit if we're in state <= SQLC_SEARCHING,
        // (per BedrockServer::shutdownComplete()), so we force that state here to allow the shutdown to proceed.
        SWARN("Sync thread exiting in state " << replicationState.load() << ". Setting to SQLC_SEARCHING.");
        replicationState.store(SQLiteNode::SEARCHING);
    } else {
        SINFO("Sync thread exiting, setting state to: " << replicationState.load());
    }

    // Wait for the worker threads to finish.
    int threadId = 0;
    for (auto& workerThread : workerThreadList) {
        SINFO("Joining worker thread '" << "worker" << threadId << "'");
        threadId++;
        workerThread.join();
    }
}

void BedrockServer::worker(SData& args,
                           atomic<SQLiteNode::State>& replicationState,
                           atomic<bool>& upgradeInProgress,
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

    // We just run this loop looking for commands to process forever. There's a check for appropriate exit conditions
    // at the bottom, which will cause our loop and thus this thread to exit when that becomes true.
    while (true) {
        try {
            // If we can't find any work to do, this will throw.
            command = server._commandQueue.get(1000000);

            // TODO: Change to the prefix of the request.
            SAUTOPREFIX(args["-nodeName"]);

            while (upgradeInProgress.load()) {
                // TODO: Make this less shitty.
                // Also, there's a race condition here if we start an upgrade after this point. What happens then? It
                // means we've switched from SLAVING to MASTERING in the middle of handling a command in a worker. The
                // worker can probably try and continue handling the command as if it were a slave.
                usleep(10000);
            }

            // We'll use the state right now for the duration of this loop. If we're promoted to master, mid loop, this
            // should be fine, we'll either complete a `peek` and respond to a client, or we'll end up escalating the
            // command to the sync node which will start with it in the MASTERING state. If we move from MASTERING to
            // STANDINGDOWN, we'll finish up processing the command as if we were master, which is the intention of the
            // STANDINGDOWN state.
            // TODO: But if we change states again from STANDINGDOWN to SLAVING (or anything else), we'll probably be
            // in an ambiguous state. We may need to let SQLiteNode ask the server if it's done standing down so that
            // we can communicate if there are any commands being handled, to prevent stand-down completing until
            // they've finished.
            SQLiteNode::State state = replicationState.load();

            // If this command is already complete, then we should be a slave, and the sync node got a response back
            // from a command that had been escalated to master, and queued it for a worker to respond to. We'll send
            // that response now.
            if (command.complete) {
                // If this command is already complete, we can return it to the caller.
                // If it has an initiator, it should be returned to a peer by a sync node instead.
                SASSERT(!command.initiatingPeerID);
                SASSERT(command.initiatingClientID);
                SASSERT(state == SQLiteNode::SLAVING);
                server._reply(command);

                // This command is done, move on to the next one.
                continue;
            }

            // We'll retry on conflict up to this many times.
            int retry = 3;
            while (retry) {
                // Try peeking the command. If this succeeds, then it's finished, and all we need to do is respond to
                // the command at the bottom.
                if (!core.peekCommand(command)) {
                    // Peek wasn't enough to handle this command. Now we need to decide if we should try and process
                    // it, or if we should send it off to the sync node.
                    if (state == SQLiteNode::SLAVING ||
                        command.httpsRequest         ||
                        command.writeConsistency != SQLiteNode::ASYNC)
                    {
                        syncNodeQueuedCommands.push(move(command));

                        // We'll break out of our retry loop here, as we don't need to do anything else, we can just
                        // look for another command to work on.
                        break;
                    }  else {
                        // In this case, there's nthing blocking us from processing this in a worker, so let's try it.
                        if (core.processCommand(command)) {
                            // If processCommand returned true, then we need to do a commit. Otherwise, the command is
                            // done, and we just need to respond.
                            if (core.commitCommand(command)) {
                                // If the commit succeeded, we'll mark the command as complete, and there's nothing
                                // else to do!
                                command.complete = true;
                            }
                        }
                    }
                }

                // If the command was completed above, then we'll go ahead and respond. Otherwise there must have been
                // a conflict, and we'll retry.
                if (command.complete) {
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
                --retry;
            }

            // We ran out of retries without finishing! We give it to the sync thread.
            if (!retry) {
                SWARN("Max retries hit in worker, forwarding command to sync node.");
                syncNodeQueuedCommands.push(move(command));
            }
        } catch(...) {
            // No commands to process after 1 second.
        }

        // Ok, we're done with this loop, see if we should exit.
        if (nodeGracefulShutdown.load() && server._commandQueue.empty()) {
            SINFO("Shutdown flag set and nothing left in queue. worker" << to_string(threadId) << " exiting.");
            break;
        }
    }
}

BedrockServer::BedrockServer(const SData& args)
  : SQLiteServer(""), _args(args), _requestCount(0), _replicationState(SQLiteNode::SEARCHING),
    _upgradeInProgress(false), _nodeGracefulShutdown(false), _suppressCommandPort(false),
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
                         ref(_upgradeInProgress),
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

    // Process any new activity from incoming sockets. In order to not modify the socket list while we're iterating
    // over it, we'll keep a list of sockets that need closing.
    list<STCPManager::Socket*> socketsToClose;
    for (auto s : socketList) {
        switch (s->state) {
            case STCPManager::Socket::CLOSED:
            {
                SAUTOLOCK(_socketIDMutex);
                _socketIDMap.erase(s->id);
                socketsToClose.push_back(s);
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
                    SAUTOLOCK(_socketIDMutex);
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
                    if (SIEquals(request["Connection"], "forget") ||
                        (uint64_t)request.calc64("commandExecuteTime") > STimeNow()) {
                        // Respond immediately to make it clear we successfully queued it, but don't add to the socket
                        // map as we don't care about the answer.
                        SINFO("Firing and forgetting '" << request.methodLine << "'");
                        SData response("202 Successfully queued");
                        s->send(response.serialize());
                    } else {
                        // Queue for later response
                        SINFO("Waiting for '" << request.methodLine << "' to complete.");
                        SAUTOLOCK(_socketIDMutex);
                        _socketIDMap[s->id] = s;
                    }

                    // Create a command.
                    BedrockCommand command(request);

                    // This is important! All commands passed through the entire cluster must have unique IDs, or they
                    // won't get routed properly from slave to master and back.
                    command.id = _args["-nodeName"] + "#" + to_string(_requestCount++);

                    // And we and keep track of the client that initiated this command, so we can respond later.
                    command.initiatingClientID = s->id;

                    // Status requests are handled specially.
                    if (_isStatusCommand(command)) {
                        _status(command);
                        _reply(command);
                    } else {
                        // Otherwise we queue it for later processing.
                        _commandQueue.push(move(command));
                    }
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

    // Now we can close any sockets that we need to.
    for (auto s: socketsToClose) {
        closeSocket(s);
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

void BedrockServer::_reply(BedrockCommand& command) {
    SAUTOLOCK(_socketIDMutex);

    // Do we have a socket for this command?
    auto socketIt = _socketIDMap.find(command.initiatingClientID);
    if (socketIt != _socketIDMap.end()) {
        socketIt->second->send(command.response.serialize());
        if (SIEquals(command.request["Connection"], "close")) {
            shutdownSocket(socketIt->second, SHUT_RD);
        }

        // We only keep track of sockets with pending commands.
        _socketIDMap.erase(socketIt);
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

bool BedrockServer::_isStatusCommand(BedrockCommand& command) {
    if (command.request.methodLine == STATUS_IS_SLAVE          ||
        command.request.methodLine == STATUS_HANDLING_COMMANDS ||
        command.request.methodLine == STATUS_PING              ||
        command.request.methodLine == STATUS_STATUS) {
        return true;
    }
    return false;
}

void BedrockServer::_status(BedrockCommand& command) {
    SData& request  = command.request;
    SData& response = command.response;

    // We'll return whether or not this server is slaving.
    if (request.methodLine == STATUS_IS_SLAVE) {
        // Used for liveness check for HAProxy. It's limited to HTTP style requests for it's liveness checks, so let's
        // pretend to be an HTTP server for this purpose. This allows us to load balance incoming requests.
        //
        // HAProxy interprets 2xx/3xx level responses as alive, 4xx/5xx level responses as dead.
        SQLiteNode::State state = _replicationState.load();
        if (state == SQLiteNode::SLAVING) {
            response.methodLine = "HTTP/1.1 200 Slaving";
        } else {
            response.methodLine = "HTTP/1.1 500 Not slaving. State="
                                  + SQLiteNode::stateNames[state];
        }
    }

    // TODO: The following is incomplete at best, and should check, if nothing else, whether the command port is open.
    else if (request.methodLine == STATUS_HANDLING_COMMANDS) {
        // This is similar to the above check, and is used for letting HAProxy load-balance commands.
        SQLiteNode::State state = _replicationState.load();
        if (state != SQLiteNode::SLAVING) {
            response.methodLine = "HTTP/1.1 500 Not slaving. State=" + SQLiteNode::stateNames[state];
        } else if (_version != _masterVersion.load()) {
            response.methodLine = "HTTP/1.1 500 Mismatched version. Version=" + _version;
        } else {
            response.methodLine = "HTTP/1.1 200 Slaving";
        }
    }

    // All a ping message requires is some response.
    else if (request.methodLine == STATUS_PING) {
        response.methodLine = "200 OK";
    }

    // This collects the current state of the server, which also includes some state from the underlying SQLiteNode.
    else if (request.methodLine == STATUS_STATUS) {
        STable content;
        SQLiteNode::State state = _replicationState.load();
        list<string> plugins;
        for (auto plugin : *BedrockPlugin::g_registeredPluginList) {
            STable pluginData;
            pluginData["name"] = plugin->getName();

            // TODO: This is another thing that isn't synchronized, but should be, we have no idea when plugins might
            // want to update this (in practice, currently, it's never).
            STable pluginInfo  = plugin->getInfo();
            for (auto row : pluginInfo) {
                pluginData[row.first] = row.second;
            }
            plugins.push_back(SComposeJSONObject(pluginData));
        }
        content["isMaster"] = state == SQLiteNode::MASTERING ? "true" : "false";
        content["plugins"]  = SComposeJSONArray(plugins);
        content["state"]    = SQLiteNode::stateNames[state];
        content["version"]  = _version;

        // Retrieve information about our peers.
        list<STable> peerData;
        // TODO: This is broken because there's nothing guaranteeing that the sync thread isn't modifying these values
        // as we read them. We could add a 'status' lock before calling SQLiteNode::update(), and also lock that here.
        for (SQLiteNode::Peer* peer : _syncNode->peerList) {
            peerData.emplace_back(peer->nameValueMap);
            peerData.back()["host"] = peer->host;
        }

        // Coalesce all of this into one value to return.
        list<string> peerList;
        for (const STable& peerTable : peerData) {
            peerList.push_back(SComposeJSONObject(peerTable));
        }
        content["peerList"]          = SComposeJSONArray(peerList);
        content["queuedCommandList"] = SComposeJSONArray(_commandQueue.getRequestMethodLines());
        /*
        TODO: Re-expose these, if we even care.
        content["priority"]    = SToStr(node->getPriority());
        content["hash"]        = node->getHash();
        content["commitCount"] = SToStr(node->getCommitCount());
        content["queuedCommandList"]    = SComposeJSONArray(node->getQueuedCommandList());
        content["escalatedCommandList"] = SComposeJSONArray(node->getEscalatedCommandList());
        content["processedCommandList"] = SComposeJSONArray(node->getProcessedCommandList());
        */

        // Done, compose the response.
        response.methodLine = "200 OK";
        response.content = SComposeJSONObject(content);
    }
}

bool BedrockServer::_upgradeDB(SQLite& db) {
    // TODO: Implement.
    return !db.getUncommittedQuery().empty();
}

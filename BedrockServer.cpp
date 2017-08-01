// Manages connections to a single instance of the bedrock server.
#include <libstuff/libstuff.h>
#include "BedrockServer.h"
#include "BedrockPlugin.h"
#include "BedrockConflictMetrics.h"
#include "BedrockCore.h"

set<string>BedrockServer::_parallelCommands;
recursive_mutex BedrockServer::_parallelCommandMutex;

void BedrockServer::acceptCommand(SQLiteCommand&& command) {
    _commandQueue.push(BedrockCommand(move(command)));
}

void BedrockServer::cancelCommand(const string& commandID) {
    _commandQueue.removeByID(commandID);
}

bool BedrockServer::canStandDown() {
    return _writableCommandsInProgress.load() == 0;
}

void BedrockServer::sync(SData& args,
                         atomic<SQLiteNode::State>& replicationState,
                         atomic<bool>& upgradeInProgress,
                         atomic<string>& masterVersion,
                         CommandQueue& syncNodeQueuedCommands,
                         BedrockServer& server)
{
    // Initialize the thread.
    SInitialize(_syncThreadName);

    // We currently have no writable commands in progress.
    server._writableCommandsInProgress.store(0);

    // Parse out the number of worker threads we'll use. The DB needs to know this because it will expect a
    // corresponding number of journal tables. "-readThreads" exists only for backwards compatibility.
    int workerThreads = args.calc("-workerThreads");

    // TODO: remove when nothing uses readThreads.
    workerThreads = workerThreads ? workerThreads : args.calc("-readThreads");

    // If still no value, use the number of cores on the machine, if available.
    workerThreads = workerThreads ? workerThreads : max(1u, thread::hardware_concurrency());

    // Initialize the DB.
    SQLite db(args["-db"], args.calc("-cacheSize"), 1024, args.calc("-maxJournalSize"), -1, workerThreads - 1);

    // And the command processor.
    BedrockCore core(db, server);

    // And the sync node.
    uint64_t firstTimeout = STIME_US_PER_M * 2 + SRandom::rand64() % STIME_US_PER_S * 30;
    SQLiteNode syncNode(server, db, args["-nodeName"], args["-nodeHost"], args["-peerList"], args.calc("-priority"),
                        firstTimeout, server._version, args.calc("-quorumCheckpoint"));

    // We expose the sync node to the server, because it needs it to respond to certain (Status) requests with data
    // about the sync node.
    server._syncNode = &syncNode;

    // We keep a queue of completed commands that workers will insert into when they've successfully finished a command
    // that just needs to be returned to a peer.
    CommandQueue completedCommands;

    // And we keep a list of commands with outstanding HTTPS requests. This is not synchronized because it's only used
    // internally in this thread. We temporarily move commands here while we wait for their HTTPS requests to complete,
    // so that we don't clog up the regular command queue with commands that are waiting.
    list<BedrockCommand> httpsCommands;

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
                                      ref(masterVersion),
                                      ref(syncNodeQueuedCommands),
                                      ref(completedCommands),
                                      ref(server),
                                      threadId,
                                      workerThreads);
    }

    // Now we jump into our main command processing loop.
    uint64_t nextActivity = STimeNow();
    BedrockCommand command;
    bool committingCommand = false;

    // We hold a lock here around all operations on `syncNode`, because `SQLiteNode` isn't thread-safe, but we need
    // `BedrockServer` to be able to introspect it in `Status` requests. We hold this lock at all times until exiting
    // our main loop, aside from when we're waiting on `poll`. Strictly, we could hold this lock less often, but there
    // are not that many status commands coming in, and they can wait for a fraction of a second, which lets us keep
    // the logic of this loop simpler.
    server._syncMutex.lock();
    while (!syncNode.shutdownComplete()) {
        // If there were commands waiting on our commit count to come up-to-date, we'll move them back to the main
        // command queue here. There's no place in particular that's best to do this, so we do it at the top of this
        // main loop, as that prevents it from ever getting skipped in the event that we `continue` early from a loop
        // iteration.
        {
            SAUTOLOCK(server._futureCommitCommandMutex);
            if (!server._futureCommitCommands.empty()) {
                uint64_t commitCount = db.getCommitCount();
                auto it = server._futureCommitCommands.begin();
                auto& eraseTo = it;
                while (it != server._futureCommitCommands.end() && it->first <= commitCount) {
                    SINFO("Returning command (" << it->second.request.methodLine << ") waiting on commit " << it->first
                          << " to queue, now have commit " << commitCount);
                    server._commandQueue.push(move(it->second));
                    eraseTo = it;
                    it++;
                }
                if (eraseTo != server._futureCommitCommands.begin()) {
                    server._futureCommitCommands.erase(server._futureCommitCommands.begin(), eraseTo);
                }
            }
        }

        // If we're in a state where we can initialize shutdown, then go ahead and do so.
        if (server._shutdownState.load() == QUEUE_PROCESSED && syncNodeQueuedCommands.empty()) {
            SINFO("Beginning sync node shutdown.");
            syncNode.beginShutdown();
        }

        // The fd_map contains a list of all file descriptors (eg, sockets, Unix pipes) that poll will wait on for
        // activity. Once any of them has activity (or the timeout ends), poll will return.
        fd_map fdm;

        // Prepare our plugins for `poll` (for instance, in case they're making HTTP requests).
        server._prePollPlugins(fdm);

        // Pre-process any sockets the sync node is managing (i.e., communication with peer nodes).
        syncNode.prePoll(fdm);

        // Add our command queues to our fd_map.
        syncNodeQueuedCommands.prePoll(fdm);
        completedCommands.prePoll(fdm);

        // Wait for activity on any of those FDs, up to a timeout.
        const uint64_t now = STimeNow();

        // Unlock our mutex, poll, and re-lock when finished.
        server._syncMutex.unlock();
        S_poll(fdm, max(nextActivity, now) - now);
        server._syncMutex.lock();

        // And set our next timeout for 1 second from now.
        nextActivity = STimeNow() + STIME_US_PER_S;

        // Process any activity in our plugins.
        server._postPollPlugins(fdm, nextActivity);

        // Process any network traffic that happened.
        syncNode.postPoll(fdm, nextActivity);
        syncNodeQueuedCommands.postPoll(fdm);
        completedCommands.postPoll(fdm);

        // If any of our plugins finished any outstanding HTTPS requests, we'll move those commands back into the
        // regular queue. This code modifies a list while iterating over it.
        auto httpsIt = httpsCommands.begin();
        while (httpsIt != httpsCommands.end()) {
            if (httpsIt->httpsRequest->response) {
                syncNodeQueuedCommands.push(move(*httpsIt));
                httpsIt = httpsCommands.erase(httpsIt);
            } else {
                httpsIt++;
            }
        }

        // Ok, let the sync node to it's updating for as many iterations as it requires. We'll update the replication
        // state when it's finished.
        SQLiteNode::State preUpdateState = syncNode.getState();
        while (syncNode.update()) {}
        SQLiteNode::State nodeState = syncNode.getState();
        replicationState.store(nodeState);
        masterVersion.store(syncNode.getMasterVersion());

        // If the node's not in a ready state at this point, we'll probably need to read from the network, so start the
        // main loop over. This can let us wait for logins from peers (for example).
        if (nodeState != SQLiteNode::MASTERING &&
            nodeState != SQLiteNode::SLAVING   &&
            nodeState != SQLiteNode::STANDINGDOWN) {
            continue;
        }

        // If we've just switched to the mastering state, we want to upgrade the DB. We'll set a global flag to let
        // worker threads know that a DB upgrade is in progress, and start the upgrade process, which works basically
        // like a regular distributed commit.
        if (preUpdateState != SQLiteNode::MASTERING && nodeState == SQLiteNode::MASTERING) {
            if (server._upgradeDB(db)) {
                upgradeInProgress.store(true);
                server._syncThreadCommitMutex.lock();
                committingCommand = true;
                server._writableCommandsInProgress++;
                syncNode.startCommit(SQLiteNode::QUORUM);

                // As it's a quorum commit, we'll need to read from peers. Let's start the next loop iteration.
                continue;
            }
        }

        // If we started a commit, and one's not in progress, then we've finished it and we'll take that command and
        // stick it back in the appropriate queue.
        if (committingCommand && !syncNode.commitInProgress()) {
            // It should be impossible to get here if we're not mastering or standing down.
            SASSERT(nodeState == SQLiteNode::MASTERING || nodeState == SQLiteNode::STANDINGDOWN);

            // Record the time spent.
            command.stopTiming(BedrockCommand::COMMIT_SYNC);

            // We're done with the commit, we unlock our mutex and decrement our counter.
            server._syncThreadCommitMutex.unlock();
            committingCommand = false;
            server._writableCommandsInProgress--;
            if (syncNode.commitSucceeded()) {
                // If we were upgrading, there's no response to send, we're just done.
                if (upgradeInProgress.load()) {
                    upgradeInProgress.store(false);
                    continue;
                }
                BedrockConflictMetrics::recordSuccess(command.request.methodLine);
                SINFO("[performance] Sync thread finished committing command " << command.request.methodLine);

                // Otherwise, mark this command as complete and reply.
                command.complete = true;
                if (command.initiatingPeerID) {
                    // This is a command that came from a peer. Have the sync node send the response back to the peer.
                    command.finalizeTimingInfo();
                    syncNode.sendResponse(command);
                } else {
                    // The only other option is this came from a client, so respond via the server.
                    server._reply(command);
                }
            } else {
                // TODO: This `else` block should be unreachable since the sync thread now blocks workers for entire
                // transactions. It should probably be removed, but we'll leave it in for the time being until the
                // final implementation of multi-write is stabilized.
                BedrockConflictMetrics::recordConflict(command.request.methodLine);

                // If the commit failed, then it must have conflicted, so we'll re-queue it to try again.
                SINFO("[performance] Conflict committing in sync thread, requeueing command "
                      << command.request.methodLine << ". Sync thread has "
                      << syncNodeQueuedCommands.size() << " queued commands.");
                syncNodeQueuedCommands.push(move(command));
            }
        }

        // We're either mastering, standing down, or slaving. There could be a commit in progress on `command`, but
        // there could also be other finished work to handle while we wait for that to complete. Let's see if we can
        // handle any of that work.
        try {
            // If there are any completed commands to respond to, we'll do that first.
            try {
                while (true) {
                    BedrockCommand completedCommand = completedCommands.pop();
                    SASSERT(completedCommand.complete);
                    SASSERT(completedCommand.initiatingPeerID);
                    SASSERT(!completedCommand.initiatingClientID);
                    completedCommand.finalizeTimingInfo();
                    syncNode.sendResponse(completedCommand);
                }
            } catch (out_of_range e) {
                // when completedCommands.pop() throws for running out of commands, we fall out of the loop.
            }

            // We don't start processing a new command until we've completed any existing ones.
            if (committingCommand) {
                continue;
            }

            // If we're STANDINGDOWN, we don't want to start on any new commands. We'll just start our next loop
            // iteration without doing anything here, and maybe we'll be either MASTERING or SLAVING on the next
            // iteration.
            if (nodeState == SQLiteNode::STANDINGDOWN) {
                continue;
            }

            // Now we can pull the next command off the queue and start on it.
            command = syncNodeQueuedCommands.pop();
            SINFO("[performance] Sync thread dequeued command " << command.request.methodLine << ". Sync thread has "
                  << syncNodeQueuedCommands.size() << " queued commands.");

            // We got a command to work on! Set our log prefix to the request ID.
            SAUTOPREFIX(command.request["requestID"]);

            // And now we'll decide how to handle it.
            if (nodeState == SQLiteNode::MASTERING) {
                // We need to grab this before peekCommand (or wherever our transaction is started), to verify that
                // no worker thread can commit in the middle of our transaction. We need our entire transaction to
                // happen with no other commits to ensure that we can't get a conflict.
                uint64_t beforeLock = STimeNow();
                server._syncThreadCommitMutex.lock();

                // It appears that this might be taking significantly longer with multi-write enabled, so we're adding
                // explicit logging for it to check.
                SINFO("[performance] Waited " << (STimeNow() - beforeLock) << "us for _syncThreadCommitMutex.");

                // We peek commands here in the sync thread to be able to run peek and process as part of the same
                // transaction. This guarantees that any checks made in peek are still valid in process, as the DB can't
                // have changed in the meantime.
                // IMPORTANT: This check is omitted for commands with an HTTPS request object, because we don't want to
                // risk duplicating that request. If your command creates an HTTPS request, it needs to explicitly
                // re-verify that any checks made in peek are still valid in process.
                if (!command.httpsRequest) {
                    if (core.peekCommand(command)) {
                        // Finished with this.
                        server._syncThreadCommitMutex.unlock();

                        // This command completed in peek, respond to it appropriately, either directly or by sending it
                        // back to the sync thread.
                        SASSERT(command.complete);
                        if (command.initiatingPeerID) {
                            command.finalizeTimingInfo();
                            syncNode.sendResponse(command);
                        } else {
                            server._reply(command);
                        }
                        continue;
                    }
                }

                // If we've dequeued a command with an incomplete HTTPS request, we move it to httpsCommands so that every
                // subsequent dequeue doesn't have to iterate past it while ignoring it. Then we'll just start on the next
                // command.
                if (command.httpsRequest && !command.httpsRequest->response) {
                    // We can't finish this transaction right now. We'll restart it later when the httpsRequest is
                    // complete.
                    if (db.insideTransaction()) {
                        // We only rollback if we're inside a transaction. This will happen if `peekCommand` created an
                        // httpsRequest above. However, if `peekCommand` was done in a worker thread, then this has
                        // already been done, so we won't roll it back again.
                        core.rollback();
                    }

                    // Done with the lock.
                    server._syncThreadCommitMutex.unlock();

                    // Set this aside and move on to the next command.
                    httpsCommands.push_back(move(command));
                    continue;
                }
                if (core.processCommand(command)) {
                    // The processor says we need to commit this, so let's start that process.
                    committingCommand = true;
                    SINFO("[performance] Sync thread beginning committing command " << command.request.methodLine);
                    server._writableCommandsInProgress++;
                    // START TIMING.
                    command.startTiming(BedrockCommand::COMMIT_SYNC);
                    syncNode.startCommit(command.writeConsistency);

                    // And we'll start the next main loop.
                    // NOTE: This will cause us to read from the network again. This, in theory, is fine, but we saw
                    // performance problems in the past trying to do something similar on every commit. This may be
                    // alleviated now that we're only doing this on *sync* commits instead of all commits, which should
                    // be a much smaller fraction of all our traffic. We set nextActivity here so that there's no
                    // timeout before we'll give up on poll() if there's nothing to read.
                    nextActivity = STimeNow();

                    // Don't unlock _syncThreadCommitMutex here, we'll hold the lock till the commit completes.
                    continue;
                } else {
                    // Otherwise, the command doesn't need a commit (maybe it was an error, or it didn't have any work
                    // to do). We'll just respond.
                    server._syncThreadCommitMutex.unlock();
                    if (command.initiatingPeerID) {
                        command.finalizeTimingInfo();
                        syncNode.sendResponse(command);
                    } else {
                        server._reply(command);
                    }
                }
            } else if (nodeState == SQLiteNode::SLAVING) {
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

    // Done with the global lock.
    server._syncMutex.unlock();

    // We've finished shutting down the sync node, tell the workers that it's finished.
    server._shutdownState.store(SYNC_SHUTDOWN);
    SINFO("SYNC_SHUTDOWN. Sync thread finished with commands.");

    // We just fell out of the loop where we were waiting for shutdown to complete. Update the state one last time when
    // the writing replication thread exits.
    replicationState.store(syncNode.getState());
    if (replicationState.load() > SQLiteNode::WAITING) {
        // This is because the graceful shutdown timer fired and syncNode.shutdownComplete() returned `true` above, but
        // the server still thinks it's in some other state. We can only exit if we're in state <= SQLC_SEARCHING,
        // (per BedrockServer::shutdownComplete()), so we force that state here to allow the shutdown to proceed.
        SWARN("Sync thread exiting in state " << replicationState.load() << ". Setting to SEARCHING.");
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

    // If there's anything left in the command queue here, we'll discard it, because we have no way of processing it.
    if (server._commandQueue.size()) {
        SWARN("Sync thread shut down with " << server._commandQueue.size() << " queued commands. Commands were: "
              << SComposeList(server._commandQueue.getRequestMethodLines()) << ". Clearing.");
        server._commandQueue.clear();
    }
}

void BedrockServer::worker(SData& args,
                           atomic<SQLiteNode::State>& replicationState,
                           atomic<bool>& upgradeInProgress,
                           atomic<string>& masterVersion,
                           CommandQueue& syncNodeQueuedCommands,
                           CommandQueue& syncNodeCompletedCommands,
                           BedrockServer& server,
                           int threadId,
                           int threadCount)
{
    SInitialize("worker" + to_string(threadId));

    // We pass `0` as the checkpoint size to disable checkpointing from workers. This can be a slow operation, and we
    // don't want workers to be able to block the sync thread while it happens.
    SQLite db(args["-db"], args.calc("-cacheSize"), 0, args.calc("-maxJournalSize"), threadId, threadCount - 1);
    BedrockCore core(db, server);

    // Command to work on. This default command is replaced when we find work to do.
    BedrockCommand command;

    // We just run this loop looking for commands to process forever. There's a check for appropriate exit conditions
    // at the bottom, which will cause our loop and thus this thread to exit when that becomes true.
    while (true) {
        try {
            // If we can't find any work to do, this will throw.
            command = server._commandQueue.get(1000000);
            SAUTOPREFIX(command.request["requestID"]);
            SINFO("[performance] Dequeued command " << command.request.methodLine << " in worker, "
                  << server._commandQueue.size() << " commands in queue.");

            // We just spin until the node looks ready to go. Typically, this doesn't happen expect briefly at startup.
            while (upgradeInProgress.load() ||
                   (replicationState.load() != SQLiteNode::MASTERING &&
                    replicationState.load() != SQLiteNode::SLAVING &&
                    replicationState.load() != SQLiteNode::STANDINGDOWN)
            ) {
                // This sleep call is pretty ugly, but it should almost never happen. We're accepting the potential
                // looping sleep call for the general case where we just check some bools and continue, instead of
                // avoiding the sleep call but having every thread lock a mutex here on every loop.
                usleep(10000);
            }

            // If this command is dependent on a commitCount newer than what we have (maybe it's a follow-up to a
            // command that was escalated to master), we'll set it aside for later processing. When the sync node
            // finishes its update loop, it will re-queue any of these commands that are no longer blocked on our
            // updated commit count.
            uint64_t commitCount = db.getCommitCount();
            uint64_t commandCommitCount = command.request.calcU64("commitCount");
            if (commandCommitCount > commitCount) {
                SAUTOLOCK(server._futureCommitCommandMutex);
                auto newQueueSize = server._futureCommitCommands.size() + 1;
                SINFO("Command (" << command.request.methodLine << ") depends on future commit(" << commandCommitCount
                      << "), Currently at: " << commitCount << ", storing for later. Queue size: " << newQueueSize);
                server._futureCommitCommands.insert(make_pair(commandCommitCount, move(command)));
                if (newQueueSize > 100) {
                    SHMMM("server._futureCommitCommands.size() == " << newQueueSize);
                }
                continue;
            }

            // OK, so this is the state right now, which isn't necessarily anything in particular, because the sync
            // node can change it at any time, and we're not synchronizing on it. We're going to go ahead and assume
            // it's something reasonable, because in most cases, that's pretty safe. If we think we're anything but
            // MASTERING, we'll just peek this command and return it's result, which should be harmless. If we think
            // we're mastering, we'll go ahead and start a `process` for the command, but we'll synchronously verify
            // our state right before we commit.
            SQLiteNode::State state = replicationState.load();

            // If we find that we've gotten a command with an initiatingPeerID, but we're not in a mastering or
            // standing down state, we'll have no way of returning this command to the caller, so we discard it. The
            // original caller will need to re-send the request. This can happen if we're mastering, and receive a
            // request from a peer, but then we stand down from mastering. The SQLiteNode should have already told its
            // peers that their outstanding requests were being canceled at this point.
            if (command.initiatingPeerID && !(state == SQLiteNode::MASTERING || SQLiteNode::STANDINGDOWN)) {
                SWARN("Found " << (command.complete ? "" : "in") << "complete " << "command "
                      << command.request.methodLine << " from peer, but not mastering. Too late for it, discarding.");
                continue;
            }

            // If this command is already complete, then we should be a slave, and the sync node got a response back
            // from a command that had been escalated to master, and queued it for a worker to respond to. We'll send
            // that response now.
            if (command.complete) {
                // If this command is already complete, we can return it to the caller.
                // If it has an initiator, it should have been returned to a peer by a sync node instead, but if we've
                // just switched states out of mastering, we might have an old command in the queue. All we can do here
                // is note that and discard it, as we have nobody to deliver it to.
                if (command.initiatingPeerID) {
                    // Let's note how old this command is.
                    uint64_t ageSeconds = (STimeNow() - command.creationTime) / STIME_US_PER_S;
                    SWARN("Found unexpected complete command " << command.request.methodLine
                          << " from peer in worker thread. Discarding (command was " << ageSeconds << "s old).");
                    continue;
                }

                // Make sure we have an initiatingClientID at this point. If we do, but it's negative, it's for a
                // client that we can't respond to, so we don't bother sending the response.
                SASSERT(command.initiatingClientID);
                if (command.initiatingClientID > 0) {
                    server._reply(command);
                }

                // This command is done, move on to the next one.
                continue;
            }

            // We'll retry on conflict up to this many times.
            int retry = 3;
            while (retry) {
                // Try peeking the command. If this succeeds, then it's finished, and all we need to do is respond to
                // the command at the bottom.
                if (!core.peekCommand(command)) {
                    // We've just unsuccessfully peeked a command, which means we're in a state where we might want to
                    // write it. We'll flag that here, to keep the node from falling out of MASTERING/STANDINGDOWN
                    // until we're finished with this command.
                    server._writableCommandsInProgress++;
                    if (command.httpsRequest) {
                        // It's an error to open an HTTPS request unless we're mastering, since we won't be able to
                        // record the response. Note that it's *possible* that the state of the node differs from what
                        // it was when we set `state`, and `peekCommand` will have looked at the current state of the
                        // node, not the one we saved in `state`. We could potentially mitigate this by only ever
                        // peeking certain commands on the sync thread, but even still, we could lose HTTP responses
                        // due to a crash or network event, so we don't try to hard to be perfect here.
                        SASSERTWARN(state == SQLiteNode::MASTERING);
                    }
                    // Peek wasn't enough to handle this command. Now we need to decide if we should try and process
                    // it, or if we should send it off to the sync node.
                    bool canWriteParallel = false;
                    { 
                        SAUTOLOCK(_parallelCommandMutex);
                        canWriteParallel =
                            (_parallelCommands.find(command.request.methodLine) != _parallelCommands.end());
                    }

                    // For now, commands need to be in `_parallelCommands` *and* `multiWriteOK`. When we're
                    // confident in BedrockConflictMetrics, we can remove `_parallelCommands`.
                    canWriteParallel = canWriteParallel && BedrockConflictMetrics::multiWriteOK(command.request.methodLine);
                    if (!canWriteParallel              ||
                        state != SQLiteNode::MASTERING ||
                        command.httpsRequest           ||
                        command.writeConsistency != SQLiteNode::ASYNC)
                    {
                        // Roll back the transaction, it'll get re-run in the sync thread.
                        core.rollback();

                        // We're not handling a writable command anymore.
                        SINFO("[performance] Sending non-parallel command " << command.request.methodLine
                              << " to sync thread. Sync thread has " << syncNodeQueuedCommands.size()
                              << " queued commands.");
                        server._writableCommandsInProgress--;
                        syncNodeQueuedCommands.push(move(command));

                        // We'll break out of our retry loop here, as we don't need to do anything else, we can just
                        // look for another command to work on.
                        break;
                    } else {
                        // In this case, there's nothing blocking us from processing this in a worker, so let's try it.
                        if (core.processCommand(command)) {
                            // If processCommand returned true, then we need to do a commit. Otherwise, the command is
                            // done, and we just need to respond. Note that this is the first place we get really
                            // particular with the state of the node from a worker thread. We only want to do this
                            // commit if we're *SURE* we're mastering, and not allow the state of the node to change
                            // while we're committing. If it turns out we've changed states, we'll roll this command
                            // back, so we lock the node's state until we complete.
                            SAUTOLOCK(server._syncNode->stateMutex);
                            if (replicationState.load() != SQLiteNode::MASTERING &&
                                replicationState.load() != SQLiteNode::STANDINGDOWN) {
                                SWARN("Node State changed from MASTERING to "
                                      << SQLiteNode::stateNames[replicationState.load()]
                                      << " during worker commit. Rolling back transaction!");
                                core.rollback();
                            } else {
                                // Before we commit, we need to grab the sync thread lock. Because the sync thread grabs
                                // an exclusive lock on this wrapping any transactions that it performs, we'll get this
                                // lock while the sync thread isn't in the process of handling a transaction, thus
                                // guaranteeing that we can't commit and cause a conflict on the sync thread. We can
                                // still get conflicts here, as the sync thread might have performed a transaction
                                // after we called `processCommand` and before we call `commit`, or we could conflict
                                // with another worker thread, but the sync thread will never see a conflict as long
                                // as we don't commit while it's performing a transaction.
                                bool commitSuccess;
                                shared_lock<decltype(server._syncThreadCommitMutex)> lock(server._syncThreadCommitMutex);
                                {
                                    // Scoped for auto-timer.
                                    BedrockCore::AutoTimer(command, BedrockCommand::COMMIT_WORKER);
                                    commitSuccess = core.commit();
                                }
                                if (commitSuccess) {
                                    BedrockConflictMetrics::recordSuccess(command.request.methodLine);
                                    SINFO("Successfully committed " << command.request.methodLine << " on worker thread.");
                                    // So we must still be mastering, and at this point our commit has succeeded, let's
                                    // mark it as complete!
                                    command.complete = true;
                                } else {
                                    BedrockConflictMetrics::recordConflict(command.request.methodLine);
                                    SINFO("Conflict committing " << command.request.methodLine
                                          << " on worker thread with " << retry << " retries remaining.");
                                }
                            }
                        }

                        // Whether we rolled it back or committed it, it's no longer potentially getting written, so we
                        // can decrement our counter.
                        server._writableCommandsInProgress--;
                    }
                }

                // If the command was completed above, then we'll go ahead and respond. Otherwise there must have been
                // a conflict, and we'll retry.
                if (command.complete) {
                    if (command.initiatingPeerID) {
                        // Escalated command. Give it back to the sync thread to respond.
                        syncNodeCompletedCommands.push(move(command));
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
                SINFO("[performance] Max retries hit in worker, forwarding command " << command.request.methodLine
                      << " to sync thread. Sync thread has " << syncNodeQueuedCommands.size() << " queued commands.");
                syncNodeQueuedCommands.push(move(command));
            }
        } catch(...) {
            // No commands to process after 1 second.
        }

        // If the server's not accepting new connections, and we don't have anything in the queue to process, we can
        // inform the sync thread that we're done with this queue.
        if (server._shutdownState.load() == PORTS_CLOSED) {
            server._shutdownState.store(QUEUE_PROCESSED);
            SINFO("QUEUE_PROCESSED, waiting for sync thread to finish.");
        }

        // If the sync thread is finished, and the worker queue is empty, then we're really done.
        if (server._shutdownState.load() == SYNC_SHUTDOWN) {
            SINFO("Shutdown state is SYNC_SHUTDOWN, and queue empty. Worker" << to_string(threadId) << " exiting.");
            server._shutdownState.store(DONE);
            break;
        }

        // If another worker marked us done, we can exit as well.
        if (server._shutdownState.load() == DONE) {
            SINFO("Shutdown state is DONE. Worker" << to_string(threadId) << " exiting.");
            break;
        }
    }
}

BedrockServer::BedrockServer(const SData& args)
  : SQLiteServer(""), _args(args), _requestCount(0), _replicationState(SQLiteNode::SEARCHING),
    _upgradeInProgress(false), _suppressCommandPort(false), _suppressCommandPortManualOverride(false),
    _syncNode(nullptr), _shutdownState(RUNNING)
{
    _version = SVERSION;

    // Output the list of plugins.
    map<string, BedrockPlugin*> registeredPluginMap;
    for (BedrockPlugin* plugin : *BedrockPlugin::g_registeredPluginList) {
        // Add one more plugin
        const string& pluginName = SToLower(plugin->getName());
        SINFO("Registering plugin '" << pluginName << "'");
        registeredPluginMap[pluginName] = plugin;
    }

    // Enable the requested plugins, and update our version string if required.
    list<string> pluginNameList = SParseList(args["-plugins"]);
    vector<string> versions = {_version};
    for (string& pluginName : pluginNameList) {
        BedrockPlugin* plugin = registeredPluginMap[SToLower(pluginName)];
        if (!plugin) {
            SERROR("Cannot find plugin '" << pluginName << "', aborting.");
        }
        plugin->initialize(args, *this);
        plugins.push_back(plugin);

        // If the plugin has version info, add it to the list.
        auto info = plugin->getInfo();
        auto iterator = info.find("version");
        if (iterator != info.end()) {
            versions.push_back(plugin->getName() + "_" + iterator->second);
        }
    }
    sort(versions.begin(), versions.end());
    _version = SComposeList(versions, ":");

    // If `versionOverride` is set, we throw away what we just did and use the overridden value.
    if (args.isSet("-versionOverride")) {
        _version = args["-versionOverride"];
    }

    // Check for commands that will be forced to use QUORUM write consistency.
    if (args.isSet("-synchronousCommands")) {
        list<string> syncCommands;
        SParseList(args["-synchronousCommands"], syncCommands);
        for (auto& command : syncCommands) {
            _syncCommands.insert(command);
        }
    }

    // Check for commands that can be written by workers.
    if (args.isSet("-parallelCommands")) {
        SAUTOLOCK(_parallelCommandMutex);
        list<string> parallelCommands;
        SParseList(args["-parallelCommands"], parallelCommands);
        for (auto& command : parallelCommands) {
            _parallelCommands.insert(command);
        }
    }

    // Start the sync thread, which will start the worker threads.
    SINFO("Launching sync thread '" << _syncThreadName << "'");
    _syncThread = thread(sync,
                         ref(_args),
                         ref(_replicationState),
                         ref(_upgradeInProgress),
                         ref(_masterVersion),
                         ref(_syncNodeQueuedCommands),
                         ref(*this));
}

BedrockServer::~BedrockServer() {
    // Just warn if we have outstanding requests
    SASSERTWARN(_requestCountSocketMap.empty());

    // Shut down any outstanding keepalive connections
    for (list<Socket*>::iterator socketIt = socketList.begin(); socketIt != socketList.end();) {
        // Shut it down and go to the next (because closeSocket will invalidate this iterator otherwise)
        Socket* s = *socketIt++;
        closeSocket(s);
    }

    // Shut down the sync thread, (which will shut down worker threads in turn).
    SINFO("Closing sync thread '" << _syncThreadName << "'");
    _syncThread.join();
    SINFO("Threads closed.");
}

bool BedrockServer::shutdownComplete() {
    // If nobody's asked us to shut down, we're not done.
    if (_shutdownState.load() == RUNNING) {
        return false;
    }

    // If we're totally done, we can return true.
    if (_shutdownState.load() == DONE) {
        return true;
    }

    // At least one of our required criteria has failed. Let's see if our timeout has elapsed. If so, we'll log and
    // return true anyway.
    if (_gracefulShutdownTimeout.ringing()) {
        // Timing out. Log some info and return true.
        map<string, int> commandsInQueue;
        auto methods = _commandQueue.getRequestMethodLines();
        for (auto method : methods) {
            auto it = commandsInQueue.find(method);
            if (it != commandsInQueue.end()) {
                (it->second)++;
            } else {
                commandsInQueue[method] = 1;
            }
        }
        string commandCounts;
        for (auto cmdPair : commandsInQueue) {
            commandCounts += cmdPair.first + ":" + to_string(cmdPair.second) + ", ";
        }
        SWARN("Graceful shutdown timed out. "
              << "Replication State: " << SQLiteNode::stateNames[_replicationState.load()] << ". "
              << "Commands queue size: " << _commandQueue.size() << ". "
              << "Command Counts: " << commandCounts << "killing non gracefully.");
        return true;
    }

    // At this point, we've got something blocking shutdown, and our timeout hasn't passed, so we'll log and return
    // false, and allow the caller to wait a bit longer.
    string logLine = "Conditions that failed and are blocking shutdown:";
    if (_replicationState.load() > SQLiteNode::WAITING) {
        logLine += " Replication State: " + SQLiteNode::stateNames[_replicationState.load()] + " > SQLC_WAITING.";
    }
    if (!_commandQueue.empty()) {
        logLine += " Commands queue not empty. Size: " + to_string(_commandQueue.size()) + ".";
    }

    // Also log the shutdown state.
    SHUTDOWN_STATE state = _shutdownState.load();
    string stateString;
    switch (state) {
        case RUNNING:
            stateString = "RUNNING";
            break;
        case START_SHUTDOWN:
            stateString = "START_SHUTDOWN";
            break;
        case PORTS_CLOSED:
            stateString = "PORTS_CLOSED";
            break;
        case QUEUE_PROCESSED:
            stateString = "QUEUE_PROCESSED";
            break;
        case SYNC_SHUTDOWN:
            stateString = "SYNC_SHUTDOWN";
            break;
        case DONE:
            stateString = "DONE";
            break;
        default:
            stateString = "UNKNOWN";
            break;
    }
    logLine += " Shutdown State: " + stateString + ".";
    SWARN(logLine);

    return false;
}

void BedrockServer::prePoll(fd_map& fdm) {
    SAUTOLOCK(_socketIDMutex);
    STCPServer::prePoll(fdm);
}

void BedrockServer::postPoll(fd_map& fdm, uint64_t& nextActivity) {
    // Let the base class do its thing. We lock around this because we allow worker threads to modify the sockets (by
    // writing to them, but this can truncate send buffers).
    {
        SAUTOLOCK(_socketIDMutex);
        STCPServer::postPoll(fdm);
    }

    // Open the port the first time we enter a command-processing state
    SQLiteNode::State state = _replicationState.load();

    // If we're a slave, and the master's on a different version than us, we don't open the command port.
    // If we do, we'll escalate all of our commands to the master, which causes undue load on master during upgrades.
    // Instead, we'll simply not respond and let this request get re-directed to another slave.
    string masterVersion = _masterVersion.load();
    if (!_suppressCommandPort && state == SQLiteNode::SLAVING && (masterVersion != _version)) {
        SINFO("Node " << _args["-nodeName"] << " slaving on version " << _version << ", master is version: "
              << masterVersion << ", not opening command port.");
        suppressCommandPort(true);
    } else if (_suppressCommandPort && (state == SQLiteNode::MASTERING || (masterVersion == _version))) {
        // If we become master, or if master's version resumes matching ours, open the command port again.
        if (!_suppressCommandPortManualOverride) {
            // Only generate this logline if we haven't manually blocked this.
            SINFO("Node " << _args["-nodeName"] << " disabling previously suppressed command port after version check.");
        }
        suppressCommandPort(false);
    }
    if (!_suppressCommandPort && portList.empty() && (state == SQLiteNode::MASTERING || state == SQLiteNode::SLAVING) &&
        _shutdownState.load() == RUNNING) {
        // Open the port
        SINFO("Ready to process commands, opening command port on '" << _args["-serverHost"] << "'");
        openPort(_args["-serverHost"]);

        // Open any plugin ports on enabled plugins
        for (auto plugin : plugins) {
            string portHost = plugin->getPort();
            if (!portHost.empty()) {
                // Open the port and associate it with the plugin
                SINFO("Opening port '" << portHost << "' for plugin '" << plugin->getName() << "'");
                Port* port = openPort(portHost);
                _portPluginMap[port] = plugin;
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
    if (SGetSignals()) {
        if (SGetSignal(SIGTTIN)) {
            // Suppress command port, but only if we haven't already cleared it
            if (!SCheckSignal(SIGTTOU)) {
                SHMMM("Suppressing command port due to SIGTTIN");
                suppressCommandPort(true, true);
            }
        } else if (SGetSignal(SIGTTOU)) {
            // Clear command port suppression
            SHMMM("Clearing command port supression due to SIGTTOU");
            suppressCommandPort(false, true);
        } else {
            // For anything else, just shutdown -- but only if we're not already shutting down
            if (_shutdownState.load() == RUNNING) {
                // Begin a graceful shutdown; close our port
                SINFO("Beginning graceful shutdown due to '" << SGetSignalDescription()
                      << "', closing command port on '" << _args["-serverHost"] << "'");
                _gracefulShutdownTimeout.alarmDuration = STIME_US_PER_S * 30; // 30s timeout before we give up
                _gracefulShutdownTimeout.start();

                // Close our listening ports, we won't accept any new connections on them.
                closePorts();
                _shutdownState.store(START_SHUTDOWN);
                SINFO("START_SHUTDOWN. Ports shutdown, will perform final socket read.");
            }
        }
    }

    // Accept any new connections
    Socket* s = nullptr;
    Port* acceptPort = nullptr;
    while ((s = acceptSocket(acceptPort))) {
        // Accepted a new socket
        // NOTE: SQLiteNode doesn't need to keep a new list; we'll just reuse the STCPManager::socketList.
        // Look up the plugin that owns this port (if any).
        if (SContains(_portPluginMap, acceptPort)) {
            BedrockPlugin* plugin = _portPluginMap[acceptPort];
            // Allow the plugin to process this
            SINFO("Plugin '" << plugin->getName() << "' accepted a socket from '" << s->addr << "'");
            plugin->onPortAccept(s);

            // Remember that this socket is owned by this plugin.
            SASSERT(!s->data);
            s->data = plugin;
        }
    }

    // Process any new activity from incoming sockets. In order to not modify the socket list while we're iterating
    // over it, we'll keep a list of sockets that need closing.
    list<STCPManager::Socket*> socketsToClose;
    for (auto s : socketList) {
        switch (s->state) {
            case STCPManager::Socket::CLOSED:
            {
                // TODO: Cancel any outstanding commands initiated by this socket. This isn't critical, and is an
                // optimization. Otherwise, they'll continue to get processed to completion, and will just never be
                // able to have their responses returned.
                SAUTOLOCK(_socketIDMutex);
                _socketIDMap.erase(s->id);
                socketsToClose.push_back(s);
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
                    // requests off the same socket at one time, or we don't guarantee their return order, thus we just
                    // wait and will try again later.
                    SAUTOLOCK(_socketIDMutex);
                    auto socketIt = _socketIDMap.find(s->id);
                    if (socketIt != _socketIDMap.end()) {
                        break;
                    }
                }

                // If there's a request, we'll dequeue it (but only the first one).
                SData request;

                // If the socket is owned by a plugin, we let the plugin populate our request.
                BedrockPlugin* plugin = static_cast<BedrockPlugin*>(s->data);
                if (plugin) {
                    // Call the plugin's handler.
                    plugin->onPortRecv(s, request);
                    if (!request.empty()) {
                        // If it populated our request, then we'll save the plugin name so we can handle the response.
                        request["plugin"] = plugin->getName();
                    }
                } else {
                    // Otherwise, handle any default request.
                    int requestSize = request.deserialize(s->recvBuffer);
                    SConsumeFront(s->recvBuffer, requestSize);
                }

                // If we have a populated request, from either a plugin or our default handling, we'll queue up the
                // command.
                if (!request.empty()) {
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
                    if (command.writeConsistency != SQLiteNode::QUORUM
                        && _syncCommands.find(command.request.methodLine) != _syncCommands.end()) {

                        command.writeConsistency = SQLiteNode::QUORUM;
                        SINFO("Forcing QUORUM consistency for command " << command.request.methodLine);
                    }

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
            case STCPManager::Socket::SHUTTINGDOWN:
            {
                // We do nothing in this state, we just wait until the next iteration of poll and let the CLOSED
                // case run. This block just prevents default warning from firing.
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
    for (auto plugin : plugins) {
        for (SStopwatch* timer : plugin->timers) {
            if (timer->ding()) {
                plugin->timerFired(timer);
            }
        }
    }

    // If we started shutting down, we can now finish that process.
    if (_shutdownState.load() == START_SHUTDOWN) {
        _shutdownState.store(PORTS_CLOSED);
        SINFO("PORTS_CLOSED. All ports closed.");
    }
}

void BedrockServer::_reply(BedrockCommand& command) {
    SAUTOLOCK(_socketIDMutex);

    // Do we have a socket for this command?
    auto socketIt = _socketIDMap.find(command.initiatingClientID);
    if (socketIt != _socketIDMap.end()) {

        // The last thing we do is total up our timing info and add it to the response.
        command.finalizeTimingInfo();
        command.response["nodeName"] = _args["-nodeName"];

        // Is a plugin handling this command? If so, it gets to send the response.
        string& pluginName = command.request["plugin"];
        if (!pluginName.empty()) {
            // Let the plugin handle it
            SINFO("Plugin '" << pluginName << "' handling response '" << command.response.methodLine
                  << "' to request '" << command.request.methodLine << "'");
            BedrockPlugin* plugin = BedrockPlugin::getPluginByName(pluginName);
            if (plugin) {
                plugin->onPortRequestComplete(command, socketIt->second);
            } else {
                SERROR("Couldn't find plugin '" << pluginName << ".");
            }
        } else {
            // Otherwise we send the standard response.
            socketIt->second->send(command.response.serialize());
        }

        // If `Connection: close` was set, shut down the socket.
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
        // Close the command port, and all plugin's ports. Won't reopen.
        SHMMM("Suppressing command port");
        if (!portList.empty())
            closePorts();
    } else {
        // Clearing past suppression, but don't reopen (It's always safe to close, but not always safe to open).
        SHMMM("Clearing command port suppression");
    }
}

bool BedrockServer::_isStatusCommand(BedrockCommand& command) {
    if (SIEquals(command.request.methodLine, STATUS_IS_SLAVE)          ||
        SIEquals(command.request.methodLine, STATUS_HANDLING_COMMANDS) ||
        SIEquals(command.request.methodLine, STATUS_PING)              ||
        SIEquals(command.request.methodLine, STATUS_STATUS)            ||
        SIEquals(command.request.methodLine, STATUS_WHITELIST)) {
        return true;
    }
    return false;
}

void BedrockServer::_status(BedrockCommand& command) {
    SData& request  = command.request;
    SData& response = command.response;

    // We'll return whether or not this server is slaving.
    if (SIEquals(request.methodLine, STATUS_IS_SLAVE)) {
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

    else if (SIEquals(request.methodLine, STATUS_HANDLING_COMMANDS)) {
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
    else if (SIEquals(request.methodLine, STATUS_PING)) {
        response.methodLine = "200 OK";
    }

    // This collects the current state of the server, which also includes some state from the underlying SQLiteNode.
    else if (SIEquals(request.methodLine, STATUS_STATUS)) {
        STable content;
        SQLiteNode::State state = _replicationState.load();
        list<string> pluginList;
        for (auto plugin : plugins) {
            STable pluginData = plugin->getInfo();
            pluginData["name"] = plugin->getName();
            pluginList.push_back(SComposeJSONObject(pluginData));
        }
        content["isMaster"] = state == SQLiteNode::MASTERING ? "true" : "false";
        content["plugins"]  = SComposeJSONArray(pluginList);
        content["state"]    = SQLiteNode::stateNames[state];
        content["version"]  = _version;
        content["host"]     = _args["-nodeHost"];

        // On master, return the current multi-write blacklist.
        if (state == SQLiteNode::MASTERING) {
            content["multiWriteBlacklist"] = BedrockConflictMetrics::getMultiWriteDeniedCommands();
            content["multiWriteWhiteList"] = SComposeJSONArray(_parallelCommands);
        }

        // We read from syncNode internal state here, so we lock to make sure that this doesn't conflict with the sync
        // thread.
        list<STable> peerData;
        list<string> escalated;
        {
            SAUTOLOCK(_syncMutex);

            // Set some information about this node.
            content["CommitCount"] = to_string(_syncNode->getCommitCount());

            // Retrieve information about our peers.
            for (SQLiteNode::Peer* peer : _syncNode->peerList) {
                peerData.emplace_back(peer->nameValueMap);
                peerData.back()["host"] = peer->host;
            }

            // Get any escalated commands that are waiting to be processed.
            escalated = _syncNode->getEscalatedCommandRequestMethodLines();
        }

        // Coalesce all of the peer data into one value to return.
        list<string> peerList;
        for (const STable& peerTable : peerData) {
            peerList.push_back(SComposeJSONObject(peerTable));
        }
        content["peerList"]             = SComposeJSONArray(peerList);
        content["queuedCommandList"]    = SComposeJSONArray(_commandQueue.getRequestMethodLines());
        content["escalatedCommandList"] = SComposeJSONArray(escalated);

        // Done, compose the response.
        response.methodLine = "200 OK";
        response.content = SComposeJSONObject(content);
    }

    else if (SIEquals(request.methodLine, STATUS_WHITELIST)) {
        SAUTOLOCK(_parallelCommandMutex);

        // Return the old list. We can check the list by not passing the "Commands" param.
        STable content;
        content["oldCommandWhitelist"] = SComposeList(_parallelCommands);

        // If the Comamnds param is set, parse it and update our value.
        if (request.isSet("Commands")) {
            _parallelCommands.clear();
            list<string> parallelCommands;
            SParseList(request["Commands"], parallelCommands);
            for (auto& command : parallelCommands) {
                _parallelCommands.insert(command);
            }
        }
        if (request.isSet("autoBlacklistConflictFraction")) {
            BedrockConflictMetrics::setFraction(SToFloat(request["autoBlacklistConflictFraction"]));
        }

        // Enable extra logging in the commit lock timer.
        decltype(SQLite::g_commitLock)::enableExtraLogging.store(!_parallelCommands.empty());

        // PRepare the command to respond to the caller.
        response.methodLine = "200 OK";
        response.content = SComposeJSONObject(content);
    }
}

bool BedrockServer::_upgradeDB(SQLite& db) {
    // These all get conglomerated into one big query.
    db.beginTransaction();
    for (auto plugin : plugins) {
        plugin->upgradeDatabase(db);
    }
    if (db.getUncommittedQuery().empty()) {
        db.rollback();
    }
    return !db.getUncommittedQuery().empty();
}

void BedrockServer::_prePollPlugins(fd_map& fdm) {
    for (auto plugin : plugins) {
        for (auto manager : plugin->httpsManagers) {
            manager->prePoll(fdm);
        }
    }
}

void BedrockServer::_postPollPlugins(fd_map& fdm, uint64_t nextActivity) {
    for (auto plugin : plugins) {
        for (auto manager : plugin->httpsManagers) {
            manager->postPoll(fdm, nextActivity);
        }
    }
}

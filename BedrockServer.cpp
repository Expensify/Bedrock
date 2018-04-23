// Manages connections to a single instance of the bedrock server.
#include <libstuff/libstuff.h>
#include "BedrockServer.h"
#include "BedrockPlugin.h"
#include "BedrockConflictMetrics.h"
#include "BedrockCore.h"
#include <iomanip>

set<string>BedrockServer::_blacklistedParallelCommands;
recursive_mutex BedrockServer::_blacklistedParallelCommandMutex;

void BedrockServer::acceptCommand(SQLiteCommand&& command, bool isNew) {
    // If the sync node tells us that a command causes a crash, we immediately save that.
    if (SIEquals(command.request.methodLine, "CRASH_COMMAND")) {
        SData request;
        request.deserialize(command.request.content);

        // Take a unique lock so nobody else can read from this table while we update it.
        unique_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);

        // Add the blacklisted command to the map.
        _crashCommands[request.methodLine].insert(request.nameValueMap);
        size_t totalCount = 0;
        for (const auto& s : _crashCommands) {
            totalCount += s.second.size();
        }
        SALERT("Blacklisting command (now have " << totalCount << " blacklisted commands): " << request.serialize());
    } else {
        if (SIEquals(command.request.methodLine, "BROADCAST_COMMAND")) {
            SData newRequest;
            newRequest.deserialize(command.request.content);
            command.request = newRequest;
            command.initiatingClientID = -1;
            command.initiatingPeerID = 0;
        }
        SAUTOPREFIX(command.request["requestID"]);
        if (command.writeConsistency != SQLiteNode::QUORUM
            && _syncCommands.find(command.request.methodLine) != _syncCommands.end()) {

            command.writeConsistency = SQLiteNode::QUORUM;
            SINFO("Forcing QUORUM consistency for command " << command.request.methodLine);
        }
        SINFO("Queued new '" << command.request.methodLine << "' command from bedrock node, with " << _commandQueue.size()
              << " commands already queued.");
        _commandQueue.push(BedrockCommand(move(command)));
        if (!isNew) {
            // If the command isn't new, then we already think it's in progress, but it's been returned to us, so reset
            // that.
            _commandsInProgress--;
        }
    }
}

void BedrockServer::cancelCommand(const string& commandID) {
    _commandQueue.removeByID(commandID);
}

bool BedrockServer::canStandDown() {
    int count = _commandsInProgress.load();
    int size = _commandQueue.size();
    if (count || size) {
        SINFO("Can't stand down with " << count << " commands in progress and " << size << " commands queued.");
        return false;
    } else {
        return true;
    }
}

void BedrockServer::syncWrapper(SData& args,
                         atomic<SQLiteNode::State>& replicationState,
                         atomic<bool>& upgradeInProgress,
                         atomic<string>& masterVersion,
                         CommandQueue& syncNodeQueuedCommands,
                         BedrockServer& server)
{
    while(true) {
        // If the server's set to be detached, we wait until that flag is unset, and then start the sync thread.
        if (server._detach) {
            // If we're set detached, we assume we'll be re-attached eventually, and then be `RUNNING`.
            SINFO("Bedrock server entering detached state.");
            server._shutdownState.store(RUNNING);
            while (server._detach) {
                // Just wait until we're attached.
                SINFO("Bedrock server sleeping in detached state.");
                sleep(1);
            }
            SINFO("Bedrock server entering attached state.");
        }
        sync(args, replicationState, upgradeInProgress, masterVersion, syncNodeQueuedCommands, server);

        // Now that we've run the sync thread, we can exit if it hasn't set _detach again.
        if (!server._detach) {
            break;
        }
    }
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

    // We currently have no commands in progress.
    server._commandsInProgress.store(0);

    // Parse out the number of worker threads we'll use. The DB needs to know this because it will expect a
    // corresponding number of journal tables. "-readThreads" exists only for backwards compatibility.
    int workerThreads = args.calc("-workerThreads");

    // TODO: remove when nothing uses readThreads.
    workerThreads = workerThreads ? workerThreads : args.calc("-readThreads");

    // If still no value, use the number of cores on the machine, if available.
    workerThreads = workerThreads ? workerThreads : max(1u, thread::hardware_concurrency());

    // Initialize the DB.
    SQLite db(args["-db"], args.calc("-cacheSize"), true, args.calc("-maxJournalSize"), -1, workerThreads - 1, args["-synchronous"]);

    // And the command processor.
    BedrockCore core(db, server);

    // And the sync node.
    uint64_t firstTimeout = STIME_US_PER_M * 2 + SRandom::rand64() % STIME_US_PER_S * 30;

    // Initialize the shared pointer to our sync node object.
    server._syncNode = make_shared<SQLiteNode>(server, db, args["-nodeName"], args["-nodeHost"],
                                               args["-peerList"], args.calc("-priority"), firstTimeout,
                                               server._version, args.calc("-quorumCheckpoint"));

    // We keep a queue of completed commands that workers will insert into when they've successfully finished a command
    // that just needs to be returned to a peer.
    CommandQueue completedCommands;

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
    do {

        // Make sure the existing command prefix is still valid since they're reset when SAUTOPREFIX goes out of scope.
        SAUTOPREFIX(command.request["requestID"]);

        // If there were commands waiting on our commit count to come up-to-date, we'll move them back to the main
        // command queue here. There's no place in particular that's best to do this, so we do it at the top of this
        // main loop, as that prevents it from ever getting skipped in the event that we `continue` early from a loop
        // iteration.
        // We also move all commands back to the main queue here if we're shutting down, just to make sure they don't
        // end up lost in the ether.
        {
            SAUTOLOCK(server._futureCommitCommandMutex);
            if (!server._futureCommitCommands.empty()) {
                uint64_t commitCount = db.getCommitCount();
                auto it = server._futureCommitCommands.begin();
                auto& eraseTo = it;
                while (it != server._futureCommitCommands.end() && (it->first <= commitCount || server._shutdownState.load() != RUNNING)) {
                    SINFO("Returning command (" << it->second.request.methodLine << ") waiting on commit " << it->first
                          << " to queue, now have commit " << commitCount);
                    server._commandQueue.push(move(it->second));
                    server._commandsInProgress--;

                    eraseTo = it;
                    it++;
                }
                if (eraseTo != server._futureCommitCommands.begin()) {
                    server._futureCommitCommands.erase(server._futureCommitCommands.begin(), eraseTo);
                }
            }
        }

        // If we're in a state where we can initialize shutdown, then go ahead and do so.
        // Having responded to all clients means there are no *local* clients, but it doesn't mean there are no
        // escalated commands. This is fine though - if we're slaving, there can't be any escalated commands, and if
        // we're mastering, then the next update() loop will set us to standing down, and then we won't accept any new
        // commands, and we'll shortly run through the existing queue.
        if (server._shutdownState.load() == CLIENTS_RESPONDED) {
            // The total time we'll wait for the sync node is whatever we haven't already waited from the original
            // timeout, minus 5 seconds to allow to clean up afterward.
            int64_t timeAllowed = server._gracefulShutdownTimeout.alarmDuration.load() - server._gracefulShutdownTimeout.elapsed();
            timeAllowed -= 5'000'000;
            server._syncNode->beginShutdown(max(timeAllowed, 1l));
        }

        // The fd_map contains a list of all file descriptors (eg, sockets, Unix pipes) that poll will wait on for
        // activity. Once any of them has activity (or the timeout ends), poll will return.
        fd_map fdm;

        // Prepare our plugins for `poll` (for instance, in case they're making HTTP requests).
        server._prePollPlugins(fdm);

        // Pre-process any sockets the sync node is managing (i.e., communication with peer nodes).
        server._syncNode->prePoll(fdm);

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

        // Process any network traffic that happened. Scope this so that we can change the log prefix and have it
        // auto-revert when we're finished.
        {
            SAUTOPREFIX("xxxxxx");

            // Process any activity in our plugins.
            server._postPollPlugins(fdm, nextActivity);
            server._syncNode->postPoll(fdm, nextActivity);
            syncNodeQueuedCommands.postPoll(fdm);
            completedCommands.postPoll(fdm);
        }

        // Ok, let the sync node to it's updating for as many iterations as it requires. We'll update the replication
        // state when it's finished.
        SQLiteNode::State preUpdateState = server._syncNode->getState();
        while (server._syncNode->update()) {}
        SQLiteNode::State nodeState = server._syncNode->getState();
        replicationState.store(nodeState);
        masterVersion.store(server._syncNode->getMasterVersion());

        // If anything was in the stand down queue, move it back to the main queue.
        if (nodeState != SQLiteNode::STANDINGDOWN) {
            while (server._standDownQueue.size()) {
                server._commandQueue.push(server._standDownQueue.pop());
            }
        } else if (preUpdateState != SQLiteNode::STANDINGDOWN) {
            // Otherwise,if we just started standing down, discard any commands that had been scheduled in the future.
            // In theory, it should be fine to keep these, as they shouldn't have sockets associated with them, and
            // they could be re-escalated to master in the future, but there's not currently a way to decide if we've
            // run through all of the commands that might need peer responses before standing down aside from seeing if
            // the entire queue is empty.
            server._commandQueue.abandonFutureCommands(5000);
        }

        // If we're not mastering, we turn off multi-write until we've finished upgrading the DB. This persists until
        // after we're mastering  again.
        if (nodeState != SQLiteNode::MASTERING) {
            server._suppressMultiWrite.store(true);
        }

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
                server._commandsInProgress++;
                server._syncNode->startCommit(SQLiteNode::QUORUM);

                // As it's a quorum commit, we'll need to read from peers. Let's start the next loop iteration.
                continue;
            } else {
                // If we're not doing an upgrade, we don't need to keep suppressing multi-write.
                server._suppressMultiWrite.store(false);
            }
        } else if ((preUpdateState == SQLiteNode::MASTERING || preUpdateState == SQLiteNode::STANDINGDOWN)
                   && nodeState == SQLiteNode::SEARCHING) {
            // If we were MASTERING, but now we're searching, then something's gone wrong (perhaps we got disconnected
            // from the cluster). We should give up an any commands, and let them be re-escalated. If commands were
            // initiated locally, we can just re-queue them, they will get re-checked once things clear up, and then
            // they'll get processed here, or escalated to the new master.
            // Commands initiated on slaves just get dropped, they will need to be re-escalated, potentially to a
            // different master.
            int requeued = 0;
            int dropped = 0;
            try {
                while (true) {
                    command = syncNodeQueuedCommands.pop();
                    if (command.initiatingClientID) {
                        // This one came from a local client, so we can save it for later.
                        server._commandQueue.push(move(command));
                        server._commandsInProgress--;
                    }
                }
            } catch (const out_of_range& e) {
                SWARN("Abruptly stopped MASTERING. Re-queued " << requeued << " commands, Dropped " << dropped << " commands.");
            }
        }

        // If we started a commit, and one's not in progress, then we've finished it and we'll take that command and
        // stick it back in the appropriate queue.
        if (committingCommand && !server._syncNode->commitInProgress()) {
            // It should be impossible to get here if we're not mastering or standing down.
            if (nodeState != SQLiteNode::MASTERING && nodeState != SQLiteNode::STANDINGDOWN) {
                SERROR("Committing command but node state is " << SQLiteNode::stateNames[nodeState]);
            }

            // Record the time spent.
            command.stopTiming(BedrockCommand::COMMIT_SYNC);

            // We're done with the commit, we unlock our mutex and decrement our counter.
            server._syncThreadCommitMutex.unlock();
            committingCommand = false;
            if (server._syncNode->commitSucceeded()) {
                // If we were upgrading, there's no response to send, we're just done.
                if (upgradeInProgress.load()) {
                    upgradeInProgress.store(false);
                    server._suppressMultiWrite.store(false);
                    server._commandsInProgress--;
                    continue;
                }
                BedrockConflictMetrics::recordSuccess(command.request.methodLine);
                SINFO("[performance] Sync thread finished committing command " << command.request.methodLine);

                // Otherwise, save the commit count, mark this command as complete, and reply.
                command.response["commitCount"] = to_string(db.getCommitCount());
                command.complete = true;
                if (command.initiatingPeerID) {
                    // This is a command that came from a peer. Have the sync node send the response back to the peer.
                    server._finishPeerCommand(command);
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

            // Prevent the requestID from a finished command from being used.
            command.request.clear();
        }

        // We're either mastering, standing down, or slaving. There could be a commit in progress on `command`, but
        // there could also be other finished work to handle while we wait for that to complete. Let's see if we can
        // handle any of that work.
        try {
            // If there are any completed commands to respond to, we'll do that first.
            try {
                while (true) {
                    BedrockCommand completedCommand = completedCommands.pop();
                    SAUTOPREFIX(completedCommand.request["requestID"]);
                    SASSERT(completedCommand.complete);
                    SASSERT(completedCommand.initiatingPeerID);
                    SASSERT(!completedCommand.initiatingClientID);
                    server._finishPeerCommand(completedCommand);
                }
            } catch (const out_of_range& e) {
                // when completedCommands.pop() throws for running out of commands, we fall out of the loop.
            }

            // We don't start processing a new command until we've completed any existing ones.
            if (committingCommand) {
                continue;
            }

            // Get the next sync node command to work on.
            command = syncNodeQueuedCommands.pop();

            // We got a command to work on! Set our log prefix to the request ID.
            SAUTOPREFIX(command.request["requestID"]);
            SINFO("[performance] Sync thread dequeued command " << command.request.methodLine << ". Sync thread has "
                  << syncNodeQueuedCommands.size() << " queued commands.");

            // Set the function that will be called if this thread's signal handler catches an unrecoverable error,
            // like a segfault. Note that it's possible we're in the middle of sending a message to peers when we call
            // this, which would probably make this message malformed. This is the best we can do.
            SSetSignalHandlerDieFunc([&](){
                server._syncNode->broadcast(_generateCrashMessage(&command));
            });

            // And now we'll decide how to handle it.
            if (nodeState == SQLiteNode::MASTERING || nodeState == SQLiteNode::STANDINGDOWN) {

                // We need to grab this before peekCommand (or wherever our transaction is started), to verify that
                // no worker thread can commit in the middle of our transaction. We need our entire transaction to
                // happen with no other commits to ensure that we can't get a conflict.
                uint64_t beforeLock = STimeNow();

                // This needs to be done before we acquire _syncThreadCommitMutex or we can deadlock.
                db.waitForCheckpoint();
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
                            server._finishPeerCommand(command);
                        } else {
                            server._reply(command);
                        }
                        continue;
                    }

                    // If we just started a new HTTPS request, save it for later.
                    if (command.httpsRequest) {
                        // Save this in our https commads queue.
                        lock_guard<mutex> lock(server._httpsCommandMutex);
                        server._outstandingHTTPSRequests.emplace(make_pair(command.httpsRequest, move(command)));

                        // Move on to the next command until this one finishes.
                        core.rollback();
                        server._syncThreadCommitMutex.unlock();
                        continue;
                    }
                }

                if (core.processCommand(command)) {
                    // The processor says we need to commit this, so let's start that process.
                    committingCommand = true;
                    SINFO("[performance] Sync thread beginning committing command " << command.request.methodLine);
                    // START TIMING.
                    command.startTiming(BedrockCommand::COMMIT_SYNC);
                    server._syncNode->startCommit(command.writeConsistency);

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
                        server._finishPeerCommand(command);
                    } else {
                        server._reply(command);
                    }
                }
            } else if (nodeState == SQLiteNode::SLAVING) {
                // If we're slaving, we just escalate directly to master without peeking. We can only get an incomplete
                // command on the slave sync thread if a slave worker thread peeked it unsuccessfully, so we don't
                // bother peeking it again.
                auto it = command.request.nameValueMap.find("Connection");
                bool forget = it != command.request.nameValueMap.end() && SIEquals(it->second, "forget");
                server._syncNode->escalateCommand(move(command), forget);
                if (forget) {
                    // Command is no longer in progress.
                    server._commandsInProgress--;
                }
            }
        } catch (const out_of_range& e) {
            // Prevent the requestID from a finished command from being used.
            command.request.clear();

            // syncNodeQueuedCommands had no commands to work on, we'll need to re-poll for some.
            continue;
        }
    } while (!server._syncNode->shutdownComplete() && !server._gracefulShutdownTimeout.ringing());

    SSetSignalHandlerDieFunc([](){SWARN("Dying in shutdown");});

    // If we forced a shutdown mid-transaction (this can happen, if, for instance, we hit our graceful timeout between
    // getting a `BEGIN_TRANSACTION` and `COMMIT_TRANSACTION`) then we need to roll back the existing transaction and
    // release the lock.
    if (server._syncNode->commitInProgress()) {
        SWARN("Shutting down mid-commit. Rolling back.");
        db.rollback();
        server._syncThreadCommitMutex.unlock();
    }

    // Done with the global lock.
    server._syncMutex.unlock();

    // We've finished shutting down the sync node, tell the workers that it's finished.
    server._shutdownState.store(DONE);
    SINFO("Sync thread finished with commands.");

    // We just fell out of the loop where we were waiting for shutdown to complete. Update the state one last time when
    // the writing replication thread exits.
    replicationState.store(server._syncNode->getState());
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

    // Release our handle to this pointer. Any other functions that are still using it will keep the object alive
    // until they return.
    server._syncNode = nullptr;

    // We're really done, store our flag so main() can be aware.
    server._syncThreadComplete.store(true);
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
    SQLite db(args["-db"], args.calc("-cacheSize"), false, args.calc("-maxJournalSize"), threadId, threadCount - 1, args["-synchronous"]);
    BedrockCore core(db, server);

    // Command to work on. This default command is replaced when we find work to do.
    BedrockCommand command;

    // We just run this loop looking for commands to process forever. There's a check for appropriate exit conditions
    // at the bottom, which will cause our loop and thus this thread to exit when that becomes true.
    while (true) {
        try {
            // Set a signal handler function that we can call even if we die early with no command.
            SSetSignalHandlerDieFunc([&](){
                SWARN("Die function called early with no command, probably died in `getSynchronized`.");
            });

            // If we can't find any work to do, this will throw. If we can, this will increment _commandsInProgress for
            // us before returning the command that it is dequeuing. We don't update _commandsInProgress before calling
            // this, as it can spend up to a second finding out that there is no command to dequeue, which makes our
            // count wrong while we wait.
            command = server._commandQueue.getSynchronized(1000000, server._commandsInProgress);

            SAUTOPREFIX(command.request["requestID"]);
            SINFO("[performance] Dequeued command " << command.request.methodLine << " in worker, "
                  << server._commandQueue.size() << " commands in queue.");

            // Set the function that lets the signal handler know which command caused a problem, in case that happens.
            // If a signal is caught on this thread, which should only happen for unrecoverable, yet synchronous
            // signals, like SIGSEGV, this function will be called.
            SSetSignalHandlerDieFunc([&](){
                server._syncNode->broadcast(_generateCrashMessage(&command));
            });

            // If we dequeue a status or control command, handle it immediately.
            if (server._handleIfStatusOrControlCommand(command)) {
                server._commandsInProgress--;
                continue;
            }

            // Check if this command would be likely to cause a crash
            if (server._wouldCrash(command)) {
                // If so, make a lot of noise, and respond 500 without processing it.
                SALERT("CRASH-INDUCING COMMAND FOUND: " << command.request.methodLine);
                command.response.methodLine = "500 Refused";
                command.complete = true;
                if (command.initiatingPeerID) {
                    // Escalated command. Give it back to the sync thread to respond.
                    syncNodeCompletedCommands.push(move(command));
                } else {
                    server._reply(command);
                }
                continue;
            }

            // If this was a command initiated by a peer as part of a cluster operation, then we process it separately
            // and respond immediately. This allows SQLiteNode to offload read-only operations to worker threads.
            if (SQLiteNode::peekPeerCommand(server._syncNode.get(), db, command)) {
                server._commandsInProgress--;
                // Move on to the next command.
                continue;
            }

            // We just spin until the node looks ready to go. Typically, this doesn't happen expect briefly at startup.
            while (upgradeInProgress.load() ||
                   (replicationState.load() != SQLiteNode::MASTERING &&
                    replicationState.load() != SQLiteNode::SLAVING &&
                    replicationState.load() != SQLiteNode::STANDINGDOWN)
            ) {
                // Make sure that the node isn't shutting down, leaving us in an endless loop.
                if (server._shutdownState.load() != RUNNING) {
                    SWARN("Sync thread shut down while were waiting for it to come up. Discarding command '"
                          << command.request.methodLine << "'.");
                    return;
                }

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
                server._commandsInProgress--;
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
                    server._commandsInProgress--;
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

            if (command.request.isSet("mockRequest")) {
                SINFO("mockRequest set for command '" << command.request.methodLine << "'.");
            }

            // We'll retry on conflict up to this many times.
            int retry = 3;

            // We check first, and allow this command to retry all three times, even if it becomes disallowed during
            // iteration.
            bool multiWriteOK = BedrockConflictMetrics::multiWriteOK(command.request.methodLine);
            while (retry) {
                // Block if a checkpoint is happening so we don't interrupt it.
                db.waitForCheckpoint();

                // If the command doesn't already have an httpsRequest from a previous peek attempt, try peeking it
                // now. We don't duplicate peeks for commands that make https requests.
                // If peek succeeds, then it's finished, and all we need to do is respond to the command at the bottom.
                bool calledPeek = false;
                bool peekResult = false;
                if (!command.httpsRequest) {
                    peekResult = core.peekCommand(command);
                    calledPeek = true;
                }

                if (!calledPeek || !peekResult) {
                    // We've just unsuccessfully peeked a command, which means we're in a state where we might want to
                    // write it. We'll flag that here, to keep the node from falling out of MASTERING/STANDINGDOWN
                    // until we're finished with this command.
                    if (command.httpsRequest) {
                        // This *should* be impossible, but previous bugs have existed where it's feasible that we call
                        // `peekCommand` while mastering, and by the time we're done, we're SLAVING, so we check just
                        // in case we ever introduce another similar bug.
                        if (state != SQLiteNode::MASTERING && state != SQLiteNode::STANDINGDOWN) {
                            SALERT("Not mastering or standing down (" << SQLiteNode::stateNames[state]
                                   << ") but have outstanding HTTPS command: " << command.request.methodLine
                                   << ", returning 500.");
                            command.response.methodLine = "500 STANDDOWN TIMEOUT";
                            server._reply(command);
                            core.rollback();
                            break;
                        }

                        // If the command isn't complete, we'll move it into our map of outstanding HTTPS requests.
                        if (!command.httpsRequest->response) {
                            // Roll back the existing transaction, but only if we are inside an transaction
                            if (calledPeek) {
                                core.rollback();
                            }

                            // We're not handling a writable command anymore (at the moment). We need to make sure we
                            // don't shut down without checking for outstanding HTTPS commands.
                            lock_guard<mutex> lock(server._httpsCommandMutex);
                            SINFO("[performance] Waiting on HTTPS response " << command.request.methodLine
                                  << ", " << server._outstandingHTTPSRequests.size() << " queued HTTPS commands.");

                            // Save this in our https commads queue.
                            server._outstandingHTTPSRequests.emplace(make_pair(command.httpsRequest, move(command)));

                            // Move on to the next command until this one finishes.
                            break;
                        }
                    }
                    // Peek wasn't enough to handle this command. Now we need to decide if we should try and process
                    // it, or if we should send it off to the sync node.
                    bool canWriteParallel = server._multiWriteEnabled.load();
                    if (canWriteParallel) {
                        // If multi-write is enabled, then we need to make sure the command isn't blacklisted.
                        SAUTOLOCK(_blacklistedParallelCommandMutex);
                        canWriteParallel =
                            (_blacklistedParallelCommands.find(command.request.methodLine) == _blacklistedParallelCommands.end());
                    }

                    // We need to have multi-write enabled, the command needs to not be explicitly blacklisted, and it
                    // needs to not be automatically blacklisted.
                    canWriteParallel = canWriteParallel && multiWriteOK;
                    if (!canWriteParallel                 ||
                        server._suppressMultiWrite.load() ||
                        state != SQLiteNode::MASTERING    ||
                        command.onlyProcessOnSyncThread   ||
                        command.writeConsistency != SQLiteNode::ASYNC)
                    {
                        // Roll back the transaction, it'll get re-run in the sync thread.
                        core.rollback();

                        // We're not handling a writable command anymore.
                        SINFO("[performance] Sending non-parallel command " << command.request.methodLine
                              << " to sync thread. Sync thread has " << syncNodeQueuedCommands.size()
                              << " queued commands.");
                        syncNodeQueuedCommands.push(move(command));

                        // We'll break out of our retry loop here, as we don't need to do anything else, we can just
                        // look for another command to work on.
                        break;
                    } else {
                        // In this case, there's nothing blocking us from processing this in a worker, so let's try it.
                        if (core.processCommand(command)) {
                            // If processCommand returned true, then we need to do a commit. Otherwise, the command is
                            // done, and we just need to respond. Before we commit, we need to grab the sync thread
                            // lock. Because the sync thread grabs an exclusive lock on this wrapping any transactions
                            // that it performs, we'll get this lock while the sync thread isn't in the process of
                            // handling a transaction, thus guaranteeing that we can't commit and cause a conflict on
                            // the sync thread. We can still get conflicts here, as the sync thread might have
                            // performed a transaction after we called `processCommand` and before we call `commit`,
                            // or we could conflict with another worker thread, but the sync thread will never see a
                            // conflict as long as we don't commit while it's performing a transaction. This is scoped
                            // to the minimum time required.
                            bool commitSuccess = false;
                            {
                                uint64_t preLockTime = STimeNow();
                                shared_lock<decltype(server._syncThreadCommitMutex)> lock1(server._syncThreadCommitMutex);
                                SINFO("_syncThreadCommitMutex acquired in worker in " << fixed << setprecision(2)
                                      << ((STimeNow() - preLockTime)/1000.0) << "ms.");

                                // This is the first place we get really particular with the state of the node from a
                                // worker thread. We only want to do this commit if we're *SURE* we're mastering, and
                                // not allow the state of the node to change while we're committing. If it turns out
                                // we've changed states, we'll roll this command back, so we lock the node's state
                                // until we complete.
                                //
                                // IMPORTANT: If we acquire both _syncThreadCommitMutex and stateMutex, they always
                                // need to be locked in that order. The reason for this is that it's possible for the
                                // sync thread to to change states mid-commit, meaning that it needs to acquire these
                                // locks in the same order. Always acquiring the locks in the same order prevents the
                                // deadlocks.
                                shared_lock<decltype(server._syncNode->stateMutex)> lock2(server._syncNode->stateMutex);
                                if (replicationState.load() != SQLiteNode::MASTERING &&
                                    replicationState.load() != SQLiteNode::STANDINGDOWN) {
                                    SALERT("Node State changed from MASTERING to "
                                           << SQLiteNode::stateNames[replicationState.load()]
                                           << " during worker commit. Rolling back transaction!");
                                    core.rollback();
                                } else {
                                    BedrockCore::AutoTimer(command, BedrockCommand::COMMIT_WORKER);
                                    commitSuccess = core.commit();
                                }
                            }
                            if (commitSuccess) {
                                BedrockConflictMetrics::recordSuccess(command.request.methodLine);
                                SINFO("Successfully committed " << command.request.methodLine << " on worker thread.");
                                // So we must still be mastering, and at this point our commit has succeeded, let's
                                // mark it as complete. We add the currentCommit count here as well.
                                command.response["commitCount"] = to_string(db.getCommitCount());
                                command.complete = true;
                            } else {
                                SINFO("Conflict or state change committing " << command.request.methodLine
                                      << " on worker thread with " << retry << " retries remaining.");
                            }
                        }
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
                BedrockConflictMetrics::recordConflict(command.request.methodLine);
                SINFO("[performance] Max retries hit in worker, forwarding command " << command.request.methodLine
                      << " to sync thread. Sync thread has " << syncNodeQueuedCommands.size() << " queued commands.");
                syncNodeQueuedCommands.push(move(command));
            }
        } catch (const BedrockCommandQueue::timeout_error& e) {
            // No commands to process after 1 second.
            // If the sync node has shut down, we can return now, there will be no more work to do.
            if  (server._shutdownState.load() == DONE) {
                SINFO("No commands found in queue and DONE.");
                return;
            }
        }

        // If we hit the timeout, doesn't matter if we've got work to do. Exit.
        if (server._gracefulShutdownTimeout.ringing()) {
            SINFO("_shutdownState is DONE and we've timed out, exiting worker.");
            return;
        }
    }
}

bool BedrockServer::_handleIfStatusOrControlCommand(BedrockCommand& command) {
    if (_isStatusCommand(command)) {
        _commandsInProgress++;
        _status(command);
        _reply(command);
        return true;
    } else if (_isControlCommand(command)) {
        _commandsInProgress++;
        // Control commands can only come from localhost (and thus have an empty `_source`).
        if (command.request["_source"].empty()) {
            _control(command);
        } else {
            SWARN("Got control command " << command.request.methodLine << " on non-localhost socket ("
                  << command.request["_source"] << "). Ignoring.");
            command.response.methodLine = "401 Unauthorized";
        }
        _reply(command);
        return true;
    }
    return false;
}

bool BedrockServer::_wouldCrash(const BedrockCommand& command) {
    // Get a shared lock so that all the workers can look at this map simultaneously.
    shared_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);

    // Typically, this map is empty and this returns no results.
    auto commandIt = _crashCommands.find(command.request.methodLine);
    if (commandIt == _crashCommands.end()) {
        return false;
    }

    // Look at each crash-inducing command that has the same methodLine.
    for (const STable& values : commandIt->second) {

        // These are all of the keys that need to match to kill this command.
        bool isMatch = true;
        for (auto& pair : values) {
            // We skip Content-Length, as it's added automatically when serializing commands.
            if (SIEquals(pair.first, "Content-Length")) {
                continue;
            }

            // See if our current command even has the blacklisted key.
            auto it = command.request.nameValueMap.find(pair.first);
            if (it ==  command.request.nameValueMap.end()) {
                // If we didn't find it, the command's not sufficiently similar, and is not blacklisted.
                isMatch = false;
                break;
            }

            // At this point, we must have the same key, but if it doesn't have the same value, then it doesn't match.
            if (it->second != pair.second) {
                isMatch = false;
                break;
            }
        }

        // If we got through the whole list and everything was a match, then this is a match, we think it'll crash.
        if (isMatch) {
            return true;
        }
    }

    // If nothing in our range returned true, then this command looks fine.
    return false;
}


BedrockServer::BedrockServer(const SData& args)
  : SQLiteServer(""), _args(args), _requestCount(0), _replicationState(SQLiteNode::SEARCHING),
    _upgradeInProgress(false), _suppressCommandPort(false), _suppressCommandPortManualOverride(false),
    _syncThreadComplete(false), _syncNode(nullptr), _suppressMultiWrite(true), _shutdownState(RUNNING),
    _multiWriteEnabled(args.test("-enableMultiWrite")), _backupOnShutdown(false), _detach(false),
    _controlPort(nullptr), _commandPort(nullptr)
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
    // We'll destruct, sort, and then reconstruct the version string passed in so we aren't relying
    // on the operator to know that they must be sorted.
    if (args.isSet("-versionOverride")) {
        list<string> versionStrings = SParseList(args["-versionOverride"], ':');
        versionStrings.sort();
        _version = SComposeList(versionStrings, ":");
    }

    // Check for commands that will be forced to use QUORUM write consistency.
    if (args.isSet("-synchronousCommands")) {
        list<string> syncCommands;
        SParseList(args["-synchronousCommands"], syncCommands);
        for (auto& command : syncCommands) {
            _syncCommands.insert(command);
        }
    }

    // Check for commands that can't be written by workers.
    if (args.isSet("-blacklistedParallelCommands")) {
        SAUTOLOCK(_blacklistedParallelCommandMutex);
        list<string> parallelCommands;
        SParseList(args["-blacklistedParallelCommands"], parallelCommands);
        for (auto& command : parallelCommands) {
            _blacklistedParallelCommands.insert(command);
        }
    }

    // Allow sending control commands when the server's not MASTERING/SLAVING.
    SINFO("Opening control port on '" << _args["-controlPort"] << "'");
    _controlPort = openPort(_args["-controlPort"]);

    // Start the sync thread, which will start the worker threads.
    SINFO("Launching sync thread '" << _syncThreadName << "'");
    _syncThread = thread(syncWrapper,
                     ref(_args),
                     ref(_replicationState),
                     ref(_upgradeInProgress),
                     ref(_masterVersion),
                     ref(_syncNodeQueuedCommands),
                     ref(*this));
}

BedrockServer::~BedrockServer() {
    // Shut down the sync thread, (which will shut down worker threads in turn).
    SINFO("Closing sync thread '" << _syncThreadName << "'");
    _syncThread.join();
    SINFO("Threads closed.");

    // Close any sockets that are still open. We wait until the sync thread has completed to do this, as until it's
    // finished, it may keep writing to these sockets.
    SAUTOLOCK(_socketIDMutex);
    if (_socketIDMap.size()) {
        SWARN("Still have " << _socketIDMap.size() << " entries in _socketIDMap.");
    }

    for (list<Socket*>::iterator socketIt = socketList.begin(); socketIt != socketList.end();) {
        // Shut it down and go to the next (because closeSocket will invalidate this iterator otherwise)
        Socket* s = *socketIt++;
        closeSocket(s);
    }
    SINFO("Sockets closed.");
}

bool BedrockServer::shutdownComplete() {
    if (_detach) {
        // We don't want main() to stop calling `poll` for us, we are listening on the control port.
        return false;
    }

    // If the sync thread is finished, we're finished.
    if (_syncThreadComplete) {
        return true;
    }

    // We have hit our timeout. This will force the sync thread to exit, so we should hit the above criteria
    // (_syncThreadComplete) in the next loop or two.
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
              << "Command queue size: " << _commandQueue.size() << ". "
              << "Commands in progress: " << _commandsInProgress.load() << ". "
              << "Command Counts: " << commandCounts << "killing non gracefully.");
    }

    // We wait until the sync thread returns.
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
        suppressCommandPort("master version mismatch", true);
    } else if (_suppressCommandPort && (state == SQLiteNode::MASTERING || (masterVersion == _version))) {
        // If we become master, or if master's version resumes matching ours, open the command port again.
        if (!_suppressCommandPortManualOverride) {
            // Only generate this logline if we haven't manually blocked this.
            SINFO("Node " << _args["-nodeName"] << " disabling previously suppressed command port after version check.");
        }
        suppressCommandPort("master version match", false);
    }
    if (!_suppressCommandPort && (state == SQLiteNode::MASTERING || state == SQLiteNode::SLAVING) &&
        _shutdownState.load() == RUNNING) {
        // Open the port
        if (!_commandPort) {
            SINFO("Ready to process commands, opening command port on '" << _args["-serverHost"] << "'");
            _commandPort = openPort(_args["-serverHost"]);
        }
        if (!_controlPort) {
            SINFO("Opening control port on '" << _args["-controlPort"] << "'");
            _controlPort = openPort(_args["-controlPort"]);
        }

        // Open any plugin ports on enabled plugins
        for (auto plugin : plugins) {
            string portHost = plugin->getPort();
            if (!portHost.empty()) {
                bool alreadyOpened = false;
                for (auto pluginPorts : _portPluginMap) {
                    if (pluginPorts.second == plugin) {
                        // We've already got this one.
                        alreadyOpened = true;
                        break;
                    }
                }
                // Open the port and associate it with the plugin
                if (!alreadyOpened) {
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
    if (SGetSignals()) {
        if (SGetSignal(SIGTTIN)) {
            // Suppress command port, but only if we haven't already cleared it
            if (!SCheckSignal(SIGTTOU)) {
                suppressCommandPort("SIGTTIN", true, true);
            }
        } else if (SGetSignal(SIGTTOU)) {
            // Clear command port suppression
            suppressCommandPort("SIGTTOU", false, true);
        } else {
            // For any other signal, just shutdown.
            _beginShutdown(SGetSignalDescription());
        }
    }

    // Timing variables.
    int deserializationAttempts = 0;
    int deserializedRequests = 0;

    // Accept any new connections
    _acceptSockets();

    // Time the end of the accept section.
    uint64_t acceptEndTime = STimeNow();

    // Process any new activity from incoming sockets. In order to not modify the socket list while we're iterating
    // over it, we'll keep a list of sockets that need closing.
    list<STCPManager::Socket*> socketsToClose;

    // This is a timestamp, after which we'll start giving up on any sockets that don't seem to be giving us any data.
    // The case for this is that once we start shutting down, we'll close any sockets when we respond to a command on
    // them, and we'll stop accepting any new sockets, but if existing sockets just sit around giving us nothing, we
    // need to figure out some way to handle them. We'll wait 5 seconds and then start killing them.
    static uint64_t lastChance = 0;
    for (auto s : socketList) {
        switch (s->state.load()) {
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
                // If we're shutting down and past our lastChance timeout, we start killing these.
                SAUTOLOCK(_socketIDMutex);
                if (_shutdownState.load() != RUNNING && lastChance && lastChance < STimeNow() && _socketIDMap.find(s->id) == _socketIDMap.end()) {
                    SINFO("Closing socket " << s->id << " with no data and no pending command: shutting down.");
                    socketsToClose.push_back(s);
                } else if (s->recvBuffer.empty()) {
                    // If nothing's been received, break early.
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
                    deserializationAttempts++;
                }

                // If we have a populated request, from either a plugin or our default handling, we'll queue up the
                // command.
                if (!request.empty()) {
                    SAUTOPREFIX(request["requestID"]);
                    deserializedRequests++;
                    // Either shut down the socket or store it so we can eventually sync out the response.
                    if (SIEquals(request["Connection"], "forget") ||
                        (uint64_t)request.calc64("commandExecuteTime") > STimeNow()) {
                        // Respond immediately to make it clear we successfully queued it, but don't add to the socket
                        // map as we don't care about the answer.
                        SINFO("Firing and forgetting '" << request.methodLine << "'");
                        SData response("202 Successfully queued");
                        if (_shutdownState.load() != RUNNING) {
                            response["Connection"] = "close";
                        }
                        s->send(response.serialize());

                        // If we're shutting down, discard this command, we won't wait for the future.
                        if (_shutdownState.load() != RUNNING) {
                            SINFO("Not queuing future command '" << request.methodLine << "' while shutting down.");
                            break;
                        }
                    } else {
                        SINFO("Waiting for '" << request.methodLine << "' to complete.");
                        SAUTOLOCK(_socketIDMutex);
                        _socketIDMap[s->id] = s;
                    }

                    // Create a command.
                    BedrockCommand command(request);

                    // Get the source ip of the command.
                    char *ip = inet_ntoa(s->addr.sin_addr);
                    if (ip != "127.0.0.1"s) {
                        // We only add this if it's not localhost because existing code expects commands that come from
                        // localhost to have it blank.
                        command.request["_source"] = ip;
                    }

                    if (command.writeConsistency != SQLiteNode::QUORUM
                        && _syncCommands.find(command.request.methodLine) != _syncCommands.end()) {

                        command.writeConsistency = SQLiteNode::QUORUM;
                        SINFO("Forcing QUORUM consistency for command " << command.request.methodLine);
                    }

                    // This is important! All commands passed through the entire cluster must have unique IDs, or they
                    // won't get routed properly from slave to master and back.
                    command.id = _args["-nodeName"] + "#" + to_string(_requestCount++);

                    // And we and keep track of the client that initiated this command, so we can respond later, except
                    // if we received connection:forget in which case we don't respond later
                    command.initiatingClientID = SIEquals(request["Connection"], "forget") ? -1 : s->id;

                    // If it's a status or control command, we handle it specially there. If not, we'll queue it for
                    // later processing.
                    if (!_handleIfStatusOrControlCommand(command)) {
                        auto _syncNodeCopy = _syncNode;
                        if (_syncNodeCopy && _syncNodeCopy->getState() == SQLiteNode::STANDINGDOWN) {
                            _standDownQueue.push(move(command));
                        } else {
                            SINFO("Queued new '" << command.request.methodLine << "' command from local client, with "
                                  << _commandQueue.size() << " commands already queued.");
                            _commandQueue.push(move(command));
                        }
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

    // Log the timing of this loop.
    uint64_t readElapsedMS = (STimeNow() - acceptEndTime) / 1000;
    SINFO("Read from " << socketList.size() << " sockets, attempted to deserialize " << deserializationAttempts
          << " commands, " << deserializedRequests << " were complete and deserialized in " << readElapsedMS << "ms.");

    // Now we can close any sockets that we need to.
    for (auto s: socketsToClose) {
        SAUTOLOCK(_socketIDMutex);
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

    // If we've been told to start shutting down, we'll set the lastChance timer.
    if (_shutdownState.load() == START_SHUTDOWN) {
        if (!lastChance) {
            lastChance = STimeNow() + 5 * 1'000'000; // 5 seconds from now.
        }
        // If we've run out of sockets or hit our timeout, we'll increment _shutdownState.
        if (socketList.empty() || _gracefulShutdownTimeout.ringing()) {
            lastChance = 0;
            _shutdownState.store(CLIENTS_RESPONDED);
        }
    }
}

void BedrockServer::_reply(BedrockCommand& command) {
    SAUTOLOCK(_socketIDMutex);

    // Finalize timing info even for commands we won't respond to (this makes this data available in logs).
    command.finalizeTimingInfo();

    // Don't reply to commands with pseudo-clients (i.e., commands that we generated by other commands).
    if (command.initiatingClientID < 0) {
        _commandsInProgress--;
        return;
    }

    // Do we have a socket for this command?
    auto socketIt = _socketIDMap.find(command.initiatingClientID);
    if (socketIt != _socketIDMap.end()) {
        command.response["nodeName"] = _args["-nodeName"];

        // Is a plugin handling this command? If so, it gets to send the response.
        string& pluginName = command.request["plugin"];

        // If we're shutting down, tell the caller to close the connection.
        if (_shutdownState.load() != RUNNING) {
            command.response["Connection"] = "close";
        }

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

        // If `Connection: close` was set, shut down the socket, in case the caller ignores us.
        if (SIEquals(command.request["Connection"], "close") || _shutdownState.load() != RUNNING) {
            shutdownSocket(socketIt->second, SHUT_RDWR);
        }

        // We only keep track of sockets with pending commands.
        _socketIDMap.erase(socketIt);
    }
    else if (!SIEquals(command.request["Connection"], "forget")) {
        SINFO("No socket to reply for: '" << command.request.methodLine << "' #" << command.initiatingClientID);
    }
    _commandsInProgress--;
}

void BedrockServer::suppressCommandPort(const string& reason, bool suppress, bool manualOverride) {
    // If we've set the manual override flag, then we'll only actually make this change if we've specified it again.
    if (_suppressCommandPortManualOverride && !manualOverride) {
        return;
    }
    SINFO((suppress ? "Suppressing" : "Clearing") << " command port due to: " << reason);

    // Save the state of manual override. Note that it's set to *suppress* on purpose.
    if (manualOverride) {
        _suppressCommandPortManualOverride = suppress;
    }
    // Process accordingly
    _suppressCommandPort = suppress;
    if (suppress) {
        // Close the command port, and all plugin's ports. Won't reopen.
        SHMMM("Suppressing command port");
        if (!portList.empty()) {
            closePorts({_controlPort});
            _portPluginMap.clear();
            _commandPort = nullptr;
        }
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
        SIEquals(command.request.methodLine, STATUS_BLACKLIST)         ||
        SIEquals(command.request.methodLine, STATUS_MULTIWRITE)) {
        return true;
    }
    return false;
}

list<STable> BedrockServer::getPeerInfo() {
    SAUTOLOCK(_syncMutex);
    list<STable> peerData;
    auto _syncNodeCopy = _syncNode;
    if (_syncNodeCopy) {
        for (SQLiteNode::Peer* peer : _syncNodeCopy->peerList) {
            peerData.emplace_back(peer->nameValueMap);
            peerData.back()["host"] = peer->host;
            peerData.back()["name"] = peer->name;
        }
    }
    return peerData;
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

        {
            // Make it known if anything is known to cause crashes.
            shared_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
            size_t totalCount = 0;
            for (const auto& s : _crashCommands) {
                totalCount += s.second.size();
            }
            content["crashCommands"] = totalCount;
        }

        // On master, return the current multi-write blacklists.
        if (state == SQLiteNode::MASTERING) {
            // Both of these need to be in the correct state for multi-write to be enabled.
            bool multiWriteOn =  _multiWriteEnabled.load() && !_suppressMultiWrite;
            content["multiWriteEnabled"] = multiWriteOn ? "true" : "false";
            content["multiWriteAutoBlacklist"] = BedrockConflictMetrics::getMultiWriteDeniedCommands();
            content["multiWriteManualBlacklist"] = SComposeJSONArray(_blacklistedParallelCommands);
        }

        // We read from syncNode internal state here, so we lock to make sure that this doesn't conflict with the sync
        // thread.
        list<STable> peerData = getPeerInfo();
        list<string> escalated;
        {
            SAUTOLOCK(_syncMutex);

            // There's no syncNode when the server is detached, so we can't get this data.
            auto _syncNodeCopy = _syncNode;
            if (_syncNodeCopy) {
                content["syncNodeAvailable"] = "true";
                // Set some information about this node.
                content["CommitCount"] = to_string(_syncNodeCopy->getCommitCount());
                content["priority"] = to_string(_syncNodeCopy->getPriority());

                // Get any escalated commands that are waiting to be processed.
                escalated = _syncNodeCopy->getEscalatedCommandRequestMethodLines();
            } else {
                content["syncNodeAvailable"] = "false";
            }
        }

        // Coalesce all of the peer data into one value to return.
        list<string> peerList;
        for (const STable& peerTable : peerData) {
            peerList.push_back(SComposeJSONObject(peerTable));
        }

        // We can use the `each` functionality to pass a lambda that will grab each method line in
        // `_syncNodeQueuedCommands`.
        list<string> syncNodeQueuedMethods;
        _syncNodeQueuedCommands.each([&syncNodeQueuedMethods](auto& item){
            syncNodeQueuedMethods.push_back(item.request.methodLine);
        });
        content["peerList"]                    = SComposeJSONArray(peerList);
        content["queuedCommandList"]           = SComposeJSONArray(_commandQueue.getRequestMethodLines());
        content["syncThreadQueuedCommandList"] = SComposeJSONArray(syncNodeQueuedMethods);
        content["escalatedCommandList"]        = SComposeJSONArray(escalated);

        // Done, compose the response.
        response.methodLine = "200 OK";
        response.content = SComposeJSONObject(content);
    }

    else if (SIEquals(request.methodLine, STATUS_BLACKLIST)) {
        SAUTOLOCK(_blacklistedParallelCommandMutex);

        // Return the old list. We can check the list by not passing the "Commands" param.
        STable content;
        content["oldCommandBlacklist"] = SComposeList(_blacklistedParallelCommands);

        // If the Commands param is set, parse it and update our value.
        if (request.isSet("Commands")) {
            _blacklistedParallelCommands.clear();
            list<string> parallelCommands;
            SParseList(request["Commands"], parallelCommands);
            for (auto& command : parallelCommands) {
                _blacklistedParallelCommands.insert(command);
            }
        }
        if (request.isSet("autoBlacklistConflictFraction")) {
            BedrockConflictMetrics::setFraction(SToFloat(request["autoBlacklistConflictFraction"]));
        }

        // Prepare the command to respond to the caller.
        response.methodLine = "200 OK";
        response.content = SComposeJSONObject(content);
    } else if (SIEquals(request.methodLine, STATUS_MULTIWRITE)) {
        if (request.isSet("Enable")) {
            _multiWriteEnabled.store(request.test("Enable"));
            response.methodLine = "200 OK";
        } else {
            response.methodLine = "500 Must Specify 'Enable'";
        }
    }
}

bool BedrockServer::_isControlCommand(BedrockCommand& command) {
    if (SIEquals(command.request.methodLine, "BeginBackup")            ||
        SIEquals(command.request.methodLine, "SuppressCommandPort")    ||
        SIEquals(command.request.methodLine, "ClearCommandPort")       ||
        SIEquals(command.request.methodLine, "ClearCrashCommands")     ||
        SIEquals(command.request.methodLine, "Detach")                 ||
        SIEquals(command.request.methodLine, "Attach")                 ||
        SIEquals(command.request.methodLine, "SetCheckpointIntervals")
        ) {
        return true;
    }
    return false;
}

void BedrockServer::_control(BedrockCommand& command) {
    SData& response = command.response;
    response.methodLine = "200 OK";
    if (SIEquals(command.request.methodLine, "BeginBackup")) {
        _backupOnShutdown = true;
        _beginShutdown("BeginBackup");
    } else if (SIEquals(command.request.methodLine, "SuppressCommandPort")) {
        suppressCommandPort("SuppressCommandPort", true, true);
    } else if (SIEquals(command.request.methodLine, "ClearCommandPort")) {
        suppressCommandPort("ClearCommandPort", false, true);
    } else if (SIEquals(command.request.methodLine, "ClearCrashCommands")) {
        unique_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
        _crashCommands.clear();
    } else if (SIEquals(command.request.methodLine, "Detach")) {
        response.methodLine = "203 DETACHING";
        _beginShutdown("Detach", true);
    } else if (SIEquals(command.request.methodLine, "Attach")) {
        response.methodLine = "204 ATTACHING";
        _detach = false;
    } else if (SIEquals(command.request.methodLine, "SetCheckpointIntervals")) {
        response["passiveCheckpointPageMin"] = to_string(SQLite::passiveCheckpointPageMin.load());
        response["fullCheckpointPageMin"] = to_string(SQLite::fullCheckpointPageMin.load());
        if (command.request.isSet("passiveCheckpointPageMin")) {
            SQLite::passiveCheckpointPageMin.store(command.request.calc("passiveCheckpointPageMin"));
        }
        if (command.request.isSet("fullCheckpointPageMin")) {
            SQLite::fullCheckpointPageMin.store(command.request.calc("fullCheckpointPageMin"));
        }
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
            list<SHTTPSManager::Transaction*> completedHTTPSRequests;
            auto _syncNodeCopy = _syncNode;
            if (_shutdownState.load() != RUNNING || (_syncNodeCopy && _syncNodeCopy->getState() == SQLiteNode::STANDINGDOWN)) {
                // If we're shutting down or standing down, we can't wait minutes for HTTPS requests. They get 5s.
                manager->postPoll(fdm, nextActivity, completedHTTPSRequests, 5000);
            } else {
                // Otherwise, use the default timeout.
                manager->postPoll(fdm, nextActivity, completedHTTPSRequests);
            }

            // Move these back to the main queue to finish.
            lock_guard<mutex> lock(_httpsCommandMutex);
            for (auto request : completedHTTPSRequests) {
                auto pairIt = _outstandingHTTPSRequests.find(request);
                if (pairIt != _outstandingHTTPSRequests.end()) {
                    _commandQueue.push(move(pairIt->second));
                    _commandsInProgress--;
                    _outstandingHTTPSRequests.erase(pairIt);
                } else {
                    SWARN("HTTPS request said it completed, but we can't find it.");
                }
            }
        }
    }
}

void BedrockServer::_beginShutdown(const string& reason, bool detach) {
    if (_shutdownState.load() == RUNNING) {
        _detach = detach;
        // Begin a graceful shutdown; close our port
        SINFO("Beginning graceful shutdown due to '" << reason
              << "', closing command port on '" << _args["-serverHost"] << "'");
        _gracefulShutdownTimeout.alarmDuration = STIME_US_PER_S * 60; // 60s timeout before we give up
        _gracefulShutdownTimeout.start();

        // Delete any commands scheduled in the future.
        _commandQueue.abandonFutureCommands(5000);

        // Accept any new connections before closing, this avoids leaving clients who had connected to in a weird
        // state.
        _acceptSockets();

        // Close our listening ports, we won't accept any new connections on them, except the control port, if we're
        // detaching. It needs to keep listening.
        if (_detach) {
            closePorts({_controlPort});
        } else {
            closePorts();
            _controlPort = nullptr;
        }
        _portPluginMap.clear();
        _commandPort = nullptr;
        _shutdownState.store(START_SHUTDOWN);
        SINFO("START_SHUTDOWN. Ports shutdown, will perform final socket read. Commands queued: " << _commandQueue.size());
    }
}

bool BedrockServer::backupOnShutdown() {
    return _backupOnShutdown;
}

SData BedrockServer::_generateCrashMessage(const BedrockCommand* command) {
    SData message("CRASH_COMMAND");
    SData subMessage(command->request.methodLine);
    for (auto& field : command->crashIdentifyingValues) {
        auto it = command->request.nameValueMap.find(field);
        if (it != command->request.nameValueMap.end()) {
            subMessage[field] = it->second;
        }
    }
    message.content = subMessage.serialize();
    return message;
}

void BedrockServer::broadcastCommand(const SData& cmd) {
    SData message("BROADCAST_COMMAND");
    message.content = cmd.serialize();
    lock_guard<recursive_mutex> lock(_syncMutex);
    auto _syncNodeCopy = _syncNode;
    if (_syncNodeCopy) {
        _syncNodeCopy->broadcast(message);
    }
}

void BedrockServer::onNodeLogin(SQLiteNode::Peer* peer)
{
    shared_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
    for (const auto& p : _crashCommands) {
        for (const auto& table : p.second) {
            SALERT("Sending crash command " << p.first << " to node " << peer->name << " on login");
            SData command(p.first);
            command.nameValueMap = table;
            BedrockCommand cmd(command);
            for (const auto& fields : command.nameValueMap) {
                cmd.crashIdentifyingValues.insert(fields.first);
            }
            auto _syncNodeCopy = _syncNode;
            if (_syncNodeCopy) {
                _syncNodeCopy->broadcast(_generateCrashMessage(&cmd), peer);
            }
        }
    }
}

void BedrockServer::_finishPeerCommand(BedrockCommand& command) {
    // See if we're supposed to forget this command (because the slave is not listening for a response).
    auto it = command.request.nameValueMap.find("Connection");
    bool forget = it != command.request.nameValueMap.end() && SIEquals(it->second, "forget");
    command.finalizeTimingInfo();
    if (forget) {
        SINFO("Not responding to 'forget' command '" << command.request.methodLine << "' from slave.");
    } else {
        auto _syncNodeCopy = _syncNode;
        if (_syncNodeCopy) {
            _syncNodeCopy->sendResponse(command);
        }
    }
    _commandsInProgress--;
}

void BedrockServer::_acceptSockets() {
    Socket* s = nullptr;
    Port* acceptPort = nullptr;
    SAUTOLOCK(_socketIDMutex);
    while ((s = acceptSocket(acceptPort))) {
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
}


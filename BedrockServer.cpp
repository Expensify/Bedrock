// Manages connections to a single instance of the bedrock server.
#include <bedrockVersion.h>
#include <libstuff/libstuff.h>
#include "BedrockServer.h"
#include "BedrockPlugin.h"
#include "BedrockCore.h"
#include <iomanip>

#include <sys/time.h>
#include <sys/resource.h>

set<string>BedrockServer::_blacklistedParallelCommands;
shared_timed_mutex BedrockServer::_blacklistedParallelCommandMutex;

void BedrockServer::acceptCommand(SQLiteCommand&& command, bool isNew) {
    acceptCommand(make_unique<SQLiteCommand>(move(command)), isNew);
}

void BedrockServer::acceptCommand(unique_ptr<SQLiteCommand>&& command, bool isNew) {
    // If the sync node tells us that a command causes a crash, we immediately save that.
    if (SIEquals(command->request.methodLine, "CRASH_COMMAND")) {
        SData request;
        request.deserialize(command->request.content);

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
        unique_ptr<BedrockCommand> newCommand(nullptr);
        if (SIEquals(command->request.methodLine, "BROADCAST_COMMAND")) {
            SData newRequest;
            newRequest.deserialize(command->request.content);
            newCommand = getCommandFromPlugins(move(newRequest));
            newCommand->initiatingClientID = -1;
            newCommand->initiatingPeerID = 0;
        } else {
            // If we already have an existing BedrockCommand (as opposed to a base class SQLiteCommand) this is
            // something that we've already constructed (most likely, a response from leader), so no need to ask the
            // plugins to build it over again from scratch (this also preserves the existing state of the command).
            BedrockCommand* isABedrockCommand = dynamic_cast<BedrockCommand*>(command.get());
            if (isABedrockCommand) {
                newCommand = unique_ptr<BedrockCommand>(isABedrockCommand);
                command.release();
            } else {
                newCommand = getCommandFromPlugins(move(command));
            }
            SINFO("Accepted command " << newCommand->request.methodLine << " from plugin " << newCommand->getName());
        }

        SAUTOPREFIX(newCommand->request);
        if (newCommand->writeConsistency != SQLiteNode::QUORUM
            && _syncCommands.find(newCommand->request.methodLine) != _syncCommands.end()) {

            newCommand->writeConsistency = SQLiteNode::QUORUM;
            _lastQuorumCommandTime = STimeNow();
            SINFO("Forcing QUORUM consistency for command " << newCommand->request.methodLine);
        }
        SINFO("Queued new '" << newCommand->request.methodLine << "' command from bedrock node, with " << _commandQueue.size()
              << " commands already queued.");

        _commandQueue.push(move(newCommand));
    }
}

void BedrockServer::cancelCommand(const string& commandID) {
    // TODO: Unimplemented (but never called, anyway)
}

bool BedrockServer::canStandDown() {
    // Here's all the commands in existence.
    size_t count = BedrockCommand::getCommandCount();
    size_t standDownQueueSize = _standDownQueue.size();

    // If we have any commands anywhere but the stand-down queue, let's log that.
    if (count && count != standDownQueueSize) {
        size_t mainQueueSize = _commandQueue.size();
        size_t blockingQueueSize = _blockingCommandQueue.size();
        size_t syncNodeQueueSize = _syncNodeQueuedCommands.size();
        size_t completedCommandsSize = _completedCommands.size();

        // These two aren't all nicely packaged so we need to lock them ourselves.
        size_t outstandingHTTPSCommandsSize = 0;
        {
            lock_guard<decltype(_httpsCommandMutex)> lock(_httpsCommandMutex);
            outstandingHTTPSCommandsSize = _outstandingHTTPSCommands.size();
        }
        size_t futureCommitCommandsSize = 0;
        {
            lock_guard<decltype(_futureCommitCommandMutex)> lock(_futureCommitCommandMutex);
            futureCommitCommandsSize = _futureCommitCommands.size();
        }

        SINFO("Can't stand down with " << count << " commands remaining. Queue sizes are: "
              << "mainQueueSize: " << mainQueueSize << ", "
              << "blockingQueueSize: " << blockingQueueSize << ", "
              << "syncNodeQueueSize: " << syncNodeQueueSize << ", "
              << "completedCommandsSize: " << completedCommandsSize << ", "
              << "outstandingHTTPSCommandsSize: " << outstandingHTTPSCommandsSize << ", "
              << "futureCommitCommandsSize: " << futureCommitCommandsSize << ", "
              << "standDownQueueSize: " << standDownQueueSize << ".");
        return false;
    } else {
        SINFO("Can stand down now.");
        return true;
    }
}

void BedrockServer::syncWrapper(const SData& args,
                         atomic<SQLiteNode::State>& replicationState,
                         atomic<bool>& upgradeInProgress,
                         atomic<string>& leaderVersion,
                         BedrockTimeoutCommandQueue& syncNodeQueuedCommands,
                         BedrockServer& server)
{
    // Initialize the thread.
    SInitialize(_syncThreadName);

    while(true) {
        // If the server's set to be detached, we wait until that flag is unset, and then start the sync thread.
        if (server._detach) {
            // If we're set detached, we assume we'll be re-attached eventually, and then be `RUNNING`.
            SINFO("Bedrock server entering detached state.");
            server._shutdownState.store(RUNNING);

            // Detach any plugins now
            for (auto plugin : server.plugins) {
                plugin.second->onDetach();
            }
            server._pluginsDetached = true;
            while (server._detach) {
                if (server.shutdownWhileDetached) {
                    SINFO("Bedrock server exiting from detached state.");
                    return;
                }
                // Just wait until we're attached.
                SINFO("Bedrock server sleeping in detached state.");
                sleep(1);
            }
            SINFO("Bedrock server entering attached state.");
            server._resetServer();
        }
        sync(args, replicationState, upgradeInProgress, leaderVersion, syncNodeQueuedCommands, server);

        // Now that we've run the sync thread, we can exit if it hasn't set _detach again.
        if (!server._detach) {
            break;
        }
    }
}

void BedrockServer::sync(const SData& args,
                         atomic<SQLiteNode::State>& replicationState,
                         atomic<bool>& upgradeInProgress,
                         atomic<string>& leaderVersion,
                         BedrockTimeoutCommandQueue& syncNodeQueuedCommands,
                         BedrockServer& server)
{
    // Parse out the number of worker threads we'll use. The DB needs to know this because it will expect a
    // corresponding number of journal tables. "-readThreads" exists only for backwards compatibility.
    int workerThreads = args.calc("-workerThreads");

    // TODO: remove when nothing uses readThreads.
    workerThreads = workerThreads ? workerThreads : args.calc("-readThreads");

    // If still no value, use the number of cores on the machine, if available.
    workerThreads = workerThreads ? workerThreads : max(1u, thread::hardware_concurrency());

    // A minumum of *2* worker threads are required. One for blocking writes, one for other commands.
    if (workerThreads < 2) {
        workerThreads = 2;
    }

    // Initialize the DB.
    int64_t mmapSizeGB = args.isSet("-mmapSizeGB") ? stoll(args["-mmapSizeGB"]) : 0;

    // We use fewer FDs on test machines that have other resource restrictions in place.
    int fdLimit = args.isSet("-live") ? 25'000 : 250;
    SINFO("Setting dbPool size to: " << fdLimit);
    SQLitePool dbPool(fdLimit, args["-db"], args.calc("-cacheSize"), true, args.calc("-maxJournalSize"), workerThreads, args["-synchronous"], mmapSizeGB, args.test("-pageLogging"));
    SQLite& db = dbPool.getBase();

    // Initialize the command processor.
    BedrockCore core(db, server);

    // And the sync node.
    uint64_t firstTimeout = STIME_US_PER_M * 2 + SRandom::rand64() % STIME_US_PER_S * 30;

    // Initialize the shared pointer to our sync node object.
    server._syncNode = make_shared<SQLiteNode>(server, dbPool, args["-nodeName"], args["-nodeHost"],
                                               args["-peerList"], args.calc("-priority"), firstTimeout,
                                               server._version, args.test("-parallelReplication"));

    // This should be empty anyway, but let's make sure.
    if (server._completedCommands.size()) {
        SWARN("_completedCommands not empty at startup of sync thread.");
    }

    // The node is now coming up, and should eventually end up in a `LEADING` or `FOLLOWING` state. We can start adding
    // our worker threads now. We don't wait until the node is `LEADING` or `FOLLOWING`, as it's state can change while
    // it's running, and our workers will have to maintain awareness of that state anyway.
    SINFO("Starting " << workerThreads << " worker threads.");
    list<thread> workerThreadList;
    for (int threadId = 0; threadId < workerThreads; threadId++) {
        workerThreadList.emplace_back(worker,
                                      ref(dbPool),
                                      ref(replicationState),
                                      ref(upgradeInProgress),
                                      ref(leaderVersion),
                                      ref(syncNodeQueuedCommands),
                                      ref(server._completedCommands),
                                      ref(server),
                                      threadId);
    }

    // Now we jump into our main command processing loop.
    uint64_t nextActivity = STimeNow();
    unique_ptr<BedrockCommand> command(nullptr);
    bool committingCommand = false;

    // Timer for S_poll performance logging. Created outside the loop because it's cumulative.
    AutoTimer pollTimer("sync thread poll");
    AutoTimer postPollTimer("sync thread PostPoll");
    AutoTimer escalateLoopTimer("sync thread escalate loop");

    // We hold a lock here around all operations on `syncNode`, because `SQLiteNode` isn't thread-safe, but we need
    // `BedrockServer` to be able to introspect it in `Status` requests. We hold this lock at all times until exiting
    // our main loop, aside from when we're waiting on `poll`. Strictly, we could hold this lock less often, but there
    // are not that many status commands coming in, and they can wait for a fraction of a second, which lets us keep
    // the logic of this loop simpler.
    server._syncMutex.lock();
    do {
        // Make sure the existing command prefix is still valid since they're reset when SAUTOPREFIX goes out of scope.
        if (command) {
            SAUTOPREFIX(command->request);
        }

        // If there were commands waiting on our commit count to come up-to-date, we'll move them back to the main
        // command queue here. There's no place in particular that's best to do this, so we do it at the top of this
        // main loop, as that prevents it from ever getting skipped in the event that we `continue` early from a loop
        // iteration.
        // We also move all commands back to the main queue here if we're shutting down, just to make sure they don't
        // end up lost in the ether.
        {
            SAUTOLOCK(server._futureCommitCommandMutex);

            // First, see if anything has timed out, and move that back to the main queue.
            if (server._futureCommitCommandTimeouts.size()) {
                uint64_t now = STimeNow();
                auto it =  server._futureCommitCommandTimeouts.begin();
                while (it != server._futureCommitCommandTimeouts.end() && it->first < now) {
                    // Find commands depending on this commit.
                    auto itPair =  server._futureCommitCommands.equal_range(it->second);
                    for (auto cmdIt = itPair.first; cmdIt != itPair.second; cmdIt++) {
                        // Check for one with this timeout.
                        if (cmdIt->second->timeout() == it->first) {
                            // This command has the right commit count *and* timeout, return it.
                            SINFO("Returning command (" << cmdIt->second->request.methodLine << ") waiting on commit " << cmdIt->first
                                  << " to queue, timed out at: " << now << ", timeout was: " << it->first << ".");

                            // Goes back to the main queue, where it will hit it's timeout in a worker thread.
                            server._commandQueue.push(move(cmdIt->second));

                            // And delete it, it's gone.
                             server._futureCommitCommands.erase(cmdIt);

                            // Done.
                            break;
                        }
                    }
                    it++;
                }

                // And remove everything we just iterated through.
                if (it != server._futureCommitCommandTimeouts.begin()) {
                    server._futureCommitCommandTimeouts.erase(server._futureCommitCommandTimeouts.begin(), it);
                }
            }

            // Anything that hasn't timed out might be ready to return because the commit count is up-to-date.
            if (!server._futureCommitCommands.empty()) {
                uint64_t commitCount = db.getCommitCount();
                auto it = server._futureCommitCommands.begin();
                while (it != server._futureCommitCommands.end() && (it->first <= commitCount || server._shutdownState.load() != RUNNING)) {
                    // Save the timeout since we'll be moving the command, thus making this inaccessible.
                    uint64_t commandTimeout = it->second->timeout();
                    SINFO("Returning command (" << it->second->request.methodLine << ") waiting on commit " << it->first
                          << " to queue, now have commit " << commitCount);
                    server._commandQueue.push(move(it->second));

                    // Remove it from the timed out list as well.
                    auto itPair = server._futureCommitCommandTimeouts.equal_range(commandTimeout);
                    for (auto timeoutIt = itPair.first; timeoutIt != itPair.second; timeoutIt++) {
                        if (timeoutIt->second == it->first) {
                             server._futureCommitCommandTimeouts.erase(timeoutIt);
                            break;
                        }
                    }
                    it++;
                }
                if (it != server._futureCommitCommands.begin()) {
                    server._futureCommitCommands.erase(server._futureCommitCommands.begin(), it);
                }
            }
        }

        // If we're in a state where we can initialize shutdown, then go ahead and do so.
        // Having responded to all clients means there are no *local* clients, but it doesn't mean there are no
        // escalated commands. This is fine though - if we're following, there can't be any escalated commands, and if
        // we're leading, then the next update() loop will set us to standing down, and then we won't accept any new
        // commands, and we'll shortly run through the existing queue.
        if (server._shutdownState.load() == CLIENTS_RESPONDED) {
            // The total time we'll wait for the sync node is whatever we haven't already waited from the original
            // timeout, minus 5 seconds to allow to clean up afterward.
            int64_t timeAllowed = server._gracefulShutdownTimeout.alarmDuration.load() - server._gracefulShutdownTimeout.elapsed();
            timeAllowed -= 5'000'000;
            server._syncNode->beginShutdown(max(timeAllowed, (int64_t)1));
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
        server._completedCommands.prePoll(fdm);

        // Wait for activity on any of those FDs, up to a timeout.
        const uint64_t now = STimeNow();

        // Unlock our mutex, poll, and re-lock when finished.
        server._syncMutex.unlock();
        {
            AutoTimerTime pollTime(pollTimer);
            S_poll(fdm, max(nextActivity, now) - now);
        }
        server._syncMutex.lock();

        // And set our next timeout for 1 second from now.
        nextActivity = STimeNow() + STIME_US_PER_S;

        // Process any network traffic that happened. Scope this so that we can change the log prefix and have it
        // auto-revert when we're finished.
        {
            // Set the default log prefix.
            SAUTOPREFIX(SData());

            // Process any activity in our plugins.
            AutoTimerTime postPollTime(postPollTimer);
            server._postPollPlugins(fdm, nextActivity);
            server._syncNode->postPoll(fdm, nextActivity);
            syncNodeQueuedCommands.postPoll(fdm);
            server._completedCommands.postPoll(fdm);
        }

        // Ok, let the sync node to it's updating for as many iterations as it requires. We'll update the replication
        // state when it's finished.
        SQLiteNode::State preUpdateState = server._syncNode->getState();
        while (server._syncNode->update()) {}
        SQLiteNode::State nodeState = server._syncNode->getState();
        replicationState.store(nodeState);
        leaderVersion.store(server._syncNode->getLeaderVersion());

        // If anything was in the stand down queue, move it back to the main queue.
        if (nodeState != SQLiteNode::STANDINGDOWN) {
            while (server._standDownQueue.size()) {
                server._commandQueue.push(server._standDownQueue.pop());
            }
        } else if (preUpdateState != SQLiteNode::STANDINGDOWN) {
            // Otherwise,if we just started standing down, discard any commands that had been scheduled in the future.
            // In theory, it should be fine to keep these, as they shouldn't have sockets associated with them, and
            // they could be re-escalated to leader in the future, but there's not currently a way to decide if we've
            // run through all of the commands that might need peer responses before standing down aside from seeing if
            // the entire queue is empty.
            server._commandQueue.abandonFutureCommands(5000);
        }

        // If we're not leading, we turn off multi-write until we've finished upgrading the DB. This persists until
        // after we're leading again.
        if (nodeState != SQLiteNode::LEADING) {
            server._suppressMultiWrite.store(true);
        }

        // If the node's not in a ready state at this point, we'll probably need to read from the network.
        // First, test conditions from transitional edge cases (e.g. interrupted db upgrades and queued commands)
        // and then start the main loop over. This can let us wait for new logins from peers (for example).
        if (nodeState != SQLiteNode::LEADING &&
            nodeState != SQLiteNode::FOLLOWING   &&
            nodeState != SQLiteNode::STANDINGDOWN) {

            // If we were LEADING, but we've transitioned, then something's gone wrong (perhaps we got disconnected
            // from the cluster).
            if (preUpdateState == SQLiteNode::LEADING || preUpdateState == SQLiteNode::STANDINGDOWN) {

                // If we bailed out while doing a upgradeDB, clear state
                if (upgradeInProgress.load()) {
                    upgradeInProgress.store(false);
                    server._syncThreadCommitMutex.unlock();
                    committingCommand = false;
                }

                // We should give up an any commands, and let them be re-escalated. If commands were initiated locally,
                // we can just re-queue them, they will get re-checked once things clear up, and then they'll get
                // processed here, or escalated to the new leader. Commands initiated on followers just get dropped,
                // they will need to be re-escalated, potentially to a different leader.
                int requeued = 0;
                int dropped = 0;
                try {
                    while (true) {
                        // Reset this to blank. This releases the existing command and allows it to get cleaned up.
                        command = unique_ptr<BedrockCommand>(nullptr);
                        command = syncNodeQueuedCommands.pop();
                        if (command->initiatingClientID) {
                            // This one came from a local client, so we can save it for later.
                            server._commandQueue.push(move(command));
                        }
                    }
                } catch (const out_of_range& e) {
                    SWARN("Abruptly stopped LEADING. Re-queued " << requeued << " commands, Dropped " << dropped << " commands.");

                    // command will be null here, we should be able to restart the loop.
                    continue;
                }
            }
            continue;
        }

        // If we've just switched to the leading state, we want to upgrade the DB. We'll set a global flag to let
        // worker threads know that a DB upgrade is in progress, and start the upgrade process, which works basically
        // like a regular distributed commit.
        if (preUpdateState != SQLiteNode::LEADING && nodeState == SQLiteNode::LEADING) {
            // Store this before we start writing to the DB, which can take a while depending on what changes were made
            // (for instance, adding an index).
            upgradeInProgress.store(true);
            if (server._upgradeDB(db)) {
                server._syncThreadCommitMutex.lock();
                committingCommand = true;
                server._syncNode->startCommit(SQLiteNode::QUORUM);
                server._lastQuorumCommandTime = STimeNow();
                SDEBUG("Finished sending distributed transaction for db upgrade.");

                // As it's a quorum commit, we'll need to read from peers. Let's start the next loop iteration.
                continue;
            } else {
                // If we're not doing an upgrade, we don't need to keep suppressing multi-write, and we're done with
                // the upgradeInProgress flag.
                upgradeInProgress.store(false);
                server._suppressMultiWrite.store(false);
            }
        }

        // If we started a commit, and one's not in progress, then we've finished it and we'll take that command and
        // stick it back in the appropriate queue.
        if (committingCommand && !server._syncNode->commitInProgress()) {
            // Record the time spent, unless we were upgrading, in which case, there's no command to write to.
            if (command) {
                command->stopTiming(BedrockCommand::COMMIT_SYNC);
            }

            // We're done with the commit, we unlock our mutex and decrement our counter.
            server._syncThreadCommitMutex.unlock();
            committingCommand = false;

            // If we were upgrading, there's no response to send, we're just done.
            if (upgradeInProgress.load()) {
                upgradeInProgress.store(false);
                server._suppressMultiWrite.store(false);
                if (!server._syncNode->commitSucceeded()) {
                    SWARN("Failed to commit DB Upgrade. Trying again from the top.");
                }
                continue;
            }

            if (server._syncNode->commitSucceeded()) {
                if (command) {
                    SINFO("[performance] Sync thread finished committing command " << command->request.methodLine);

                    // Otherwise, save the commit count, mark this command as complete, and reply.
                    command->response["commitCount"] = to_string(db.getCommitCount());
                    command->complete = true;
                    if (command->initiatingPeerID) {
                        // This is a command that came from a peer. Have the sync node send the response back to the peer.
                        server._finishPeerCommand(command);
                    } else {
                        // The only other option is this came from a client, so respond via the server.
                        server._reply(command);
                    }
                } else {
                    SINFO("Sync thread finished committing non-command");
                }
            } else {
                // This should only happen if the cluster becomes largely disconnected while we were in the process of
                // committing a QUORUM command - if we no longer have enough peers to reach QUORUM, we'll fall out of
                // leading. This code won't actually run until the node comes back up in a LEADING or FOLLOWING
                // state, because this loop is skipped except when LEADING, FOLLOWING, or STANDINGDOWN. It's also
                // theoretically feasible for this to happen if a follower fails to commit a transaction, but that
                // probably indicates a bug (or a follower disk failure).

                // While _upgradeDB isn't a normal command, it is still run as a QUORUM transaction, and so it can
                // still fail if peers reject it (for example if run on low priority follower node that becomes
                // leader during cluster instability) In that case, we will fail an upgradeDB distributed transaction, and we need to reset that state.
                if (upgradeInProgress.load()) {
                    SINFO("clearing stale _upgradeDB() state variables");
                    upgradeInProgress.store(false);
                    server._suppressMultiWrite.store(false);
                } else if (command) {
                    SINFO("requeueing command " << command->request.methodLine
                          << " after failed sync commit. Sync thread has " << syncNodeQueuedCommands.size()
                          << " queued commands.");
                    syncNodeQueuedCommands.push(move(command));
                } else {
                    SERROR("Unexpected sync thread commit state.");
                }
            }
        }

        // We're either leading, standing down, or following. There could be a commit in progress on `command`, but
        // there could also be other finished work to handle while we wait for that to complete. Let's see if we can
        // handle any of that work.
        try {
            // If there are any completed commands to respond to, we'll do that first.
            try {
                while (true) {
                    unique_ptr<BedrockCommand> completedCommand = server._completedCommands.pop();
                    SAUTOPREFIX(completedCommand->request);
                    SASSERT(completedCommand->complete);
                    SASSERT(completedCommand->initiatingPeerID);
                    SASSERT(!completedCommand->initiatingClientID);
                    server._finishPeerCommand(completedCommand);
                }
            } catch (const out_of_range& e) {
                // when _completedCommands.pop() throws for running out of commands, we fall out of the loop.
            }

            // We don't start processing a new command until we've completed any existing ones.
            if (committingCommand) {
                continue;
            }

            // Don't escalate, leader can't handle the command anyway. Don't even dequeue the command, just leave it
            // until one of these states changes. This prevents an endless loop of escalating commands, having
            // SQLiteNode re-queue them because leader is standing down, and then escalating them again until leader
            // sorts itself out.
            if (nodeState == SQLiteNode::FOLLOWING && server._syncNode->leaderState() == SQLiteNode::STANDINGDOWN) {
                continue;
            }

            // We want to run through all of the commands in our queue. However, we set a maximum limit. This list is
            // potentially infinite, as we can add new commands to the list as we iterate across it (coming from
            // workers), and we will need to break and read from the network to see what to do next at some point.
            // Additionally, in exceptional cases, if we get stuck in this loop for more than 64k commands, we can hit
            // the internal limit of the buffer for the pipe inside syncNodeQueuedCommands, and writes there will
            // block, and this can cause deadlocks in various places. This is cleared every time we run `postPoll` for
            // syncNodeQueuedCommands, which occurs when break out of this loop, so we do so periodically to avoid
            // this.
            // TODO: We could potentially make writes to the pipe in the queue non-blocking and help to mitigate that
            // part of this issue as well.
            size_t escalateCount = 0;
            while (++escalateCount < 1000) {
                AutoTimerTime escalateTime(escalateLoopTimer);

                // Reset this to blank. This releases the existing command and allows it to get cleaned up.
                command = unique_ptr<BedrockCommand>(nullptr);

                // Get the next sync node command to work on.
                command = syncNodeQueuedCommands.pop();

                // We got a command to work on! Set our log prefix to the request ID.
                SAUTOPREFIX(command->request);
                SINFO("Sync thread dequeued command " << command->request.methodLine << ". Sync thread has "
                      << syncNodeQueuedCommands.size() << " queued commands.");

                if (command->timeout() < STimeNow()) {
                    SINFO("Command '" << command->request.methodLine << "' timed out in sync thread queue, sending back to main queue.");
                    server._commandQueue.push(move(command));
                    break;
                }

                // Set the function that will be called if this thread's signal handler catches an unrecoverable error,
                // like a segfault. Note that it's possible we're in the middle of sending a message to peers when we call
                // this, which would probably make this message malformed. This is the best we can do.
                SSetSignalHandlerDieFunc([&](){
                    server._syncNode->broadcast(_generateCrashMessage(command));
                });

                // And now we'll decide how to handle it.
                if (nodeState == SQLiteNode::LEADING || nodeState == SQLiteNode::STANDINGDOWN) {

                    // We need to grab this before peekCommand (or wherever our transaction is started), to verify that
                    // no worker thread can commit in the middle of our transaction. We need our entire transaction to
                    // happen with no other commits to ensure that we can't get a conflict.
                    uint64_t beforeLock = STimeNow();

                    // This needs to be done before we acquire _syncThreadCommitMutex or we can deadlock.
                    db.waitForCheckpoint();
                    server._syncThreadCommitMutex.lock();

                    // It appears that this might be taking significantly longer with multi-write enabled, so we're adding
                    // explicit logging for it to check.
                    SINFO("[performance] Waited " << (STimeNow() - beforeLock)/1000 << "ms for _syncThreadCommitMutex.");

                    // We peek commands here in the sync thread to be able to run peek and process as part of the same
                    // transaction. This guarantees that any checks made in peek are still valid in process, as the DB can't
                    // have changed in the meantime.
                    // IMPORTANT: This check is omitted for commands with an HTTPS request object, because we don't want to
                    // risk duplicating that request. If your command creates an HTTPS request, it needs to explicitly
                    // re-verify that any checks made in peek are still valid in process.
                    if (!command->httpsRequests.size()) {
                        BedrockCore::RESULT result = core.peekCommand(command, true);
                        if (result == BedrockCore::RESULT::COMPLETE) {

                            // Finished with this.
                            server._syncThreadCommitMutex.unlock();

                            // This command completed in peek, respond to it appropriately, either directly or by sending it
                            // back to the sync thread.
                            SASSERT(command->complete);
                            if (command->initiatingPeerID) {
                                server._finishPeerCommand(command);
                            } else {
                                server._reply(command);
                            }
                            break;
                        } else if (result == BedrockCore::RESULT::SHOULD_PROCESS) {
                            // This is sort of the "default" case after checking if this command was complete above. If so,
                            // we'll fall through to calling processCommand below.
                        } else if (result == BedrockCore::RESULT::ABANDONED_FOR_CHECKPOINT) {
                            // Finished with this.
                            server._syncThreadCommitMutex.unlock();
                            SINFO("[checkpoint] Re-queuing abandoned command (from peek) in sync thread");
                            server._commandQueue.push(move(command));
                            break;
                        } else {
                            SERROR("peekCommand (" << command->request.getVerb() << ") returned invalid result code: " << (int)result);
                        }

                        // If we just started a new HTTPS request, save it for later.
                        if (command->httpsRequests.size()) {
                            server.waitForHTTPS(move(command));

                            // Move on to the next command until this one finishes.
                            core.rollback();
                            server._syncThreadCommitMutex.unlock();
                            break;
                        }
                    }

                    BedrockCore::RESULT result = core.processCommand(command, true);
                    if (result == BedrockCore::RESULT::NEEDS_COMMIT) {
                        // The processor says we need to commit this, so let's start that process.
                        committingCommand = true;
                        SINFO("[performance] Sync thread beginning committing command " << command->request.methodLine);
                        // START TIMING.
                        command->startTiming(BedrockCommand::COMMIT_SYNC);
                        server._syncNode->startCommit(command->writeConsistency);

                        // And we'll start the next main loop.
                        // NOTE: This will cause us to read from the network again. This, in theory, is fine, but we saw
                        // performance problems in the past trying to do something similar on every commit. This may be
                        // alleviated now that we're only doing this on *sync* commits instead of all commits, which should
                        // be a much smaller fraction of all our traffic. We set nextActivity here so that there's no
                        // timeout before we'll give up on poll() if there's nothing to read.
                        nextActivity = STimeNow();

                        // Don't unlock _syncThreadCommitMutex here, we'll hold the lock till the commit completes.
                        break;
                    } else if (result == BedrockCore::RESULT::NO_COMMIT_REQUIRED) {
                        // Otherwise, the command doesn't need a commit (maybe it was an error, or it didn't have any work
                        // to do). We'll just respond.
                        server._syncThreadCommitMutex.unlock();
                        if (command->initiatingPeerID) {
                            server._finishPeerCommand(command);
                        } else {
                            server._reply(command);
                        }
                    } else if (result == BedrockCore::RESULT::ABANDONED_FOR_CHECKPOINT) {
                        // Finished with this.
                        server._syncThreadCommitMutex.unlock();
                        SINFO("[checkpoint] Re-queuing abandoned command (from process) in sync thread");
                        server._commandQueue.push(move(command));
                        break;
                    } else {
                        SERROR("processCommand (" << command->request.getVerb() << ") returned invalid result code: " << (int)result);
                    }

                    // When we're leading, we'll try and handle one command and then stop.
                    break;
                } else if (nodeState == SQLiteNode::FOLLOWING) {
                    // If we're following, we just escalate directly to leader without peeking. We can only get an incomplete
                    // command on the follower sync thread if a follower worker thread peeked it unsuccessfully, so we don't
                    // bother peeking it again.
                    auto it = command->request.nameValueMap.find("Connection");
                    bool forget = it != command->request.nameValueMap.end() && SIEquals(it->second, "forget");
                    server._syncNode->escalateCommand(move(command), forget);
                }
            }
            if (escalateCount == 1000) {
                SINFO("Escalated 1000 commands without hitting the end of the queue. Breaking.");
            }
        } catch (const out_of_range& e) {
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

    // Same for the blocking queue.
    if (server._blockingCommandQueue.size()) {
        SWARN("Sync thread shut down with " << server._blockingCommandQueue.size() << " blocking queued commands. Commands were: "
              << SComposeList(server._blockingCommandQueue.getRequestMethodLines()) << ". Clearing.");
        server._blockingCommandQueue.clear();
    }

    // Release our handle to this pointer. Any other functions that are still using it will keep the object alive
    // until they return.
    server._syncNode = nullptr;

    // We're really done, store our flag so main() can be aware.
    server._syncThreadComplete.store(true);
}

void BedrockServer::worker(SQLitePool& dbPool,
                           atomic<SQLiteNode::State>& replicationState,
                           atomic<bool>& upgradeInProgress,
                           atomic<string>& leaderVersion,
                           BedrockTimeoutCommandQueue& syncNodeQueuedCommands,
                           BedrockTimeoutCommandQueue& syncNodeCompletedCommands,
                           BedrockServer& server,
                           int threadId)
{
    // Worker 0 is the "blockingCommit" thread.
    SInitialize(threadId ? "worker" + to_string(threadId) : "blockingCommit");

    // Get a DB handle to work on. This will automatically be returned when dbScope goes out of scope.
    SQLiteScopedHandle dbScope(dbPool, dbPool.getIndex());
    SQLite& db = dbScope.db();
    BedrockCore core(db, server);

    // Command to work on. This default command is replaced when we find work to do.
    unique_ptr<BedrockCommand> command(nullptr);

    // Which command queue do we use? The blockingCommit thread special and does blocking commits from the blocking queue.
    BedrockCommandQueue& commandQueue = threadId ? server._commandQueue : server._blockingCommandQueue;

    // We just run this loop looking for commands to process forever. There's a check for appropriate exit conditions
    // at the bottom, which will cause our loop and thus this thread to exit when that becomes true.
    while (true) {
        try {
            // Set a signal handler function that we can call even if we die early with no command.
            SSetSignalHandlerDieFunc([&](){
                SWARN("Die function called early with no command, probably died in `commandQueue.get`.");
            });

            // Reset this to blank. This releases the existing command and allows it to get cleaned up.
            command = unique_ptr<BedrockCommand>(nullptr);

            // And get another one.
            command = commandQueue.get(1000000);

            SAUTOPREFIX(command->request);
            SINFO("Dequeued command " << command->request.methodLine << " in worker, "
                  << commandQueue.size() << " commands in " << (threadId ? "" : "blocking") << " queue.");

            // Set the function that lets the signal handler know which command caused a problem, in case that happens.
            // If a signal is caught on this thread, which should only happen for unrecoverable, yet synchronous
            // signals, like SIGSEGV, this function will be called.
            SSetSignalHandlerDieFunc([&](){
                server._syncNode->broadcast(_generateCrashMessage(command));
            });

            // If we dequeue a status or control command, handle it immediately.
            if (server._handleIfStatusOrControlCommand(command)) {
                continue;
            }

            // If the command has already timed out when we get it, we can return early here without peeking it.
            // We'd also catch that the command timed out in `peek`, but this can cause some weird side-effects. For
            // instance, we saw QUORUM commands that make HTTPS requests time out in the sync thread, which caused them
            // to be returned to the main queue, where they would have timed out in `peek`, but it was never called
            // because the commands already had a HTTPS request attached, and then they were immediately re-sent to the
            // sync queue, because of the QUORUM consistency requirement, resulting in an endless loop.
            if (core.isTimedOut(command)) {
                if (command->initiatingPeerID) {
                    // Escalated command. Give it back to the sync thread to respond.
                    syncNodeCompletedCommands.push(move(command));
                } else {
                    server._reply(command);
                }
                continue;
            }

            // Check if this command would be likely to cause a crash
            if (server._wouldCrash(command)) {
                // If so, make a lot of noise, and respond 500 without processing it.
                SALERT("CRASH-INDUCING COMMAND FOUND: " << command->request.methodLine);
                command->response.methodLine = "500 Refused";
                command->complete = true;
                if (command->initiatingPeerID) {
                    // Escalated command. Give it back to the sync thread to respond.
                    syncNodeCompletedCommands.push(move(command));
                } else {
                    server._reply(command);
                }
                continue;
            }

            // If this was a command initiated by a peer as part of a cluster operation, then we process it separately
            // and respond immediately. This allows SQLiteNode to offload read-only operations to worker threads.
            if (SQLiteNode::peekPeerCommand(server._syncNode.get(), db, *command)) {
                // Move on to the next command.
                continue;
            }

            // We just spin until the node looks ready to go. Typically, this doesn't happen expect briefly at startup.
            while (upgradeInProgress.load() ||
                   (replicationState.load() != SQLiteNode::LEADING &&
                    replicationState.load() != SQLiteNode::FOLLOWING &&
                    replicationState.load() != SQLiteNode::STANDINGDOWN)
            ) {
                // Make sure that the node isn't shutting down, leaving us in an endless loop.
                if (server._shutdownState.load() != RUNNING) {
                    SWARN("Sync thread shut down while were waiting for it to come up. Discarding command '"
                          << command->request.methodLine << "'.");
                    return;
                }

                // This sleep call is pretty ugly, but it should almost never happen. We're accepting the potential
                // looping sleep call for the general case where we just check some bools and continue, instead of
                // avoiding the sleep call but having every thread lock a mutex here on every loop.
                usleep(10000);
            }

            // OK, so this is the state right now, which isn't necessarily anything in particular, because the sync
            // node can change it at any time, and we're not synchronizing on it. We're going to go ahead and assume
            // it's something reasonable, because in most cases, that's pretty safe. If we think we're anything but
            // LEADING, we'll just peek this command and return it's result, which should be harmless. If we think
            // we're leading, we'll go ahead and start a `process` for the command, but we'll synchronously verify
            // our state right before we commit.
            SQLiteNode::State state = replicationState.load();

            // If we're following, any incomplete commands can be immediately escalated to leader. This saves the work
            // of a `peek` operation, but more importantly, it skips any delays that might be introduced by waiting in
            // the `_futureCommitCommands` queue.
            if (state == SQLiteNode::FOLLOWING && command->escalateImmediately && !command->complete) {
                SINFO("Immediately escalating " << command->request.methodLine << " to leader. Sync thread has " << syncNodeQueuedCommands.size() << " queued commands.");
                syncNodeQueuedCommands.push(move(command));
                continue;
            }

            // If we find that we've gotten a command with an initiatingPeerID, but we're not in a leading or
            // standing down state, we'll have no way of returning this command to the caller, so we discard it. The
            // original caller will need to re-send the request. This can happen if we're leading, and receive a
            // request from a peer, but then we stand down from leading. The SQLiteNode should have already told its
            // peers that their outstanding requests were being canceled at this point.
            if (command->initiatingPeerID && !(state == SQLiteNode::LEADING || state == SQLiteNode::STANDINGDOWN)) {
                SWARN("Found " << (command->complete ? "" : "in") << "complete " << "command "
                      << command->request.methodLine << " from peer, but not leading. Too late for it, discarding.");

                // If the command was processed, tell the plugin we couldn't send the response.
                command->handleFailedReply();

                continue;
            }

            // If this command is already complete, then we should be a follower, and the sync node got a response back
            // from a command that had been escalated to leader, and queued it for a worker to respond to. We'll send
            // that response now.
            if (command->complete) {
                // If this command is already complete, we can return it to the caller.
                // If it has an initiator, it should have been returned to a peer by a sync node instead, but if we've
                // just switched states out of leading, we might have an old command in the queue. All we can do here
                // is note that and discard it, as we have nobody to deliver it to.
                if (command->initiatingPeerID) {
                    // Let's note how old this command is.
                    uint64_t ageSeconds = (STimeNow() - command->creationTime) / STIME_US_PER_S;
                    SWARN("Found unexpected complete command " << command->request.methodLine
                          << " from peer in worker thread. Discarding (command was " << ageSeconds << "s old).");
                    continue;
                }

                // Make sure we have an initiatingClientID at this point. If we do, but it's negative, it's for a
                // client that we can't respond to, so we don't bother sending the response.
                SASSERT(command->initiatingClientID);
                if (command->initiatingClientID > 0) {
                    server._reply(command);
                }

                // This command is done, move on to the next one.
                continue;
            }

            // If this command is dependent on a commitCount newer than what we have (maybe it's a follow-up to a
            // command that was escalated to leader), we'll set it aside for later processing. When the sync node
            // finishes its update loop, it will re-queue any of these commands that are no longer blocked on our
            // updated commit count.
            uint64_t commitCount = db.getCommitCount();
            uint64_t commandCommitCount = command->request.calcU64("commitCount");
            if (commandCommitCount > commitCount) {
                SAUTOLOCK(server._futureCommitCommandMutex);
                auto newQueueSize = server._futureCommitCommands.size() + 1;
                SINFO("Command (" << command->request.methodLine << ") depends on future commit (" << commandCommitCount
                      << "), Currently at: " << commitCount << ", storing for later. Queue size: " << newQueueSize);
                server._futureCommitCommandTimeouts.insert(make_pair(command->timeout(), commandCommitCount));
                server._futureCommitCommands.insert(make_pair(commandCommitCount, move(command)));

                // Don't count this as `in progress`, it's just sitting there.
                if (newQueueSize > 100) {
                    SHMMM("server._futureCommitCommands.size() == " << newQueueSize);
                }
                continue;
            }

            if (command->request.isSet("mockRequest")) {
                SINFO("mockRequest set for command '" << command->request.methodLine << "'.");
            }

            // See if this is a feasible command to write parallel. If not, then be ready to forward it to the sync
            // thread, if it doesn't finish in peek.
            bool canWriteParallel = server._multiWriteEnabled.load();
            if (canWriteParallel) {
                // If multi-write is enabled, then we need to make sure the command isn't blacklisted.
                shared_lock<decltype(_blacklistedParallelCommandMutex)> lock(_blacklistedParallelCommandMutex);
                canWriteParallel =
                    (_blacklistedParallelCommands.find(command->request.methodLine) == _blacklistedParallelCommands.end());
            }

            // More checks for parallel writing.
            canWriteParallel = canWriteParallel && !server._suppressMultiWrite.load();
            canWriteParallel = canWriteParallel && (state == SQLiteNode::LEADING);
            canWriteParallel = canWriteParallel && (command->writeConsistency == SQLiteNode::ASYNC);

            // If all the other checks have passed, and we haven't sent a quorum command to the sync thread in a while,
            // auto-promote one.
            if (canWriteParallel) {
                uint64_t now = STimeNow();
                if (now > (server._lastQuorumCommandTime + (server._quorumCheckpointSeconds * 1'000'000))) {
                    SINFO("Forcing QUORUM for command '" << command->request.methodLine << "'.");
                    server._lastQuorumCommandTime = now;
                    command->writeConsistency = SQLiteNode::QUORUM;
                    canWriteParallel = false;
                }
            }

            // We'll retry on conflict up to this many times.
            int retry = server._maxConflictRetries.load();
            while (retry) {
                // Block if a checkpoint is happening so we don't interrupt it.
                db.waitForCheckpoint();

                // If we're going to force a blocking commit, we lock now.
                unique_lock<decltype(server._syncThreadCommitMutex)> blockingLock(server._syncThreadCommitMutex, defer_lock);
                if (threadId == 0) {
                    uint64_t preLockTime = STimeNow();
                    blockingLock.lock();
                    SINFO("_syncThreadCommitMutex (unique) acquired in worker in " << fixed << setprecision(2)
                          << ((STimeNow() - preLockTime)/1000) << "ms.");
                }

                // If the command has any httpsRequests from a previous `peek`, we won't peek it again unless the
                // command has specifically asked for that.
                // If peek succeeds, then it's finished, and all we need to do is respond to the command at the bottom.
                bool calledPeek = false;
                BedrockCore::RESULT peekResult = BedrockCore::RESULT::INVALID;
                if (command->repeek || !command->httpsRequests.size()) {
                    peekResult = core.peekCommand(command, threadId == 0);
                    calledPeek = true;
                }

                // This drops us back to the top of the loop.
                if (peekResult == BedrockCore::RESULT::ABANDONED_FOR_CHECKPOINT) {
                    SINFO("[checkpoint] Re-trying abandoned command (from peek) in worker thread");
                    continue;
                }

                if (!calledPeek || peekResult == BedrockCore::RESULT::SHOULD_PROCESS) {
                    // We've just unsuccessfully peeked a command, which means we're in a state where we might want to
                    // write it. We'll flag that here, to keep the node from falling out of LEADING/STANDINGDOWN
                    // until we're finished with this command.
                    if (command->httpsRequests.size()) {
                        // This *should* be impossible, but previous bugs have existed where it's feasible that we call
                        // `peekCommand` while leading, and by the time we're done, we're FOLLOWING, so we check just
                        // in case we ever introduce another similar bug.
                        if (state != SQLiteNode::LEADING && state != SQLiteNode::STANDINGDOWN) {
                            SALERT("Not leading or standing down (" << SQLiteNode::stateName(state)
                                   << ") but have outstanding HTTPS command: " << command->request.methodLine
                                   << ", returning 500.");
                            command->response.methodLine = "500 STANDDOWN TIMEOUT";
                            server._reply(command);
                            core.rollback();
                            break;
                        }

                        // If the command isn't complete, we'll re-queue it.
                        if (command->repeek || !command->areHttpsRequestsComplete()) {
                            // Roll back the existing transaction, but only if we are inside an transaction
                            if (calledPeek) {
                                core.rollback();
                            }

                            if (!command->areHttpsRequestsComplete()) {
                                // If it has outstanding HTTPS requests, we'll wait for them.
                                server.waitForHTTPS(move(command));
                            } else if (command->repeek) {
                                // Otherwise, it needs to be re-peeked, but had no outstanding requests, so it goes
                                // back in the main queue.
                                commandQueue.push(move(command));
                            }

                            // Move on to the next command until this one finishes.
                            break;
                        }
                    }

                    // Peek wasn't enough to handle this command. See if we think it should be writable in parallel.
                    // We check `onlyProcessOnSyncThread` here, rather than before processing the command, because it's
                    // not set at creation time, it's set in `peek`, so we need to wait at least until after peek is
                    // called to check it.
                    if (command->onlyProcessOnSyncThread() || !canWriteParallel) {
                        // Roll back the transaction, it'll get re-run in the sync thread.
                        core.rollback();

                        // We're not handling a writable command anymore.
                        SINFO("Sending non-parallel command " << command->request.methodLine
                              << " to sync thread. Sync thread has " << syncNodeQueuedCommands.size()
                              << " queued commands.");
                        syncNodeQueuedCommands.push(move(command));

                        // Done with this command, look for the next one.
                        break;
                    }

                    // In this case, there's nothing blocking us from processing this in a worker, so let's try it.
                    BedrockCore::RESULT result = core.processCommand(command, threadId == 0);
                    if (result == BedrockCore::RESULT::NEEDS_COMMIT) {
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
                            shared_lock<decltype(server._syncThreadCommitMutex)> lock1(server._syncThreadCommitMutex, defer_lock);
                            if (threadId) {
                                uint64_t preLockTime = STimeNow();
                                lock1.lock();
                                SINFO("_syncThreadCommitMutex (shared) acquired in worker in " << fixed << setprecision(2)
                                      << ((STimeNow() - preLockTime)/1000) << "ms.");
                            }

                            // This is the first place we get really particular with the state of the node from a
                            // worker thread. We only want to do this commit if we're *SURE* we're leading, and
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
                            if (replicationState.load() != SQLiteNode::LEADING &&
                                replicationState.load() != SQLiteNode::STANDINGDOWN) {
                                SALERT("Node State changed from LEADING to "
                                       << SQLiteNode::stateName(replicationState.load())
                                       << " during worker commit. Rolling back transaction!");
                                core.rollback();
                            } else {
                                BedrockCore::AutoTimer(command, BedrockCommand::COMMIT_WORKER);
                                commitSuccess = core.commit();
                            }
                        }
                        if (commitSuccess) {
                            SINFO("Successfully committed " << command->request.methodLine << " on worker thread. blocking: "
                                  << (threadId ? "false" : "true"));
                            // So we must still be leading, and at this point our commit has succeeded, let's
                            // mark it as complete. We add the currentCommit count here as well.
                            command->response["commitCount"] = to_string(db.getCommitCount());
                            command->complete = true;
                        } else {
                            SINFO("Conflict or state change committing " << command->request.methodLine
                                  << " on worker thread with " << retry << " retries remaining.");
                        }
                    } else if (result == BedrockCore::RESULT::ABANDONED_FOR_CHECKPOINT) {
                        SINFO("[checkpoint] Re-trying abandoned command (from process) in worker thread");
                        // Add a retry so that we don't hit out conflict limit for checkpoints.
                        ++retry;
                    } else if (result == BedrockCore::RESULT::NO_COMMIT_REQUIRED) {
                        // Nothing to do in this case, `command->complete` will be set and we'll finish as we fall out
                        // of this block.
                    } else {
                        SERROR("processCommand (" << command->request.getVerb() << ") returned invalid result code: " << (int)result);
                    }
                }

                // If the command was completed above, then we'll go ahead and respond. Otherwise there must have been
                // a conflict or the command was abandoned for a checkpoint, and we'll retry.
                if (command->complete) {
                    if (command->initiatingPeerID) {
                        // Escalated command. Send it back to the peer.
                        server._finishPeerCommand(command);
                    } else {
                        server._reply(command);
                    }

                    // Don't need to retry.
                    break;
                }

                // We're about to retry, decrement the retry count.
                --retry;

                if (!retry) {
                    SINFO("Max retries hit in worker, sending '" << command->request.methodLine << "' to blocking queue.");
                   server._blockingCommandQueue.push(move(command));
                }
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

bool BedrockServer::_handleIfStatusOrControlCommand(unique_ptr<BedrockCommand>& command) {
    if (_isStatusCommand(command)) {
        _status(command);
        _reply(command);
        return true;
    } else if (_isControlCommand(command)) {
        // Control commands can only come from localhost (and thus have an empty `_source`)
        // with the exception of non-secure control commands
        if (command->request["_source"].empty() || _isNonSecureControlCommand(command)) {
            _control(command);
        } else {
            SWARN("Got control command " << command->request.methodLine << " on non-localhost socket ("
                  << command->request["_source"] << "). Ignoring.");
            command->response.methodLine = "401 Unauthorized";
        }
        _reply(command);
        return true;
    }
    return false;
}

bool BedrockServer::_wouldCrash(const unique_ptr<BedrockCommand>& command) {
    // Get a shared lock so that all the workers can look at this map simultaneously.
    shared_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);

    // Typically, this map is empty and this returns no results.
    auto commandIt = _crashCommands.find(command->request.methodLine);
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
            auto it = command->request.nameValueMap.find(pair.first);
            if (it ==  command->request.nameValueMap.end()) {
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

void BedrockServer::_resetServer() {
    _requestCount = 0;
    _replicationState = SQLiteNode::SEARCHING;
    _upgradeInProgress = false;
    _suppressCommandPort = false;
    _suppressCommandPortManualOverride = false;
    _syncThreadComplete = false;
    _syncNode = nullptr;
    _suppressMultiWrite = true;
    _shutdownState = RUNNING;
    _shouldBackup = false;
    _commandPort = nullptr;
    _gracefulShutdownTimeout.alarmDuration = 0;
    _pluginsDetached = false;

    // Tell any plugins that they can attach now
    for (auto plugin : plugins) {
        plugin.second->onAttach();
    }
}

BedrockServer::BedrockServer(SQLiteNode::State state, const SData& args_) : SQLiteServer(""), args(args_), _replicationState(SQLiteNode::LEADING)
{}

BedrockServer::BedrockServer(const SData& args_)
  : SQLiteServer(""), shutdownWhileDetached(false), args(args_), _requestCount(0), _replicationState(SQLiteNode::SEARCHING),
    _upgradeInProgress(false), _suppressCommandPort(false), _suppressCommandPortManualOverride(false),
    _syncThreadComplete(false), _syncNode(nullptr), _suppressMultiWrite(true), _shutdownState(RUNNING),
    _multiWriteEnabled(args.test("-enableMultiWrite")), _shouldBackup(false), _detach(args.isSet("-bootstrap")),
    _controlPort(nullptr), _commandPort(nullptr), _maxConflictRetries(3), _lastQuorumCommandTime(STimeNow()),
    _pluginsDetached(false)
{
    _version = VERSION;

    // Enable the requested plugins, and update our version string if required.
    list<string> pluginNameList = SParseList(args["-plugins"]);
    SINFO("Loading plugins: " << args["-plugins"]);
    vector<string> versions = {_version};
    for (string& pluginName : pluginNameList) {
        auto it = BedrockPlugin::g_registeredPluginList.find(SToUpper(pluginName));
        if (it == BedrockPlugin::g_registeredPluginList.end()) {
            SERROR("Cannot find plugin '" << pluginName << "', aborting.");
        }

        // Create an instance of this plugin.
        BedrockPlugin* plugin = it->second(*this);
        plugins.emplace(make_pair(plugin->getName(), plugin));

        // If the plugin has version info, add it to the list.
        auto info = plugin->getInfo();
        auto iterator = info.find("version");
        if (iterator != info.end()) {
            versions.push_back(plugin->getName() + "_" + iterator->second);
        }
    }
    sort(versions.begin(), versions.end());
    _version = SComposeList(versions, ":");

    list<string> pluginString;
    for (auto& p : plugins) {
        pluginString.emplace_back(p.first);
    }
    SINFO("Creating BedrockServer with plugins: " << SComposeList(pluginString));

    // If `versionOverride` is set, we throw away what we just did and use the overridden value.
    // We'll destruct, sort, and then reconstruct the version string passed in so we aren't relying
    // on the operator to know that they must be sorted.
    if (args.isSet("-versionOverride")) {
        list<string> versionStrings = SParseList(args["-versionOverride"], ':');
        versionStrings.sort();
        _version = SComposeList(versionStrings, ":");
    }

    // Allow enabling tracing at startup.
    if (args.isSet("-enableSQLTracing")) {
        SQLite::enableTrace.store(true);
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
        unique_lock<decltype(_blacklistedParallelCommandMutex)> lock(_blacklistedParallelCommandMutex);
        list<string> parallelCommands;
        SParseList(args["-blacklistedParallelCommands"], parallelCommands);
        for (auto& command : parallelCommands) {
            _blacklistedParallelCommands.insert(command);
        }
    }

    // Allow sending control commands when the server's not LEADING/FOLLOWING.
    SINFO("Opening control port on '" << args["-controlPort"] << "'");
    _controlPort = openPort(args["-controlPort"]);

    // If we're bootstraping this node we need to go into detached mode here.
    // The syncWrapper will handle this for us.
    if (_detach) {
        SINFO("Bootstrap flag detected, starting sync node in detach mode.");
    }

    // Set the quorum checkpoint, or default if not specified.
    _quorumCheckpointSeconds = args.isSet("-quorumCheckpointSeconds") ? args.calc("-quorumCheckpointSeconds") : 60;

    // Start the sync thread, which will start the worker threads.
    SINFO("Launching sync thread '" << _syncThreadName << "'");
    _syncThread = thread(syncWrapper,
                     ref(args),
                     ref(_replicationState),
                     ref(_upgradeInProgress),
                     ref(_leaderVersion),
                     ref(_syncNodeQueuedCommands),
                     ref(*this));
}

BedrockServer::~BedrockServer() {
    // Shut down the sync thread, (which will shut down worker threads in turn).
    SINFO("Closing sync thread '" << _syncThreadName << "'");
    if (_syncThread.joinable()) {
        _syncThread.join();
    }
    SINFO("Threads closed.");

    // Close any sockets that are still open. We wait until the sync thread has completed to do this, as until it's
    // finished, it may keep writing to these sockets.
    if (_socketIDMap.size()) {
        SWARN("Still have " << _socketIDMap.size() << " entries in _socketIDMap.");
    }

    if (socketList.size()) {
        SWARN("Still have " << socketList.size() << " entries in socketList.");
        for (list<Socket*>::iterator socketIt = socketList.begin(); socketIt != socketList.end();) {
            // Shut it down and go to the next (because closeSocket will invalidate this iterator otherwise)
            Socket* s = *socketIt++;
            closeSocket(s);
        }
    }

    // Delete our plugins.
    for (auto& p : plugins) {
        delete p.second;
    }
}

bool BedrockServer::shutdownComplete() {
    if (_detach) {
        if (shutdownWhileDetached) {
            return true;
        }
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
        string commandCounts;
        string blockingCommandCounts;
        list<pair<string*, BedrockCommandQueue*>> queuesToCount = {
            {&commandCounts, &_commandQueue},
            {&blockingCommandCounts, &_blockingCommandQueue}
        };
        for (auto queueCountPair : queuesToCount) {
            map<string, int> commandsInQueue;
            auto methods = queueCountPair.second->getRequestMethodLines();
            for (auto method : methods) {
                auto it = commandsInQueue.find(method);
                if (it != commandsInQueue.end()) {
                    (it->second)++;
                } else {
                    commandsInQueue[method] = 1;
                }
            }
            for (auto cmdPair : commandsInQueue) {
                *(queueCountPair.first) += cmdPair.first + ":" + to_string(cmdPair.second) + ", ";
            }
        }
        SWARN("Graceful shutdown timed out. "
              << "Replication State: " << SQLiteNode::stateName(_replicationState.load()) << ". "
              << "Command queue size: " << _commandQueue.size() << ". "
              << "Blocking command queue size: " << _blockingCommandQueue.size() << ". "
              << "Commands queued: " << commandCounts << ". "
              << "Blocking commands queued: " << blockingCommandCounts << ". "
              << "Killing non-gracefully.");
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

    // If we're a follower, and the leader's on a different version than us, we don't open the command port.
    // If we do, we'll escalate all of our commands to the leader, which causes undue load on leader during upgrades.
    // Instead, we'll simply not respond and let this request get re-directed to another follower.
    string leaderVersion = _leaderVersion.load();
    if (!_suppressCommandPort && state == SQLiteNode::FOLLOWING && (leaderVersion != _version)) {
        SINFO("Node " << args["-nodeName"] << " following on version " << _version << ", leader is version: "
              << leaderVersion << ", not opening command port.");
        suppressCommandPort("leader version mismatch", true);
    } else if (_suppressCommandPort && (state == SQLiteNode::LEADING || (leaderVersion == _version))) {
        // If we become leader, or if leader's version resumes matching ours, open the command port again.
        if (!_suppressCommandPortManualOverride) {
            // Only generate this logline if we haven't manually blocked this.
            SINFO("Node " << args["-nodeName"] << " disabling previously suppressed command port after version check.");
        }
        suppressCommandPort("leader version match", false);
    }
    if (!_suppressCommandPort && (state == SQLiteNode::LEADING || state == SQLiteNode::FOLLOWING) &&
        _shutdownState.load() == RUNNING) {
        // Open the port
        if (!_commandPort) {
            SINFO("Ready to process commands, opening command port on '" << args["-serverHost"] << "'");
            _commandPort = openPort(args["-serverHost"]);
        }
        if (!_controlPort) {
            SINFO("Opening control port on '" << args["-controlPort"] << "'");
            _controlPort = openPort(args["-controlPort"]);
        }

        // Open any plugin ports on enabled plugins
        for (auto plugin : plugins) {
            string portHost = plugin.second->getPort();
            if (!portHost.empty()) {
                bool alreadyOpened = false;
                for (auto pluginPorts : _portPluginMap) {
                    if (pluginPorts.second == plugin.second) {
                        // We've already got this one.
                        alreadyOpened = true;
                        break;
                    }
                }
                // Open the port and associate it with the plugin
                if (!alreadyOpened) {
                    SINFO("Opening port '" << portHost << "' for plugin '" << plugin.second->getName() << "'");
                    Port* port = openPort(portHost);
                    _portPluginMap[port] = plugin.second;
                }
            }
        }
    }

    // **NOTE: We leave the port open between startup and shutdown, even if we enter a state where
    //         we can't process commands -- such as a non leader/follower state.  The reason is we
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
                {
                    SAUTOLOCK(_socketIDMutex);
                    if (s->recvBuffer.empty()) {
                        // If nothing's been received, break early.
                        if (_shutdownState.load() != RUNNING && lastChance && lastChance < STimeNow() && _socketIDMap.find(s->id) == _socketIDMap.end()) {
                            // If we're shutting down and past our lastChance timeout, we start killing these.
                            SINFO("Closing socket " << s->id << " with no data and no pending command: shutting down.");
                            socketsToClose.push_back(s);
                        }
                        break;
                    } else {
                        // Otherwise, we'll see if there's any activity on this socket. Currently, we don't handle clients
                        // pipelining requests well. We process commands in no particular order, so we can't dequeue two
                        // requests off the same socket at one time, or we don't guarantee their return order, thus we just
                        // wait and will try again later.
                        auto socketIt = _socketIDMap.find(s->id);
                        if (socketIt != _socketIDMap.end()) {
                            break;
                        }
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
                    s->recvBuffer.consumeFront(requestSize);
                    deserializationAttempts++;
                }

                // If we have a populated request, from either a plugin or our default handling, we'll queue up the
                // command.
                if (!request.empty()) {
                    SAUTOPREFIX(request);
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

                    // Get the source ip of the command.
                    char *ip = inet_ntoa(s->addr.sin_addr);
                    if (ip != "127.0.0.1"s) {
                        // We only add this if it's not localhost because existing code expects commands that come from
                        // localhost to have it blank.
                        request["_source"] = ip;
                    }

                    // Create a command.
                    unique_ptr<BedrockCommand> command = getCommandFromPlugins(move(request));

                    if (command->writeConsistency != SQLiteNode::QUORUM
                        && _syncCommands.find(command->request.methodLine) != _syncCommands.end()) {

                        command->writeConsistency = SQLiteNode::QUORUM;
                        _lastQuorumCommandTime = STimeNow();
                        SINFO("Forcing QUORUM consistency for command " << command->request.methodLine);
                    }

                    // This is important! All commands passed through the entire cluster must have unique IDs, or they
                    // won't get routed properly from follower to leader and back.
                    command->id = args["-nodeName"] + "#" + to_string(_requestCount++);

                    // And we and keep track of the client that initiated this command, so we can respond later, except
                    // if we received connection:forget in which case we don't respond later
                    command->initiatingClientID = SIEquals(command->request["Connection"], "forget") ? -1 : s->id;

                    // If it's a status or control command, we handle it specially there. If not, we'll queue it for
                    // later processing.
                    if (!_handleIfStatusOrControlCommand(command)) {
                        auto _syncNodeCopy = _syncNode;
                        if (_syncNodeCopy && _syncNodeCopy->getState() == SQLiteNode::STANDINGDOWN) {
                            _standDownQueue.push(move(command));
                        } else {
                            SINFO("Queued new '" << command->request.methodLine << "' command from local client, with "
                                  << _commandQueue.size() << " commands already queued.");
                            _commandQueue.push(move(command));
                        }
                    }
                } else {
                    SAUTOLOCK(_socketIDMutex);
                    // If we weren't able to deserialize a complete request, and we're shutting down, give up.
                    if (_shutdownState.load() != RUNNING && lastChance && lastChance < STimeNow() && _socketIDMap.find(s->id) == _socketIDMap.end()) {
                        SINFO("Closing socket " << s->id << " with incomplete data and no pending command: shutting down.");
                        socketsToClose.push_back(s);
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
    SINFO("[performance] Read from " << socketList.size() << " sockets, attempted to deserialize " << deserializationAttempts
          << " commands, " << deserializedRequests << " were complete and deserialized in " << readElapsedMS << "ms.");

    // Now we can close any sockets that we need to.
    for (auto s: socketsToClose) {
        closeSocket(s);
    }

    // If any plugin timers are firing, let the plugins know.
    for (auto plugin : plugins) {
        for (SStopwatch* timer : plugin.second->timers) {
            if (timer->ding()) {
                plugin.second->timerFired(timer);
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

            // We empty the socket list here, we will no longer allow new requests to come in, as the sync node can
            // shutdown any time after here, and we'll have no way to handle new requests.
            if (socketList.size()) {
                SAUTOLOCK(_socketIDMutex);
                SINFO("Killing " << socketList.size() << " remaining sockets at graceful shutdown timeout.");
                while(socketList.size()) {
                    auto s = socketList.front();
                    _socketIDMap.erase(s->id);
                    closeSocket(s);
                }
            }
            _shutdownState.store(CLIENTS_RESPONDED);
        }
    }
}

unique_ptr<BedrockCommand> BedrockServer::getCommandFromPlugins(SData&& request) {
    return getCommandFromPlugins(make_unique<SQLiteCommand>(move(request)));
}

unique_ptr<BedrockCommand> BedrockServer::getCommandFromPlugins(unique_ptr<SQLiteCommand>&& baseCommand) {
    for (auto pair : plugins) {

        // This is a bit weird to avoid changing this signature in all the plugins. It would be more straightforward if
        // the plugins just accepted a `unique_ptr<SQLiteCommand>&&`, but this still works.
        auto command = pair.second->getCommand(move(*baseCommand));
        if (command) {
            SDEBUG("Plugin " << pair.first << " handling command " << command->request.methodLine);
            return command;
        }
    }

    // Same weirdness as above, but for default commands.
    return make_unique<BedrockCommand>(move(*baseCommand), nullptr);
}

void BedrockServer::_reply(unique_ptr<BedrockCommand>& command) {
    SAUTOLOCK(_socketIDMutex);

    // Finalize timing info even for commands we won't respond to (this makes this data available in logs).
    command->finalizeTimingInfo();

    // Don't reply to commands with pseudo-clients (i.e., commands that we generated by other commands).
    if (command->initiatingClientID < 0) {
        return;
    }

    // Do we have a socket for this command?
    auto socketIt = _socketIDMap.find(command->initiatingClientID);
    if (socketIt != _socketIDMap.end()) {
        command->response["nodeName"] = args["-nodeName"];

        // If we're shutting down, tell the caller to close the connection.
        if (_shutdownState.load() != RUNNING) {
            command->response["Connection"] = "close";
        }

        // Is a plugin handling this command? If so, it gets to send the response.
        const string& pluginName = command->request["plugin"];

        if (!pluginName.empty()) {
            // Let the plugin handle it
            SINFO("Plugin '" << pluginName << "' handling response '" << command->response.methodLine
                  << "' to request '" << command->request.methodLine << "'");
            auto it = plugins.find(pluginName);
            if (it != plugins.end()) {
                it->second->onPortRequestComplete(*command, socketIt->second);
            } else {
                SERROR("Couldn't find plugin '" << pluginName << ".");
            }
        } else {
            // Otherwise we send the standard response.
            socketIt->second->send(command->response.serialize());
        }

        // If `Connection: close` was set, shut down the socket, in case the caller ignores us.
        if (SIEquals(command->request["Connection"], "close") || _shutdownState.load() != RUNNING) {
            shutdownSocket(socketIt->second, SHUT_RDWR);
        }

        // We only keep track of sockets with pending commands.
        _socketIDMap.erase(socketIt);
    } else {
        if (!SIEquals(command->request["Connection"], "forget")) {
            SINFO("No socket to reply for: '" << command->request.methodLine << "' #" << command->initiatingClientID);
        }

        // If the command was processed, tell the plugin we couldn't send the response.
        command->handleFailedReply();
    }
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

bool BedrockServer::_isStatusCommand(const unique_ptr<BedrockCommand>& command) {
    if (SIEquals(command->request.methodLine, STATUS_IS_FOLLOWER)       ||
        SIEquals(command->request.methodLine, STATUS_HANDLING_COMMANDS) ||
        SIEquals(command->request.methodLine, STATUS_PING)              ||
        SIEquals(command->request.methodLine, STATUS_STATUS)            ||
        SIEquals(command->request.methodLine, STATUS_BLACKLIST)         ||
        SIEquals(command->request.methodLine, STATUS_MULTIWRITE)) {
        return true;
    }
    return false;
}

bool BedrockServer::getPeerInfo(list<STable>& peerData) {
    if (_syncMutex.try_lock_for(chrono::milliseconds(10))) {
        lock_guard<decltype(_syncMutex)> lock(_syncMutex, adopt_lock_t());
        auto _syncNodeCopy = _syncNode;
        if (_syncNodeCopy) {
            for (SQLiteNode::Peer* peer : _syncNodeCopy->peerList) {
                peerData.emplace_back(peer->nameValueMap);
                for (auto it : peer->params) {
                    peerData.back()[it.first] = it.second;
                }
                peerData.back()["host"] = peer->host;
                peerData.back()["name"] = peer->name;
                peerData.back()["State"] = SQLiteNode::stateName(peer->state);
            }
        }
    } else {
        return false;
    }
    return true;
}

list<STable> BedrockServer::getPeerInfo() {
    list<STable> peerData;
    getPeerInfo(peerData);
    return peerData;
}

void BedrockServer::setDetach(bool detach) {
    if (detach) {
        _beginShutdown("Detach", true);
    } else {
        _detach = false;
    }
}

bool BedrockServer::isDetached() {
    return _detach && _syncThreadComplete && _pluginsDetached;
}

void BedrockServer::_status(unique_ptr<BedrockCommand>& command) {
    const SData& request  = command->request;
    SData& response = command->response;

    // We'll return whether or not this server is following.
    if (SIEquals(request.methodLine, STATUS_IS_FOLLOWER)) {
        // Used for liveness check for HAProxy. It's limited to HTTP style requests for it's liveness checks, so let's
        // pretend to be an HTTP server for this purpose. This allows us to load balance incoming requests.
        //
        // HAProxy interprets 2xx/3xx level responses as alive, 4xx/5xx level responses as dead.
        SQLiteNode::State state = _replicationState.load();
        if (state == SQLiteNode::FOLLOWING) {
            response.methodLine = "HTTP/1.1 200 Following";
        } else {
            response.methodLine = "HTTP/1.1 500 Not Following. State=" + SQLiteNode::stateName(state);
        }
    } else if (SIEquals(request.methodLine, STATUS_HANDLING_COMMANDS)) {
        // This is similar to the above check, and is used for letting HAProxy load-balance commands.

        if (_version != _leaderVersion.load()) {
            response.methodLine = "HTTP/1.1 500 Mismatched version. Version=" + _version;
        } else {
            SQLiteNode::State state = _replicationState.load();
            string method = "HTTP/1.1 ";

            if (state == SQLiteNode::FOLLOWING || state == SQLiteNode::LEADING || state == SQLiteNode::STANDINGDOWN) {
                method += "200";
            } else {
                method += "500";
            }
            response.methodLine = method + " " + SQLiteNode::stateName(state);
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
            STable pluginData = plugin.second->getInfo();
            pluginData["name"] = plugin.second->getName();
            pluginList.push_back(SComposeJSONObject(pluginData));
        }
        content["isLeader"] = state == SQLiteNode::LEADING ? "true" : "false";
        content["plugins"]  = SComposeJSONArray(pluginList);
        content["state"]    = SQLiteNode::stateName(state);
        content["version"]  = _version;
        content["host"]     = args["-nodeHost"];

        {
            // Make it known if anything is known to cause crashes.
            shared_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
            size_t totalCount = 0;
            for (const auto& s : _crashCommands) {
                totalCount += s.second.size();
            }
            content["crashCommands"] = totalCount;
        }

        // On leader, return the current multi-write blacklists.
        if (state == SQLiteNode::LEADING) {
            // Both of these need to be in the correct state for multi-write to be enabled.
            bool multiWriteOn =  _multiWriteEnabled.load() && !_suppressMultiWrite;
            content["multiWriteEnabled"] = multiWriteOn ? "true" : "false";
            content["multiWriteManualBlacklist"] = SComposeJSONArray(_blacklistedParallelCommands);
        }

        // We read from syncNode internal state here, so we lock to make sure that this doesn't conflict with the sync
        // thread.
        list<string> escalated;
        {
            if (_syncMutex.try_lock_for(chrono::milliseconds(10))) {
                lock_guard<decltype(_syncMutex)> lock(_syncMutex, adopt_lock_t());

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
            } else {
                content["syncNodeBlocked"] = "true";
            }
        }

        // Coalesce all of the peer data into one value to return or return
        // an error message if we timed out getting the peerList data.
        list<string> peerList;
        list<STable> peerData;
        if (getPeerInfo(peerData)) {
            for (const STable& peerTable : peerData) {
                peerList.push_back(SComposeJSONObject(peerTable));
            }
        } else {
            STable peerListResponse;
            peerListResponse["peerListBlocked"] = "true";
            peerList.push_back(SComposeJSONObject(peerListResponse));
        }

        // We can use the `each` functionality to pass a lambda that will grab each method line in
        // `_syncNodeQueuedCommands`.
        list<string> syncNodeQueuedMethods;
        _syncNodeQueuedCommands.each([&syncNodeQueuedMethods](auto& item){
            syncNodeQueuedMethods.push_back(item->request.methodLine);
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
        unique_lock<decltype(_blacklistedParallelCommandMutex)> lock(_blacklistedParallelCommandMutex);

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

bool BedrockServer::_isControlCommand(const unique_ptr<BedrockCommand>& command) {
    if (SIEquals(command->request.methodLine, "BeginBackup")            ||
        SIEquals(command->request.methodLine, "SuppressCommandPort")    ||
        SIEquals(command->request.methodLine, "ClearCommandPort")       ||
        SIEquals(command->request.methodLine, "ClearCrashCommands")     ||
        SIEquals(command->request.methodLine, "Detach")                 ||
        SIEquals(command->request.methodLine, "Attach")                 ||
        SIEquals(command->request.methodLine, "SetConflictParams")      ||
        SIEquals(command->request.methodLine, "SetCheckpointIntervals") ||
        SIEquals(command->request.methodLine, "EnableSQLTracing")
        ) {
        return true;
    }
    return false;
}

bool BedrockServer::_isNonSecureControlCommand(const unique_ptr<BedrockCommand>& command) {
    // A list of non-secure control commands that can be run from another host
    return SIEquals(command->request.methodLine, "SuppressCommandPort") || SIEquals(command->request.methodLine, "ClearCommandPort");
}

void BedrockServer::_control(unique_ptr<BedrockCommand>& command) {
    SData& response = command->response;
    response.methodLine = "200 OK";
    if (SIEquals(command->request.methodLine, "BeginBackup")) {
        _shouldBackup = true;
        _beginShutdown("Detach", true);
    } else if (SIEquals(command->request.methodLine, "SuppressCommandPort")) {
        suppressCommandPort("SuppressCommandPort", true, true);
    } else if (SIEquals(command->request.methodLine, "ClearCommandPort")) {
        suppressCommandPort("ClearCommandPort", false, true);
    } else if (SIEquals(command->request.methodLine, "ClearCrashCommands")) {
        unique_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
        _crashCommands.clear();
    } else if (SIEquals(command->request.methodLine, "Detach")) {
        response.methodLine = "203 DETACHING";
        _beginShutdown("Detach", true);
    } else if (SIEquals(command->request.methodLine, "Attach")) {
        // Ensure none of our plugins are blocking attaching
        list<string> blockingPlugins;
        for (auto plugin : plugins) {
            if (plugin.second->preventAttach()) {
                blockingPlugins.emplace_back(plugin.second->getName());
            }
        }
        if (blockingPlugins.size()) {
            response.methodLine = "401 Attaching prevented by " + SComposeList(blockingPlugins);
        } else {
            response.methodLine = "204 ATTACHING";
            _detach = false;
        }
    } else if (SIEquals(command->request.methodLine, "SetCheckpointIntervals")) {
        response["passiveCheckpointPageMin"] = to_string(SQLite::passiveCheckpointPageMin.load());
        response["fullCheckpointPageMin"] = to_string(SQLite::fullCheckpointPageMin.load());
        if (command->request.isSet("passiveCheckpointPageMin")) {
            SQLite::passiveCheckpointPageMin.store(command->request.calc("passiveCheckpointPageMin"));
        }
        if (command->request.isSet("fullCheckpointPageMin")) {
            SQLite::fullCheckpointPageMin.store(command->request.calc("fullCheckpointPageMin"));
        }
        if (command->request.isSet("MaxConflictRetries")) {
            int retries = command->request.calc("MaxConflictRetries");
            if (retries > 0 && retries <= 100) {
                SINFO("Updating _maxConflictRetries to: " << retries);
                _maxConflictRetries.store(retries);
            }
        }
    } else if (SIEquals(command->request.methodLine, "EnableSQLTracing")) {
        response["oldValue"] = SQLite::enableTrace ? "true" : "false";
        if (command->request.isSet("enable")) {
            SQLite::enableTrace.store(command->request.test("enable"));
            response["newValue"] = SQLite::enableTrace ? "true" : "false";
        }
    }
}

bool BedrockServer::_upgradeDB(SQLite& db) {
    // These all get conglomerated into one big query.
    db.beginTransaction();
    for (auto plugin : plugins) {
        plugin.second->upgradeDatabase(db);
    }
    if (db.getUncommittedQuery().empty()) {
        db.rollback();
    }
    SINFO("Finished running DB upgrade.");
    return !db.getUncommittedQuery().empty();
}

void BedrockServer::_prePollPlugins(fd_map& fdm) {
    for (auto plugin : plugins) {
        for (auto manager : plugin.second->httpsManagers) {
            manager->prePoll(fdm);
        }
    }
}

void BedrockServer::_postPollPlugins(fd_map& fdm, uint64_t nextActivity) {
    // Only pass timeouts for transactions belonging to timed out commands.
    uint64_t now = STimeNow();
    map<SHTTPSManager::Transaction*, uint64_t> transactionTimeouts;
    {
        lock_guard<mutex> lock(_httpsCommandMutex);
        auto timeoutIt = _outstandingHTTPSCommands.begin();
        while (timeoutIt != _outstandingHTTPSCommands.end() && (*timeoutIt)->timeout() < now) {
            // Add all the transactions for this command, even if some are already complete, they'll just get ignored.
            for (auto transaction : (*timeoutIt)->httpsRequests) {
                transactionTimeouts[transaction] = (*timeoutIt)->timeout();
            }
            timeoutIt++;
        }
    }

    for (auto plugin : plugins) {
        for (auto manager : plugin.second->httpsManagers) {
            list<SHTTPSManager::Transaction*> completedHTTPSRequests;
            auto _syncNodeCopy = _syncNode;
            if (_shutdownState.load() != RUNNING || (_syncNodeCopy && _syncNodeCopy->getState() == SQLiteNode::STANDINGDOWN)) {
                // If we're shutting down or standing down, we can't wait minutes for HTTPS requests. They get 5s.
                manager->postPoll(fdm, nextActivity, completedHTTPSRequests, transactionTimeouts, 5000);
            } else {
                // Otherwise, use the default timeout.
                manager->postPoll(fdm, nextActivity, completedHTTPSRequests, transactionTimeouts);
            }

            // Move any fully completed commands back to the main queue.
            finishWaitingForHTTPS(completedHTTPSRequests);
        }
    }
}

void BedrockServer::_beginShutdown(const string& reason, bool detach) {
    if (_shutdownState.load() == RUNNING) {
        _detach = detach;
        // Begin a graceful shutdown; close our port
        SINFO("Beginning graceful shutdown due to '" << reason
              << "', closing command port on '" << args["-serverHost"] << "'");
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
        SINFO("START_SHUTDOWN. Ports shutdown, will perform final socket read. Commands queued: " << _commandQueue.size()
              << ", blocking commands queued: " << _blockingCommandQueue.size());
    }
}

bool BedrockServer::shouldBackup() {
    return _shouldBackup;
}

SData BedrockServer::_generateCrashMessage(const unique_ptr<BedrockCommand>& command) {
    SData message("CRASH_COMMAND");
    SData subMessage(command->request.methodLine);
    for (auto& pair : command->crashIdentifyingValues) {
        subMessage.emplace(pair);
    }
    message.content = subMessage.serialize();
    return message;
}

void BedrockServer::broadcastCommand(const SData& cmd) {
    SData message("BROADCAST_COMMAND");
    message.content = cmd.serialize();
    lock_guard<decltype(_syncMutex)> lock(_syncMutex);
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
            unique_ptr<BedrockCommand> cmd = getCommandFromPlugins(move(command));
            for (const auto& fields : table) {
                cmd->crashIdentifyingValues.insert(fields.first);
            }
            auto _syncNodeCopy = _syncNode;
            if (_syncNodeCopy) {
                _syncNodeCopy->broadcast(_generateCrashMessage(cmd), peer);
            }
        }
    }
}

void BedrockServer::_finishPeerCommand(unique_ptr<BedrockCommand>& command) {
    // See if we're supposed to forget this command (because the follower is not listening for a response).
    auto it = command->request.nameValueMap.find("Connection");
    bool forget = it != command->request.nameValueMap.end() && SIEquals(it->second, "forget");
    command->finalizeTimingInfo();
    if (forget) {
        SINFO("Not responding to 'forget' command '" << command->request.methodLine << "' from follower.");
    } else {
        auto _syncNodeCopy = _syncNode;
        if (_syncNodeCopy) {
            _syncNodeCopy->sendResponse(*command);
        }
    }
}

void BedrockServer::_acceptSockets() {
    Socket* s = nullptr;
    Port* acceptPort = nullptr;
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

void BedrockServer::waitForHTTPS(unique_ptr<BedrockCommand>&& command) {
    lock_guard<mutex> lock(_httpsCommandMutex);

    // Un-uniquify the unique_ptr. I don't love this, but it works better with the code we've already got.
    BedrockCommand* commandPtr = command.get();
    command.release();

    // And we keep it in a set of all commands with outstanding HTTPS requests.
    _outstandingHTTPSCommands.insert(commandPtr);

    for (auto request : commandPtr->httpsRequests) {
        if (!request->response) {
            _outstandingHTTPSRequests.emplace(make_pair(request, commandPtr));
        }
    }
}

int BedrockServer::finishWaitingForHTTPS(list<SHTTPSManager::Transaction*>& completedHTTPSRequests) {
    lock_guard<mutex> lock(_httpsCommandMutex);
    int commandsCompleted = 0;
    for (auto transaction : completedHTTPSRequests) {
        auto transactionIt = _outstandingHTTPSRequests.find(transaction);
        if (transactionIt == _outstandingHTTPSRequests.end()) {
            // We should never be looking for a transaction in this list that isn't there.
            SWARN("Couldn't locate transaction in _outstandingHTTPSRequests. Skipping.");
            continue;
        }
        auto commandPtr = transactionIt->second;

        // It's possible that we've already completed this command (imagine if completedHTTPSRequests contained more
        // than one request for the same command, the first one we looked at will have finished the command), so if we
        // can't find it in _outstandingHTTPSCommands, it must be done.
        auto commandPtrIt = _outstandingHTTPSCommands.find(commandPtr);
        if (commandPtrIt != _outstandingHTTPSCommands.end()) {
            // I guess it's still here! Is it done?
            if (commandPtr->areHttpsRequestsComplete()) {
                // If so, add it back to the main queue, erase its entry in _outstandingHTTPSCommands, and delete it.
                _commandQueue.push(unique_ptr<BedrockCommand>(commandPtr));
                _outstandingHTTPSCommands.erase(commandPtrIt);
                commandsCompleted++;
            }
        }

        // Now we can erase the transaction, as it's no longer outstanding.
        _outstandingHTTPSRequests.erase(transactionIt);
    }
    return commandsCompleted;
}

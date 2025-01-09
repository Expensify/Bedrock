// Manages connections to a single instance of the bedrock server.
#include "BedrockServer.h"

#include <arpa/inet.h>
#include <csignal>
#include <cstring>
#include <fstream>
#include <sys/resource.h>
#include <sys/time.h>

#include <bedrockVersion.h>
#include <BedrockCore.h>
#include <BedrockPlugin.h>
#include <libstuff/libstuff.h>
#include <libstuff/SRandom.h>
#include <libstuff/AutoTimer.h>
#include <PageLockGuard.h>
#include <sqlitecluster/SQLitePeer.h>

set<string>BedrockServer::_blacklistedParallelCommands;
shared_timed_mutex BedrockServer::_blacklistedParallelCommandMutex;
thread_local atomic<SQLiteNodeState> BedrockServer::_nodeStateSnapshot = SQLiteNodeState::UNKNOWN;

void BedrockServer::syncWrapper()
{
    // Initialize the thread.
    SInitialize(_syncThreadName);
    isSyncThread = true;

    while(true) {
        // If the server's set to be detached, we wait until that flag is unset, and then start the sync thread.
        if (_detach) {
            // If we're set detached, we assume we'll be re-attached eventually, and then be `RUNNING`.
            SINFO("Bedrock server entering detached state.");
            _shutdownState.store(RUNNING);

            // Detach any plugins now
            for (auto plugin : plugins) {
                plugin.second->onDetach();
            }
            _pluginsDetached = true;
            while (_detach) {
                if (shutdownWhileDetached) {
                    SINFO("Bedrock server exiting from detached state.");
                    return;
                }
                // Just wait until we're attached.
                SINFO("Bedrock server sleeping in detached state.");
                sleep(1);
            }
            SINFO("Bedrock server entering attached state.");
            _resetServer();
        }
        sync();

        // Now that we've run the sync thread, we can exit if it hasn't set _detach again.
        if (!_detach) {
            break;
        }
    }

    // Break out of `poll` in main.cpp.
    _notifyDone.push(true);
    SINFO("Exiting syncWrapper");
}

shared_ptr<SQLitePool> BedrockServer::getDBPool() {
    return _dbPool;
}

void BedrockServer::sync()
{
    // Parse out the number of worker threads we'll use. The DB needs to know this because it will expect a
    // corresponding number of journal tables. "-readThreads" exists only for backwards compatibility.
    int workerThreads = args.calc("-workerThreads");

    // TODO: remove when nothing uses readThreads.
    workerThreads = workerThreads ? workerThreads : args.calc("-readThreads");

    // If still no value, use the number of cores on the machine, if available.
    SINFO("Note: thread::hardware_concurrency() is: " << thread::hardware_concurrency());
    workerThreads = workerThreads ? workerThreads : max(1u, thread::hardware_concurrency());

    // A minimum of *2* worker threads are required. One for blocking writes, one for other commands.
    if (workerThreads < 2) {
        workerThreads = 2;
    }

    size_t journalTables = workerThreads;
    if (args.isSet("-journalTables")) {
        journalTables = args.calcU64("-journalTables");
    }

    // Initialize the DB.
    int64_t mmapSizeGB = args.isSet("-mmapSizeGB") ? stoll(args["-mmapSizeGB"]) : 0;

    // We use fewer FDs on test machines that have other resource restrictions in place.

    SINFO("Setting dbPool size to: " << _dbPoolSize);
    _dbPool = make_shared<SQLitePool>(_dbPoolSize, args["-db"], args.calc("-cacheSize"), args.calc("-maxJournalSize"), journalTables, mmapSizeGB, args.isSet("-hctree"), args["-checkpointMode"]);
    SQLite& db = _dbPool->getBase();

    // Initialize the command processor.
    BedrockCore core(db, *this);

    // And the sync node.
    uint64_t firstTimeout = STIME_US_PER_M * 2 + SRandom::rand64() % STIME_US_PER_S * 30;

    // Initialize the shared pointer to our sync node object.
    atomic_store(&_syncNode, make_shared<SQLiteNode>(*this, _dbPool, args["-nodeName"], args["-nodeHost"],
                                                            args["-peerList"], args.calc("-priority"), firstTimeout,
                                                            _version, args["-commandPortPrivate"]));

    _clusterMessenger = make_shared<SQLiteClusterMessenger>(_syncNode);

    // The node is now coming up, and should eventually end up in a `LEADING` or `FOLLOWING` state. We can start adding
    // our worker threads now. We don't wait until the node is `LEADING` or `FOLLOWING`, as it's state can change while
    // it's running, and our workers will have to maintain awareness of that state anyway.
    SINFO("Starting " << workerThreads << " worker threads.");
    list<thread> workerThreadList;
    for (int threadId = 0; threadId < workerThreads; threadId++) {
        workerThreadList.emplace_back(&BedrockServer::worker, this, threadId);
    }

    // Now we jump into our main command processing loop.
    uint64_t nextActivity = STimeNow();
    unique_ptr<BedrockCommand> command(nullptr);
    bool committingCommand = false;

    // Timer for S_poll performance logging. Created outside the loop because it's cumulative.
    AutoTimer pollTimer("sync thread poll");
    AutoTimer postPollTimer("sync thread PostPoll");
    AutoTimer escalateLoopTimer("sync thread escalate loop");

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
            SAUTOLOCK(_futureCommitCommandMutex);

            // First, see if anything has timed out, and move that back to the main queue.
            if (_futureCommitCommandTimeouts.size()) {
                uint64_t now = STimeNow();
                auto it =  _futureCommitCommandTimeouts.begin();
                while (it != _futureCommitCommandTimeouts.end() && it->first < now) {
                    // Find commands depending on this commit.
                    auto itPair =  _futureCommitCommands.equal_range(it->second);
                    for (auto cmdIt = itPair.first; cmdIt != itPair.second; cmdIt++) {
                        // Check for one with this timeout.
                        if (cmdIt->second->timeout() == it->first) {
                            // This command has the right commit count *and* timeout, return it.
                            SINFO("Returning command (" << cmdIt->second->request.methodLine << ") waiting on commit " << cmdIt->first
                                  << " to queue, timed out at: " << now << ", timeout was: " << it->first << ".");

                            // Goes back to the main queue, where it will hit it's timeout in a worker thread.
                            _commandQueue.push(move(cmdIt->second));

                            // And delete it, it's gone.
                             _futureCommitCommands.erase(cmdIt);

                            // Done.
                            break;
                        }
                    }
                    it++;
                }

                // And remove everything we just iterated through.
                if (it != _futureCommitCommandTimeouts.begin()) {
                    _futureCommitCommandTimeouts.erase(_futureCommitCommandTimeouts.begin(), it);
                }
            }

            // Anything that hasn't timed out might be ready to return because the commit count is up-to-date.
            if (!_futureCommitCommands.empty()) {
                uint64_t commitCount = db.getCommitCount();
                auto it = _futureCommitCommands.begin();
                while (it != _futureCommitCommands.end() && (it->first <= commitCount || _shutdownState.load() != RUNNING)) {
                    // Save the timeout since we'll be moving the command, thus making this inaccessible.
                    uint64_t commandTimeout = it->second->timeout();
                    SINFO("Returning command (" << it->second->request.methodLine << ") waiting on commit " << it->first
                          << " to queue, now have commit " << commitCount);
                    _commandQueue.push(move(it->second));

                    // Remove it from the timed out list as well.
                    auto itPair = _futureCommitCommandTimeouts.equal_range(commandTimeout);
                    for (auto timeoutIt = itPair.first; timeoutIt != itPair.second; timeoutIt++) {
                        if (timeoutIt->second == it->first) {
                             _futureCommitCommandTimeouts.erase(timeoutIt);
                            break;
                        }
                    }
                    it++;
                }
                if (it != _futureCommitCommands.begin()) {
                    _futureCommitCommands.erase(_futureCommitCommands.begin(), it);
                }
            }
        }

        // If we're in a state where we can initialize shutdown, then go ahead and do so.
        // Having responded to all clients means there are no *local* clients, but it doesn't mean there are no
        // escalated commands. This is fine though - if we're following, there can't be any escalated commands, and if
        // we're leading, then the next update() loop will set us to standing down, and then we won't accept any new
        // commands, and we'll shortly run through the existing queue.
        if (_shutdownState.load() == COMMANDS_FINISHED) {
            SINFO("All clients responded to, " << BedrockCommand::getCommandCount() << " commands remaining.");
            if (_syncNode->beginShutdown()) {
                SINFO("Beginning shuttdown of sync node.");
                // This will cause us to skip the next `poll` iteration which avoids a 1 second wait.
                _notifyDoneSync.push(true);
            }
        }

        // The fd_map contains a list of all file descriptors (eg, sockets, Unix pipes) that poll will wait on for
        // activity. Once any of them has activity (or the timeout ends), poll will return.
        fd_map fdm;

        // Pre-process any sockets the sync node is managing (i.e., communication with peer nodes).
        _notifyDoneSync.prePoll(fdm);
        _syncNode->prePoll(fdm);

        // Add our command queues to our fd_map.
        _syncNodeQueuedCommands.prePoll(fdm);

        // Wait for activity on any of those FDs, up to a timeout.
        const uint64_t now = STimeNow();
        {
            AutoTimerTime pollTime(pollTimer);
            S_poll(fdm, max(nextActivity, now) - now);
            if (SCheckSignal(SIGTERM)) {
                _notifyDone.push(true);
            }
        }

        // And set our next timeout for 1 second from now.
        nextActivity = STimeNow() + STIME_US_PER_S;

        // Process any network traffic that happened. Scope this so that we can change the log prefix and have it
        // auto-revert when we're finished.
        {
            // Set the default log prefix.
            SAUTOPREFIX(SData{});

            // Process any activity in our plugins.
            AutoTimerTime postPollTime(postPollTimer);
            _syncNode->postPoll(fdm, nextActivity);
            _syncNodeQueuedCommands.postPoll(fdm);
            _notifyDoneSync.postPoll(fdm);
        }

        // Ok, let the sync node to it's updating for as many iterations as it requires. We'll update the replication
        // state when it's finished.
        SQLiteNodeState preUpdateState = _syncNode->getState();
        if(command && committingCommand) {
            void (*onPrepareHandler)(SQLite& db, int64_t tableID) = nullptr;
            bool enabled = command->shouldEnableOnPrepareNotification(db, &onPrepareHandler);
            if (enabled) {
                _syncNode->onPrepareHandlerEnabled = enabled;
                _syncNode->onPrepareHandler = onPrepareHandler;
            }
        } else {
            _syncNode->onPrepareHandlerEnabled = false;
            _syncNode->onPrepareHandler = nullptr;
        }
        while (_syncNode->update()) {}
        _leaderVersion.store(_syncNode->getLeaderVersion());

        // If we're not leading, move any commands from the blocking queue back to the main queue.
        if (getState() != SQLiteNodeState::LEADING && getState() != preUpdateState) {
            auto commands = _blockingCommandQueue.getAll();
            SINFO("Moving " << commands.size() << " commands from blocking queue to main queue.");
            for (auto& cmd : commands) {
                _commandQueue.push(move(cmd));
            }
        }

        // If we were LEADING, but we've transitioned, then something's gone wrong (perhaps we got disconnected
        // from the cluster). Reset some state and try again.
        if ((preUpdateState == SQLiteNodeState::LEADING || preUpdateState == SQLiteNodeState::STANDINGDOWN) &&
            (getState() != SQLiteNodeState::LEADING && getState() != SQLiteNodeState::STANDINGDOWN)) {

            // If we bailed out while doing a upgradeDB, clear state
            if (_upgradeInProgress) {
                _upgradeInProgress = false;
                if (committingCommand) {
                    db.rollback();
                    committingCommand = false;
                }
            }

            // If we're not leading, we're not upgrading, but we will need to check for upgrades again next time we go leading, so be ready for that.
            _upgradeCompleted = false;

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
                    command = _syncNodeQueuedCommands.pop();
                    if (command->initiatingClientID) {
                        // This one came from a local client, so we can save it for later.
                        _commandQueue.push(move(command));
                    }
                }
            } catch (const out_of_range& e) {
                if (dropped) {
                    SWARN("Abruptly stopped LEADING. Re-queued " << requeued << " commands, Dropped " << dropped << " commands.");
                } else {
                    SINFO("Abruptly stopped LEADING. Re-queued " << requeued << " commands, Dropped " << dropped << " commands.");
                }

                // command will be null here, we should be able to restart the loop.
                continue;
            }
        }

        // Now that we've cleared any state associated with switching away from leading, we can bail out and try again
        // until we're either leading or following.
        if (getState() != SQLiteNodeState::LEADING && getState() != SQLiteNodeState::FOLLOWING && getState() != SQLiteNodeState::STANDINGDOWN) {
            continue;
        }

        // If we've just switched to the leading state, we want to upgrade the DB. We set a global `upgradeInProgress`
        // flag to prevent workers from trying to use the DB while we do this.
        // It's also possible for the upgrade to fail on the first try, in the case that our followers weren't ready to
        // receive the transaction when we started. In this case, we'll try the upgrade again if we were already
        // leading, and the upgrade is still in progress (because the first try failed), and we're not currently
        // attempting to commit it.
        if ((preUpdateState != SQLiteNodeState::LEADING && getState() == SQLiteNodeState::LEADING) ||
            (getState() == SQLiteNodeState::LEADING && _upgradeInProgress && !committingCommand)) {
            // Store this before we start writing to the DB, which can take a while depending on what changes were made
            // (for instance, adding an index).
            _upgradeInProgress = true;
            if (!_syncNode->hasQuorum()) {
                // We are now "upgrading" but we won't actually start the commit until the cluster is sufficiently
                // connected. This is because if we need to roll back the commit, it disconnects the entire cluster,
                // which is more likely to trigger the same thing to happen again, making cluster startup take
                // significantly longer. In this case we'll just loop again, like if the upgrade failed.
                SINFO("Waiting for quorum availability before running UpgradeDB.");
                continue;
            }
            if (_upgradeDB(db)) {
                committingCommand = true;
                _syncNode->startCommit(SQLiteNode::QUORUM);
                _lastQuorumCommandTime = STimeNow();

                // This interrupts the next poll loop immediately. This prevents a 1-second wait when running as a single server.
                _notifyDoneSync.push(true);
                SDEBUG("Finished sending distributed transaction for db upgrade.");

                // As it's a quorum commit, we'll need to read from peers. Let's start the next loop iteration.
                continue;
            } else {
                // If we're not doing an upgrade, we don't need to keep suppressing multi-write, and we're done with
                // the upgradeInProgress flag.
                _upgradeInProgress = false;
                _upgradeCompleted = true;
                SINFO("UpgradeDB skipped, done.");
            }
        }

        // If we started a commit, and one's not in progress, then we've finished it and we'll take that command and
        // stick it back in the appropriate queue.
        if (committingCommand && !_syncNode->commitInProgress()) {
            // Record the time spent, unless we were upgrading, in which case, there's no command to write to.
            if (command) {
                command->stopTiming(BedrockCommand::COMMIT_SYNC);
            }
            committingCommand = false;

            // If we were upgrading, there's no response to send, we're just done.
            if (_upgradeInProgress) {
                if (_syncNode->commitSucceeded()) {
                    _upgradeInProgress = false;
                    _upgradeCompleted = true;
                    SINFO("UpgradeDB succeeded, done.");
                    _notifyDone.push(true);
                } else {
                    SINFO("UpgradeDB failed, trying again.");
                }
                continue;
            }

            if (command->shouldPostProcess() && command->response.methodLine == "200 OK") {
                // PostProcess if the command should run postProcess, and there have been no errors thrown thus far.
                core.postProcessCommand(command, false);
            }

            if (_syncNode->commitSucceeded()) {
                if (command) {
                    SINFO("[performance] Sync thread finished committing command " << command->request.methodLine);
                    _conflictManager.recordTables(command->request.methodLine, db.getTablesUsed());

                    // Otherwise, save the commit count, mark this command as complete, and reply.
                    command->response["commitCount"] = to_string(db.getCommitCount());
                    command->complete = true;
                    _reply(command);
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
                if (command) {
                    SINFO("requeueing command " << command->request.methodLine
                          << " after failed sync commit. Sync thread has " << _syncNodeQueuedCommands.size()
                          << " queued commands.");
                    _syncNodeQueuedCommands.push(move(command));
                } else {
                    SERROR("Unexpected sync thread commit state.");
                }
            }
        }

        // We're either leading, standing down, or following. There could be a commit in progress on `command`, but
        // there could also be other finished work to handle while we wait for that to complete. Let's see if we can
        // handle any of that work.
        try {
            // We don't start processing a new command until we've completed any existing ones.
            if (committingCommand) {
                continue;
            }

            // Don't escalate, leader can't handle the command anyway. Don't even dequeue the command, just leave it
            // until one of these states changes. This prevents an endless loop of escalating commands, having
            // SQLiteNode re-queue them because leader is standing down, and then escalating them again until leader
            // sorts itself out.
            if (getState() == SQLiteNodeState::FOLLOWING && _syncNode->leaderState() == SQLiteNodeState::STANDINGDOWN) {
                continue;
            }

            // We want to run through all of the commands in our queue. However, we set a maximum limit. This list is
            // potentially infinite, as we can add new commands to the list as we iterate across it (coming from
            // workers), and we will need to break and read from the network to see what to do next at some point.
            // Additionally, in exceptional cases, if we get stuck in this loop for more than 64k commands, we can hit
            // the internal limit of the buffer for the pipe inside _syncNodeQueuedCommands, and writes there will
            // block, and this can cause deadlocks in various places. This is cleared every time we run `postPoll` for
            // _syncNodeQueuedCommands, which occurs when break out of this loop, so we do so periodically to avoid
            // this.
            // TODO: We could potentially make writes to the pipe in the queue non-blocking and help to mitigate that
            // part of this issue as well.
            size_t escalateCount = 0;
            while (++escalateCount < 1000) {
                AutoTimerTime escalateTime(escalateLoopTimer);

                // Reset this to blank. This releases the existing command and allows it to get cleaned up.
                command = unique_ptr<BedrockCommand>(nullptr);

                // Get the next sync node command to work on.
                command = _syncNodeQueuedCommands.pop();

                // We got a command to work on! Set our log prefix to the request ID.
                SAUTOPREFIX(command->request);
                SINFO("Sync thread dequeued command " << command->request.methodLine << ". Sync thread has "
                      << _syncNodeQueuedCommands.size() << " queued commands.");

                if (command->timeout() < STimeNow()) {
                    SINFO("Command '" << command->request.methodLine << "' timed out in sync thread queue, sending back to main queue.");
                    _commandQueue.push(move(command));
                    break;
                }

                // Set the function that will be called if this thread's signal handler catches an unrecoverable error,
                // like a segfault. Note that it's possible we're in the middle of sending a message to peers when we call
                // this, which would probably make this message malformed. This is the best we can do.
                SSetSignalHandlerDieFunc([&](){
                    _clusterMessenger->runOnAll(_generateCrashMessage(command));
                    return addLogParams("CRASHING from BedrockServer::sync, command:" +  command->request.methodLine, command->request.nameValueMap);
                });

                // And now we'll decide how to handle it.
                if (getState() == SQLiteNodeState::LEADING || getState() == SQLiteNodeState::STANDINGDOWN) {
                    // We peek commands here in the sync thread to be able to run peek and process as part of the same
                    // transaction. This guarantees that any checks made in peek are still valid in process, as the DB can't
                    // have changed in the meantime.
                    // IMPORTANT: This check is omitted for commands with an HTTPS request object, because we don't want to
                    // risk duplicating that request. If your command creates an HTTPS request, it needs to explicitly
                    // re-verify that any checks made in peek are still valid in process.
                    if (!command->httpsRequests.size()) {
                        if (command->shouldPrePeek() && !command->repeek) {
                            core.prePeekCommand(command, false);
                        }

                        // This command finsihed in prePeek, which likely means it threw.
                        // We'll respond to it now, either directly or by sending it back to the sync thread.
                        if (command->complete) {
                            SINFO("Command completed in prePeek, replying now.");
                            _reply(command);
                            break;
                        }

                        BedrockCore::RESULT result = core.peekCommand(command, true);
                        if (result == BedrockCore::RESULT::COMPLETE) {
                            // This command completed in peek, respond to it appropriately, either directly or by sending it
                            // back to the sync thread.
                            SASSERT(command->complete);
                            _reply(command);

                            break;
                        } else if (result == BedrockCore::RESULT::SHOULD_PROCESS) {
                            // This is sort of the "default" case after checking if this command was complete above. If so,
                            // we'll fall through to calling processCommand below.
                        } else {
                            SERROR("peekCommand (" << command->request.getVerb() << ") returned invalid result code: " << (int)result);
                        }

                        // If this command attempted an HTTP request, kill it.
                        if (command->httpsRequests.size()) {
                            SWARN("Killing command " << command->request.methodLine << " that attempted HTTPS request in sync thread.");
                            command->response.clear();
                            command->response.methodLine = "500 Refused";
                            command->complete = true;
                            _reply(command);
                            core.rollback();
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
                        _syncNode->startCommit(command->writeConsistency);

                        // And we'll start the next main loop.
                        // NOTE: This will cause us to read from the network again. This, in theory, is fine, but we saw
                        // performance problems in the past trying to do something similar on every commit. This may be
                        // alleviated now that we're only doing this on *sync* commits instead of all commits, which should
                        // be a much smaller fraction of all our traffic. We set nextActivity here so that there's no
                        // timeout before we'll give up on poll() if there's nothing to read.
                        nextActivity = STimeNow();
                        break;
                    } else if (result == BedrockCore::RESULT::NO_COMMIT_REQUIRED) {
                        // Otherwise, the command doesn't need a commit (maybe it was an error, or it didn't have any work
                        // to do). We'll just respond.
                        _reply(command);
                    } else if (result == BedrockCore::RESULT::SERVER_NOT_LEADING) {
                        SINFO("Server stopped leading, re-queueing commad");
                        _commandQueue.push(move(command));
                        break;
                    } else {
                        SERROR("processCommand (" << command->request.getVerb() << ") returned invalid result code: " << (int)result);
                    }

                    // When we're leading, we'll try and handle one command and then stop.
                    break;
                } else if (getState() == SQLiteNodeState::FOLLOWING) {
                    SWARN("Sync thread has command when following. Re-queueing");
                    _commandQueue.push(move(command));
                }
            }
            if (escalateCount == 1000) {
                SINFO("Escalated 1000 commands without hitting the end of the queue. Breaking.");
            }
        } catch (const out_of_range& e) {
            // _syncNodeQueuedCommands had no commands to work on, we'll need to re-poll for some.
            continue;
        }
    } while (!_syncNode->shutdownComplete() || BedrockCommand::getCommandCount());

    SSetSignalHandlerDieFunc([](){
        return "Dying in shutdown";
    });

    // If we forced a shutdown mid-transaction (this can happen, if, for instance, we hit our graceful timeout between
    // getting a `BEGIN_TRANSACTION` and `COMMIT_TRANSACTION`) then we need to roll back the existing transaction and
    // release the lock.
    if (_syncNode->commitInProgress()) {
        SWARN("Shutting down mid-commit. Rolling back.");
        db.rollback();
    }

    // We've finished shutting down the sync node, tell the workers that it's finished.
    _shutdownState.store(DONE);
    SINFO("Sync thread finished with commands.");

    // We just fell out of the loop where we were waiting for shutdown to complete. Update the state one last time when
    // the writing replication thread exits.
    if (getState() > SQLiteNodeState::WAITING) {
        // This should no longer be possible with fast shutdown.
        SWARN("Sync thread exiting in state " << SQLiteNode::stateName(getState()) << ".");
    }

    // Wait for the worker threads to finish.
    int threadId = 0;
    for (auto& workerThread : workerThreadList) {
        SINFO("Joining worker thread '" << "worker" << threadId << "'");
        threadId++;
        workerThread.join();
    }

    // If there's anything left in the command queue here, we'll discard it, because we have no way of processing it.
    if (_commandQueue.size()) {
        SWARN("Sync thread shut down with " << _commandQueue.size() << " queued commands. Commands were: "
              << SComposeList(_commandQueue.getRequestMethodLines()) << ". Clearing.");
        _commandQueue.clear();
    }

    // Same for the blocking queue.
    if (_blockingCommandQueue.size()) {
        SWARN("Sync thread shut down with " << _blockingCommandQueue.size() << " blocking queued commands. Commands were: "
              << SComposeList(_blockingCommandQueue.getRequestMethodLines()) << ". Clearing.");
        _blockingCommandQueue.clear();
    }

    for (auto plugin : plugins) {
        plugin.second->serverStopping();
    }

    // We clear this before the _syncNode that it references.
    _clusterMessenger.reset();

    // Release our handle to this pointer. Any other functions that are still using it will keep the object alive
    // until they return.
    atomic_store(&_syncNode, shared_ptr<SQLiteNode>(nullptr));

    // If we're not detaching, save that we're shutting down.
    if (!_detach) {
        ofstream file("/var/log/bedrock_shutdown", std::ios::app);
        if (file) {
            file << "shutdown " << getpid() << " " << SComposeTime("%Y-%m-%dT%H:%M:%S", STimeNow()) << endl;
            file.close();
        }
    }

    // Release the current DB pool, and zero out our pointer. If any socket threads hold a handle to `_syncNode`, they will keep this in existence
    // until they release it.
    _dbPool = nullptr;

    // We're really done, store our flag so main() can be aware.
    _syncLoopShouldBeRunning.store(false);
}

void BedrockServer::worker(int threadId)
{
    // Worker 0 is the "blockingCommit" thread.
    SInitialize(threadId ? "worker" + to_string(threadId) : "blockingCommit");

    // Command to work on. This default command is replaced when we find work to do.
    unique_ptr<BedrockCommand> command(nullptr);

    // Which command queue do we use? The blockingCommit thread special and does blocking commits from the blocking queue.
    BedrockCommandQueue& commandQueue = threadId ? _commandQueue : _blockingCommandQueue;

    // We just run this loop looking for commands to process forever. There's a check for appropriate exit conditions
    // at the bottom, which will cause our loop and thus this thread to exit when that becomes true.
    while (true) {
        try {
            // Set a signal handler function that we can call even if we die early with no command.
            SSetSignalHandlerDieFunc([&](){
                return "Die function called early with no command, probably died in `commandQueue.get`.";
            });

            // Get the next one.
            command = commandQueue.get(100000);

            SAUTOPREFIX(command->request);
            SINFO("Dequeued command " << command->request.methodLine << " (" << command->id << ") in worker, "
                  << commandQueue.size() << " commands in " << (threadId ? "" : "blocking") << " queue.");

            runCommand(move(command), threadId == 0, false);
        } catch (const BedrockCommandQueue::timeout_error& e) {
            // No commands to process after 1 second.
            // If the sync node has shut down, we can return now, there will be no more work to do.
            if  (_shutdownState.load() == DONE) {
                SINFO("No commands found in queue and DONE.");
                return;
            }
        }
    }
}

void BedrockServer::runCommand(unique_ptr<BedrockCommand>&& _command, bool isBlocking, bool hasDedicatedThread) {
    // If there's no sync node (because we're detaching/attaching), we can only queue a command for later.
    // Also,if this command is scheduled in the future, we can't just run it, we need to enqueue it to run at that point.
    // This functionality will go away as we remove the queues from bedrock, and so this can be removed at that time.
    {
        auto _syncNodeCopy = atomic_load(&_syncNode);
        if (!_syncNodeCopy || _command->request.calcU64("commandExecuteTime") > STimeNow()) {
            _commandQueue.push(move(_command));
            return;
        }
    }

    // This takes ownership of the passed command. By calling the move constructor, the caller's unique_ptr is now empty, and so when the one here goes out of scope (i.e., this function
    // returns), the command is destroyed.
    unique_ptr<BedrockCommand> command(move(_command));

    SAUTOPREFIX(command->request);

    // Set the function that lets the signal handler know which command caused a problem, in case that happens.
    // If a signal is caught on this thread, which should only happen for unrecoverable, yet synchronous
    // signals, like SIGSEGV, this function will be called.
    SSetSignalHandlerDieFunc([&](){
        _clusterMessenger->runOnAll(_generateCrashMessage(command));
        return addLogParams("CRASHING from BedrockServer::runCommand, command:" +  command->request.methodLine, command->request.nameValueMap);
    });

    // If we dequeue a status or control command, handle it immediately.
    if (_handleIfStatusOrControlCommand(command)) {
        return;
    }

    // Check if this command would be likely to cause a crash
    if (_wouldCrash(command)) {
        // If so, make a lot of noise, and respond 500 without processing it.
        SALERT("REJECTING CRASH-INDUCING COMMAND, command:" +  command->request.methodLine, command->request.nameValueMap);
        command->response.methodLine = "500 Refused";
        command->complete = true;
        _reply(command);
        return;
    }

    // If we're following, we will automatically escalate any command that's:
    // 1. Not already complete (complete commands are likely already returned from leader with legacy escalation)
    // and is marked as `escalateImmediately` (which lets them skip the queue, which is particularly useful if they're waiting
    // for a previous commit to be delivered to this follower);
    // 2. Any commands if the current version of the code is not the same one as leader is executing.
    if (getState() == SQLiteNodeState::FOLLOWING && !command->complete && (command->escalateImmediately || _version != _leaderVersion.load())) {
        auto _clusterMessengerCopy = _clusterMessenger;
        if (command->escalateImmediately && _clusterMessengerCopy && _clusterMessengerCopy->runOnPeer(*command, true)) {
            // command->complete is now true for this command. It will get handled a few lines below.
            SINFO("Immediately escalated " << command->request.methodLine << " to leader.");
        } else if (_version != _leaderVersion.load() && _clusterMessengerCopy && _clusterMessengerCopy->runOnPeer(*command, false)) {
            SINFO("Escalated " << command->request.methodLine << " to follower peer.");
        } else {
            SINFO("Couldn't escalate command " << command->request.methodLine << " to " << (command->escalateImmediately ? "leader" : "follower peer") << ", queuing it again.");
            _commandQueue.push(move(command));
            return;
        }
    }

    // If we happen to be synchronizing but the command port is open, which is an uncommon but possible scenario (i.e., we were momentarily disconnected from leader and need to catch back
    // up), we will forward commands to any other follower similar to if we were running as a different version from leader.
    if (getState() == SQLiteNodeState::SYNCHRONIZING) {
        auto _clusterMessengerCopy = _clusterMessenger;
        bool result = _clusterMessengerCopy->runOnPeer(*command, false);
        if (result) {
            SINFO("Synchronizing while accepting commands; successfully forwarded the command to peer", {{"command", command->request.methodLine}});
        } else {
            SWARN("Synchronizing while accepting commands, but failed to forward the command to peer.", {{"command", command->request.methodLine}});
        }
    }

    // If this command is already complete, then we should be a follower, and the sync node got a response back
    // from a command that had been escalated to leader, and queued it for a worker to respond to. We'll send
    // that response now.
    if (command->complete) {
        // If this command is already complete, we can return it to the caller.
        // Make sure we have an initiatingClientID at this point. If we do, but it's negative, it's for a
        // client that we can't respond to, so we don't bother sending the response.
        SASSERT(command->initiatingClientID);
        if (command->initiatingClientID > 0) {
            _reply(command);
        }

        // This command is done, move on to the next one.
        return;
    }

    if (command->request.isSet("mockRequest")) {
        SINFO("mockRequest set for command '" << command->request.methodLine << "'.");
    }

    // See if this is a feasible command to write parallel. If not, then be ready to forward it to the sync
    // thread, if it doesn't finish in peek.
    bool canWriteParallel = _multiWriteEnabled.load();
    if (canWriteParallel) {
        // If multi-write is enabled, then we need to make sure the command isn't blacklisted.
        shared_lock<decltype(_blacklistedParallelCommandMutex)> lock(_blacklistedParallelCommandMutex);
        canWriteParallel =
            (_blacklistedParallelCommands.find(command->request.methodLine) == _blacklistedParallelCommands.end());
    }

    int64_t lastConflictPage = 0;
    string lastConflictTable;
    while (true) {

        // We just spin until the node looks ready to go. Typically, this doesn't happen expect briefly at startup.
        size_t waitCount = 0;
        while (_upgradeInProgress || (getState() != SQLiteNodeState::LEADING && getState() != SQLiteNodeState::FOLLOWING)) {
            // This sleep call is pretty ugly, but it should almost never happen. We're accepting the potential
            // looping sleep call for the general case where we just check some bools and continue, instead of
            // avoiding the sleep call but having every thread lock a mutex here on every loop.
            usleep(10000);
            waitCount++;
        }
        if (waitCount) {
            SINFO("Waited for " << waitCount << " loops for node to be ready.");
        }

        // More checks for parallel writing.
        canWriteParallel = canWriteParallel && (getState() == SQLiteNodeState::LEADING);
        canWriteParallel = canWriteParallel && (command->writeConsistency == SQLiteNode::ASYNC);

        // If there are outstanding HTTPS requests on this command (from a previous call to `peek`) we process them here.
        size_t networkLoopCount = 0;
        uint64_t postPollCumulativeTime = 0;
        while (!command->areHttpsRequestsComplete()) {
            networkLoopCount++;
            fd_map fdm;
            command->prePoll(fdm);

            // Determine how long we'll wait in `poll`.
            uint64_t maxWaitUs = 0;

            // The default case is to wait until the command will time out.
            uint64_t now = STimeNow();
            if (now < command->timeout()) {
                maxWaitUs = command->timeout() - now;
            } else {
                // The command is already timed out. This will hit the check for core.isTimedOut(command) below.
                break;
            }

            // We never wait more than 1 second in `poll`. There are two uses for this. One is that at shutdown, we want to kill any sockets that have are making no progress.
            // We don't want these to be stuck sitting for 5 minutes doing nothing while thew server hangs, so we will interrupt every second to check on them.
            // The other case is that there can be no sockets at all.
            // Why would there be no sockets? It's because Auth::Stripe, as a rate-limiting feature, attaches sockets to requests after their made.
            // This means a request can sit around with no actual socket attached to it for some length of time until it's turn to talk to Stripe comes up.
            // If that happens though, and we're sitting in `poll` when it becomes our turn, we will wait the full five minute timeout of the original `poll`
            // call before we time out and try again wit the newly-attached socket.
            // Setting this to one second lets us try again more frequently.
            maxWaitUs = min(maxWaitUs, 1'000'000ul);
            bool shuttingDown = false;
            auto _syncNodeCopy = atomic_load(&_syncNode);
            if (_shutdownState.load() != RUNNING || (_syncNodeCopy && _syncNodeCopy->getState() == SQLiteNodeState::STANDINGDOWN)) {
                shuttingDown = true;
            }

            // Ok, go ahead and `poll`.
            S_poll(fdm, maxWaitUs);

            // The 3rd parameter to `postPoll` here is the total allowed idle time on this connection. We will kill connections that do nothing at all after 5 minutes normally,
            // or after only 5 seconds when we're shutting down so that we can clean up and move along.
            uint64_t ignore{0};
            auto start = STimeNow();
            command->postPoll(fdm, ignore, shuttingDown ? 5'000 : 300'000);
            postPollCumulativeTime += (STimeNow() - start);
        }

        if (networkLoopCount) {
            SINFO("Completed HTTPS request in " << networkLoopCount << " loops with " << postPollCumulativeTime << "us total time in postPoll");
        }

        // Get a DB handle to work on. This will automatically be returned when dbScope goes out of scope.
        if (!_dbPool) {
            SERROR("Can't run a command with no DB pool");
        }
        {
            SQLiteScopedHandle dbScope(*_dbPool, _dbPool->getIndex());
            SQLite& db = dbScope.db();
            BedrockCore core(db, *this);

            // If the command has already timed out when we get it, we can return early here without peeking it.
            // We'd also catch that the command timed out in `peek`, but this can cause some weird side-effects. For
            // instance, we saw QUORUM commands that make HTTPS requests time out in the sync thread, which caused them
            // to be returned to the main queue, where they would have timed out in `peek`, but it was never called
            // because the commands already had a HTTPS request attached, and then they were immediately re-sent to the
            // sync queue, because of the QUORUM consistency requirement, resulting in an endless loop.
            if (core.isTimedOut(command)) {
                _reply(command);
                return;
            }

            // If this command is dependent on a commitCount newer than what we have (maybe it's a follow-up to a
            // command that was escalated to leader), we'll set it aside for later processing. When the sync node
            // finishes its update loop, it will re-queue any of these commands that are no longer blocked on our
            // updated commit count.
            uint64_t commitCount = db.getCommitCount();
            uint64_t commandCommitCount = command->request.calcU64("commitCount");
            if (commandCommitCount > commitCount) {
                SAUTOLOCK(_futureCommitCommandMutex);
                auto newQueueSize = _futureCommitCommands.size() + 1;
                SINFO("Command (" << command->request.methodLine << ") depends on future commit (" << commandCommitCount
                      << "), Currently at: " << commitCount << ", storing for later. Queue size: " << newQueueSize);
                _futureCommitCommandTimeouts.insert(make_pair(command->timeout(), commandCommitCount));
                _futureCommitCommands.insert(make_pair(commandCommitCount, move(command)));

                // Don't count this as `in progress`, it's just sitting there.
                if (newQueueSize > 100) {
                    SHMMM("_futureCommitCommands.size() == " << newQueueSize);
                }
                return;
            }

            // If we've changed out of leading, we need to notice that.
            canWriteParallel = canWriteParallel && (getState() == SQLiteNodeState::LEADING);

            // If the command should run prePeek, do that now .
            if (!command->repeek && !command->httpsRequests.size() && command->shouldPrePeek()) {
                core.prePeekCommand(command, isBlocking);

                if (command->complete) {
                    _reply(command);
                    break;
                }
            }

            auto *timer = new BedrockCore::AutoTimer(command, BedrockCommand::QUEUE_PAGE_LOCK);
            uint64_t conflictLockStartTime = 0;
            if (lastConflictPage) {
                conflictLockStartTime = STimeNow();
            }
            {
                PageLockGuard pageLock(lastConflictPage);
                if (lastConflictPage) {
                    SINFO("Waited " << (STimeNow() - conflictLockStartTime) << "us for lock on db page " << lastConflictPage << ".");
                }
                delete timer;

                // If the command has any httpsRequests from a previous `peek`, we won't peek it again unless the
                // command has specifically asked for that.
                // If peek succeeds, then it's finished, and all we need to do is respond to the command at the bottom.
                bool calledPeek = false;
                BedrockCore::RESULT peekResult = BedrockCore::RESULT::INVALID;
                if (command->repeek || !command->httpsRequests.size()) {
                    peekResult = core.peekCommand(command, isBlocking);
                    calledPeek = true;
                }

                if (!calledPeek || peekResult == BedrockCore::RESULT::SHOULD_PROCESS) {
                    // We've just unsuccessfully peeked a command, which means we're in a state where we might want to
                    // write it. We'll flag that here, to keep the node from falling out of LEADING/STANDINGDOWN
                    // until we're finished with this command.
                    if (command->httpsRequests.size()) {
                        if (command->repeek || !command->areHttpsRequestsComplete()) {
                            // Roll back the existing transaction, but only if we are inside an transaction
                            if (calledPeek) {
                                core.rollback();
                            }

                            // Jump back to the top of our main `while (true)` loop and run the network activity loop again.
                            continue;
                        }
                    } else {
                        // If we haven't sent a quorum command to the sync thread in a while, auto-promote one.
                        uint64_t now = STimeNow();
                        if (now > (_lastQuorumCommandTime + (_quorumCheckpointSeconds * 1'000'000))) {
                            SINFO("Forcing QUORUM for command '" << command->request.methodLine << "'.");
                            _lastQuorumCommandTime = now;
                            command->writeConsistency = SQLiteNode::QUORUM;
                            canWriteParallel = false;
                        }
                    }

                    // Peek wasn't enough to handle this command. See if we think it should be writable in parallel.
                    if (!canWriteParallel) {
                        // Roll back the transaction, it'll get re-run in the sync thread.
                        core.rollback();
                        dbScope.release();
                        auto _clusterMessengerCopy = _clusterMessenger;
                        if (getState() == SQLiteNodeState::LEADING) {
                            // Limit the command timeout to 20s to avoid blocking the sync thread long enough to cause the cluster to give up and elect a new leader (causing a fork), which happens
                            // after 30s.
                            command->setTimeout(20'000);
                            SINFO("Sending non-parallel command " << command->request.methodLine
                                  << " to sync thread. Sync thread has " << _syncNodeQueuedCommands.size() << " queued commands.");
                            _syncNodeQueuedCommands.push(move(command));
                        } else if (_clusterMessengerCopy && _clusterMessengerCopy->runOnPeer(*command, true)) {
                            SINFO("Escalated " << command->request.methodLine << " to leader and complete, responding.");
                            _reply(command);
                        } else {
                            // TODO: Something less naive that considers how these failures happen rather than a simple
                            // endless loop of requeue and retry.
                            SINFO("Couldn't escalate command " << command->request.methodLine << " to leader. We are in state: " << SQLiteNode::stateName(getState()));
                            _commandQueue.push(move(command));
                        }

                        // Done with this command, look for the next one.
                        break;
                    }

                    // In this case, there's nothing blocking us from processing this in a worker, so let's try it.
                    BedrockCore::RESULT result = core.processCommand(command, isBlocking);
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
                        uint64_t transactionID = 0;
                        string transactionHash;
                        {
                            BedrockCore::AutoTimer timer(command, isBlocking ? BedrockCommand::BLOCKING_COMMIT_WORKER : BedrockCommand::COMMIT_WORKER);
                            void (*onPrepareHandler)(SQLite& db, int64_t tableID) = nullptr;
                            bool enableOnPrepareNotifications = command->shouldEnableOnPrepareNotification(db, &onPrepareHandler);
                            commitSuccess = core.commit(*_syncNode, transactionID, transactionHash, enableOnPrepareNotifications, onPrepareHandler);

                            if (getState() != SQLiteNodeState::LEADING) {
                                SINFO("Stopped leading while trying to commit, will retry.");

                                // Jump back to the top of the main loop but skip the check that would push this to the blocking commit queue.
                                // It doesn't make sense to run anything in the blocking queue on a follower. There's a possible edge case where this node
                                // begins leading again before this command runs. In that case, the command will still end up in the blocking commit queue,
                                // possibly with an extra call to `process` over the maximum expected to be allowed. This is very unlikely, though.
                                continue;
                            }
                        }
                        if (commitSuccess) {
                            // Tell the sync node that there's been a commit so that it can jump out of it's "poll"
                            // loop and send it to followers. NOTE: we don't check for null here, that should be
                            // impossible inside a worker thread.
                            _syncNode->notifyCommit();
                            SINFO("Committed leader transaction #" << transactionID << "(" << transactionHash << "). Command: '" << command->request.methodLine << "', blocking: "
                                  << (isBlocking ? "true" : "false"));
                            _conflictManager.recordTables(command->request.methodLine, db.getTablesUsed());
                            // So we must still be leading, and at this point our commit has succeeded, let's
                            // mark it as complete. We add the currentCommit count here as well.
                            command->response["commitCount"] = to_string(db.getCommitCount());
                            command->complete = true;
                        } else {
                            SINFO("Conflict or state change committing " << command->request.methodLine << " on worker thread.");
                            if (_enableConflictPageLocks) {
                                lastConflictTable = db.getLastConflictTable();

                                // Journals and Onyx tables are always chosen at the time of commit. So in case there was a conflict on these tables in
                                // the previous commit, the chances are very low that we'll choose the same table, thus, we
                                // don't need to lock our next commit on this page conflict.
                                if (!SStartsWith(lastConflictTable, "journal") && !SStartsWith(lastConflictTable, "onyx")) {
                                    lastConflictPage = db.getLastConflictPage();
                                }
                            }
                        }
                    } else if (result == BedrockCore::RESULT::NO_COMMIT_REQUIRED) {
                        // Nothing to do in this case, `command->complete` will be set and we'll finish as we fall out
                        // of this block.
                    } else if (result == BedrockCore::RESULT::SERVER_NOT_LEADING) {
                        // We won't write regardless.
                        core.rollback();

                        // If there are no HTTPS requests, we can just re-queue this command, otherwise, we will
                        // potentially run the same HTTPS requests twice.
                        if (command->httpsRequests.size()) {
                            SALERT("Server stopped leading while running command with HTTPS requests!");
                            command->response.methodLine = "500 Leader stopped leading";
                            _reply(command);
                            break;
                        } else {
                            // Allow for an extra retry and start from the top.
                            SINFO("State changed before 'processCommand' but no HTTPS requests so retrying.");
                        }
                    } else {
                        SERROR("processCommand (" << command->request.getVerb() << ") returned invalid result code: " << (int)result);
                    }
                }
            }
            // If the command was completed above, then we'll go ahead and respond. Otherwise there must have been
            // a conflict or the command was abandoned for a checkpoint, and we'll retry.
            if (command->complete) {
                if (command->shouldPostProcess() && command->response.methodLine == "200 OK") {
                    // PostProcess if the command should run postProcess, and there have been no errors thrown thus far.
                    core.postProcessCommand(command, isBlocking);
                }
                _reply(command);

                // Don't need to retry.
                break;
            }
        }

        // If we're shutting down, or have set a specific max retries, we just try several times in a row and then move the command to the blocking queue.
        int maxRetries = _maxConflictRetries.load();
        if (maxRetries || _shutdownState.load() != RUNNING) {
            if (command->processCount > maxRetries) {
                SINFO("Max retries (" << maxRetries << ") hit in worker, sending '" << command->request.methodLine << "' to blocking queue with size " << _blockingCommandQueue.size());
                _blockingCommandQueue.push(move(command));
                return;
            }
        } else {
            // If we're not shutting down, see how long we want to wait until we'll try this command again.
            size_t millisecondsToWait = 0;
            switch (command->processCount) {
                case 1:
                    millisecondsToWait = 10;
                    break;
                case 2:
                    millisecondsToWait = 25;
                    break;
                case 3:
                    millisecondsToWait = 50;
                    break;
                case 4:
                    millisecondsToWait = 100;
                    break;
                case 5:
                    millisecondsToWait = 250;
                    break;
                default:
                    millisecondsToWait = 500;
            }

            // Apply jitter. Take a value that's a whole number up to 50% of ideal time. This allows for adding or subtracting up to 25%.
            millisecondsToWait += ((SRandom::rand64() % (millisecondsToWait / 2)) - (millisecondsToWait / 4));
            SINFO("Waiting " << millisecondsToWait << "ms before retrying command '" << command->request.methodLine << "'.");
            if (hasDedicatedThread) {
                // If we have a dedicated socket thread for this command, we can just sleep here.
                this_thread::sleep_for(chrono::milliseconds(millisecondsToWait));
            } else {
                // Otherwise, re-queue and let another thread try again.
                _commandQueue.push(move(command), STimeNow() + millisecondsToWait * 1000);
                return;
            }
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
    lock_guard<mutex> lock(_portMutex);

    _requestCount = 0;
    _upgradeInProgress = false;
    if (_commandPortBlockReasons.size()) {
        SWARN("Clearing leftover command port blocks in resetServer (" << _commandPortBlockReasons.size() << " blocks remaining).");
        _commandPortBlockReasons.clear();
    }
    _syncLoopShouldBeRunning = true;
    atomic_store(&_syncNode, shared_ptr<SQLiteNode>(nullptr));
    _shutdownState = RUNNING;
    _shouldBackup = false;
    _commandPortPublic = nullptr;
    _commandPortPrivate = nullptr;
    _pluginsDetached = false;
    _upgradeCompleted = false;

    // Tell any plugins that they can attach now
    for (auto plugin : plugins) {
        plugin.second->onAttach();
    }
}

BedrockServer::BedrockServer(SQLiteNodeState state, const SData& args_)
  : SQLiteServer(), args(args_), _syncNode(nullptr), _clusterMessenger(nullptr)
{}

BedrockServer::BedrockServer(const SData& args_)
  : SQLiteServer(), shutdownWhileDetached(false), args(args_), _requestCount(0),
    _upgradeInProgress(false),
    _isCommandPortLikelyBlocked(false),
    _syncLoopShouldBeRunning(true), _syncNode(nullptr), _clusterMessenger(nullptr), _shutdownState(RUNNING),
    _multiWriteEnabled(args.test("-enableMultiWrite")), _enableConflictPageLocks(args.test("-enableConflictPageLocks")), _shouldBackup(false), _detach(args.isSet("-bootstrap")),
    _controlPort(nullptr), _commandPortPublic(nullptr), _commandPortPrivate(nullptr), _maxConflictRetries(3),
    _lastQuorumCommandTime(STimeNow()), _pluginsDetached(false), _socketThreadNumber(0),
    _outstandingSocketThreads(0), _shouldBlockNewSocketThreads(false), _upgradeCompleted(false)
{
    _version = VERSION;

    // This allows the signal thread to notify us when a signal is received to interrupt the current poll loop.
    SSIGNAL_NOTIFY_INTERRUPT = &_notifyDoneSync;

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

    // Bypass journald.
    if (args.isSet("-logDirectlyToSyslogSocket")) {
        SSyslogFunc = &SSyslogSocketDirect;
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
    {
        lock_guard<mutex> lock(_portMutex);
        _controlPort = openPort(args["-controlPort"]);
    }

    // If we're bootstraping this node we need to go into detached mode here.
    // The syncWrapper will handle this for us.
    if (_detach) {
        SINFO("Bootstrap flag detected, starting sync node in detach mode.");
    }

    // Set the quorum checkpoint, or default if not specified.
    _quorumCheckpointSeconds = args.isSet("-quorumCheckpointSeconds") ? args.calc("-quorumCheckpointSeconds") : 60;

    if (args.isSet("-dbPoolSize")){
        _dbPoolSize = args.calcU64("-dbPoolSize");
    } else {
        _dbPoolSize = args.isSet("-live") ? 25'000 : 250;
    }

    if (args.isSet("-maxSocketThreads")){
        _maxSocketThreads = args.calcU64("-maxSocketThreads");
    }

    // Start the sync thread, which will start the worker threads.
    SINFO("Launching sync thread '" << _syncThreadName << "'");
    _syncThread = thread(&BedrockServer::syncWrapper, this);
}

BedrockServer::~BedrockServer() {
    // Shut down the sync thread, (which will shut down worker threads in turn).
    SINFO("Closing sync thread '" << _syncThreadName << "'");
    if (_syncThread.joinable()) {
        _syncThread.join();
    }
    SINFO("Threads closed.");

    if (_outstandingSocketThreads) {
        SWARN("Shutting down with " << _outstandingSocketThreads << " socket threads remaining.");
    }

    // Delete our plugins.
    for (auto& p : plugins) {
        delete p.second;
    }
}

bool BedrockServer::shutdownComplete() {
    if (_detach) {
        return shutdownWhileDetached;
    }

    // We're done when the sync thread is done.
    return !_syncLoopShouldBeRunning;
}

void BedrockServer::prePoll(fd_map& fdm) {
    lock_guard<mutex> lock(_portMutex);

    // This will interrupt poll when we shut down.
    _notifyDone.prePoll(fdm);

    // Add all our ports. There are no sockets directly managed here.
    if (_commandPortPublic) {
        SFDset(fdm, _commandPortPublic->s, SREADEVTS);
    }
    if (_commandPortPrivate) {
        SFDset(fdm, _commandPortPrivate->s, SREADEVTS);
    }
    if (_controlPort) {
        SFDset(fdm, _controlPort->s, SREADEVTS);
    }
    for (const auto& p : _portPluginMap) {
        SFDset(fdm, p.first->s, SREADEVTS);
    }
}

void BedrockServer::postPoll(fd_map& fdm, uint64_t& nextActivity) {
    _notifyDone.postPoll(fdm);

    // NOTE: There are no sockets managed here, just ports.
    // Open the port the first time we enter a command-processing state
    {
        lock_guard<mutex> lock(_portMutex);
        if (_commandPortBlockReasons.empty() && (getState() == SQLiteNodeState::LEADING || getState() == SQLiteNodeState::FOLLOWING) && _shutdownState.load() == RUNNING) {

            // Open the port
            if (!_commandPortPublic) {
                SINFO("Ready to process commands, opening public command port on '" << args["-serverHost"] << "'");
                _commandPortPublic = openPort(args["-serverHost"]);
            }
            if (!_commandPortPrivate) {
                SINFO("Ready to process commands, opening private command port on '" << args["-commandPortPrivate"] << "'");
                _commandPortPrivate = openPort(args["-commandPortPrivate"]);
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
                    for (auto& pluginPorts : _portPluginMap) {
                        if (pluginPorts.second == plugin.second) {
                            // We've already got this one.
                            alreadyOpened = true;
                            break;
                        }
                    }
                    // Open the port and associate it with the plugin
                    if (!alreadyOpened) {
                        SINFO("Opening port '" << portHost << "' for plugin '" << plugin.second->getName() << "'");
                        _portPluginMap[openPort(portHost)] = plugin.second;
                    }
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
        _beginShutdown(SGetSignalDescription());
    }

    // Accept any new connections
    _acceptSockets();

    // If any plugin timers are firing, let the plugins know.
    for (auto plugin : plugins) {
        for (SStopwatch* timer : plugin.second->timers) {
            if (timer->ding()) {
                plugin.second->timerFired(timer);
            }
        }
    }

    // If we've been told to start shutting down, we'll set the shut down timer.
    if (_shutdownState.load() == START_SHUTDOWN) {
        // Locking here means that no commands can be running when we do these checks and then switch to
        // `COMMANDS_FINISHED` because we have a shared lock on this mutex in `handleSocket`. This means this check can
        // only run between commands, and `_outstandingSocketThreads` will have been incremented already when we check
        // it here. So, if the check below for `_outstandingSocketThreads` passes at this point, it means there are no
        // commands at this point in time. However, new commands may still be received on the control port after this,
        // if we are detaching.
        unique_lock<shared_mutex> lock(_controlPortExclusionMutex);

        size_t count = BedrockCommand::getCommandCount();
        SINFO("SHUTDOWN Have " << _outstandingSocketThreads << " socket threads and " << count << " commands remaining.");

        // Don't tell the sync node to shut down while we still have commands or sockets left.
        if (!_outstandingSocketThreads && !count) {
            _shutdownState.store(COMMANDS_FINISHED);

            // This interrupts the sync thread's poll() loop so it doesn't wait for up to an extra second to finish.
            // When it wakes up, it will begin its own shutdown.
            _syncNode->notifyCommit();
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
    // Finalize timing info even for commands we won't respond to (this makes this data available in logs).
    command->finalizeTimingInfo();

    // Don't reply to commands with pseudo-clients (i.e., commands that we generated by other commands, or using
    // `Connection: forget`.
    if (command->initiatingClientID < 0) {
        command->handleFailedReply();
        return;
    }

    command->response["nodeName"] = args["-nodeName"];

    // If we're shutting down, tell the caller to close the connection.
    // Also, if the caller wanted us to close the connection, we'll parrot that back.
    if (_shutdownState.load() != RUNNING || command->request["Connection"] == "close") {
        command->response["Connection"] = "close";
    }

    // Is a plugin handling this command? If so, it gets to send the response.
    const string& pluginName = command->request["plugin"];

    if (command->socket) {
        SINFO("[performance] Command " << command->request.methodLine << " has a socket, going to try to reply.");
        if (!pluginName.empty()) {
            // Let the plugin handle it
            SINFO("Plugin '" << pluginName << "' handling response '" << command->response.methodLine
                  << "' to request '" << command->request.methodLine << "'");
            auto it = plugins.find(pluginName);
            if (it != plugins.end()) {
                it->second->onPortRequestComplete(*command, command->socket);
            } else {
                SERROR("Couldn't find plugin '" << pluginName << ".");
            }
        } else {
            // Otherwise we send the standard response.
            if (!command->socket->send(command->response.serialize())) {
                // If we can't send (client closed the socket?), alert our plugin it's response was never sent.
                SINFO("No socket to reply for: '" << command->request.methodLine << "' #" << command->initiatingClientID);
                command->handleFailedReply();
            } else {
                SINFO("[performance] Replied");
            }
        }

        // If `Connection: close` was set, shut down the socket, in case the caller ignores us.
        if (SIEquals(command->request["Connection"], "close") || _shutdownState.load() != RUNNING) {
            command->socket->shutdown();
        }
    } else {
        // This is the case for a fire-and-forget command, such as one set to run in the future. If `Connection:
        // forget` was specified, this is normal and we won't log.
        if (!SIEquals(command->request["Connection"], "forget")) {
            SINFO("No socket to reply for: '" << command->request.methodLine << "' #" << command->initiatingClientID);
        }
        command->handleFailedReply();
    }
    SINFO("[performance] Finished replying to command " << command->request.methodLine << " moving on to the next command.");
}


void BedrockServer::blockCommandPort(const string& reason) {
    lock_guard<mutex> lock(_portMutex);
    _commandPortBlockReasons.insert(reason);
    _isCommandPortLikelyBlocked = true;
    if (_commandPortBlockReasons.size() == 1) {
        _commandPortPublic = nullptr;
        _portPluginMap.clear();
    }
    SINFO("Blocking command port due to: " << reason << (_commandPortBlockReasons.size() > 1 ? " (already blocked)" : "") << ".");
}

void BedrockServer::unblockCommandPort(const string& reason) {
    lock_guard<mutex> lock(_portMutex);
    auto it = _commandPortBlockReasons.find(reason);
    if (it == _commandPortBlockReasons.end()) {
        SWARN("Tried to remove command port block because: " << reason << ", but it wasn't blocked for that reason!");
    } else {
        _commandPortBlockReasons.erase(it);
        SINFO("Removing command port block due to: " <<  reason << (_commandPortBlockReasons.size() > 0 ? " (blocks remaining)" : "") << ".");
    }
    if (_commandPortBlockReasons.empty()) {
        _isCommandPortLikelyBlocked = false;
    }
}

void BedrockServer::suppressCommandPort(const string& reason, bool suppress, bool manualOverride) {
    if (suppress) {
        blockCommandPort("LEGACY_" + reason);
    } else {
        unblockCommandPort("LEGACY_" + reason);
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

list<STable> BedrockServer::getPeerInfo() {
    list<STable> peerData;
    auto _syncNodeCopy = atomic_load(&_syncNode);
    if (_syncNodeCopy) {
        peerData =  _syncNodeCopy->getPeerInfo();
    }
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
    return _detach && !_syncLoopShouldBeRunning && _pluginsDetached;
}

bool BedrockServer::isUpgradeComplete() {
    return _upgradeCompleted;
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
        if (getState() == SQLiteNodeState::FOLLOWING) {
            response.methodLine = "HTTP/1.1 200 Following";
        } else {
            response.methodLine = "HTTP/1.1 500 Not Following. State=" + SQLiteNode::stateName(getState());
        }
    } else if (SIEquals(request.methodLine, STATUS_HANDLING_COMMANDS)) {
        // This is similar to the above check, and is used for letting HAProxy load-balance commands.

        if (_version != _leaderVersion.load()) {
            response.methodLine = "HTTP/1.1 500 Mismatched version. Version=" + _version;
        } else {
            string method = "HTTP/1.1 ";

            if (getState() == SQLiteNodeState::FOLLOWING || getState() == SQLiteNodeState::LEADING || getState() == SQLiteNodeState::STANDINGDOWN) {
                method += "200";
            } else {
                method += "500";
            }
            response.methodLine = method + " " + SQLiteNode::stateName(getState());
        }
    }

    // All a ping message requires is some response.
    else if (SIEquals(request.methodLine, STATUS_PING)) {
        response.methodLine = "200 OK";
    }

    // This collects the current state of the server, which also includes some state from the underlying SQLiteNode.
    else if (SIEquals(request.methodLine, STATUS_STATUS)) {
        STable content;
        list<string> pluginList;
        for (auto plugin : plugins) {
            STable pluginData = plugin.second->getInfo();
            pluginData["name"] = plugin.second->getName();
            pluginList.push_back(SComposeJSONObject(pluginData));
        }
        content["isLeader"] = getState() == SQLiteNodeState::LEADING ? "true" : "false";
        content["plugins"] = SComposeJSONArray(pluginList);
        content["state"] = SQLiteNode::stateName(getState());
        content["version"] = _version;
        content["host"] = args["-nodeHost"];
        content["commandCount"] = BedrockCommand::getCommandCount();

        {
            // Make it known if anything is known to cause crashes.
            shared_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
            vector<string> crashCommandListArray;

            size_t totalCount = 0;
            for (const auto& s : _crashCommands) {
                totalCount += s.second.size();

                vector<string> paramsArray;
                for (const STable& params : s.second) {
                    if (!params.empty()) {
                        paramsArray.push_back(SComposeJSONObject(params));
                    }
                }

                STable commandObject;
                commandObject[s.first] = SComposeJSONArray(paramsArray);
                crashCommandListArray.push_back(SComposeJSONObject(commandObject));
            }
            content["crashCommands"] = totalCount;
            content["crashCommandList"] = SComposeJSONArray(crashCommandListArray);
        }

        // On leader, return the current multi-write blacklists.
        if (getState() == SQLiteNodeState::LEADING) {
            // Both of these need to be in the correct state for multi-write to be enabled.
            content["multiWriteEnabled"] = _multiWriteEnabled ? "true" : "false";
            content["multiWriteManualBlacklist"] = SComposeJSONArray(_blacklistedParallelCommands);
        }

        // Coalesce all of the peer data into one value to return or return
        // an error message if we timed out getting the peerList data.
        list<string> peerList;
        list<STable> peerData = getPeerInfo();
        for (const STable& peerTable : peerData) {
            peerList.push_back(SComposeJSONObject(peerTable));
        }

        {
            lock_guard<mutex> lock(_portMutex);
            content["commandPortBlockReasons"] = SComposeJSONArray(_commandPortBlockReasons);
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

        auto _syncNodeCopy = atomic_load(&_syncNode);
        if (_syncNodeCopy) {
            content["syncNodeAvailable"] = "true";
            // Set some information about this node.
            content["CommitCount"] = to_string(_syncNodeCopy->getCommitCount());
            content["priority"] = to_string(_syncNodeCopy->getPriority());
            _syncNodeCopy = nullptr;
        } else {
            content["syncNodeAvailable"] = "false";
        }

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
        SIEquals(command->request.methodLine, "ConflictReport")         ||
        SIEquals(command->request.methodLine, "Detach")                 ||
        SIEquals(command->request.methodLine, "Attach")                 ||
        SIEquals(command->request.methodLine, "SetConflictParams")      ||
        SIEquals(command->request.methodLine, "SetConflictPageLocks")   ||
        SIEquals(command->request.methodLine, "EnableSQLTracing")       ||
        SIEquals(command->request.methodLine, "BlockWrites")            ||
        SIEquals(command->request.methodLine, "UnblockWrites")          ||
        SIEquals(command->request.methodLine, "SetMaxSocketThreads")    ||
        SIEquals(command->request.methodLine, "CRASH_COMMAND")
        ) {
        return true;
    }
    return false;
}

bool BedrockServer::_isNonSecureControlCommand(const unique_ptr<BedrockCommand>& command) {
    // A list of non-secure control commands that can be run from another host
    // TODO: Have some other way to specify privileged commands that can be
    // sent from other nodes on the private command port.
    return SIEquals(command->request.methodLine, "SuppressCommandPort") ||
        SIEquals(command->request.methodLine, "ClearCommandPort") ||
        SIEquals(command->request.methodLine, "CRASH_COMMAND");
}

// State management for blocking writes to the DB.
mutex __quiesceLock;
atomic<bool> __quiesceShouldUnlock(false);
thread* __quiesceThread = nullptr;

void BedrockServer::_control(unique_ptr<BedrockCommand>& command) {
    SINFO("Received control command: " << command->request.methodLine);
    SData& response = command->response;
    string reason = "MANUAL";
    response.methodLine = "200 OK";
    if (SIEquals(command->request.methodLine, "BeginBackup")) {
        _shouldBackup = true;
        _beginShutdown("Detach", true);
    } else if (SIEquals(command->request.methodLine, "SuppressCommandPort")) {
        if (command->request.isSet("reason") && command->request["reason"].size()) {
            reason = command->request["reason"];
        }
        blockCommandPort(reason);
    } else if (SIEquals(command->request.methodLine, "ClearCommandPort")) {
        if (command->request.isSet("reason") && command->request["reason"].size()) {
            reason = command->request["reason"];
        }
        unblockCommandPort(reason);
    } else if (SIEquals(command->request.methodLine, "ClearCrashCommands")) {
        unique_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
        _crashCommands.clear();
    } else if (SIEquals(command->request.methodLine, "ConflictReport")) {
        response.content = _conflictManager.generateReport();
    } else if (SIEquals(command->request.methodLine, "Detach")) {
        if (isDetached()) {
            response.methodLine = "400 Already detached";
        } else {
            response.methodLine = "203 DETACHING";
            _beginShutdown("Detach", true);
        }
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
        } else if (_shutdownState.load() != RUNNING) {
            // Wait to confirm that we're in the final _shutdownState "RUNNING" before reattaching
            response.methodLine = "401 Attaching prevented by server not ready";
        } else {
            response.methodLine = "204 ATTACHING";
            _syncLoopShouldBeRunning = true;
            _detach = false;
        }
    } else if (SIEquals(command->request.methodLine, "EnableSQLTracing")) {
        response["oldValue"] = SQLite::enableTrace ? "true" : "false";
        if (command->request.isSet("enable")) {
            SQLite::enableTrace.store(command->request.test("enable"));
            response["newValue"] = SQLite::enableTrace ? "true" : "false";
        }
    } else if (SIEquals(command->request.methodLine, "CRASH_COMMAND")) {
        SData request;
        request.deserialize(command->request.content);
        if (request.empty()) {
            SINFO("Got CRASH_COMMAND with malformed command body. Is someone running this by hand? Nothing to blacklist.");
            return;
        }

        // Take a unique lock so nobody else can read from this table while we update it.
        unique_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);

        // Add the blacklisted command to the map.
        _crashCommands[request.methodLine].insert(request.nameValueMap);
        size_t totalCount = 0;
        for (const auto& s : _crashCommands) {
            totalCount += s.second.size();
        }
        SALERT("Blacklisting command (now have " << totalCount << " blacklisted commands): " << request.serialize());
    } else if (SIEquals(command->request.methodLine, "SetConflictParams")) {
        int64_t maxConflictRetries = command->request.calc64("MaxConflictRetries");
        if (maxConflictRetries >= 0) {
            SINFO("Setting _maxConflictRetries to " << _maxConflictRetries);
            response["previousMaxConflictRetries"] = to_string(_maxConflictRetries.load());
            _maxConflictRetries.store(maxConflictRetries);
        }
    } else if (SIEquals(command->request.methodLine, "SetConflictPageLocks")) {
        _enableConflictPageLocks = command->request.test("enable");
    } else if (SIEquals(command->request.methodLine, "BlockWrites")) {
        atomic<bool> locked(false);
        lock_guard lock(__quiesceLock);
        if (__quiesceThread) {
            response.methodLine = "400 Already Blocked";
        } else {
            __quiesceThread = new thread([&]() {
                shared_ptr<SQLitePool> dbPoolCopy = _dbPool;
                if (dbPoolCopy) {
                    SQLiteScopedHandle dbScope(*_dbPool, _dbPool->getIndex());
                    SQLite& db = dbScope.db();
                    SINFO("[quiesce] Exclusive locking DB");
                    db.exclusiveLockDB();
                    SINFO("[quiesce] Exclusive locked DB");
                    locked = true;
                    while (true) {
                        if (__quiesceShouldUnlock) {
                            db.exclusiveUnlockDB();
                            __quiesceShouldUnlock = false;
                            return;
                        }

                        // Wait 10ms for the next check.
                        usleep(10'000);
                    }
                }
            });

            // Repeatedly wait 10ms for the lock until the thread indicates it's been acquired.
            while (locked == false) {
                usleep(10'000);
            }

            response.methodLine = "200 Blocked";
        }
    } else if (SIEquals(command->request.methodLine, "UnblockWrites")) {
        SINFO("[quiesce] Locking __quiesceLock");
        lock_guard lock(__quiesceLock);
        SINFO("[quiesce] __quiesceLock locked");
        if (!__quiesceThread) {
            response.methodLine = "200 Not Blocked";
        } else {
            __quiesceShouldUnlock = true;
            SINFO("[quiesce] Joining __quiesceThread");
            __quiesceThread->join();
            SINFO("[quiesce] __quiesceThread joined");
            delete __quiesceThread;
            __quiesceThread = nullptr;
            response.methodLine = "200 Unblocked";
        }
    } else if (SIEquals(command->request.methodLine, "SetMaxSocketThreads")) {
        size_t newMax = command->request.calcU64("socketThreadsCount");
        if (newMax) {
            SINFO("Setting _maxSocketThreads to " << newMax << " from " << _maxSocketThreads);
            _maxSocketThreads = newMax;
        } else {
            response.methodLine = "401 Don't Use Zero";
        }
    }
}

bool BedrockServer::_upgradeDB(SQLite& db) {
    // These all get conglomerated into one big query.
    try {
        db.beginTransaction(SQLite::TRANSACTION_TYPE::EXCLUSIVE);
        for (auto plugin : plugins) {
            plugin.second->upgradeDatabase(db);
        }
        if (db.getUncommittedQuery().empty()) {
            db.rollback();
        }
    } catch (const system_error& e) {
        SWARN("Caught system_error in _upgradeDB, code: " << e.code() << ", message: " << e.what());
        throw;
    }
    SINFO("Finished running DB upgrade.");
    return !db.getUncommittedQuery().empty();
}

void BedrockServer::_beginShutdown(const string& reason, bool detach) {
    if (_shutdownState.load() == RUNNING) {
        _detach = detach;
        // Begin a graceful shutdown; close our port
        SINFO("Beginning graceful shutdown due to '" << reason << "', closing command port on '" << args["-serverHost"] << "'.");

        // Delete any commands scheduled in the future.
        _commandQueue.abandonFutureCommands(5000);

        // Accept any new connections before closing, this avoids leaving clients who had connected to in a weird
        // state.
        _acceptSockets();

        // Close our listening ports, we won't accept any new connections on them, except the control port, if we're
        // detaching. It needs to keep listening.
        // We lock around changing the shutdown state because `postPoll` will open these ports if we're not shutting
        // down, so otherwise there's a race condition where that happens just after we close them but before we
        // change the state.
        {
            lock_guard<mutex> lock(_portMutex);
            _commandPortPublic = nullptr;
            _commandPortPrivate = nullptr;
            if (!_detach) {
                _controlPort = nullptr;
            }
            _portPluginMap.clear();
            _shutdownState.store(START_SHUTDOWN);
        }
        SQLiteNodeState currentState = SQLiteNodeState::UNKNOWN;
        auto syncNodeCopy = atomic_load(&_syncNode);
        if (syncNodeCopy) {
            currentState = syncNodeCopy->getState();
            syncNodeCopy->setShutdownPriority();
        }
        SINFO("START_SHUTDOWN. Ports shutdown, will perform final socket read. Commands queued: " << _commandQueue.size()
              << ", blocking commands queued: " << _blockingCommandQueue.size() << ", total commands: " << BedrockCommand::getCommandCount()
              << ", state: " << SQLiteNode::stateName(currentState));
    }
}

bool BedrockServer::shouldBackup() {
    return _shouldBackup;
}

SData BedrockServer::_generateCrashMessage(const unique_ptr<BedrockCommand>& command) {
    SHMMM("Generating CRASH_COMMAND command for " << command->request.methodLine);
    SData message("CRASH_COMMAND");
    SData subMessage(command->request.methodLine);
    for (auto& pair : command->crashIdentifyingValues) {
        subMessage.emplace(pair);
    }
    message.content = subMessage.serialize();
    return message;
}

void BedrockServer::broadcastCommand(const SData& command) {
    auto _clusterMessengerCopy = _clusterMessenger;
    if (!_clusterMessengerCopy) {
        SINFO("Failed to broadcast command " << command.methodLine << " to all nodes, cluster messenger does not exist.");
        return;
    }

    _clusterMessengerCopy->runOnAll(command);
    SINFO("Completed broadcast of command " << command.methodLine << ".");
}

void BedrockServer::onNodeLogin(SQLitePeer* peer)
{
    shared_lock<decltype(_crashCommandMutex)> lock(_crashCommandMutex);
    for (const auto& p : _crashCommands) {
        for (const auto& table : p.second) {
            SALERT("Sending crash command " << p.first << " to node " << peer->name << " on login");
            SData crashCommandSpec(p.first);
            crashCommandSpec.nameValueMap = table;
            unique_ptr<BedrockCommand> crashCommand = getCommandFromPlugins(move(crashCommandSpec));
            for (const auto& fields : table) {
                crashCommand->crashIdentifyingValues.insert(fields.first);
            }
            auto _clusterMessengerCopy = _clusterMessenger;
            if (_clusterMessengerCopy) {
                BedrockCommand peerCommand(_generateCrashMessage(crashCommand), nullptr);
                _clusterMessengerCopy->runOnPeer(peerCommand, peer->name);
            }
        }
    }
}

void BedrockServer::_acceptSockets() {
    // Try block because we sometimes catch `std::system_error` from in here (likely from the thread code) and we're
    // trying to diagnose exactly what's happening.
    static uint64_t lastLogged = 0;
    try {
        // Make a list of ports to accept on.
        // We'll check the control port, command port, and any plugin ports for new connections.
        list<reference_wrapper<const unique_ptr<Port>>> portList = {_controlPort, _commandPortPrivate, _commandPortPublic};

        // Lock _portMutex so suppressing the port does not cause it to be null
        // in the middle of this function.
        lock_guard<mutex> lock(_portMutex);

        for (auto& p : _portPluginMap) {
            portList.push_back(reference_wrapper<const unique_ptr<Port>>(p.first));
        }

        // Try each port.
        for (auto portWrapper : portList) {
            const unique_ptr<Port>& port = portWrapper.get();

            // Skip null ports (if the command or control port are closed).
            if (!port) {
                continue;
            }

            // Accept as many sockets as we can.
            while (true) {
                uint64_t now = STimeNow();
                if ((port != _controlPort) && (_outstandingSocketThreads >= _maxSocketThreads)) {
                    if ((lastLogged < now - 3'000'000)) {
                        SWARN("Not accepting any new socket threads as we already have " << _outstandingSocketThreads << " of " << _maxSocketThreads);
                        lastLogged = now;
                    }
                    return;
                }

                sockaddr_in addr;
                int s = S_accept(port->s, addr, true); // Note that this sets the newly accepted socket to be blocking.

                // If we got an error or no socket, done accepting for now.
                if (s <= 0) {
                    break;
                }

                // Otherwise create the object for this new socket.
                SDEBUG("Accepting socket from '" << addr << "' on port '" << port->host << "'");
                Socket socket(s, Socket::CONNECTED);
                socket.addr = addr;

                // If it came from a plugin, record that.
                auto plugin = _portPluginMap.find(port);
                if (plugin != _portPluginMap.end()) {
                    socket.data = plugin->second;
                }

                // And start up this socket's thread.
                _outstandingSocketThreads++;
                thread t;
                bool threadStarted = false;
                while (!threadStarted) {
                    try {
                        t = thread(&BedrockServer::handleSocket, this, move(socket), port == _controlPort, port == _commandPortPublic, port == _commandPortPrivate);
                        threadStarted = true;
                    } catch (const system_error& e) {
                        // We don't care about this lock here from a performance perspective, it only happens when we
                        // are unable to do any work anyway (i.e., we can't start threads).
                        lock_guard<mutex> lock(_newSocketThreadBlockedMutex);
                        if (_outstandingSocketThreads < 100) {
                            // We don't expect this to ever happen - we only seem to get `system_error` here when we
                            // have lots (thousands) of threads running. Because of this, our handling of this in
                            // `handleSocket` only works correctly if this happens with greater than 50 threads, and if
                            // we were to block new threads with less than 50 threads already running, we'd never
                            // unblock new threads. Instead, if that happens, we throw this error and crash (which was
                            // the behavior we saw here before handling `system_error`).
                            // We check for 100 threads here instead of the 50 we check for in `handleSocket` to
                            // minimize the risk of race conditions pushing this number through `50` (either up or
                            // down) as we're checking this. For such a race condition to happen here, we'd need to
                            // increment/decrement all the way from 50-100 (or vice versa) to hit such a race condition,
                            // which is theoretically possible but exceedingly unlikely.
                            SERROR("Got system_error creating thread with only " << _outstandingSocketThreads << " threads!");
                        }
                        if (!_shouldBlockNewSocketThreads) {
                            // Block any new socket threads and warn.
                            _shouldBlockNewSocketThreads = true;
                            SWARN("Caught system_error in thread constructor (with " << _outstandingSocketThreads
                                  << " threads): " << e.code() << ", message: " << e.what() << ", blocking new socket threads.");
                            blockCommandPort("NOT_ENOUGH_THREADS");
                        }

                        // We just loop until socket threads are unblocked.
                        SINFO("Waiting 1 more second for socket threads to be available.");
                        sleep(1);
                    }
                }
                try {
                    t.detach();
                } catch (const system_error& e) {
                    SALERT("Caught system_error in thread detach: " << e.code() << ", message: " << e.what());
                    throw;
                }
            }
        }
    } catch (const system_error& e) {
        SALERT("Caught system_error outside thread startup: " << e.code() << ", message: " << e.what());
        throw;
    }
}

unique_ptr<BedrockCommand> BedrockServer::buildCommandFromRequest(SData&& request, Socket& socket, bool shouldTreatAsLocalhost) {
    SAUTOPREFIX(request);

    bool fireAndForget = false;
    if (SIEquals(request["Connection"], "forget") || (uint64_t)request.calc64("commandExecuteTime") > STimeNow()) {
        // Respond immediately to make it clear we successfully queued it. We won't store the socket on the command.
        SINFO("Firing and forgetting '" << request.methodLine << "'");
        SData response("202 Successfully queued");
        response["Connection"] = "close";
        socket.send(response.serialize());
        socket.shutdown(Socket::CLOSED);
        fireAndForget = true;

        // If we're shutting down, discard this command, we won't wait for the future.
        if (_shutdownState.load() != RUNNING) {
            SINFO("Not queuing future command '" << request.methodLine << "' while shutting down.");
            return nullptr;
        }
    }

    // Get the source ip of the command.
    char *ip = inet_ntoa(socket.addr.sin_addr);
    if (!shouldTreatAsLocalhost && ip != "127.0.0.1"s) {
        // Auth checks to see that this value is missing/blank as a security check, so we leave it out for anything
        // originating on 127.0.0.1, or for which we've specified shouldTreatAsLocalhost (which are commands escalated
        // via the private command port). This is the *only* use of the `_source` attribute, so the only consideration
        // we need to make for this.
        request["_source"] = ip;
    }

    // Pull any serialized https requests off the requests object to apply to the command.
    string serializedHTTPSRequests = request["httpsRequests"];
    string serializedData = request["serializedData"];
    request.erase("httpsRequests");
    request.erase("serializedData");

    // Create a command.
    unique_ptr<BedrockCommand> command = getCommandFromPlugins(move(request));

    // Apply HTTPS requests.
    if (serializedHTTPSRequests.size()) {
        command->deserializeHTTPSRequests(serializedHTTPSRequests);
        SINFO("Deserialized " << command->httpsRequests.size() << " HTTPS requests for command " << command->request.methodLine << ".");
    }
    if (serializedData.size()) {
        command->deserializeData(serializedData);
    }

    SDEBUG("Deserialized command " << command->request.methodLine);
    command->socket = fireAndForget ? nullptr : &socket;

    if (command->writeConsistency != SQLiteNode::QUORUM && _syncCommands.find(command->request.methodLine) != _syncCommands.end()) {
        command->writeConsistency = SQLiteNode::QUORUM;
        _lastQuorumCommandTime = STimeNow();
        SINFO("Forcing QUORUM consistency for command " << command->request.methodLine);
    }

    // This is important! All commands passed through the entire cluster must have unique IDs, or they
    // won't get routed properly from follower to leader and back.
    // If the command specifies an ID header (for HTTP escalations) use that, otherwise generate one.
    auto existingID = command->request.nameValueMap.find("ID");
    if (existingID != command->request.nameValueMap.end()) {
        command->id = existingID->second;
    } else {
        command->id = args["-nodeName"] + "#" + to_string(_requestCount++);
    }

    // And we and keep track of the client that initiated this command, so we can respond later, except
    // if we received connection:forget in which case we don't respond later
    command->initiatingClientID = SIEquals(command->request["Connection"], "forget") ? -1 : socket.id;

    return command;
}

void BedrockServer::handleSocket(Socket&& socket, bool fromControlPort, bool fromPublicCommandPort, bool fromPrivateCommandPort) {
    shared_lock<shared_mutex> controlPortLock(_controlPortExclusionMutex, defer_lock);
    if (fromControlPort) {
        controlPortLock.lock();
    }

    // Initialize and get a unique thread ID.
    SInitialize("socket" + to_string(_socketThreadNumber++));
    SINFO("[performance] Socket thread starting");

    // This outer loop just runs until the entire socket life cycle is done, meaning it deserializes a command,
    // waits for it to get processed, deserializes another, etc, until the socket gets closed.
    // This whole block is largely duplicated from `postPoll` and modified to work on a single non-blocking socket.
    while (socket.state != STCPManager::Socket::CLOSED) {
        // We are going to call `poll` in a loop with only this one socket as a file descriptor.
        // The reason for this is because it's possible that a client is connected to us, and not sending us any data.
        // It may be waiting for it's own data before it can send us a request, or it may have just forgotten to
        // disconnect. In the normal case, this is no big deal, we can wait inside `recv` until it either sends us some
        // data or it disconnects. The exception is if we want to shut down. In that case, we need to know to close the
        // socket at some point, so what we do is `poll` with a 1 second timeout, and if we ever hit the timeout and
        // are in a `shutting down` state, then we finish up and exit. In any other case, we just wait in `poll` again
        // until we get some data or a disconnection.
        int pollResult = 0;
        struct pollfd pollStruct = { socket.s, POLLIN, 0 };

        // As long as `poll` returns 0 we've timed out, indicating that we're still waiting for something to happen. In
        // that case, we'll loop again *unless* we're shutting down.
        while (!(pollResult = poll(&pollStruct, 1, 1'000))) {
            if (_shutdownState != RUNNING) {
                SINFO("Socket thread exiting because no data and shutting down.");
                socket.shutdown(Socket::CLOSED);
                break;
            }
        }

        // If the above loop didn't close the socket due to inactivity at shutdown, let's handle the activity.
        if (socket.state != STCPManager::Socket::CLOSED) {
            if (pollResult < 0) {
                // This is an exceptional case, we'll just kill the socket if this happens and let the client reconnect.
                SINFO("Poll failed: " << strerror(errno));
                socket.shutdown(Socket::CLOSED);
            } else {
                // We've either got new data, or an error on the socket. Let's determine which by trying to read.
                if (!socket.recv()) {
                    // If reading failed, then the socket was closed.
                    socket.shutdown(Socket::CLOSED);
                }
            }
        }

        // Now, if the socket hasn't been closed, we'll try to handle the new data on it appropriately.
        if (socket.state == STCPManager::Socket::CONNECTED) {
            // If there's a request, we'll dequeue it.
            SData request;

            // If the socket is owned by a plugin, we let the plugin populate our request.
            BedrockPlugin* plugin = static_cast<BedrockPlugin*>(socket.data);
            if (plugin) {
                // Call the plugin's handler.
                plugin->onPortRecv(&socket, request);
                if (!request.empty()) {
                    // If it populated our request, then we'll save the plugin name so we can handle the response.
                    request["plugin"] = plugin->getName();
                }
            } else {
                // Otherwise, handle any default request.
                int requestSize = 0;
                if (socket.recvBuffer.startsWithHTTPRequest()) {
                    requestSize = request.deserialize(socket.recvBuffer);
                    socket.recvBuffer.consumeFront(requestSize);
                }

                // If this socket was accepted from the public command port, and that's supposed to be closed now, set
                // `Connection: close` so that we don't keep doing a bunch of activity on it.
                if (requestSize && fromPublicCommandPort && _isCommandPortLikelyBlocked) {
                    request["Connection"] = "close";
                }
            }

            // If we have a populated request, from either a plugin or our default handling, we'll queue up the
            // command.
            if (!request.empty()) {
                // Make a command from our request.
                unique_ptr<BedrockCommand> command = buildCommandFromRequest(move(request), socket, fromPrivateCommandPort);

                if (!command) {
                    // If we couldn't build a command, this was some sort of unusual exception case (like trying to
                    // schedule a command in the future while shutting down). We can just give up.
                    SINFO("No command from request, closing socket.");
                    socket.shutdown(Socket::CLOSED);
                } else if (!_handleIfStatusOrControlCommand(command)) {
                    if (fromControlPort && _shutdownState != RUNNING) {
                        // Don't handle non-control commands on the control port if we're shutting down. As the control
                        // port can remain open through shutdown (in the case of detaching) and can expect DB access,
                        // which is being turned off, these could cause weird crashes. Instead, just return an error.
                        command->response.methodLine = "500 Server Shutting Down";
                        _reply(command);
                    } else {
                        // If it's not handled by `_handleIfStatusOrControlCommand` we fall into the queuing logic.
                        // If the command has a socket (it's this socket) then we need to wait for it to finish before
                        // we can dequeue the next command, so that the responses all end up delivered in order.
                        // If a command *doesn't* have a socket, then that's a special case for a `fire and forget`
                        // command that was already responded to in `buildCommandFromRequest` and we can move on to the
                        // next thing immediately.
                        mutex m;
                        condition_variable cv;
                        atomic<bool> finished = false;

                        function<void()> callback = [&m, &cv, &finished]() {
                            // Lock the mutex above (which will be locked by this thread while we're queuing), which waits
                            // for `handleSocket` to release it's lock (by calling `wait`), and then notify the waiting
                            // socket thread.
                            lock_guard lock(m);
                            finished = true;
                            cv.notify_all();
                        };

                        // Ok, none of above synchronization code gets called unless the command has a socket to respond on.
                        bool hasSocket = command->socket;
                        if (hasSocket) {
                            // Set the destructor callback for when the command finishes.
                            command->destructionCallback = &callback;
                        }

                        runCommand(move(command));

                        // Now that the command is queued, we wait for it to complete (if it's has a socket, and hasn't finished by the time we get to this point).
                        // When this happens, destructionCallback fires, sets `finished` to true, and we can move on to the next request.
                        unique_lock<mutex> lock(m);
                        if (!finished && hasSocket) {
                            cv.wait(lock, [&]{return finished.load();});
                        }
                    }
                }
            }
        } else if (socket.state == STCPManager::Socket::SHUTTINGDOWN || socket.state == STCPManager::Socket::CLOSED) {
            // Do nothing here except prevent the warning below from firing. This loop should exit on the next
            // iteration.
        } else {
            SWARN("Socket in unhandled state: " << socket.state);
        }
    }

    // At this point out socket is closed and we can clean up.
    // Note that we never return early, we always want to hit this code and decrement our counter and clean up our socket.
    _outstandingSocketThreads--;
    SINFO("[performance] Socket thread complete (" << _outstandingSocketThreads << " remaining).");

    // Check to see if we need to unblock creating new socket threads. We do this each time we cross having 50 active
    // threads. We are guaranteed to hit this as the thread count decrements to 0, as _shouldBlockNewSocketThreads is
    // atomic and every value must be hit as threads complete.
    // Note that if we were to start blocking the command port for NOT_ENOUGH_THREADS with less than 50 threads, we
    // will never hit this unlock case, but we only ever see this problem with thousands of threads, so we don't have
    // to try and handle that case, and don't need to lock this mutex on every thread's completion then.
    if (_outstandingSocketThreads == 50) {
        lock_guard<mutex> lock(_newSocketThreadBlockedMutex);
        if (_shouldBlockNewSocketThreads) {
            _shouldBlockNewSocketThreads = false;
            unblockCommandPort("NOT_ENOUGH_THREADS");
        }
    }
}

void BedrockServer::notifyStateChangeToPlugins(SQLite& db, SQLiteNodeState newState) {
    for (auto plugin : plugins) {
        plugin.second->stateChanged(db, newState);
    }
}

SQLiteNodeState BedrockServer::getState() const {
    auto _syncNodeCopy = atomic_load(&_syncNode);
    if (_syncNodeCopy) {
        return _syncNodeCopy->getState();
    }

    // For historical reasons, we return "SEARCHING" instead of "UNKNOWN" when the node is not available.
    // Scripts and tests, as well as the status command, expect this result.
    return SQLiteNodeState::SEARCHING;
}

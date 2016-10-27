/// /svn/src/sqlitecluster/SQLiteNode.cpp
/// =======================
#include <libstuff/libstuff.h>
#include "SQLiteNode.h"

/// Introduction
/// ------------
/// SQLiteNode builds atop STCPNode and SQLite to provide a distributed
/// transactional SQL database.  The STCPNode base class establishes and maintains
/// connections with all peers: if any connection fails, it forever attempts to
/// re-establish.  This frees the SQLiteNode layer to focus on the high-level
/// distributed database state machine.
///
/// **FIXME**: Assertion in WAITING for no currentMaster is sometimes false;
///          appears to block the STANDUP
///
/// **FIXME**: Handle the case where two nodes have conflicting databases;
///          should find where they fork, tag the affected accounts for manual
///          review, and adopt the higher-priority
///
/// **FIXME**: Master should detect whether any slaves fall out of sync for any
///          reason, identify/tag affected accounts, and resynchronize.
///
/// **FIXME**: Add test to measure how long it takes for master to stabalize
///
/// **FIXME**: Add 'nextActivity' to update()
///
/// **FIXME**: If master dies before sending ESCALATE_RESPONSE (or if slave dies
///            before receiving it), then a command might have been committed to
///            the database without notifying whoever initiated it.  Perhaps have
///            the caller identify each command with a unique command guid, and
///            verify inside the query that the command hasn't been executed yet?
///
// --------------------------------------------------------------------------
#undef SLOGPREFIX
#define SLOGPREFIX "{" << name << "/" << SQLCStateNames[_state] << "} "

// Convenience macro for iterating over a priorty queue that is map of priority -> ordered list. Abstracts away map and
// list iteration into one loop.
#define SFOREACHPRIORITYQUEUE(_CT_, _C_, _I_)                                                                          \
    for (map<int, list<_CT_>>::reverse_iterator i = (_C_).rbegin(); i != (_C_).rend(); ++i)                            \
        for (list<_CT_>::iterator _I_ = (i)->second.begin(); _I_ != (i)->second.end(); ++_I_)

// We've bumped these values back up to 5 minutes because some of the billing commands take over 1 minute to process.
#define SQL_NODE_DEFAULT_RECV_TIMEOUT STIME_US_PER_M * 5 // Receive timeout for 'normal' SQLiteNode messages
#define SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT                                                                            \
    STIME_US_PER_S * 60 // Seperate timeout for receiving and applying synchronization commits
                        // Increase me during a rekey if you need larger commits to have more time

const char* SQLCStateNames[] = {"SEARCHING", "SYNCHRONIZING", "WAITING",     "STANDINGUP",
                                "MASTERING", "STANDINGDOWN",  "SUBSCRIBING", "SLAVING"};
const char* SQLCConsistencyLevelNames[] = {"ASYNC", "ONE", "QUORUM"};

// --------------------------------------------------------------------------
SQLiteNode::SQLiteNode(const string& filename, const string& name, const string& host, int priority, int cacheSize,
                       int autoCheckpoint, uint64_t firstTimeout, const string& version, int quorumCheckpoint,
                       const string& synchronousCommands, bool readOnly, int maxJournalSize)
    : STCPNode(name, host, max(SQL_NODE_DEFAULT_RECV_TIMEOUT, SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT)),
      _db(filename, cacheSize, autoCheckpoint, readOnly, maxJournalSize) {
    SASSERT(readOnly || !portList.empty());
    SASSERT(priority >= 0);
    // Initialize
    _priority = priority;
    _state = SQLC_SEARCHING;
    _currentCommand = 0;
    _syncPeer = 0;
    _masterPeer = 0;
    _stateTimeout = STimeNow() + firstTimeout;
    _commandCount = 0;
    _version = version;
    _commitsSinceCheckpoint = 0;
    _quorumCheckpoint = quorumCheckpoint;
    _synchronousCommands = SParseList(SToLower(synchronousCommands));
    _synchronousCommands.push_back("upgradedatabase");
    _readOnly = readOnly;

    // Get this party started
    _changeState(SQLC_SEARCHING);
}

// --------------------------------------------------------------------------
SQLiteNode::~SQLiteNode() {
    // Make sure it's a clean shutdown
    SASSERTWARN(_isQueuedCommandMapEmpty());
    SASSERTWARN(_escalatedCommandMap.empty());
    SASSERTWARN(_processedCommandList.empty());
}

// --------------------------------------------------------------------------
void SQLiteNode::beginShutdown() {
    // Ignore redundant
    if (!gracefulShutdown()) {
        // Start graceful shutdown
        SINFO("Beginning graceful shutdown.");
        _gracefulShutdownTimeout.alarmDuration = STIME_US_PER_S * 30; // 30s timeout before we give up
        _gracefulShutdownTimeout.start();
    }
}

// --------------------------------------------------------------------------
bool SQLiteNode::_isNothingBlockingShutdown() {
    // Don't shutdown if in the middle of a transaction
    if (_db.insideTransaction())
        return false;

    // If we have non-held commands still queued for processing, not done
    SFOREACHMAP (int, list<Command*>, _queuedCommandMap, queuedCommandIt)
        SFOREACH (list<Command*>, queuedCommandIt->second, commandIt)
            if (!(*commandIt)->request.isSet("HeldBy"))
                return false;

    // If we have non-"Connection: wait" commands escalated to master, not done
    SFOREACHMAP (string, Command*, _escalatedCommandMap, escalatedCommandIt)
        if (!SIEquals(escalatedCommandIt->second->request["Connection"], "wait"))
            return false;

    // Finally, we can shut down if we have no open processed commands.
    return _processedCommandList.empty();
}

// --------------------------------------------------------------------------
bool SQLiteNode::shutdownComplete() {
    // First even see if we're shutting down
    if (!gracefulShutdown())
        return false;

    // Next, see if we're timing out the graceful shutdown and killing non-gracefully
    if (_gracefulShutdownTimeout.ringing()) {
        // Timing out
        SWARN("Graceful shutdown timed out, killing non gracefully.");
        return true;
    }

    // Not complete unless we're SEARCHING, SYNCHRONIZING, or WAITING
    if (_state > SQLC_WAITING) {
        // Not in a shutdown state
        SINFO("Can't graceful shutdown yet because state="
              << SQLCStateNames[_state] << ", queued=" << getQueuedCommandList().size()
              << ", escalated=" << _escalatedCommandMap.size() << ", processed=" << _processedCommandList.size());

        // If we end up with anything left in the escalated command map when we're trying to shut down, let's log it,
        // so we can try and diagnose what's happening.
        if (_escalatedCommandMap.size()) {
            for_each(_escalatedCommandMap.begin(), _escalatedCommandMap.end(), [&](std::pair<string, Command*> cmd) {
                string name = cmd.first;
                Command* command = cmd.second;
                int64_t created = command->creationTimestamp;
                int64_t elapsed = STimeNow() - created;
                double elapsedSeconds = (double)elapsed / STIME_US_PER_S;
                string hasHTTPS = (command->httpsRequest) ? "true" : "false";
                SINFO("Escalated command remaining at shutdown("
                      << name << "): " << command->request.methodLine << ". Created: " << command->creationTimestamp
                      << " (" << elapsedSeconds << "s ago), has HTTPS request? " << hasHTTPS);
            });
        }
        return false;
    }

    // If we have unsent data, not done
    SFOREACH (list<Peer*>, peerList, peerIt)
        if ((*peerIt)->s && !(*peerIt)->s->sendBuffer.empty()) {
            // Still sending data
            SINFO("Can't graceful shutdown yet because unsent data to peer '" << (*peerIt)->name << "'");
            return false;
        }

    // Finally, make sure nothing is blocking shutdown
    if (_isNothingBlockingShutdown()) {
        // Yes!
        SINFO("Graceful shutdown is complete");
        return true;
    } else {
        // Not done yet
        SINFO("Can't graceful shutdown yet because waiting on commands: queued="
              << getQueuedCommandList().size() << ", escalated=" << _escalatedCommandMap.size()
              << ", processed=" << _processedCommandList.size());
        return false;
    }
}

// --------------------------------------------------------------------------
SQLiteNode::Command* SQLiteNode::openCommand(const SData& request, int priority, bool unique,
                                             int64_t commandExecuteTime) {
    SASSERT(!request.empty());
    SASSERT(priority <= SPRIORITY_MAX); // Else will trump UpgradeDatabase
    // If unique, then make sure another one isnt on the queue already.
    SAUTOPREFIX(request["requestID"]);
    if (unique)
        SFOREACHPRIORITYQUEUE(Command*, _queuedCommandMap, commandIt)
    if ((*commandIt)->request.methodLine == request.methodLine) {
        // Already got one.  No thanks.
        SINFO("'" << request.methodLine << " already in queue, ignoring");
        return NULL;
    }

    // Wrap in a command for processing
    const string& id = name + "#" + SToStr(_commandCount++);
    Command* command = new Command;
    command->id = id;
    command->request = request;
    command->priority = priority;

    // If we want to execute the command at a particular time.
    // **FIXME: This is not preserved if we escalate this command.
    //          That's fine for now given our use case for it, but
    //          may not be the case in the future.
    int64_t now = STimeNow();
    if (commandExecuteTime > now) {
        command->creationTimestamp = commandExecuteTime;
        SINFO("Scheduling command " << command->id << " for "
                                    << (((int64_t)command->creationTimestamp - now) / STIME_US_PER_S)
                                    << "s in the future.");
    }

    // Pre-process this command if we're MASTER or SLAVE.
    //
    // **NOTE: This is only a "best attempt" -- if the node isn't MASTERING or
    //         SLAVING the command won't get peeked.  If so, when we go
    //         MASTERING we'll have queued commands that haven't been peeked.
    //         Alternatively, when we go SLAVING we'll ESCALATE commands that
    //         haven't been peeked.
    //
    // **NOTE: It's possible the node's state will change after peeking and before
    //         the command is either begun or escalated.
    //
    bool nodeUpToDate = request["commitCount"].empty() || request.calcU64("commitCount") <= getCommitCount();
    bool peekIt = (_state == SQLC_MASTERING || _state == SQLC_SLAVING) && (int64_t)command->creationTimestamp <= now &&
                  nodeUpToDate;

    // Process status commands special, as we never want to escalate to the master
    bool forcePeekIt = false;
    if (SIEquals(request.methodLine, "Status")) {
        // If we're a slave, we never want to escalate this to the master, else
        // it'll return the master's status (which will be super confusion).
        // However, if we're a read only thread on a slave, then we *do* want
        // to escalate to the write thread -- because only the write thread
        // knows the peer status (which is the primary point of this command).
        // So, we want to peek if we're *not* read only.  This way:
        //
        // - If we're a read thread on the slave, we *don't* peek -- instead,
        //   we escalate this internally to the write thread
        //
        // - If we're a write thread on a slave, we *do* peek -- this will
        //   return the peer status from the perspective of this slave.
        //
        // - If we're a read thread on the master, again, we still *don't*
        //   peek, as we want the master's write thread's status
        //
        // - If we're a write thread on the master, we *do* peek.
        forcePeekIt = !_readOnly;
        peekIt = !_readOnly;
    } else if (SIEquals(request.methodLine, "Ping") || SIEquals(request.methodLine, "GET /status/isSlave HTTP/1.1") ||
               SIEquals(request.methodLine, "GET /status/handlingCommands HTTP/1.1")) {
        // Force peek these always -- even if read only -- because any read
        // thread knows our status, and thus can handle these.
        forcePeekIt = true;
    }

    // Do we peek this command?
    if (peekIt) {
        // Don't peek if the versions aren't identical.  If we're newer, then our
        // code might not work until the master upgrades the schema.  On the other
        // hand, if we're older, then the master might have upgraded the schema
        // and is using code we don't have.
        if (_state == SQLC_SLAVING && _version != getMasterVersion()) {
            // Skip
            SHMMM("Skipping slave peek because our version (" << _version << ") doesn't match master ("
                                                              << getMasterVersion() << ").");
            peekIt = false;
        }

        // Don't process if we are not the master of the majority.  A split
        // brain could occur if we are the master of less than half of the peers.
        if (_state == SQLC_MASTERING) {
            // We need at least half the non-permaslave peers to form a majority in order to commit.
            int numFullPeers = 0;
            int numFullSlaves = 0;
            if (!_majoritySubscribed(numFullPeers, numFullSlaves)) {
                // Skip
                SHMMM("Skipping master peek because only "
                      << numFullSlaves << " of " << numFullPeers << " full peers (" << peerList.size()
                      << " with permaslaves) subscribed so remote commit isn't possible.");
                peekIt = false;
            }
        }
    }

    // Ok, peek if we're in the right state to, or if we're forcing it
    if ((peekIt || forcePeekIt) && _peekCommandWrapper(_db, command)) {
        // Done with this command.  Make sure it only started a secondary command
        // if we are in fact mastering.
        SINFO("Processed peekable command '" << request.methodLine << "' (" << id << ")");
        SASSERTWARN(_state == SQLC_MASTERING || !command->httpsRequest);
        _processedCommandList.push_back(command);
    } else if (_masterPeer && SIEquals((*_masterPeer)["State"], "MASTERING")) {
        // We are a slave (because we have a master) and we weren't able to
        // peek the command, which means we're just going to queue it and
        // escalate it in our update loop.  Rather than waiting until then,
        // let's just skip the queue and escalate it right now.
        _escalateCommand(command);
    } else {
        // Queue it.
        SINFO("Queuing new non-peekable command '" << request.methodLine << "' (" << id << "), priority=" << priority
                                                   << (!nodeUpToDate ? " because node out of date." : ""));
        _queueCommand(command);
    }

    // Return the command so the caller can get progress
    return command;
}

// --------------------------------------------------------------------------
SQLiteNode::Command* SQLiteNode::getQueuedCommand(int priority) {
    // Return the first queued command, if any
    list<Command*>& commandList = _queuedCommandMap[priority];
    if (commandList.empty())
        return 0;

    // Pop and return
    Command* front = commandList.front();
    commandList.pop_front();
    SAUTOPREFIX(front->request["requestID"]);

    // And if the list is empty now, then tear down this map index.
    if (commandList.empty()) {
        _queuedCommandMap.erase(priority);
    }
    SINFO("Returning queued command '" << front->request.methodLine << "' (" << front->id << ")");
    return front;
}

// --------------------------------------------------------------------------
SQLiteNode::Command* SQLiteNode::getProcessedCommand() {
    // Return the first processed command, if any
    if (_processedCommandList.empty())
        return 0;

    // Pop and return
    Command* front = _processedCommandList.front();
    SAUTOPREFIX(front->request["requestID"]);
    _processedCommandList.pop_front();
    SINFO("Returning processed command '" << front->request.methodLine << "' (" << front->id << ")");
    return front;
}

// --------------------------------------------------------------------------
SQLiteNode::Command* SQLiteNode::findCommand(const string& name, const string& value) {
    // Search everywhere to see if we have it
    SFOREACHPRIORITYQUEUE(Command*, _queuedCommandMap, commandIt) {
        if ((*commandIt)->request[name] == value) {
            return *commandIt;
        }
    }
    SFOREACHMAP (string, Command*, _escalatedCommandMap, commandIt) {
        if (commandIt->second->request[name] == value) {
            return commandIt->second;
        }
    }
    SFOREACH (list<Command*>, _processedCommandList, commandIt) {
        if ((*commandIt)->request[name] == value) {
            return *commandIt;
        }
    }

    // Didn't find it
    return 0;
}

// --------------------------------------------------------------------------
void SQLiteNode::clearCommandHolds(const string& heldBy) {
    // Loop across and release everything waiting on this command
    // **NOTE: It's tempting to only release some specific number of commands (eg, one)
    //         waiting on this hold, such as to release only one worker waiting on a
    //         job of a particular type.  However, it's possible that in the time between
    //         the command being woken up and when it's processed, the command might get
    //         closed.  It's a super edge case, but it's safer to just wake them
    //         all up, to ensure at least one gets it.  (The others will be put back to
    //         sleep when processed.)
    SINFO("Clearing all commands held by '" << heldBy << "'");
    SFOREACHPRIORITYQUEUE(Command*, _queuedCommandMap, commandIt) {
        // See if this command is held by anything
        Command* command = *commandIt;
        const string& commandHeldBy = command->request["HeldBy"];
        if (!commandHeldBy.empty()) {
            // Convert from SQL "LIKE" syntax to regular expression.  This will generate false
            // positives, but no false negatives.
            if (SREMatch(SReplace(SToLower(commandHeldBy), "%", ".*"), SToLower(heldBy))) {
                // Release this one
                SINFO("Releasing command '" << command->request.methodLine << "' (#" << command->id << ") held by '"
                                            << commandHeldBy << "'");
                command->request["HeldBy"].clear();
            } else {
                // Continue holding
                SINFO("Continuing to hold '" << command->request.methodLine << "' (#" << command->id << ") held by '"
                                             << commandHeldBy << "'");
            }
        }
    }
}

// --------------------------------------------------------------------------
void SQLiteNode::closeCommand(Command* command) {
    SASSERT(command);
    // Note that we've closed the command
    SDEBUG("Closing command '" << command->id << "'");

    // Verify the command has completed before closing.  Three exceptions:
    // 1) If we self-initiated the command.  (**FIXME: Why is this ok?)
    // 2) If we we're waiting on some hold, then we'll close it without
    //    response if the connection fails.
    // 3) This is a read only node.  In this case, we close a command that
    //    we haven't processed in order to send it to the write thread.
    if (!command->initiator && command->request["HeldBy"].empty() && !_readOnly) {
        SASSERTWARN(!command->response.empty());
    }

    // ***NOTE: Not using list.remove( ) because that will always iterate the entire list removing all matching values.
    //          However, the command is distinct and near the front so using list.erase( ).
    list<Command*>::iterator commandIt =
        ::find(_queuedCommandMap[command->priority].begin(), _queuedCommandMap[command->priority].end(), command);
    if (commandIt != _queuedCommandMap[command->priority].end())
        _queuedCommandMap[command->priority].erase(commandIt);

    // Verify we're not closing a command while it's being worked on
    if (_currentCommand == command) {
        // This can happen if a master needs to do an emergency standdown while
        // waiting for a response to a distributed transaction.
        uint64_t elapsed = STimeNow() - _currentCommand->creationTimestamp;
        SWARN("Closing outstanding command '" << command->request.methodLine << "' (" << command->id << ") after "
                                              << elapsed / STIME_US_PER_MS << "ms, aborting.");
        _abortCommand(_db, command);
        if (_db.insideTransaction())
            _db.rollback();
        _currentCommand = 0;
    }

    // Are we closing a command for which we're still awaiting a response from the master?
    if (SContains(_escalatedCommandMap, command->id)) {
        // Tell the master not to bother processing this command
        SINFO("Canceling escalated command " << command->id);
        if (_masterPeer && _masterPeer->s) {
            SData escalate("ESCALATE_CANCEL");
            escalate["ID"] = command->id;
            escalate.content = command->request.serialize();
            _sendToPeer(_masterPeer, escalate);
        } else {
            SWARN("Trying to cancel escalated command " << command->id << ", but no master. Just erasing it.");
        }
        _escalatedCommandMap.erase(command->id);
    }

    // Finish the cleanup
    _processedCommandList.remove(command);
    delete command;
}

// --------------------------------------------------------------------------
list<string> SQLiteNode::getQueuedCommandList() {
    list<string> commandList;
    SFOREACHPRIORITYQUEUE(Command*, _queuedCommandMap, commandIt)
    commandList.push_back((*commandIt)->request.methodLine + " (" + (*commandIt)->id + ")");
    return commandList;
}

// --------------------------------------------------------------------------
list<string> SQLiteNode::getEscalatedCommandList() {
    list<Command*> orderedEscalatedCommands = _getOrderedCommandListFromMap(_escalatedCommandMap);
    list<string> commandList;
    SFOREACH (list<Command*>, orderedEscalatedCommands, it)
        commandList.push_back((*it)->request.methodLine + " (" + (*it)->id + ")");
    return commandList;
}

// --------------------------------------------------------------------------
list<string> SQLiteNode::getProcessedCommandList() {
    list<string> commandList;
    SFOREACH (list<Command*>, _processedCommandList, it)
        commandList.push_back((*it)->request.methodLine + " (" + (*it)->id + ")");
    return commandList;
}

// --------------------------------------------------------------------------
void SQLiteNode::_queueCommand(Command* command) {
    // Specify the consistency level.  Certain commands require quorum and are
    // specified at launch with command line args.  Clients can also specify
    // consistency requirements on a per-command basis.
    if (SContains(_synchronousCommands, SToLower(command->request.methodLine))) {
        // This was hard-configured via the comand line to use full quorum
        SINFO("'" << command->request.methodLine << "' in synchronous comand list; setting QUORUM consistency");
        command->writeConsistency = SQLC_QUORUM;
    } else if (command->request.isSet("writeConsistency")) {
        // The caller is requesting some amount of consistency
        int wc = SStateNameToInt(SQLCConsistencyLevelNames, command->request["writeConsistency"],
                                 SQLC_NUM_CONSISTENCY_LEVELS);
        if (wc >= 0)
            command->writeConsistency = (SQLCConsistencyLevel)wc;
        else
            SWARN("Unknown write consistency supplied '" << command->request["writeConsistency"] << "'. Ignoring.");
        SINFO("'" << command->request.methodLine << "' has overridden default consistency with '"
                  << command->request["writeConsistency"] << "' ("
                  << SQLCConsistencyLevelNames[command->writeConsistency] << ")");
    }

    // Insert in order of priority if the command is scheduled to be executed
    // now.  However, absolutely never ever insert a command after one that is
    // scheduled to run after the command you are inserting.  So, let's take
    // two examples:
    // 1. A command scheduled to be run in the future will skip past the front
    //    part of the queue because it is creationTimestamp will be greater
    //    than all the messages scheduled to be run now.  Then, it will keep
    //    going comparing to ever increasing creationTimestamps until its own
    //    timestamp is lower.  Insert there.
    // 2. A command scheduled to be run now with minimal priority will keep
    //    going through the queue comapring its priority until it hits commands
    //    scheduled for the future.  There it will stop even though its
    //    priority is lower than the future commands because the its timestamp
    //    will be lower.  Its timestamp was not lower than all the other
    //    commands scheduled to run now because we are queuing it later.  So,
    //    we insert this command at the boundery of commands to run now and
    //    commands scheduled for the future.  This was a serious bug before!
    // **FIXME: If a command execute time is provided, priority is rendered
    //          mostly meaningless.  It is not enforced when two commands are
    //          scheduled for the same moment and it will effectively be at the
    //          end of the queue after we pass its createdTimestamp.  This
    //          will create a slightly odd queue where you could have a highest
    //          priority command in the back.
    if (_state != SQLC_MASTERING && command->httpsRequest)
        SWARN("Only MASTERING nodes should trigger secondary requests.");

    // If the priority queue is empty or the command timestamp is after all other commands, append it to the
    // end of it's priority list (99.9% of the time). Otherwise insert the command into the priority queue
    // at the proper position, based on command timestamp. Start from the back because the proper place
    // for the command is likely near the back.
    if (_queuedCommandMap[command->priority].empty() ||
        command->creationTimestamp > _queuedCommandMap[command->priority].back()->creationTimestamp)
        _queuedCommandMap[command->priority].push_back(command);
    else
        SFOREACH (list<Command*>, _queuedCommandMap[command->priority], commandIt)
            if (command->creationTimestamp < (*commandIt)->creationTimestamp) {
                _queuedCommandMap[command->priority].insert(commandIt, command);
                return;
            }
}

// --------------------------------------------------------------------------
void SQLiteNode::_escalateCommand(Command* command) {
    // Send this to the MASTER
    SASSERT(_masterPeer);
    SASSERTEQUALS((*_masterPeer)["State"], "MASTERING");
    uint64_t elapsed = STimeNow() - command->creationTimestamp;
    SINFO("Escalating '" << command->request.methodLine << "' (" << command->id << ") to MASTER '" << _masterPeer->name
                         << "' after " << elapsed / STIME_US_PER_MS << " ms");
    _escalatedCommandMap.insert(pair<string, Command*>(command->id, command));
    SData escalate("ESCALATE");
    escalate["ID"] = command->id;
    escalate["priority"] = SToStr(command->priority);
    escalate.content = command->request.serialize();
    _sendToPeer(_masterPeer, escalate);
}

// --------------------------------------------------------------------------
void SQLiteNode::_finishCommand(Command* command) {
    SASSERT(command);
    // Done with this command
    // **FIXME: I'd prefer _cleanCommand() to be in closeCommand(), but it needs
    //          to be here because it cleans up the global _prepaidTransaction, etc.
    //          variables.  These must be cleaned before the next command is
    //          started.
    uint64_t elapsed = STimeNow() - command->creationTimestamp;
    uint64_t httpElapsed =
        (command->httpsRequest ? command->httpsRequest->finished - command->httpsRequest->created : 0);
    uint64_t replicationElapsed =
        (command->replicationStartTimestamp ? STimeNow() - command->replicationStartTimestamp : 0);
    SINFO("Finishing command '" << command->request.methodLine << "'(" << command->id << ")->'"
                                << command->response.methodLine << "' after " << elapsed / STIME_US_PER_MS << "ms ("
                                << httpElapsed / STIME_US_PER_MS << "ms in http) ("
                                << replicationElapsed / STIME_US_PER_MS << " ms in replication)");
    _cleanCommand(command);

    // Is it a slave-initiated, special-case, or locally-initiated command?
    if (command->initiator) {
        // This should never get called if the initiating slave was lost; on
        // slave drop we clean out any pending commands.
        uint64_t elapsed = STimeNow() - command->creationTimestamp;
        if (elapsed < STIME_US_PER_S * 4 || !command->request["HeldBy"].empty()) {
            SINFO("Responding to escalation for transaction '" << command->request.methodLine << "' (" << command->id
                                                               << ") after " << elapsed / STIME_US_PER_MS << " ms");
        } else {
            SWARN("Responding to escalation for transaction '" << command->request.methodLine << "' (" << command->id
                                                               << ") after " << elapsed / STIME_US_PER_MS
                                                               << " ms (slow)");
        }
        SASSERT(command->initiator->s);
        SASSERTWARN((*command->initiator)["Subscribed"] == "true");
        SData escalate("ESCALATE_RESPONSE");
        escalate["ID"] = command->id;
        escalate.content = command->response.serialize();
        _sendToPeer(command->initiator, escalate);
        delete command;
    } else if (SIEquals(command->request.methodLine, "UpgradeDatabase")) {
        // Special command, just delete it.
        SINFO("Database upgrade complete");
        delete command;
    } else {
        // Locally-initiated command -- hold onto it until the caller cleans up.
        _processedCommandList.push_back(command);
    }
}

// --------------------------------------------------------------------------
/// State Machine
/// -------------
/// Here is a simplified state diagram showing the major state transitions:
///
///                              SEARCHING
///                                  |
///                            SYNCHRONIZING
///                                  |
///                               WAITING
///                    ___________/     \____________
///                   |                              |
///              STANDINGUP                    SUBSCRIBING
///                   |                              |
///               MASTERING                       SLAVING
///                   |                              |
///             STANDINGDOWN                         |
///                   |___________       ____________|
///                               \     /
///                              SEARCHING
///
/// In short, every node starts out in the SEARCHING state, where it simply tries
/// to establish all its peer connections.  Once done, each node SYNCHRONIZES with
/// the freshest peer, meaning they download whatever "commits" they are
/// missing.  Then they WAIT until the highest priority node "stands up" to become
/// the new "master".  All other nodes then SUBSCRIBE and become "slaves".  If the
/// master "stands down", then all slaves unsubscribe and everybody goes back into
/// the SEARCHING state and tries it all over again.
///
///
/// State Transitions
/// -----------------
/// Each state transitions according to the following events and operates as follows:
///
bool SQLiteNode::update(uint64_t& nextActivity) {
    // Process the database state machine
    switch (_state) {
    /// - SEARCHING: Wait for a a period and try to connect to all known
    ///     peers.  After a timeout, give up and go ahead with whoever
    ///     we were able to successfully connect to -- if anyone.  The
    ///     logic for this state is as follows:
    ///
    ///         if( no peers configured )             goto MASTERING
    ///         if( !timeout )                        keep waiting
    ///         if( no peers connected )              goto MASTERING
    ///         if( nobody has more commits than us ) goto WAITING
    ///         else send SYNCHRONIZE and goto SYNCHRONIZING
    ///
    case SQLC_SEARCHING: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // If we're trying to shut down, just do nothing
        if (shutdownComplete())
            return false; // Don't re-update

        // If no peers, we're the master
        if (peerList.empty()) {
            // There are no peers, jump straight to mastering
            SHMMM("No peers configured, jumping to MASTERING");
            _changeState(SQLC_MASTERING);
            return true; // Re-update immediately
        }

        // How many peers have we logged in to?
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        Peer* freshestPeer = 0;
        Peer* nearestSlave = 0;
        SFOREACH (list<Peer*>, peerList, peerIt) {
            // Wait until all connected (or failed) and logged in
            Peer* peer = *peerIt;
            bool permaSlave = peer->test("Permaslave");
            bool loggedIn = peer->test("LoggedIn");

            // Count how many full peers (non-permaslaves) we have
            numFullPeers += !permaSlave;

            // Count how many full peers are logged in
            numLoggedInFullPeers += (!permaSlave) && loggedIn;

            // Find the freshest peer and fastest slave
            if (loggedIn) {
                // The freshest peer is the one that has the most commits.
                if (!freshestPeer || peer->calcU64("CommitCount") > freshestPeer->calcU64("CommitCount")) {
                    freshestPeer = peer;
                }

                // Additionally, if the remote peer is a slave AND
                // higher commit count than us, is it nearer
                // (latency-wise to us) than any other?  If so, we want
                // to synchronize from it *even if* it's not the
                // freshest.  (We'll get the rest from the master when
                // we begin slaving, but by actively seeking out the
                // fastest slave, we remove extraneous load from the
                // master while also accelerating the synchronization
                // by avoiding going over any WAN connections.)
                //
                // **NOTE: It takes a moment to measure peer latency,
                //         so 0 means it's not yet set
                bool peerIsSlaving = SIEquals((*peer)["State"], "SLAVING");
                bool peerLatencyKnown = (peer->latency > 0);
                bool peerIsFresherThanUs = (peer->calcU64("CommitCount") > _db.getCommitCount());
                if (peerIsSlaving && peerLatencyKnown && peerIsFresherThanUs &&
                    (!nearestSlave || peer->latency < nearestSlave->latency)) {
                    // Found a closer slave that has data we don't
                    nearestSlave = peer;
                }
            }
        }

        // Keep searching until we connect to at least half our non-permaslave peers OR timeout
        SINFO("Signed in to " << numLoggedInFullPeers << " of " << numFullPeers << " full peers (" << peerList.size()
                              << " with permaslaves), timeout in " << (_stateTimeout - STimeNow()) / STIME_US_PER_MS
                              << "ms");
        if (((float)numLoggedInFullPeers < numFullPeers / 2.0) && (STimeNow() < _stateTimeout))
            return false;

        // We've given up searching; did we time out?
        if (STimeNow() >= _stateTimeout)
            SHMMM("Timeout SEARCHING for peers, continuing.");

        // If no freshest (not connected to anyone), wait
        if (!freshestPeer) {
            // Unable to connect to anyone
            SHMMM("Unable to connect to any peer, WAITING.");
            _changeState(SQLC_WAITING);
            return true; // Re-update
        }

        // How does our state compare with the freshest peer?
        SASSERT(freshestPeer);
        uint64_t freshestPeerCommitCount = freshestPeer->calcU64("CommitCount");
        if (freshestPeerCommitCount == _db.getCommitCount()) {
            // We're up to date
            SINFO("Synchronized with the freshest peer '" << freshestPeer->name << "', WAITING.");
            _changeState(SQLC_WAITING);
            return true; // Re-update
        }

        // Are we fresher than the freshest peer?
        if (freshestPeerCommitCount < _db.getCommitCount()) {
            // Looks like we're the freshest peer overall
            SINFO("We're the freshest peer, WAITING.");
            _changeState(SQLC_WAITING);
            return true; // Re-update
        }

        // It has a higher commit count than us, synchronize.
        SASSERT(freshestPeerCommitCount > _db.getCommitCount());
        SASSERTWARN(!_syncPeer);
        if (nearestSlave) {
            // Always sync from the nearest slave (if available), even if
            // it's not the freshest.  We'll catch up the difference when
            // we slave from the master.
            SINFO("SYNCHRONIZING from nearest slave '" << nearestSlave->name << " ("
                                                       << nearestSlave->latency / STIME_US_PER_MS << "ms latency)");
            _syncPeer = nearestSlave;
        } else {
            // No slave nearby, so let's just go to the freshest peer.  (We
            // only go with the nearest peer if it's slaving, otherwise it
            // might be out of sync with the master.  But if there is no
            // master we go with the freshest, as we assume it's probably
            // going to be the master.)
            SINFO("SYNCHRONIZING from freshest peer '" << freshestPeer->name << " ("
                                                       << freshestPeer->latency / STIME_US_PER_MS << "ms latency)");
            _syncPeer = freshestPeer;
        }
        _sendToPeer(_syncPeer, SData("SYNCHRONIZE"));
        _changeState(SQLC_SYNCHRONIZING);
        return true; // Re-update
    }

    /// - SYNCHRONIZING: We only stay in this state while waiting for
    ///     the SYNCHRONIZE_RESPONSE.  When we receive it, we'll enter
    ///     the WAITING state.  Alternately, give up waitng after a
    ///     period and go SEARCHING.
    ///
    case SQLC_SYNCHRONIZING: {
        SASSERTWARN(_syncPeer);
        SASSERTWARN(!_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Nothing to do but wait
        if (STimeNow() > _stateTimeout) {
            // Give up on synchronization; reconnect that peer and go searching
            SHMMM("Timed out while waiting for SYNCHRONIZE_RESPONSE, searching.");
            _reconnectPeer(_syncPeer);
            _syncPeer = 0;
            _changeState(SQLC_SEARCHING);
            return true; // Re-update
        }
        break;
    }

    /// - WAITING: As the name implies, wait until something happens.  The
    ///     logic for this state is as follows:
    ///
    ///         loop across "LoggedIn" peers to find the following:
    ///             - freshest peer (most commits)
    ///             - highest priority peer
    ///             - current master (might be STANDINGUP or STANDINGDOWN)
    ///         if( no peers logged in )
    ///             goto SEARCHING
    ///         if( a higher-priority MASTERING master exists )
    ///             send SUBSCRIBE and go SUBSCRIBING
    ///         if( the freshest peer has more commits han us )
    ///             goto SEARCHING
    ///         if( no master and we're the highest prioriy )
    ///             clear "StandupResponse" on all peers
    ///             goto STANDINGUP
    ///
    case SQLC_WAITING: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        SASSERTWARN(_escalatedCommandMap.empty());
        // If we're trying and ready to shut down, do nothing.
        if (gracefulShutdown()) {
            // Do we have any outstanding commands?
            if (_queuedCommandMap.empty()) {
                // Nope!  Let's just halt the FSM here until we shutdown so as to
                // avoid potential confusion.  (Technically it would be fine to continue
                // the FSM, but it makes the logs clearer to just stop here.)
                SINFO("Graceful shutdown underway and no queued commands, do nothing.");
                return false; // No fast update
            } else {
                // We do have outstanding commands, even though a graceful shutdown
                // has been requested.  This is probably due to us previously being a master
                // to which commands had been sent directly -- we got the signal to shutdown,
                // and stood down immediately.  All the slaves will re-escalate whatever
                // commands they were waiting on us to process, so they're fine.  But our own
                // commands still need to be processed.  We're no longer the master, so we
                // can't do it.  Rather, even though we're trying to do a graceful shutdown,
                // we need to find and slave to the new master, and have it process our
                // commands.  Once the new master has processed our commands, then we can
                // shut down gracefully.
                SHMMM("Graceful shutdown underway but queued commands so continuing...");
            }
        }

        // Loop across peers and find the highest priority and master
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        Peer* highestPriorityPeer = 0;
        Peer* freshestPeer = 0;
        Peer* currentMaster = 0;
        SFOREACH (list<Peer*>, peerList, peerIt) {
            // Make sure we're a full peer
            Peer* peer = *peerIt;
            if (peer->params["Permaslave"] != "true") {
                // Verify we're logged in
                ++numFullPeers;
                if (SIEquals((*peer)["LoggedIn"], "true")) {
                    // Verify we're still fresh
                    ++numLoggedInFullPeers;
                    if (!freshestPeer || peer->calcU64("CommitCount") > freshestPeer->calcU64("CommitCount"))
                        freshestPeer = peer;

                    // See if it's the highest priority
                    if (!highestPriorityPeer || peer->calc("Priority") > highestPriorityPeer->calc("Priority"))
                        highestPriorityPeer = peer;

                    // See if it is currently the master (or standing up/down)
                    const string& peerState = (*peer)["State"];
                    if (SIEquals(peerState, "STANDINGUP") || SIEquals(peerState, "MASTERING") ||
                        SIEquals(peerState, "STANDINGDOWN")) {
                        // Found the current master
                        if (currentMaster)
                            PHMMM("Multiple peers trying to stand up (also '" << currentMaster->name
                                                                              << "'), let's hope they sort it out.");
                        currentMaster = peer;
                    }
                }
            }
        }

        // If there are no logged in peers, then go back to SEARCHING.
        if (!highestPriorityPeer) {
            // Not connected to any other peers
            SHMMM("Configured to have peers but can't connect to any, re-SEARCHING.");
            _changeState(SQLC_SEARCHING);
            return true; // Re-update
        }
        SASSERT(highestPriorityPeer);
        SASSERT(freshestPeer);

        // If there is already a master that is higher priority than us,
        // subscribe -- even if we're not in sync with it.  (It'll bring
        // us back up to speed while subscribing.)
        if (currentMaster && _priority < highestPriorityPeer->calc("Priority") &&
            SIEquals((*currentMaster)["State"], "MASTERING")) {
            // Subscribe to the master
            SINFO("Subscribing to master '" << currentMaster->name << "'");
            _masterPeer = currentMaster;
            _masterVersion = (*_masterPeer)["Version"];
            _sendToPeer(currentMaster, SData("SUBSCRIBE"));
            _changeState(SQLC_SUBSCRIBING);
            return true; // Re-update
        }

        // No master to subscribe to, let's see if there's anybody else
        // out there with commits we don't have.  Might as well synchronize
        // while waiting.
        if (freshestPeer->calcU64("CommitCount") > _db.getCommitCount()) {
            // Out of sync with a peer -- resynchronize
            SHMMM("Lost synchronization while waiting; re-SEARCHING.");
            _changeState(SQLC_SEARCHING);
            return true; // Re-update
        }

        // No master and we're in sync, perhaps everybody is waiting for us
        // to stand up?  If we're higher than the highest priority, and are
        // connected to enough full peers to achieve quorum we should be
        // master.
        if (!currentMaster && numLoggedInFullPeers * 2 >= numFullPeers &&
            _priority > highestPriorityPeer->calc("Priority")) {
            // Yep -- time for us to stand up -- clear everyone's
            // last approval status as they're about to send them.
            SASSERT(_priority > 0); // Permaslave should never stand up
            SINFO("No master and we're highest priority (over " << highestPriorityPeer->name << "), STANDINGUP");
            SFOREACH (list<Peer*>, peerList, peerIt)
                (*peerIt)->erase("StandupResponse");
            _changeState(SQLC_STANDINGUP);
            return true; // Re-update
        }

        // Keep waiting
        SDEBUG("Connected to " << numLoggedInFullPeers << " of " << numFullPeers << " full peers (" << peerList.size()
                               << " with permaslaves), priority=" << _priority);
        break;
    }

    /// - STANDINGUP: We're waiting for peers to approve or deny our standup
    ///     request.  The logic for this state is:
    ///
    ///         if( at least one peer has denied standup )
    ///             goto SEARCHING
    ///         if( everybody has responded and approved )
    ///             goto MASTERING
    ///         if( somebody hasn't responded but we're timing out )
    ///             goto SEARCHING
    ///
    case SQLC_STANDINGUP: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Wait for everyone to respond
        bool allResponded = true;
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        SFOREACH (list<Peer*>, peerList, peerIt) {
            // Check this peer; if not logged in, tacit approval
            Peer* peer = *peerIt;
            if (peer->params["Permaslave"] != "true") {
                ++numFullPeers;
                if (SIEquals((*peer)["LoggedIn"], "true")) {
                    // Connected and logged in.
                    numLoggedInFullPeers++;

                    // Has it responded yet?
                    if (!peer->isSet("StandupResponse")) {
                        // At least one logged in full peer hasn't responded
                        allResponded = false;
                    } else if (!SIEquals((*peer)["StandupResponse"], "approve")) {
                        // It responeded, but didn't approve -- abort
                        PHMMM("Refused our STANDUP (" << (*peer)["Reason"] << "), cancel and RESEARCH");
                        _changeState(SQLC_SEARCHING);
                        return true; // Re-update
                    }
                }
            }
        }

        // If everyone's responded with approval and we form a majority, then finish standup.
        bool majorityConnected = numLoggedInFullPeers * 2 >= numFullPeers;
        if (allResponded && majorityConnected) {
            // Complete standup
            SINFO("All peers approved standup, going MASTERING.");
            _changeState(SQLC_MASTERING);
            return true; // Re-update
        }

        // See if we're taking too long
        if (STimeNow() > _stateTimeout) {
            // Timed out
            SHMMM("Timed out waiting for STANDUP approval; reconnect all and re-SEARCHING.");
            _reconnectAll();
            _changeState(SQLC_SEARCHING);
            return true; // Re-update
        }
        break;
    }

    /// - MASTERING / STANDINGDOWN : These are the states where the magic
    ///     happens.  In both states, the node will execute distributed
    ///     transactions.  However, new transactions are only
    ///     started in the MASTERING state (while existing transactions are
    ///     concluded in the STANDINGDOWN) state.  The logic for this state
    ///     is as follows:
    ///
    ///         if( we're processing a transaction )
    ///             if( all subscribed slaves have responded/approved )
    ///                 commit this transaction to the local DB
    ///                 broadcast COMMIT_TRANSACTION to all subscribed slaves
    ///                 send a STATE to show we've committed a new transaction
    ///                 notify the caller that the command is complete
    ///         if( we're MASTERING and not processing a command )
    ///             if( there is another MASTER )         goto STANDINGDOWN
    ///             if( there is a higher priority peer ) goto STANDINGDOWN
    ///             if( a command is queued )
    ///                 if( processing the command affects the database )
    ///                    clear the TransactionResponse of all peers
    ///                    broadcast BEGIN_TRANSACTION to subscribed slaves
    ///         if( we're standing down and all slaves have unsubscribed )
    ///             goto SEARCHING
    ///
    case SQLC_MASTERING:
    case SQLC_STANDINGDOWN: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);

        // Are we waiting for approval of a distributed transaction?
        if (_currentCommand && !_currentCommand->transaction.empty()) {
            // Loop across all peers configured to see how many are:
            SAUTOPREFIX(_currentCommand->request["requestID"]);
            int numFullPeers = 0;     // Num non-permaslaves configured
            int numFullSlaves = 0;    // Num full peers that are "subscribed"
            int numFullResponded = 0; // Num full peers that have responded approve/deny
            int numFullApproved = 0;  // Num full peers that have approved
            SFOREACH (list<Peer*>, peerList, peerIt) {
                // Check this peer to see if it's full or a permaslave
                Peer* peer = *peerIt;
                if (peer->params["Permaslave"] != "true") {
                    // It's a full peer -- is it subscribed, and if so, how did it respond?
                    ++numFullPeers;
                    if ((*peer)["Subscribed"] == "true") {
                        // Subscribed, did it respond?
                        numFullSlaves++;
                        const string& response = (*peer)["TransactionResponse"];
                        if (response.empty())
                            continue;
                        numFullResponded++;
                        numFullApproved += SIEquals(response, "approve");
                        if (!SIEquals(response, "approve"))
                            SWARN("Peer '" << peer->name << "' declined '" << _currentCommand->request.methodLine
                                           << "'");
                        else
                            SDEBUG("Peer '" << peer->name << "' has approved.");
                    }
                }
            }

            // Did we get a majority?  This is important whether or not our consistency level needs it, as it will
            // reset the checkpoint limit either way.
            bool majorityApproved = (numFullApproved * 2 >= numFullPeers);

            // Figure out how much consistency we need.  Go with whatever
            // is specified in the command, unless we're over our
            // checkpoint limit.
            SQLCConsistencyLevel consistencyRequired =
                (_commitsSinceCheckpoint >= _quorumCheckpoint) ? SQLC_QUORUM : _currentCommand->writeConsistency;

            // Figure out if we have enough consistency
            bool consistentEnough = false;
            switch (consistencyRequired) {
            case SQLC_ASYNC:
                // Always consistent enough if we don't care!
                consistentEnough = true;
                break;

            case SQLC_ONE:
                // So long at least one full approved (if we have any peers, that is), we're good
                consistentEnough = !numFullPeers || (numFullApproved > 0);
                break;

            case SQLC_QUORUM:
                // This one requires a majority
                consistentEnough = majorityApproved;
                break;

            default:
                SERROR("Invalid write consistency.");
                break;
            }

            // See if all active non-permaslaves have responded.
            // **NOTE: This can be true if nobody responds if numFullSlaves=0
            bool everybodyResponded = numFullResponded >= numFullSlaves;

            // Only approve if the peer who initiated the transaction is
            // still alive.  (If there is no initiator, then it's assumed
            // to be us -- and we're implicitly subscribed to ourselves.)
            // If the initiating peer died, then abort the transaction:
            // either it doesn't need the result, or it will be
            // resubmitted.
            bool initiatorSubscribed = (!_currentCommand->initiator || _currentCommand->initiator->test("Subscribed"));

            // Record these for posterity
            SDEBUG("'" << _currentCommand->request.methodLine << "' (#" << _currentCommand->id << "): "
                                                                                                  "numFullPeers="
                       << numFullPeers << ", "
                                          "numFullSlaves="
                       << numFullSlaves << ", "
                                           "numFullResponded="
                       << numFullResponded << ", "
                                              "numFullApproved="
                       << numFullApproved << ", "
                                             "majorityApproved="
                       << majorityApproved << ", "
                                              "writeConsistency="
                       << SQLCConsistencyLevelNames[_currentCommand->writeConsistency] << ", "
                                                                                          "consistencyRequired="
                       << SQLCConsistencyLevelNames[consistencyRequired] << ", "
                                                                            "consistentEnough="
                       << consistentEnough << ", "
                                              "everybodyResponded="
                       << everybodyResponded << ", "
                                                "initiatorSubscribed="
                       << initiatorSubscribed << ", "
                                                 "commitsSinceCheckpoint="
                       << _commitsSinceCheckpoint);

            // Ok, let's see if we have enough to finish this command
            bool commandFinished = false;
            if (!initiatorSubscribed || (everybodyResponded && !consistentEnough)) {
                // Abort this distributed transaction.  Either happen
                // because the initiator disappeard, or everybody has
                // responded without enough consistency (indicating it'll
                // never come).  Failed, tell everyone to rollback.
                SWARN("Rolling back transaction for '"
                      << _currentCommand->request.methodLine << "' because initiatorSubscribed=" << initiatorSubscribed
                      << ", everybodyResponded=" << everybodyResponded << ", consistentEnough=" << consistentEnough);
                _db.rollback();
                commandFinished = true;

                // Notify everybody to rollback
                SData rollback("ROLLBACK_TRANSACTION");
                rollback["ID"] = _currentCommand->id;
                _sendToAllPeers(rollback, true); // subscribed only

                // Notify the caller that this command failed
                _currentCommand->response.clear();
                _currentCommand->response.methodLine = "500 Failed to get adequate consistency";
            } else if (consistentEnough) {
                // Commit this distributed transaction.  Either we have quorum, or we don't need it.
                uint64_t start = STimeNow();
                _db.commit();
                _currentCommand->processingTime += STimeNow() - start;
                commandFinished = true;

                // Record how long it took
                uint64_t beginElapsed, readElapsed, writeElapsed, prepareElapsed, commitElapsed, rollbackElapsed;
                uint64_t totalElapsed = _db.getLastTransactionTiming(beginElapsed, readElapsed, writeElapsed,
                                                                     prepareElapsed, commitElapsed, rollbackElapsed);
                SINFO("Committed master transaction for '"
                      << _currentCommand->request.methodLine << "' (" << _currentCommand->id << ") "
                                                                                                "#"
                      << _db.getCommitCount() + 1 << " (" << _db.getUncommittedHash() << "). "
                      << _commitsSinceCheckpoint << " commits since quorum "
                                                    "(consistencyRequired="
                      << SQLCConsistencyLevelNames[consistencyRequired] << "), " << numFullApproved << " of "
                      << numFullPeers << " approved "
                                         "("
                      << peerList.size() << " total) in " << totalElapsed / STIME_US_PER_MS << " ms ("
                      << beginElapsed / STIME_US_PER_MS << "+" << readElapsed / STIME_US_PER_MS << "+"
                      << writeElapsed / STIME_US_PER_MS << "+" << prepareElapsed / STIME_US_PER_MS << "+"
                      << commitElapsed / STIME_US_PER_MS << "+" << rollbackElapsed / STIME_US_PER_MS << "ms)");

                // Try to flush the send buffer here in order to
                // prevent the case of the master sending out the
                // commits but the next command taking for ever and
                // causing the other nodes to timeout.  Note that
                // no flush happens before we start processing the
                // next commands below.  That situation can result in
                // an out of sync db on the master due to the master
                // committing and the slaves rolling back on disconnect.
                SData commit("COMMIT_TRANSACTION");
                commit["ID"] = _currentCommand->id;
                _sendToAllPeers(commit, true); // subscribed only
            }

            // Did we finish the command:
            if (commandFinished) {
                // Either way, we're done with this command
                _finishCommand(_currentCommand);
                _currentCommand = 0;

                // If we havne't received majority approval, increment how
                // many commits we've had without a "checkpoint"
                if (majorityApproved) {
                    // Safe!
                    SINFO("Commit checkpoint achieved, resetting uncheckpointed commit count to 0.");
                    _commitsSinceCheckpoint = 0;
                } else {
                    // We're going further out on a limb...
                    _commitsSinceCheckpoint++;
                }
            } else {
                // Still waiting
                SINFO("Waiting to commit: '"
                      << _currentCommand->request.methodLine << "' (#" << _currentCommand->id
                      << "): consistencyRequired=" << SQLCConsistencyLevelNames[consistencyRequired]
                      << ", commitsSinceCheckpoint=" << _commitsSinceCheckpoint);
            }
        }

        // If we're the master, see if we're to stand down (and if not, start a new command)
        if (_state == SQLC_MASTERING && !_currentCommand) {
            // See if it's time to stand down
            string standDownReason;
            if (gracefulShutdown()) {
                // Graceful shutdown.  Set priority 1 and stand down so
                // we'll re-connect to the new master and finish up our
                // commands.
                standDownReason = "Shutting down, setting priority 1 and STANDINGDOWN.";
                _priority = 1;
            } else {
                // Loop across peers
                SFOREACH (list<Peer*>, peerList, peerIt) {
                    // Check this peer
                    Peer* peer = *peerIt;
                    if (SIEquals((*peer)["State"], "MASTERING")) {
                        // Hm... somehow we're in a multi-master scenario -- not
                        // good.  Let's get out of this as soon as possible.
                        standDownReason = "Found another MASTER (" + peer->name + "), STANDINGDOWN to clean it up.";
                    } else if (SIEquals((*peer)["State"], "WAITING")) {
                        // We have a WAITING peer; is it waiting to STANDUP?
                        if (peer->calc("Priority") > _priority) {
                            // We've got a higher priority peer in the works; stand
                            // down so it can stand up.
                            standDownReason =
                                "Found higher priority WAITING peer (" + peer->name + ") while MASTERING, STANDINGDOWN";
                        } else if (peer->calcU64("CommitCount") > _db.getCommitCount()) {
                            // It's got data that we don't, stand down so we can get it.
                            standDownReason = "Found WAITING peer (" + peer->name +
                                              ") with more data than us (we have " + SToStr(_db.getCommitCount()) +
                                              "/" + _db.getCommittedHash() + ", it has " + (*peer)["CommitCount"] +
                                              "/" + (*peer)["Hash"] + ") while MASTERING, STANDINGDOWN";
                        }
                    }
                }
            }

            // Do we want to stand down, and can we?
            if (!standDownReason.empty()) {
                // Do it
                SHMMM(standDownReason);
                _changeState(SQLC_STANDINGDOWN);
                return true; // Re-update
            }

            // Not standing down -- do we have any commands to start?  Only
            // dequeue if we either have no peers configured (meaning
            // remote commits are non-mandatory), or if at least half of
            // the peers are connected.  Otherwise we're in a live
            // environment but can't commit anything because we may cause a
            // split brain scenario.
            if (!_isQueuedCommandMapEmpty() && _majoritySubscribed()) {
                // Have commands and a majority, so let's start a new one.
                SFOREACHMAPREVERSE(int, list<Command*>, _queuedCommandMap, it) {
                    // **XXX: Using pointer to list because STL containers copy on assignment.
                    list<Command*>* commandList = &it->second;
                    if (!commandList->empty()) {
                        // Find the first command that either has no httpsRequest,
                        // or has a completed one.
                        int64_t now = STimeNow();
                        list<Command*>::iterator nextIt = commandList->begin();
                        while (nextIt != commandList->end()) {
                            // See if this command has an outstanding https
                            // transaction.  If so, wait for it to complete.
                            list<Command*>::iterator commandIt = nextIt++;
                            Command* command = *commandIt;
                            if (command->httpsRequest && !command->httpsRequest->response)
                                continue;
                            SAUTOPREFIX(command->request["requestID"]);

                            // See if this command has a "Hold" on it.  If
                            // so, just skip it until whatever put the hold
                            // on clears it.
                            if (!command->request["HeldBy"].empty()) {
                                // It's being held -- have we exceeded the timeout?
                                uint64_t elapsed = STimeNow() - command->creationTimestamp;
                                if (command->request.isSet("Timeout") &&
                                    elapsed > command->request.calc64("Timeout") * STIME_US_PER_MS) {
                                    // Command timed out, return the result
                                    SINFO("Command '" << command->id << "' timed out after "
                                                      << elapsed / STIME_US_PER_MS << "ms ("
                                                      << command->request["Timeout"]
                                                      << "ms configured): " << command->request.methodLine);
                                    command->response = SData("303 Timeout");
                                    commandList->erase(commandIt);
                                    _finishCommand(command);
                                    continue;
                                } else {
                                    // Not timed out; just skip
                                    continue;
                                }
                            }

                            // Make sure the command isn't scheduled for the future.
                            // **NOTE: We break because all commands after this one
                            //         will be for the future too.  Clearly, this
                            //         is a dicey optimization givent that things
                            //         could break catastrophically if a command
                            //         scheduled for the distant future gets jammed
                            //         accidentally in the front of the queue.
                            if ((int64_t)command->creationTimestamp > now)
                                break;

                            // Did we already peek this command?  If it has an
                            // httpsRequest, then we know it's already been peeked.
                            if (!command->httpsRequest) {
                                // We don't know if it's peen peeked; Peek this
                                // command if we haven't already.  (ESCALATED
                                // commands aren't peeked until now.  Local
                                // commands will have already been peeked, but it's
                                // non-damaging to do it again.)
                                if (_peekCommandWrapper(_db, command)) {
                                    // Done -- respond immediately to this.  This
                                    // should only happen in really rare cases
                                    // because most "pure peekable" commands (eg,
                                    // purely read-only) will be handled
                                    // immediately when queued and never get here
                                    // However, this can happen when a command is
                                    // queued while the node is in a transitional
                                    // state (eg, while SEARCHING), in which it
                                    // won't get peeked at all until now.  This can
                                    // also happen if something changes or expires,
                                    // between the first peek and this one.
                                    SINFO("Finished processing peekable command '" << command->request.methodLine
                                                                                   << "' (" << command->id
                                                                                   << "), nothing to commit.");
                                    SASSERT(!_db.insideTransaction());
                                    commandList->erase(commandIt);
                                    _finishCommand(command);
                                    continue;
                                }

                                // Did the peek place a hold on this command?
                                // If so, put it back in the list and try a
                                // new one.
                                if (!command->request["HeldBy"].empty()) {
                                    // Skipping it for later
                                    SINFO("Hold re-placed on command by '" << command->request["HeldBy"]
                                                                           << "', skipping.");
                                    continue;
                                }

                                // Peek complete; now let's see if it's started a
                                // secondary command.  If so, just go on to the
                                // next command while we wait for this one to
                                // complete.  (This is a duplicate of the above
                                // line but is still needed.)
                                if (command->httpsRequest && !command->httpsRequest->response)
                                    continue;
                            }

                            // Process this transactional command
                            _currentCommand = command;
                            commandList->erase(commandIt);
                            SASSERTWARN(_currentCommand->transaction.empty());
                            SINFO("Starting processing command '" << _currentCommand->request.methodLine << "' ("
                                                                  << _currentCommand->id << ")");
                            _processCommandWrapper(_db, _currentCommand);
                            SASSERT(!_currentCommand->response.empty()); // Must set a response

                            // Anything to commit?
                            if (_db.insideTransaction()) {
                                // Begin the distributed transaction
                                SASSERT(!_db.getUncommittedQuery().empty());
                                SINFO("Finished processing command '"
                                      << _currentCommand->request.methodLine << "' (" << _currentCommand->id
                                      << "), beginning distributed transaction for commit #" << _db.getCommitCount() + 1
                                      << " (" << _db.getUncommittedHash() << ")");
                                _currentCommand->replicationStartTimestamp = STimeNow();
                                _currentCommand->transaction.methodLine = "BEGIN_TRANSACTION";
                                _currentCommand->transaction["Command"] = _currentCommand->request.methodLine;
                                _currentCommand->transaction["NewCount"] = SToStr(_db.getCommitCount() + 1);
                                _currentCommand->transaction["NewHash"] = _db.getUncommittedHash();
                                _currentCommand->transaction["ID"] = _currentCommand->id;
                                _currentCommand->transaction.content = _db.getUncommittedQuery();
                                _sendToAllPeers(_currentCommand->transaction, true); // subscribed only
                                SFOREACH (list<Peer*>, peerList, peerIt) {
                                    // Clear the response flag from the last transaction
                                    Peer* peer = *peerIt;
                                    (*peer)["TransactionResponse"].clear();
                                }

                                // By returning 'true', we update the FSM immediately, and thus evaluate whether or not
                                // we need to wait for quorum.  This keeps all the quorum logic in the same place.
                                return true;
                            } else {
                                // Doesn't need to commit anything; done processing.
                                SINFO("Finished processing command '" << _currentCommand->request.methodLine << "' ("
                                                                      << _currentCommand->id
                                                                      << "), nothing to commit.");
                                SASSERT(!_db.insideTransaction());
                                _finishCommand(_currentCommand);
                                _currentCommand = 0;
                            }

                            // **NOTE: This loops back and starts the next command of the same priority immediately
                        }
                    }

                    // **NOTE: This loops back and starts the next command of the next lower priority immediately
                }

                // **NOTE: we've exhausted the current batch of queued commands and can continue
            }
        }

        // We're standing down; wait until there are no more subscribed peers
        if (_state == SQLC_STANDINGDOWN) {
            // Loop across and search for subscribed peers
            bool allUnsubscribed = true;
            SFOREACH (list<Peer*>, peerList, peerIt) {
                // See if this peer is still subscribed
                Peer* peer = *peerIt;
                if (SIEquals((*peer)["Subscribed"], "true")) {
                    // Found one; keep waiting
                    allUnsubscribed = false;
                    break;
                }
            }

            // See if we're done
            // **FIXME: Add timeout?
            if (allUnsubscribed) {
                // Standdown complete
                SINFO("STANDDOWN complete, SEARCHING");
                SASSERTWARN(!_currentCommand);
                _changeState(SQLC_SEARCHING);
                return true; // Re-update
            }
        }
        break;
    }

    /// - SUBSCRIBING: We're waiting for a SUBSCRIPTION_APPROVED from the
    ///     master.  When we receive it, we'll go SLAVING.  Otherwise, if we
    ///     timeout, go SEARCHING.
    ///
    case SQLC_SUBSCRIBING:
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Nothing to do but wait
        if (STimeNow() > _stateTimeout) {
            // Give up
            SHMMM("Timed out waiting for SUBSCRIPTION_APPROVED, reconnecting to master and re-SEARCHING.");
            _reconnectPeer(_masterPeer);
            _masterPeer = 0;
            _changeState(SQLC_SEARCHING);
            return true; // Re-update
        }
        break;

    /// - SLAVING: This is where the other half of the magic happens.  Most
    ///     nodes will (hopefully) spend 99.999% of their time in this state.
    ///     SLAVING nodes simply begin and commit transactions with the
    ///     following logic:
    ///
    ///         if( master steps down or disconnects ) goto SEARCHING
    ///         if( new queued commands ) send ESCALATE to master
    ///
    case SQLC_SLAVING:
        SASSERTWARN(!_syncPeer);
        SASSERT(_masterPeer);
        // If graceful shutdown requested, stop slaving once there is
        // nothing blocking shutdown.  We stop listening for new commands
        // immediately upon TERM.)
        if (gracefulShutdown() && _isNothingBlockingShutdown()) {
            // Go searching so we stop slaving
            SINFO("Stopping SLAVING in order to gracefully shut down, SEARCHING.");
            _changeState(SQLC_SEARCHING);
            return false; // Don't update
        }

        // If the master stands down, stop slaving
        // **FIXME: Wait for all commands to finish
        if (!SIEquals((*_masterPeer)["State"], "MASTERING")) {
            // Master stepping down
            SHMMM("Master stepping down, re-queueing commands and re-SEARCHING.");

            // Get an ordered list of commands
            list<Command*> escalatedCommandList = _getOrderedCommandListFromMap(_escalatedCommandMap);
            while (!escalatedCommandList.empty()) {
                // Reschedule the escalated commands to the front of the pack.
                Command* command = escalatedCommandList.back();
                SHMMM("Re-queueing escalated command '" << command->request.methodLine << "' (" << command->id << ")");
                _queuedCommandMap[command->priority].push_front(command);
                escalatedCommandList.pop_back();
            }
            _escalatedCommandMap.clear();
            _changeState(SQLC_SEARCHING);
            return true; // Re-update
        }

        // If there are any commands, send to the master
        SFOREACHMAPREVERSE(int, list<Command*>, _queuedCommandMap, it) {
            // **XXX: Using a pointer to list because STL copies by value
            list<Command*>* commandList = &it->second;
            while (!commandList->empty()) {
                // Just send on to the master.
                Command* command = commandList->front();
                if (command->httpsRequest) {
                    // If a MASTER has oustanding commands with active httpsRequest
                    // when it STANDSDOWN (such as as part of a graceful shutdown)
                    // then it will escalate commands that have already been peeked
                    // and have oustanding httpsRequests.  This isn't *too* bad, as
                    // the new MASTER will just re-peek and re-issue the secondary
                    // request.  So we'll just abort ours and hope for the best.
                    // but this could be a problem if the secondary command itself
                    // performs some action.  The worst case we can think of is we
                    // might send multiple AUTHs, but even that's not so bad.
                    SHMMM("Escalated command '"
                          << command->request.methodLine << "' (" << command->id
                          << ") has secondary request; cancelling and new MASTER will re-submit.");
                    _cleanCommand(command);
                }
                commandList->pop_front();
                _escalateCommand(command);
            }
        }
        break;

    default:
        SERROR("Invalid state #" << _state);
    }

    // Don't update immediately
    return false;
}

// --------------------------------------------------------------------------
/// Messages
/// --------
/// Here are the messages that can be received, and how a cluster node will
/// respond to each based on its state:
///
void SQLiteNode::_onMESSAGE(Peer* peer, const SData& message) {
    SASSERT(peer);
    SASSERTWARN(!message.empty());
    // Every message broadcasts the current state of the node
    if (!message.isSet("CommitCount"))
        throw "missing CommitCount";
    if (!message.isSet("Hash"))
        throw "missing Hash";
    (*peer)["CommitCount"] = message["CommitCount"];
    (*peer)["Hash"] = message["Hash"];

    // Classify and process the message
    if (SIEquals(message.methodLine, "LOGIN")) {
        /// - LOGIN: This is the first message sent to and recieved from a new
        ///     peer.  It communicates the current state of the peer (hash
        ///     and commit count), as well as the peer's priority.  Peers
        ///     can connect in any state, so this message can be sent and
        ///     received in any state.
        ///
        if ((*peer)["LoggedIn"] == "true")
            throw "already logged in";
        if (!message.isSet("Priority"))
            throw "missing Priority";
        if (!message.isSet("State"))
            throw "missing State";
        if (!message.isSet("Version"))
            throw "missing Version";
        if (peer->params["Permaslave"] == "true" && message.calc("Priority"))
            throw "you're supposed to be a 0-priority permaslave";
        if (peer->params["Permaslave"] != "true" && !message.calc("Priority"))
            throw "you're *not* supposed to be a 0-priority permaslave";
        SASSERT(!_priority ||
                message.calc("Priority") != _priority); // to prevent misconfig: only one at each priority, except 0
        PINFO("Peer logged in at '" << message["State"] << "', priority #" << message["Priority"] << " commit #"
                                    << message["CommitCount"] << " (" << message["Hash"] << ")");
        (*peer)["Priority"] = message["Priority"];
        (*peer)["State"] = message["State"];
        (*peer)["LoggedIn"] = "true";
        (*peer)["Version"] = message["Version"];
    } else if (!SIEquals((*peer)["LoggedIn"], "true"))
        throw "not logged in";
    else if (SIEquals(message.methodLine, "STATE")) {
        /// - STATE: Broadcast to all peers whenever a node's FSM state changes.
        ///     Also sent whenever a node commits a new query (and thus has
        ///     a new commit count and hash).  A peer can react or respond
        ///     to a peer's state change as follows:
        ///
        if (!message.isSet("State"))
            throw "missing State";
        if (!message.isSet("Priority"))
            throw "missing Priority";
        string oldState = (*peer)["State"];
        (*peer)["State"] = message["State"];
        (*peer)["Priority"] = message["Priority"];
        const string& newState = (*peer)["State"];
        if (oldState == newState) {
            // No state change, just new commits?
            PINFO("Peer received new commit in state '" << oldState << "', commit #" << message["CommitCount"] << " ("
                                                        << message["Hash"] << ")");
        } else {
            // State changed -- first see if it's doing anything unusual
            PINFO("Peer switched from '" << oldState << "' to '" << newState << "' commit #" << message["CommitCount"]
                                         << " (" << message["Hash"] << ")");
            int from = 0, to = 0;
            for (from = SQLC_SEARCHING; from <= SQLC_SLAVING; from++)
                if (SIEquals(oldState, SQLCStateNames[from]))
                    break;
            for (to = SQLC_SEARCHING; to <= SQLC_SLAVING; to++)
                if (SIEquals(newState, SQLCStateNames[to]))
                    break;
            if (from > SQLC_SLAVING)
                PWARN("Peer coming from unrecognized state '" << oldState << "'");
            if (to > SQLC_SLAVING)
                PWARN("Peer going to unrecognized state '" << newState << "'");
            bool okTransition = false;
            switch (from) {
            case SQLC_SEARCHING:
                okTransition = (to == SQLC_SYNCHRONIZING || to == SQLC_WAITING || to == SQLC_MASTERING);
                break;
            case SQLC_SYNCHRONIZING:
                okTransition = (to == SQLC_SEARCHING || to == SQLC_WAITING);
                break;
            case SQLC_WAITING:
                okTransition = (to == SQLC_SEARCHING || to == SQLC_STANDINGUP || to == SQLC_SUBSCRIBING);
                break;
            case SQLC_STANDINGUP:
                okTransition = (to == SQLC_SEARCHING || to == SQLC_MASTERING);
                break;
            case SQLC_MASTERING:
                okTransition = (to == SQLC_SEARCHING || to == SQLC_STANDINGDOWN);
                break;
            case SQLC_STANDINGDOWN:
                okTransition = (to == SQLC_SEARCHING);
                break;
            case SQLC_SUBSCRIBING:
                okTransition = (to == SQLC_SEARCHING || to == SQLC_SLAVING);
                break;
            case SQLC_SLAVING:
                okTransition = (to == SQLC_SEARCHING);
                break;
            }
            if (!okTransition)
                PWARN("Peer making invalid transition from '" << SQLCStateNames[from] << "' to '" << SQLCStateNames[to]
                                                              << "'");

            // Next, should we do something about it?
            if (to == SQLC_SEARCHING) {
                ///     * SEARCHING: If anything ever goes wrong, a node
                ///         reverts to the SEARCHING state.  Thus if
                ///         we see a peer go SEARCHING, we reset its
                ///         accumulated state.  Specifically, we
                ///         mark it is no longer being "subscribed",
                ///         and we clear its last transaction
                ///         response.
                ///
                peer->erase("TransactionResponse");
                peer->erase("Subscribed");
            } else if (to == SQLC_STANDINGUP) {
                ///     * STANDINGUP: When a peer announces it intends to stand
                ///         up, we immediately respond with approval or denial.
                ///         We determine this by checking to see if there is any
                ///         other peer who is already master or also trying to
                ///         stand up.
                ///
                ///         * **FIXME**: Should it also deny if it knows of a
                ///             higher priority peer?
                ///
                SData response("STANDUP_RESPONSE");
                if (peer->params["Permaslave"] == "true") {
                    // We think it's a permaslave, deny
                    PHMMM("Permaslave trying to stand up, denying.");
                    response["Response"] = "deny";
                    response["Reason"] = "You're a permaslave";
                }

                // What's our state
                if (SWITHIN(SQLC_STANDINGUP, _state, SQLC_STANDINGDOWN)) {
                    // Oh crap, it's trying to stand up while we're mastering.
                    // Who is higher priority?
                    if (peer->calc("Priority") > _priority) {
                        // Not good -- we're in the way.  Not sure how we got
                        // here, so just reconnect and start over.
                        PWARN("Higher-priority peer is trying to stand up while we are "
                              << SQLCStateNames[_state] << ", reconnecting and SEARCHING.");
                        _reconnectAll();
                        _changeState(SQLC_SEARCHING);
                    } else {
                        // Deny because we're currently in the process of mastering
                        // and we're higher priority
                        response["Response"] = "deny";
                        response["Reason"] = "I am mastering";

                        // Hmm, why is a lower priority peer trying to stand up?  Is it possble we're not longer in
                        // control
                        // of the cluster?  Let's see how many nodes are subscribed.
                        if (_majoritySubscribed()) {
                            // we have a majority of the cluster, so ignore this oddity.
                            PHMMM("Lower-priority peer is trying to stand up while we are "
                                  << SQLCStateNames[_state]
                                  << " with a majority of the cluster; denying and ignoring.");
                        } else {
                            // We don't have a majority of the cluster -- maybe
                            // it knows something we don't?  For example, it
                            // could be that the rest of the cluster has forked
                            // away from us.  This can happen if the master
                            // hangs while processing a command: by the time it
                            // finishes, the cluster might have elected a new
                            // master, forked, and be a thosuand commits in the
                            // future.  In this case, let's just reset
                            // everything anyway to be safe.
                            PWARN("Lower-priority peer is trying to stand up while we are "
                                  << SQLCStateNames[_state]
                                  << ", but we don't have a majority of the cluster so reconnecting and SEARCHING.");
                            _reconnectAll();
                            _changeState(SQLC_SEARCHING);
                        }
                    }
                } else {
                    // Approve if nobody else is trying to stand up
                    response["Response"] = "approve"; // Optimistic; will override
                    SFOREACH (list<Peer*>, peerList, otherPeerIt)
                        if (*otherPeerIt != peer) {
                            // See if it's trying to be master
                            Peer* otherPeer = *otherPeerIt;
                            const string& state = (*otherPeer)["State"];
                            if (SIEquals(state, "STANDINGUP") || SIEquals(state, "MASTERING") ||
                                SIEquals(state, "STANDINGDOWN")) {
                                // We need to contest this standup
                                response["Response"] = "deny";
                                response["Reason"] = "peer '" + otherPeer->name + "' is '" + state + "'";
                                break;
                            }
                        }
                }

                // Send the response
                if (SIEquals(response["Response"], "approve"))
                    PINFO("Approving standup request");
                else
                    PHMMM("Denying standup request because " << response["Reason"]);
                _sendToPeer(peer, response);
            } else if (from == SQLC_STANDINGDOWN) {
                ///     * STANDINGDOWN: When a peer stands down we double-check
                ///         to make sure we don't have any outstanding transaction
                ///         (and if we do, we warn and rollback).
                ///
                if (!_db.getUncommittedHash().empty()) {
                    // Crap, we were waiting for a response that will apparently
                    // never come.  I guess roll it back?  This should never
                    // happen, however, as the master shouldn't STANDOWN unless
                    // all subscribed slaves (including us) have already
                    // unsubscribed, and we wouldn't do that in the middle of a
                    // transaction.  But just in case...
                    SASSERTWARN(_state == SQLC_SLAVING);
                    PWARN("Was expecting a response for transaction #"
                          << _db.getCommitCount() + 1 << " (" << _db.getUncommittedHash()
                          << ") but stood down prematurely, rolling back and hoping for the best.");
                    _db.rollback();
                }
            }
        }
    } else if (SIEquals(message.methodLine, "STANDUP_RESPONSE")) {
        /// - STANDUP_RESPONSE: Sent in response to the STATE message generated
        ///     when a node enters the STANDINGUP state.  Contains a header
        ///     "Response" with either the value "approve" or "deny".  This
        ///     response is stored within the peer for testing in the update loop.
        ///
        if (_state != SQLC_STANDINGUP)
            throw "not standing up";
        if (!message.isSet("Response"))
            throw "missing Response";
        if (peer->isSet("StandupResponse"))
            PWARN("Already received standup response '" << (*peer)["StandupResponse"] << "', now receiving '"
                                                        << message["Response"]
                                                        << "', odd -- multiple masters competing?");
        if (SIEquals(message["Response"], "approve"))
            PINFO("Received standup approval");
        else
            PHMMM("Received standup denial: reason='" << message["Reason"] << "'");
        (*peer)["StandupResponse"] = message["Response"];
    } else if (SIEquals(message.methodLine, "SYNCHRONIZE")) {
        /// - SYNCHRONIZE: Sent by a node in the SEARCHING state to a peer that
        ///     has new commits.  Respond with a SYNCHRONIZE_RESPONSE containing
        ///     all COMMITs the requesting peer lacks.
        ///
        SData response("SYNCHRONIZE_RESPONSE");
        _queueSynchronize(peer, response, false);
        _sendToPeer(peer, response);
    } else if (SIEquals(message.methodLine, "SYNCHRONIZE_RESPONSE")) {
        /// - SYNCHRONIZE_RESPONSE: Sent in response to a SYNCHRONIZE request.
        ///     Contains a payload of zero or more COMMIT messages, all of which
        ///     are immediately committed to the local database.
        ///
        if (_state != SQLC_SYNCHRONIZING)
            throw "not synchronizing";
        if (!_syncPeer)
            throw "too late, gave up on you";
        if (peer != _syncPeer)
            throw "sync peer mismatch";
        PINFO("Beginning synchronization");
        try {
            // Received this synchronization response; are we done?
            _recvSynchronize(peer, message);
            uint64_t peerCommitCount = _syncPeer->calcU64("CommitCount");
            if (_db.getCommitCount() == peerCommitCount) {
                // All done
                SINFO("Synchronization complete, at commitCount #" << _db.getCommitCount() << " ("
                                                                   << _db.getCommittedHash() << "), WAITING");
                _syncPeer = 0;
                _changeState(SQLC_WAITING);
            } else if (_db.getCommitCount() > peerCommitCount) {
                // How did this happen?  Something is screwed up.
                SWARN("We have more data (" << _db.getCommitCount() << ") than our sync peer '" << _syncPeer->name
                                            << "' (" << peerCommitCount << "), reconnecting and SEARCHING.");
                _reconnectPeer(_syncPeer);
                _syncPeer = 0;
                _changeState(SQLC_SEARCHING);
            } else {
                // Otherwise, more to go
                SINFO("Synchronization underway, at commitCount #"
                      << _db.getCommitCount() << " (" << _db.getCommittedHash() << "), "
                      << peerCommitCount - _db.getCommitCount() << " to go.");
                _sendToPeer(_syncPeer, SData("SYNCHRONIZE"));

                // Also, extend our timeout so long as we're still alive
                _stateTimeout = STimeNow() + SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_M * 5;
            }
        } catch (const char* e) {
            // Transaction failed
            SWARN("Synchronization failed '" << e << "', reconnecting and re-SEARCHING.");
            _reconnectPeer(_syncPeer);
            _syncPeer = 0;
            _changeState(SQLC_SEARCHING);
            throw e;
        }
    } else if (SIEquals(message.methodLine, "SUBSCRIBE")) {
        /// - SUBSCRIBE: Sent by a node in the WAITING state to the current
        ///     master to begin SLAVING.  Respond SUBSCRIPTION_APPROVED with any
        ///     COMMITs that the subscribing peer lacks (for example, any commits
        ///     that have occured after it completed SYNCHRONIZING but before this
        ///     SUBSCRIBE was received).  Tag this peer as "subscribed" for use
        ///     in the MASTERING and STANDINGDOWN update loops.  Finally, if there
        ///     is an outstanding distributed transaction being processed, send it
        ///     to this new slave.
        ///
        if (_state != SQLC_MASTERING)
            throw "not mastering";
        PINFO("Received SUBSCRIBE, accepting new slave");
        SData response("SUBSCRIPTION_APPROVED");
        _queueSynchronize(peer, response, true); // Send everything it's missing
        _sendToPeer(peer, response);
        SASSERTWARN(!SIEquals((*peer)["Subscribed"], "true"));
        (*peer)["Subscribed"] = "true";

        // New slave; are we in the midst of a transaction?
        if (_currentCommand) {
            // Invite the new peer to participate in the transaction
            SINFO("Inviting peer into distributed transaction already underway '"
                  << _currentCommand->request.methodLine << "' #" << _db.getCommitCount() + 1 << " ("
                  << _db.getUncommittedHash() << ")");
            _sendToPeer(peer, _currentCommand->transaction);
        }
    } else if (SIEquals(message.methodLine, "SUBSCRIPTION_APPROVED")) {
        /// - SUBSCRIPTION_APPROVED: Sent by a slave's new master to complete the
        ///     subscription process.  Includes zero or more COMMITS that should be
        ///     immediately applied to the database.
        ///
        if (_state != SQLC_SUBSCRIBING)
            throw "not subscribing";
        if (_masterPeer != peer)
            throw "not subscribing to you";
        SINFO("Received SUBSCRIPTION_APPROVED, final synchronization.");
        try {
            // Done synchronizing
            _recvSynchronize(peer, message);
            if (_db.getCommitCount() != _masterPeer->calcU64("CommitCount"))
                throw "Incomplete synchronizationg";
            SINFO("Subscription complete, at commitCount #" << _db.getCommitCount() << " (" << _db.getCommittedHash()
                                                            << "), SLAVING");
            _changeState(SQLC_SLAVING);
        } catch (const char* e) {
            // Transaction failed
            SWARN("Subscription failed '" << e << "', reconnecting to master and re-SEARCHING.");
            _reconnectPeer(_masterPeer);
            _changeState(SQLC_SEARCHING);
            throw e;
        }
    } else if (SIEquals(message.methodLine, "BEGIN_TRANSACTION")) {
        /// - BEGIN_TRANSACTION: Sent by the MASTER to all subscribed slaves to
        ///     begin a new distributed transaction.  Each slave begins a local
        ///     transaction with this query and responds APPROVE_TRANSACTION.
        ///     If the slave cannot start the transaction for any reason, it
        ///     is broken somehow -- disconnect from the master.
        ///
        ///     * **FIXME**: What happens if MASTER steps down before sending BEGIN?
        ///     * **FIXME**: What happens if MASTER steps down or disconnects after BEGIN?
        ///
        if (!message.isSet("ID"))
            throw "missing ID";
        if (!message.isSet("NewCount"))
            throw "missing NewCount";
        if (!message.isSet("NewHash"))
            throw "missing NewHash";
        if (_state != SQLC_SLAVING)
            throw "not slaving";
        if (!_masterPeer)
            throw "no master?";
        if (!_db.getUncommittedHash().empty())
            throw "already in a transaction";
        if (_db.getCommitCount() + 1 != message.calcU64("NewCount"))
            throw "commit count mismatch";
        if (!_db.beginTransaction())
            throw "failed to begin transaction";
        try {
            // Inside transaction; get ready to back out on error
            if (!_db.write(message.content))
                throw "failed to write transaction";
            if (!_db.prepare())
                throw "failed to prepare transaction";
        } catch (const char* e) {
            // Transaction failed, clean up
            SERROR("Can't begin master transaction (" << e << "); shutting down.");
            // **FIXME: Remove the above line once we can automatically handle?
            _db.rollback();
            throw e;
        }

        // Successful commit; we in the right state?
        _currentTransactionCommand = message["Command"];
        if (_db.getUncommittedHash() != message["NewHash"]) {
            // Something is screwed up
            PWARN("New hash mismatch: command='"
                  << _currentTransactionCommand << "', commitCount=#" << _db.getCommitCount() << "', committedHash='"
                  << _db.getCommittedHash() << "', uncommittedHash='" << _db.getUncommittedHash()
                  << "', uncommittedQuery='" << _db.getUncommittedQuery() << "'");
            throw "new hash mismatch";
        }

        // Are we participating in quorum?
        if (_priority) {
            // Not a permaslave, approve the transaction
            PINFO("Approving transaction #" << _db.getCommitCount() + 1 << " (" << _db.getUncommittedHash()
                                            << ") for command '" << _currentTransactionCommand << "'");
            SData approve("APPROVE_TRANSACTION");
            approve["NewCount"] = SToStr(_db.getCommitCount() + 1);
            approve["NewHash"] = _db.getUncommittedHash();
            approve["ID"] = message["ID"];
            _sendToPeer(_masterPeer, approve);
        } else {
            PINFO("Would approve transaction #" << _db.getCommitCount() + 1 << " (" << _db.getUncommittedHash()
                                                << ") for command '" << _currentTransactionCommand
                                                << "', but a permaslave -- keeping quiet.");
        }

        // Check our escalated commands and see if it's one being processed
        CommandMapIt commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this
            // so we know that this command might be corrupted if the master
            // crashes.
            SINFO("Master is processing our command " << message["ID"] << " (" << _currentTransactionCommand << ")");
            commandIt->second->transaction = message;
        }
    } else if (SIEquals(message.methodLine, "APPROVE_TRANSACTION")) {
        /// - APPROVE_TRANSACTION: Sent to the master by a slave when it confirms
        ///     it was able to begin a transaction and is ready to commit.  Note
        ///     that this peer approves the transaction for use in the MASTERING
        ///     and STANDINGDOWN update loop.
        ///
        if (!message.isSet("ID"))
            throw "missing ID";
        if (!message.isSet("NewCount"))
            throw "missing NewCount";
        if (!message.isSet("NewHash"))
            throw "missing NewHash";
        if (_state != SQLC_MASTERING && _state != SQLC_STANDINGDOWN)
            throw "not mastering";
        try {
            // Approval of current command, or an old one?
            if (_currentCommand && _currentCommand->id == message["ID"]) {
                // Current, make sure it all matches.
                if (message["NewHash"] != _db.getUncommittedHash())
                    throw "new hash mismatch";
                if (message.calcU64("NewCount") != _db.getCommitCount() + 1)
                    throw "commit count mismatch";
                if (peer->params["Permaslave"] == "true")
                    throw "permaslaves shouldn't approve";
                uint64_t replicationElapsed = STimeNow() - _currentCommand->replicationStartTimestamp;
                PINFO("Peer approved transaction #" << message["NewCount"] << " (" << message["NewHash"] << ") after "
                                                    << replicationElapsed / STIME_US_PER_MS << "ms");
                (*peer)["TransactionResponse"] = "approve";
            } else
                // Old command.  Nothing to do.  We already sent a commit or rollback.
                PINFO("Peer approved transaction #" << message["NewCount"] << " (" << message["NewHash"]
                                                    << ") after commit.");
        } catch (const char* e) {
            // Doesn't correspond to the outstanding transaction not necessarily
            // fatal.  This can happen if, for example, a command is escalated from
            // one slave, approved by the second, but where the first slave dies
            // before the second's approval is received by the master.  In this
            // case the master will drop the command when the initiating peer is
            // lost, and thus won't have an outstanding transaction (or will be
            // processing a new transaction) when the old, outdated approval is
            // received.  Furthermore, in this case we will have already sent a
            // ROLLBACK, so it will already correct itself.  If not, then we'll
            // wait for the slave to determine it's screwed and reconnect.
            SWARN("Received APPROVE_TRANSACTION for transaction #"
                  << message.calc("NewCount") << " (" << message["NewHash"] << ", " << message["ID"] << ") but '" << e
                  << "', ignoring.");
        }
    } else if (SIEquals(message.methodLine, "COMMIT_TRANSACTION")) {
        /// - COMMIT_TRANSACTION: Sent to all subscribed slaves by the master when
        ///     it determines that the current outstanding transaction should be
        ///     committed to the database.  This completes a given distributed
        ///     transaction.
        ///
        if (_state != SQLC_SLAVING)
            throw "not slaving";
        if (_db.getUncommittedHash().empty())
            throw "no outstanding transaction";
        if (message.calcU64("CommitCount") != _db.getCommitCount() + 1)
            throw "commit count mismatch";
        if (message["Hash"] != _db.getUncommittedHash())
            throw "hash mismatch";
        _db.commit();
        uint64_t beginElapsed, readElapsed, writeElapsed, prepareElapsed, commitElapsed, rollbackElapsed;
        uint64_t totalElapsed = _db.getLastTransactionTiming(beginElapsed, readElapsed, writeElapsed, prepareElapsed,
                                                             commitElapsed, rollbackElapsed);
        SINFO("Committed slave transaction #"
              << message["CommitCount"] << " (" << message["Hash"] << ") for "
                                                                      "'"
              << _currentTransactionCommand << "' in " << totalElapsed / STIME_US_PER_MS << " ms ("
              << beginElapsed / STIME_US_PER_MS << "+" << readElapsed / STIME_US_PER_MS << "+"
              << writeElapsed / STIME_US_PER_MS << "+" << prepareElapsed / STIME_US_PER_MS << "+"
              << commitElapsed / STIME_US_PER_MS << "+" << rollbackElapsed / STIME_US_PER_MS << "ms)");

        // Look up in our escalated commands and see if it's one being processed
        CommandMapIt commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this
            // so we know that this command might be corrupted if the master
            // crashes.
            SINFO("Master has committed in response to our command " << message["ID"]);
            commandIt->second->transaction = message;
        }
    } else if (SIEquals(message.methodLine, "ROLLBACK_TRANSACTION")) {
        /// - ROLLBACK_TRANSACTION: Sent to all subscribed slaves by the master when
        ///     it determines that the current outstanding transaction should be
        ///     rolled back.  This completes a given distributed transaction.
        ///
        if (!message.isSet("ID"))
            throw "missing ID";
        if (_state != SQLC_SLAVING)
            throw "not slaving";
        if (_db.getUncommittedHash().empty())
            throw "no outstanding transaction";
        SINFO("Rolling back slave transaction " << message["ID"]);
        _db.rollback();

        // Look through our escalated commands and see if it's one being processed
        CommandMapIt commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this
            // so we know that this command might be corrupted if the master
            // crashes.
            SINFO("Master has rolled back in response to our command " << message["ID"]);
            commandIt->second->transaction = message;
        }
    } else if (SIEquals(message.methodLine, "ESCALATE")) {
        /// - ESCALATE: Sent to the master by a slave.  Is processed like a normal
        ///     command, except when complete an ESCALATE_RESPONSE is sent to the
        ///     slave that initiaed the escalation.
        ///
        if (!message.isSet("ID"))
            throw "missing ID";
        if (_state != SQLC_MASTERING) {
            // Reject escalation because we're no longer mastering
            PWARN("Received ESCALATE but not MASTERING, aborting.");
            SData aborted("ESCALATE_ABORTED");
            aborted["ID"] = message["ID"];
            aborted["Reason"] = "not mastering";
            _sendToPeer(peer, aborted);
        } else {
            // We're mastering, make sure the rest checks out
            SData request;
            if (!request.deserialize(message.content))
                throw "malformed request";
            if ((*peer)["Subscribed"] != "true")
                throw "not subscribed";
            if (!message.isSet("ID"))
                throw "missing ID";
            PINFO("Received ESCALATE command for '" << message["ID"] << "' (" << request.methodLine << ")");
            Command* command = new Command;
            command->initiator = peer;
            command->id = message["ID"];
            command->request = request;
            command->priority = message.calc("priority");
            _queueCommand(command);
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_CANCEL")) {
        /// - ESCALATE_CANCEL: Sent to the master by a slave.  Indicates that the
        ///     slave would like to cancel the escalated command, such that it is
        ///     not processed.  For example, if the client that sent the original
        ///     request disconnects from the slave before an answer is returned,
        ///     there is no value (and sometimes a negative value) to the master
        ///     going ahead and completing it.
        ///
        if (!message.isSet("ID"))
            throw "missing ID";
        if (_state != SQLC_MASTERING) {
            // Reject escalation because we're no longer mastering
            PWARN("Received ESCALATE_CANCEL but not MASTERING, ignoring.");
        } else {
            // We're mastering, make sure the rest checks out
            SData request;
            if (!request.deserialize(message.content))
                throw "malformed request";
            if ((*peer)["Subscribed"] != "true")
                throw "not subscribed";
            if (!message.isSet("ID"))
                throw "missing ID";
            const string& commandID = SToLower(message["ID"]);
            PINFO("Received ESCALATE_CANCEL command for '" << commandID << "'");

            // See if there is a command with that ID
            bool foundIt = false;
            SFOREACHMAP (int, list<Command*>, _queuedCommandMap, queuedCommandIt)
                SFOREACH (list<Command*>, queuedCommandIt->second, commandIt)
                    if (SIEquals((*commandIt)->id, commandID)) {
                        // Cancel that command
                        Command* command = *commandIt;
                        SINFO("Cancelling escalated command " << command->id << " (" << command->request.methodLine
                                                              << ")");
                        closeCommand(command);
                        foundIt = true;
                        break;
                    }
            SASSERTWARN(foundIt);
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_RESPONSE")) {
        /// - ESCALATE_RESPONSE: Sent when the master processes the ESCALATE.
        ///
        if (_state != SQLC_SLAVING)
            throw "not slaving";
        if (!message.isSet("ID"))
            throw "missing ID";
        SData response;
        if (!response.deserialize(message.content))
            throw "malformed contet";

        // Go find the the escalated command
        PINFO("Received ESCALATE_RESPONSE for '" << message["ID"] << "'");
        CommandMapIt commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Process the escalated command response
            Command* command = commandIt->second;
            _escalatedCommandMap.erase(command->id);
            command->response = response;
            _finishCommand(command);
        } else
            SHMMM("Received ESCALATE_RESPONSE for unknown command ID '" << message["ID"] << "', ignoring.");
    } else if (SIEquals(message.methodLine, "ESCALATE_ABORTED")) {
        /// - ESCALATE_RESPONSE: Sent when the master aborts processing an
        ///     escalated command.  Re-submit to the new master.
        ///
        if (_state != SQLC_SLAVING)
            throw "not slaving";
        if (!message.isSet("ID"))
            throw "missing ID";
        PINFO("Received ESCALATE_ABORTED for '" << message["ID"] << "' (" << message["Reason"] << ")");

        // Look for that command
        CommandMapIt commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Re-queue this
            Command* command = commandIt->second;
            PINFO("Re-queueing command '" << message["ID"] << "' (" << command->request.methodLine << ") ("
                                          << command->id << ")");
            _queuedCommandMap[command->priority].push_front(command);
            _escalatedCommandMap.erase(commandIt);
        } else
            SWARN("Received ESCALATE_ABORTED for unescalated command " << message["ID"] << ", ignoring.");
    } else
        throw "unrecognized message";
}

// --------------------------------------------------------------------------
void SQLiteNode::_onConnect(Peer* peer) {
    SASSERT(peer);
    SASSERTWARN(!SIEquals((*peer)["LoggedIn"], "true"));
    // Send the LOGIN
    PINFO("Sending LOGIN");
    SData login("LOGIN");
    login["Priority"] = SToStr(_priority);
    login["State"] = SQLCStateNames[_state];
    login["Version"] = _version;
    _sendToPeer(peer, login);
}

// --------------------------------------------------------------------------
/// On Peer Disconnections
/// ----------------------
/// Whenever a peer disconnects, the following checks are made to verify no
/// internal consistency has been lost:  (Technically these checks need only be
/// made in certain states, but we'll check them in all states just to be sure.)
///
void SQLiteNode::_onDisconnect(Peer* peer) {
    SASSERT(peer);
    /// - Verify we don't have any important data buffered for sending to this
    ///   peer.  In particular, make sure we're not sending an ESCALATION_RESPONSE
    ///   because that means the initiating slave's command was successfully
    ///   processed, but it died before learning this.  This won't corrupt the
    ///   database per se (all nodes will still be synchronized, or will repair
    ///   themselves on reconnect), but it means that the data in the database
    ///   is out of touch with reality: we processed a command and reality doesn't
    ///   know it.  Not cool!
    ///
    if (peer->s && peer->s->sendBuffer.find("ESCALATE_RESPONSE") != string::npos)
        PWARN("Initiating slave died before receiving response to escalation: " << peer->s->sendBuffer);

    /// - Verify we didn't just lose contact with our master.  This should
    ///   only be possible if we're SUBSCRIBING or SLAVING.  If we did lose our
    ///   master, roll back any uncommitted transaction and go SEARCHING.
    ///
    if (peer == _masterPeer) {
        // We've lost our master: make sure we aren't waiting for
        // transaction response and re-SEARCH
        PHMMM("Lost our MASTER, re-SEARCHING.");
        SASSERTWARN(_state == SQLC_SUBSCRIBING || _state == SQLC_SLAVING);
        _masterPeer = 0;
        if (!_db.getUncommittedHash().empty()) {
            // We're in the middle of a transaction and waiting for it to
            // approve or deny, but we'll never get its response.  Roll it
            // back and synchronize when we reconnect.
            PHMMM("Was expecting a response for transaction #" << _db.getCommitCount() + 1 << " ("
                                                               << _db.getUncommittedHash()
                                                               << ") but disconnected prematurely; rolling back.");
            _db.rollback();
        }

        // Get an ordered list of commands
        list<Command*> escalatedCommandList = _getOrderedCommandListFromMap(_escalatedCommandMap);
        while (!escalatedCommandList.empty()) {
            // Reschedule the escalated commands to the front of the pack.
            Command* command = escalatedCommandList.back();
            if (command->transaction.methodLine.empty()) {
                // Doesn't look like the master was processing this when it
                // died, let's resubmit
                PHMMM("Re-queueing escalated command '" << command->request.methodLine << "' (" << command->id << ")");
                _queuedCommandMap[command->priority].push_front(command);
            } else {
                // The master was actively processing our command when it
                // died.  That's a bummer -- let's not resubmit else we
                // might submit it twice.
                PWARN("Aborting escalated command '" << command->request.methodLine << "' (" << command->id
                                                     << ") in transaction state '" << command->transaction.methodLine
                                                     << "'");
                _abortCommand(_db, command);
                _processedCommandList.push_back(command);
            }
            escalatedCommandList.pop_back();
        }
        _escalatedCommandMap.clear();
        _changeState(SQLC_SEARCHING);
    }

    /// - Verify we didn't just lose contact with the peer we're synchronizing
    ///   with.  This should only be possible if we're SYNCHRONIZING.  If we did
    ///   lose our sync peer, give up and go back to SEARCHING.
    ///
    if (peer == _syncPeer) {
        // Synchronization failed
        PHMMM("Lost our synchronization peer, re-SEARCHING.");
        SASSERTWARN(_state == SQLC_SYNCHRONIZING);
        _syncPeer = 0;
        _changeState(SQLC_SEARCHING);
    }

    /// - Verify no queued commands were initiated by this peer.  This should only
    ///   be possible if we're MASTERING or STANDINGDOWN.  If we did lose a
    ///   command initiator (and it should only be possible if it's also SLAVING)
    ///   then drop the command -- if the initiator is still alive it'll resubmit.
    ///
    SFOREACHMAP (int, list<Command*>, _queuedCommandMap, it) {
        // **XXX: Using pointer to list because STL containers copy on assignment.
        list<Command*>* commandList = &it->second;
        list<Command*>::iterator nextCommandIt = commandList->begin();
        while (nextCommandIt != commandList->end()) {
            // Check this command and go to the next
            list<Command*>::iterator commandIt = nextCommandIt++;
            Command* command = *commandIt;
            if (command->initiator == peer) {
                // Drop this command
                PHMMM("Queued command initiator disconnected, dropping '" << command->request.methodLine << "' ("
                                                                          << command->id << ")");
                SASSERTWARN(_state == SQLC_MASTERING || _state == SQLC_STANDINGDOWN);
                SASSERTWARN(SIEquals((*peer)["State"], "SLAVING"));
                commandList->erase(commandIt);
                delete command;
            }
        }
    }

    /// - Verify the current command we're processing wasn't initiated by this
    ///   peer.  This should only be possible if we're MASTERING or STANDINGDOWN,
    ///   and if it's SLAVING.  If this happens, drop the command (it'll resubmit)
    ///   and instruct all other subscribed slaves to roll-back the transaction.
    ///
    // **FIXME: Perhaps log if any initiator dies within a timeout of sending
    //          the response; th migh be in jeopardy of not being sent out.  Or..
    //          we'll know which had responses go out at the 56K layer -- perhaps
    //          in a crash verify it went out... Or just always verify?
    if (_currentCommand && _currentCommand->initiator == peer) {
        // Clean up this command and notify everyone to roll it back
        PHMMM("Current command initiator disconnected, aborting '" << _currentCommand->request.methodLine << "' ("
                                                                   << _currentCommand->id << ") and rolling back.");
        SASSERTWARN(_state == SQLC_MASTERING || _state == SQLC_STANDINGDOWN);
        SASSERTWARN(SIEquals((*peer)["State"], "SLAVING"));
        _abortCommand(_db, _currentCommand);
        if (_db.insideTransaction())
            _db.rollback();
        SData rollback("ROLLBACK_TRANSACTION");
        rollback["ID"] = _currentCommand->id;
        SFOREACH (list<Peer*>, peerList, peerIt)
            if ((**peerIt)["Subscribed"] == "true") {
                // Send the rollback command
                Peer* peer = *peerIt;
                SASSERT(peer->s);
                _sendToPeer(peer, rollback);
            }
        delete _currentCommand;
        _currentCommand = 0;
    }
}

// --------------------------------------------------------------------------
void SQLiteNode::_sendToPeer(Peer* peer, const SData& message) {
    SASSERT(peer);
    SASSERT(!message.empty());
    // Piggyback on whatever we're sending to add the CommitCount/Hash
    SData messageCopy = message;
    messageCopy["CommitCount"] = SToStr(_db.getCommitCount());
    messageCopy["Hash"] = _db.getCommittedHash();
    peer->s->send(messageCopy.serialize());
}

// --------------------------------------------------------------------------
void SQLiteNode::_sendToAllPeers(const SData& message, bool subscribedOnly) {
    // Piggyback on whatever we're sending to add the CommitCount/Hash, but then
    // only serialize once before broadcasting
    SData messageCopy = message;
    messageCopy["CommitCount"] = SToStr(_db.getCommitCount());
    messageCopy["Hash"] = _db.getCommittedHash();
    const string& serializedMessage = messageCopy.serialize();

    // Loop across all connected peers and send the message
    SFOREACH (list<Peer*>, peerList, peerIt) {
        // Send either to everybody, or just subscribed peers.
        Peer* peer = *peerIt;
        if (peer->s && (!subscribedOnly || SIEquals((*peer)["Subscribed"], "true"))) {
            // Send it now, without waiting for the outer event loop
            peer->s->send(serializedMessage);
        }
    }
}

// --------------------------------------------------------------------------
void SQLiteNode::_changeState(SQLCState newState) {
    // Did we actually change _state?
    SQLCState oldState = _state;
    if (newState != oldState) {
        // Depending on the state, set a timeout
        SDEBUG("Switching from '" << SQLCStateNames[_state] << "' to '" << SQLCStateNames[newState] << "'");
        uint64_t timeout = 0;
        if (newState == SQLC_SEARCHING || newState == SQLC_STANDINGUP || newState == SQLC_SUBSCRIBING)
            timeout = SQL_NODE_DEFAULT_RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_S * 5;
        else if (newState == SQLC_SYNCHRONIZING)
            timeout = SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_M * 5;
        else
            timeout = 0;
        SDEBUG("Setting state timeout of " << timeout / STIME_US_PER_MS << "ms");
        _stateTimeout = STimeNow() + timeout;

        // Additional logic for some old states
        if (SWITHIN(SQLC_MASTERING, _state, SQLC_STANDINGDOWN) &&
            !SWITHIN(SQLC_MASTERING, newState, SQLC_STANDINGDOWN)) {
            // We are no longer mastering.  Are we processing a command?
            if (_currentCommand) {
                // Abort this command
                SWARN("No longer mastering, aborting current command");
                closeCommand(_currentCommand);
                _currentCommand = 0;
            }
        }

        // Clear some state if we can
        if (newState < SQLC_SUBSCRIBING) {
            // We're no longer SUBSCRIBING or SLAVING, so we have no master
            _masterPeer = 0;
        }

        // Additional logic for some new states
        if (newState == SQLC_MASTERING) {
            // If we're switching to master, upgrade the database.
            // **NOTE: We'll detect this special command on destruction and clean it
            openCommand(SData("UpgradeDatabase"), SPRIORITY_MAX); // High priority
        } else if (newState == SQLC_STANDINGDOWN) {
            // Abort all remote initiated commands if no longer MASTERING
            SFOREACHMAP (int, list<Command*>, _queuedCommandMap, it) {
                // **XXX: Using pointer to list because STL containers copy on assignment.
                list<Command*>* commandList = &it->second;
                list<Command*>::iterator nextIt = commandList->begin();
                while (nextIt != commandList->end()) {
                    // Check this command
                    list<Command*>::iterator commandIt = nextIt++;
                    Command* command = *commandIt;
                    if (command->initiator) {
                        // No need to wait, explicitly abort
                        SINFO("STANDINGDOWN and aborting " << command->id);
                        SData aborted("ESCALATE_ABORTED");
                        aborted["ID"] = command->id;
                        aborted["Reason"] = "Standing down";
                        _sendToPeer(command->initiator, aborted);
                        commandList->erase(commandIt);
                        delete command;
                    }
                }
            }
        } else if (newState == SQLC_SEARCHING) {
            if (!_escalatedCommandMap.empty()) {
                // This isn't supposed to happen, though we've seen in logs where it can.
                // So what we'll do is try and correct the problem and log the state we're coming from to see if that
                // gives us any more useful info in the future.
                _escalatedCommandMap.clear();
                SWARN(
                    "Switching from '" << SQLCStateNames[_state] << "' to '" << SQLCStateNames[newState]
                                       << "' but _escalatedCommandMap not empty. Clearing it and hoping for the best.");
            }
        }

        // Send to everyone we're connected to, whether or not
        // we're "LoggedIn" (else we might change state after sending LOGIN,
        // but before we receive theirs, and they'll miss it).
        // Broadcast the new state
        _state = newState;
        SData state("STATE");
        state["State"] = SQLCStateNames[_state];
        state["Priority"] = SToStr(_priority);
        _sendToAllPeers(state);
    }

    // Verify some invariants with the new state
    SASSERT(!_currentCommand || (_state != SQLC_MASTERING && _state != SQLC_STANDINGDOWN));
}

// --------------------------------------------------------------------------
void SQLiteNode::_queueSynchronize(Peer* peer, SData& response, bool sendAll) {
    SASSERT(peer);
    // Peer is requesting synchronization.  First, does it have any data?
    SQResult result;
    uint64_t peerCommitCount = peer->calcU64("CommitCount");
    if (peerCommitCount > _db.getCommitCount())
        throw "you have more data than me";
    if (peerCommitCount) {
        // It has some data -- do we agree on what we share?
        string myHash, ignore;
        if (!_db.getCommit(peerCommitCount, ignore, myHash))
            throw "error getting hash";
        if (myHash != (*peer)["Hash"])
            throw "hash mismatch";
        PINFO("Latest commit hash matches our records, beginning synchronization.");
    } else
        PINFO("Peer has no commits, beginning synchronization.");

    // We agree on what we share, do we need to give it more?
    if (peerCommitCount == _db.getCommitCount()) {
        // Already synchronized; nothing to send
        PINFO("Peer is already synchronized");
        response["NumCommits"] = "0";
    } else {
        // Figure out how much to send it
        uint64_t fromIndex = peerCommitCount + 1;
        uint64_t toIndex = _db.getCommitCount();
        if (!sendAll)
            toIndex = SMin(toIndex, fromIndex + 100); // 100 transactions at a time
        if (!_db.getCommits(fromIndex, toIndex, result))
            throw "error getting commits";
        if ((uint64_t)result.size() != toIndex - fromIndex + 1)
            throw "mismatched commit count";

        // Wrap everything into one huge message
        PINFO("Synchronizing commits from " << peerCommitCount + 1 << "-" << _db.getCommitCount());
        response["NumCommits"] = SToStr(result.size());
        for (size_t c = 0; c < result.size(); ++c) {
            // Queue the result
            SASSERT(result[c].size() == 2);
            SData commit("COMMIT");
            commit["CommitIndex"] = SToStr(peerCommitCount + c + 1);
            commit["Hash"] = result[c][0];
            commit.content = result[c][1];
            response.content += commit.serialize();
        }
        SASSERTWARN(response.content.size() < 10 * 1024 * 1024); // Let's watch if it gets over 10MB
    }
}

// --------------------------------------------------------------------------
void SQLiteNode::_recvSynchronize(Peer* peer, const SData& message) {
    SASSERT(peer);
    // Walk across the content and commit in order
    if (!message.isSet("NumCommits"))
        throw "missing NumCommits";
    int commitsRemaining = message.calc("NumCommits");
    SData commit;
    const char* content = message.content.c_str();
    int messageSize = 0;
    int remaining = (int)message.content.size();
    while ((messageSize = commit.deserialize(content, remaining))) {
        // Consume this message and process
        // **FIXME: This could be optimized to commit in one huge transaction
        content += messageSize;
        remaining -= messageSize;
        if (!SIEquals(commit.methodLine, "COMMIT"))
            throw "expecting COMMIT";
        if (!commit.isSet("CommitIndex"))
            throw "missing CommitIndex";
        if (commit.calc64("CommitIndex") < 0)
            throw "invalid CommitIndex";
        if (!commit.isSet("Hash"))
            throw "missing Hash";
        if (commit.content.empty())
            throw "missing content";
        if (commit.calcU64("CommitIndex") != _db.getCommitCount() + 1)
            throw "commit index mismatch";
        if (!_db.beginTransaction())
            throw "failed to begin transaction";
        try {
            // Inside a transaction; get ready to back out if an error
            if (!_db.write(commit.content))
                throw "failed to write transaction";
            if (!_db.prepare())
                throw "failed to prepare transaction";
        } catch (const char* e) {
            // Transaction failed, clean up
            SERROR("Can't synchronize (" << e << "); shutting down.");
            // **FIXME: Remove the above line once we can automatically handle?
            _db.rollback();
            throw e;
        }

        // Transaction succeeded, commit and go to the next
        _db.commit();
        if (_db.getCommittedHash() != commit["Hash"])
            throw "potential hash mismatch";
        --commitsRemaining;
    }

    // Did we get all our commits?
    if (commitsRemaining)
        throw "commits remaining at end";
}

// --------------------------------------------------------------------------
void SQLiteNode::_reconnectPeer(Peer* peer) {
    // If we're connected, just kill the conection
    if (peer->s) {
        // Reset
        SWARN("Reconnecting to '" << peer->name << "'");
        shutdownSocket(peer->s);
        (*peer)["LoggedIn"] = "false";
    }
}

// --------------------------------------------------------------------------
void SQLiteNode::_reconnectAll() {
    // Loop across and reconnect
    SFOREACH (list<Peer*>, peerList, peerIt)
        _reconnectPeer(*peerIt);
}

// --------------------------------------------------------------------------
list<SQLiteNode::Command*> SQLiteNode::_getOrderedCommandListFromMap(const map<string, Command*> commandMap) {
    list<Command*> commandList;
    SFOREACHMAPCONST(string, Command*, commandMap, cmdMapIt)
    commandList.push_back(cmdMapIt->second);
    commandList.sort(Command_ptr_cmp());
    return commandList;
}

// --------------------------------------------------------------------------
bool SQLiteNode::_isQueuedCommandMapEmpty() {
    SFOREACHMAPCONST(int, list<Command*>, _queuedCommandMap, it)
    if (!it->second.empty())
        return false;
    return true;
}

// --------------------------------------------------------------------------
bool SQLiteNode::_majoritySubscribed(int& numFullPeersOut, int& numFullSlavesOut) {
    // Count up how may full and subscribed peers we have.  (A "full" peer is
    // one that *isn't* a permaslave.)
    int numFullPeers = 0;
    int numFullSlaves = 0;
    SFOREACH (list<Peer*>, peerList, peerIt)
        if ((*peerIt)->params["Permaslave"] != "true") {
            ++numFullPeers;
            if ((*peerIt)->test("Subscribed"))
                numFullSlaves++;
        }

    // Output the subscription status to the caller (using a copy in case the
    // caller is passing in the same variable and ignoring it)
    numFullPeersOut = numFullPeers;
    numFullSlavesOut = numFullSlaves;

    // Done!
    return (numFullSlaves * 2 >= numFullPeers);
}

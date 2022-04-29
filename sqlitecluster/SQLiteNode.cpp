#include "SQLiteNode.h"

#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SRandom.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLiteCommand.h>
#include <sqlitecluster/SQLitePeer.h>
#include <sqlitecluster/SQLiteServer.h>

// Convenience class for maintaining connections with a mesh of peers
#define PDEBUG(_MSG_) SDEBUG("->{" << peer->name << "} " << _MSG_)
#define PINFO(_MSG_) SINFO("->{" << peer->name << "} " << _MSG_)
#define PHMMM(_MSG_) SHMMM("->{" << peer->name << "} " << _MSG_)
#define PWARN(_MSG_) SWARN("->{" << peer->name << "} " << _MSG_)

// Introduction
// ------------
// SQLiteNode builds atop SQLite to provide a distributed transactional SQL database. It establishes and maintains
// connections with all peers: if any connection fails, it forever attempts to re-establish.
//
// FIXME: Handle the case where two nodes have conflicting databases. Should find where they fork, tag the affected
//        accounts for manual review, and adopt the higher-priority
//
// FIXME: Leader should detect whether any followers fall out of sync for any reason, identify/tag affected accounts, and
//        re-synchronize.
//
// FIXME: Add test to measure how long it takes for leader to stabilize.
//
// FIXME: If leader dies before sending ESCALATE_RESPONSE (or if follower dies before receiving it), then a command might
//        have been committed to the database without notifying whoever initiated it. Perhaps have the caller identify
//        each command with a unique command id, and verify inside the query that the command hasn't been executed yet?

// *** DOCUMENTATION OF MESSAGE FIELDS ***
// Note: Yes, two of these fields start with lowercase chars.
// CommitCount:      The highest committed transaction ID in the DB currently. This can be higher than any transaction
//                   currently being handled. This exists to answer the question "how much data does this peer have?"
//                   and not to communicate any information about a specific transaction in progress.
// Hash:             The hash corresponding to the value in CommitCount.
// ID:               The ID of the transaction currently being operated on. It is the same type of information as
//                   "CommitCount", but not necessarily for the most recent transaction in the DB. It can be prefixed
//                   with "ASYNC_" for asynchronous transactions.
// NewHash:          Hash, but for "ID" instead of "CommitCount".
//                   Proposal: rename to "currentTransactionHash".
// NewCount:         Same as "ID" except without the "ASYNC_" prefix.
// State:            The state of the peer sending the message (i.e., SEARCHING, LEADING).
// Version:          The version string of the node sending the message.
// Permafollower:    Boolean value (string "true" or "false") indicating if the node sending the message is a
//                   permafollower.
// Priority:         The priority of the node. 0 for permafollowers.
// StateChangeCount: The number of state changes that this node has performed since startup. This is useful because
//                   it's sent at STANDINGUP, and parroted back by followers with an "approve" or "deny". This allows
//                   the leader to confirm that these responses were in fact sent in response to the correct message,
//                   and not some old out-of-date message from the past.
// Response:         Sent in STANDUP_RESPONSE, either "approve" or "deny".
// NumCommits:       With a "SYNCHRONIZE_RESPONSE" message, indicates the number of commits returned.
// leaderSendTime:   Timestamp in microseconds that leader sent a message, for performance analysis.
// dbCountAtStart:   The highest committed transaction in the DB at the start of this transaction on leader, for
//                   optimizing replication.

#undef SLOGPREFIX
#define SLOGPREFIX "{" << _name << "/" << SQLiteNode::stateName(_state) << "} "

// Initializations for static vars.
const uint64_t SQLiteNode::RECV_TIMEOUT{STIME_US_PER_S * 30};

const string SQLiteNode::CONSISTENCY_LEVEL_NAMES[] = {"ASYNC",
                                                    "ONE",
                                                    "QUORUM"};

atomic<int64_t> SQLiteNode::currentReplicateThreadID(0);

const vector<SQLitePeer*> SQLiteNode::_initPeers(const string& peerListString) {
    // Make the logging macro work in the static initializer.
    auto _name = "init";
    State _state = UNKNOWN;

    vector<SQLitePeer*> peerList;
    list<string> parsedPeerList = SParseList(peerListString);
    for (const string& peerString : parsedPeerList) {
        // Get the params from this peer, if any
        string host;
        STable params;
        SASSERT(SParseURIPath(peerString, host, params));
        string name = SGetDomain(host);
        if (params.find("nodeName") != params.end()) {
            name = params["nodeName"];
        }

        // Create a new peer and ready it for connection
        SASSERT(SHostIsValid(host));
        SINFO("Adding peer #" << peerList.size() << ": " << name << " (" << host << "), " << SComposeJSONObject(params));
        SQLitePeer* peer = new SQLitePeer(name, host, params, peerList.size() + 1);

        // Wait up to 2s before trying the first time
        peer->nextReconnect = STimeNow() + SRandom::rand64() % (STIME_US_PER_S * 2);
        peerList.push_back(peer);
    }
    return peerList;
}

SQLiteNode::SQLiteNode(SQLiteServer& server, shared_ptr<SQLitePool> dbPool, const string& name,
                       const string& host, const string& peerList, int priority, uint64_t firstTimeout,
                       const string& version, const string& commandPort)
    : STCPManager(),
      _commandAddress(commandPort),
      _name(name),
      _peerList(_initPeers(peerList)),
      _originalPriority(priority),
      _port(host.empty() ? nullptr : openPort(host, 30)),
      _version(version),
      _commitState(CommitState::UNINITIALIZED),
      _db(dbPool->getBase()),
      _dbPool(dbPool),
      _lastSentTransactionID(0),
      _leadPeer(nullptr),
      _priority(-1),
      _replicationThreadCount(0),
      _replicationThreadsShouldExit(false),
      _server(server),
      _state(UNKNOWN),
      _stateChangeCount(0),
      _stateTimeout(STimeNow() + firstTimeout),
      _syncPeer(nullptr)
{
    SASSERT(_originalPriority >= 0);
    SINFO("[NOTIFY] setting commit count to: " << _db.getCommitCount());
    _localCommitNotifier.notifyThrough(_db.getCommitCount());

    // Get this party started
    _changeState(SEARCHING);
}

SQLiteNode::~SQLiteNode() {
    // Make sure it's a clean shutdown
    SASSERTWARN(_escalatedCommandMap.empty());
    SASSERTWARN(!commitInProgress());

    // Clean up all the sockets and peers
    for (Socket* socket : _acceptedSocketList) {
        delete socket;
    }
    _acceptedSocketList.clear();

    for (SQLitePeer* peer : _peerList) {
        delete peer;
    }
}

void SQLiteNode::_replicate(SQLitePeer* peer, SData command, size_t sqlitePoolIndex) {
    // Initialize each new thread with a new number.
    SInitialize("replicate" + to_string(currentReplicateThreadID.fetch_add(1)));

    // Allow the DB handle to be returned regardless of how this function exits.
    SQLiteScopedHandle dbScope(*_dbPool, sqlitePoolIndex);
    SQLite& db = dbScope.db();

    bool goSearchingOnExit = false;
    {
        // Make sure when this thread exits we decrement our thread counter.
        ScopedDecrement<decltype(_replicationThreadCount)> decrementer(_replicationThreadCount);

        SDEBUG("Replicate thread started: " << command.methodLine);
        if (SIEquals(command.methodLine, "BEGIN_TRANSACTION")) {
            uint64_t newCount = command.calcU64("NewCount");
            uint64_t currentCount = newCount - 1;

            // Transactions are either ASYNC or QUORUM. QUORUM transactions can only start when the DB is completely
            // up-to-date. ASYNC transactions can start as soon as the DB is at `dbCountAtStart` (the same value that
            // the DB was at when the transaction began on leader).
            bool quorum = !SStartsWith(command["ID"], "ASYNC");
            uint64_t waitForCount = SStartsWith(command["ID"], "ASYNC") ? command.calcU64("dbCountAtStart") : currentCount;
            SDEBUG("Thread for commit " << newCount << " waiting on DB count " << waitForCount << " (" << (quorum ? "QUORUM" : "ASYNC") << ")");
            while (true) {
                SQLiteSequentialNotifier::RESULT result = _localCommitNotifier.waitFor(waitForCount, false);
                if (result == SQLiteSequentialNotifier::RESULT::UNKNOWN) {
                    // This should be impossible.
                    SERROR("Got UNKNOWN result from waitFor, which shouldn't happen");
                } else if (result == SQLiteSequentialNotifier::RESULT::COMPLETED) {
                    // Success case.
                    break;
                } else if (result == SQLiteSequentialNotifier::RESULT::CANCELED) {
                    SINFO("_localCommitNotifier.waitFor canceled early, returning.");
                    return;
                } else {
                    SERROR("Got unhandled SQLiteSequentialNotifier::RESULT value, did someone update the enum without updating this block?");
                }
            }

            try {
                int result = -1;
                int commitAttemptCount = 1;
                while (result != SQLITE_OK) {
                    if (commitAttemptCount > 1) {
                        SINFO("Commit attempt number " << commitAttemptCount << " for concurrent replication.");
                    }
                    SDEBUG("BEGIN for commit " << newCount);
                    bool uniqueContraintsError = false;
                    try {
                        _handleBeginTransaction(db, peer, command, commitAttemptCount > 1);

                        // Now we need to wait for the DB to be up-to-date (if the transaction is QUORUM, we can
                        // skip this, we did it above) to enforce that commits are in the same order on followers as on
                        // leader.
                        if (!quorum) {
                            // If we get here, we're *in* a transaction (begin ran) so the checkpoint thread is blocked
                            // waiting for us to finish. But the thread that needs to commit to unblock us can be blocked
                            // on the checkpoint if these are started out of order.
                            //
                            // Let's see if we can verify that happened.
                            // Yes, we get this line logged 4 times from four threads as their last activity and then:
                            // (SQLite.cpp:403) operator() [checkpoint] [info] [checkpoint] Waiting on 4 remaining transactions.
                            SDEBUG("Waiting at commit " << db.getCommitCount() << " for commit " << currentCount);
                            SQLiteSequentialNotifier::RESULT waitResult = _localCommitNotifier.waitFor(currentCount, true);
                            if (waitResult == SQLiteSequentialNotifier::RESULT::CANCELED) {
                                SINFO("Replication canceled mid-transaction, stopping.");
                                db.rollback();
                                break;
                            }
                        }

                        // Ok, almost ready.
                        // Note:: calls _sendToPeer() which is a write operation.
                        _handlePrepareTransaction(db, peer, command);
                    } catch (const SQLite::constraint_error& e) {
                        // We could `continue` immediately upon catching this exception, but instead, we wait for the
                        // leader commit notifier to be ready. This prevents us from spinning in an endless loop on the
                        // same error over and over until whatever thread we're waiting for finishes.
                        uniqueContraintsError = true;
                    }
                    // Now see if we can commit. We wait until *after* prepare because for QUORUM transactions, we
                    // don't send LEADER the approval for this until inside of `prepare`. This potentially makes us
                    // wait while holding the commit lock for non-concurrent transactions, but I guess nobody else with
                    // a commit after us will be able to commit, either.
                    SQLiteSequentialNotifier::RESULT waitResult = _leaderCommitNotifier.waitFor(command.calcU64("NewCount"), true);
                    if (uniqueContraintsError) {
                        SINFO("Got unique constraints error in replication, restarting.");
                        db.rollback();
                        continue;
                    } else if (waitResult == SQLiteSequentialNotifier::RESULT::CANCELED) {
                        SINFO("Replication canceled mid-transaction, stopping.");
                        db.rollback();
                        break;
                    }

                    // Leader says it has committed this transaction, so we can too.
                    ++commitAttemptCount;
                    result = _handleCommitTransaction(db, peer, command.calcU64("NewCount"), command["NewHash"]);
                    if (result != SQLITE_OK) {
                        db.rollback();
                    }
                }

                // Notify that we've succeeded (it actually also notifies if we were canceled, but that's fine).
                _localCommitNotifier.notifyThrough(db.getCommitCount());
            } catch (const SException& e) {
                SALERT("Caught exception in replication thread. Assuming this means we want to stop following. Exception: " << e.what());
                goSearchingOnExit = true;
                db.rollback();
            }
        } else if (SIEquals(command.methodLine, "ROLLBACK_TRANSACTION")) {
            // `decrementer` needs to be destroyed to decrement our thread count before we can change state out of
            // FOLLOWING.
            _handleRollbackTransaction(db, peer, command);
            goSearchingOnExit = true;
        } else if (SIEquals(command.methodLine, "COMMIT_TRANSACTION")) {
            _leaderCommitNotifier.notifyThrough(command.calcU64("CommitCount"));
        }
    }
    if (goSearchingOnExit) {
        // We can lock here for this state change because we're in our own thread, and this won't be recursive with
        // the calling thread. This is also a really weird exception case that should never happen, so the performance
        // implications aren't significant so long as we don't break.
        unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);
        _changeState(SEARCHING);
    }
}

void SQLiteNode::startCommit(ConsistencyLevel consistency) {
    unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);

    // Verify we're not already committing something, and then record that we have begun. This doesn't actually *do*
    // anything, but `update()` will pick up the state in its next invocation and start the actual commit.
    SASSERT(_commitState == CommitState::UNINITIALIZED ||
            _commitState == CommitState::SUCCESS       ||
            _commitState == CommitState::FAILED);
    _commitState = CommitState::WAITING;
    _commitConsistency = consistency;
    if (_commitConsistency != QUORUM) {
        SHMMM("Non-quorum transaction running in the sync thread.");
    }
}

void SQLiteNode::sendResponse(const SQLiteCommand& command) {
    unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);
    SQLitePeer* peer = _getPeerByID(command.initiatingPeerID);
    SASSERT(peer);
    // If it was a peer message, we don't need to wrap it in an escalation response.
    SData escalate("ESCALATE_RESPONSE");
    escalate["ID"] = command.id;
    escalate.content = command.response.serialize();
    SINFO("Sending ESCALATE_RESPONSE to " << peer->name << " for " << command.id << ".");
    _sendToPeer(peer, escalate);
}

void SQLiteNode::beginShutdown(uint64_t usToWait) {
    unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);
    // Ignore redundant
    if (!_gracefulShutdown()) {
        // Start graceful shutdown
        SINFO("Beginning graceful shutdown.");
        _shutdownTimeout.alarmDuration = usToWait;
        _shutdownTimeout.start();
    }
}

bool SQLiteNode::_isNothingBlockingShutdown() const {
    // Don't shutdown if in the middle of a transaction
    if (_db.insideTransaction())
        return false;

    // If we're doing a commit, don't shut down.
    if (commitInProgress()) {
        return false;
    }

    // If we have non-"Connection: wait" commands escalated to leader, not done
    if (!_escalatedCommandMap.empty()) {
        return false;
    }

    return true;
}

bool SQLiteNode::shutdownComplete() const {
    shared_lock<decltype(_stateMutex)> sharedLock(_stateMutex);

    // First even see if we're shutting down
    if (!_gracefulShutdown()) {
        return false;
    }

    // Next, see if we're timing out the graceful shutdown and killing non-gracefully
    if (_shutdownTimeout.ringing()) {
        SWARN("Graceful shutdown timed out, killing non gracefully.");
        return true;
    }

    // Not complete unless we're SEARCHING, SYNCHRONIZING, or WAITING
    if (_state > WAITING) {
        // Not in a shutdown state
        SINFO("Can't graceful shutdown yet because state=" << stateName(_state) << ", commitInProgress=" << commitInProgress() << ", escalated=" << _escalatedCommandMap.size());
        return false;
    }

    // If we have unsent data, not done
    for (auto peer : _peerList) {
        if (peer->socket && !peer->socket->sendBufferEmpty()) {
            // Still sending data
            SINFO("Can't graceful shutdown yet because unsent data to peer '" << peer->name << "'");
            return false;
        }
    }

    // Finally, make sure nothing is blocking shutdown
    if (_isNothingBlockingShutdown()) {
        SINFO("Graceful shutdown is complete");
        return true;
    } else {
        // Not done yet
        SINFO("Can't graceful shutdown yet because waiting on commands: commitInProgress=" << commitInProgress() << ".");
        return false;
    }
}

SQLiteNode::State SQLiteNode::getState() const {
    // Note: this can skip locking because it only accesses a single atomic variable, which makes it safe to call in
    // private methods.
    return _state;
}

int SQLiteNode::getPriority() const {
    // Note: this can skip locking because it only accesses a single atomic variable, which makes it safe to call in
    // private methods.
    return _priority;
}

const string SQLiteNode::getLeaderVersion() const {
    shared_lock<decltype(_stateMutex)> sharedLock(_stateMutex);
    if (_state == LEADING || _state == STANDINGDOWN) {
        return _version;
    } else if (_leadPeer) {
        return _leadPeer.load()->version;
    }
    return "";
}

uint64_t SQLiteNode::getCommitCount() const {
    // Note: this can skip locking because it only accesses a single atomic variable, which makes it safe to call in
    // private methods. (Yes, SQLite::SharedData::commitCount is atomic, go check).
    return _db.getCommitCount();
}

bool SQLiteNode::_gracefulShutdown() const {
    return (_shutdownTimeout.alarmDuration != 0);
}

bool SQLiteNode::commitInProgress() const {
    // Note: this can skip locking because it only accesses a single atomic variable, which makes it safe to call in
    // private methods.
    CommitState commitState = _commitState.load();
    return (commitState == CommitState::WAITING || commitState == CommitState::COMMITTING);
}

bool SQLiteNode::commitSucceeded() const {
    // Note: this can skip locking because it only accesses a single atomic variable, which makes it safe to call in
    // private methods.
    return _commitState == CommitState::SUCCESS;
}

void SQLiteNode::_sendOutstandingTransactions(const set<uint64_t>& commitOnlyIDs) {
    auto transactions = _db.popCommittedTransactions();
    if (transactions.empty()) {
        // Nothing to do.
        return;
    }
    string sendTime = to_string(STimeNow());
    for (auto& i : transactions) {
        uint64_t id = i.first;
        if (id <= _lastSentTransactionID) {
            SALERT("Already sent a transaction in committed transaction list");
            continue;
        }
        string& query = get<0>(i.second);
        string& hash = get<1>(i.second);
        uint64_t dbCountAtStart = get<2>(i.second);
        string idHeader = to_string(id);

        // If this is marked as "commitOnly", we won't send the BEGIN for it.
        if (commitOnlyIDs.find(id) == commitOnlyIDs.end()) {
            // Any commit where we can send a BEGIN and a COMMIT without waiting for acknowledgement is ASYNC.
            idHeader = "ASYNC_" + idHeader;
            SData transaction("BEGIN_TRANSACTION");
            transaction["NewCount"] = to_string(id);
            transaction["NewHash"] = hash;
            transaction["leaderSendTime"] = sendTime;
            transaction["dbCountAtStart"] = to_string(dbCountAtStart);
            transaction["ID"] = idHeader;
            transaction.content = query;
            for (auto peer : _peerList) {
                // Clear the response flag from the last transaction
                peer->transactionResponse = SQLitePeer::Response::NONE;
            }

            // Allows us to easily figure out how far behind followers are by analyzing the logs.
            SINFO("Sending COMMIT for ASYNC transaction " << id << " to followers");
            _sendToAllPeers(transaction, true); // subscribed only
        } else {
            SINFO("Sending COMMIT for QUORUM transaction " << id << " to followers");
        }
        SData commit("COMMIT_TRANSACTION");
        commit["ID"] = idHeader;
        commit["NewCount"] = to_string(id);
        commit["NewHash"] = hash;
        _sendToAllPeers(commit, true); // subscribed only
        _lastSentTransactionID = id;
    }
}

list<STable> SQLiteNode::getPeerInfo() const {
    shared_lock<decltype(_stateMutex)> sharedLock(_stateMutex);
    list<STable> peerData;
    for (SQLitePeer* peer : _peerList) {
        peerData.emplace_back(peer->getData());
    }
    return peerData;
}

void SQLiteNode::escalateCommand(unique_ptr<SQLiteCommand>&& command, bool forget) {
    unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);
    // Send this to the leader
    SASSERT(_leadPeer);

    // If the leader is currently standing down, we won't escalate, we'll give the command back to the caller.
    if(_leadPeer.load()->state == STANDINGDOWN) {
        SINFO("Asked to escalate command but leader standing down, letting server retry.");
        _server.acceptCommand(move(command), false);
        return;
    }

    SASSERTEQUALS(_leadPeer.load()->state, LEADING);
    uint64_t elapsed = STimeNow() - command->request.calcU64("commandExecuteTime");
    SINFO("Escalating '" << command->request.methodLine << "' (" << command->id << ") to leader '" << _leadPeer.load()->name
          << "' after " << elapsed / 1000 << " ms");

    // Create a command to send to our leader.
    SData escalate("ESCALATE");
    escalate["ID"] = command->id;
    escalate.content = command->request.serialize();

    // Marking the command as escalated, even if we are going to forget it, because the command's destructor may need
    // this info.
    command->escalated = true;

    // Store the command as escalated, unless we intend to forget about it anyway.
    if (forget) {
        SINFO("Firing and forgetting command '" << command->request.methodLine << "' to leader.");
    } else {
        command->escalationTimeUS = STimeNow();
        _escalatedCommandMap.emplace(command->id, move(command));
    }

    // And send to leader.
    _sendToPeer(_leadPeer, escalate);
}

// --------------------------------------------------------------------------
// State Machine
// --------------------------------------------------------------------------
// Here is a simplified state diagram showing the major state transitions:
//
//                              SEARCHING
//                                  |
//                            SYNCHRONIZING
//                                  |
//                               WAITING
//                    ___________/     \____________
//                   |                              |
//              STANDINGUP                     SUBSCRIBING
//                   |                              |
//                LEADING                       FOLLOWING
//                   |                              |
//             STANDINGDOWN                         |
//                   |___________       ____________|
//                               \     /
//                              SEARCHING
//
// In short, every node starts out in the SEARCHING state, where it simply tries
// to establish all its peer connections.  Once done, each node SYNCHRONIZES with
// the freshest peer, meaning they download whatever "commits" they are
// missing.  Then they WAIT until the highest priority node "stands up" to become
// the new "leader".  All other nodes then SUBSCRIBE and become "followers".  If the
// leader "stands down", then all followers unsubscribe and everybody goes back into
// the SEARCHING state and tries it all over again.
//
//
// State Transitions
// -----------------
// Each state transitions according to the following events and operates as follows:
bool SQLiteNode::update() {
    unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);

    // Process the database state machine
    switch (_state) {
    /// - SEARCHING: Wait for a period and try to connect to all known
    ///     peers.  After a timeout, give up and go ahead with whoever
    ///     we were able to successfully connect to -- if anyone.  The
    ///     logic for this state is as follows:
    ///
    ///         if( no peers configured )             goto LEADING
    ///         if( !timeout )                        keep waiting
    ///         if( no peers connected )              goto LEADING
    ///         if( nobody has more commits than us ) goto WAITING
    ///         else send SYNCHRONIZE and goto SYNCHRONIZING
    ///
    case SEARCHING: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_leadPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // If we're trying to shut down, just do nothing, especially don't jump directly to leading and get stuck in an
        // endless loop.
        if (_gracefulShutdown()) {
            return false; // Don't re-update
        }

        // If no peers, we're the leader, unless we're shutting down.
        if (_peerList.empty()) {
            // There are no peers, jump straight to leading
            SHMMM("No peers configured, jumping to LEADING");
            _changeState(LEADING);
            return true; // Re-update immediately
        }

        // How many peers have we logged in to?
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        SQLitePeer* freshestPeer = nullptr;
        for (auto peer : _peerList) {
            // Wait until all connected (or failed) and logged in
            bool permaFollower = peer->permaFollower;
            bool loggedIn = peer->loggedIn;

            // Count how many full peers (non-permafollowers) we have
            numFullPeers += !permaFollower;

            // Count how many full peers are logged in
            numLoggedInFullPeers += (!permaFollower) && loggedIn;

            // Find the freshest peer
            if (loggedIn) {
                // The freshest peer is the one that has the most commits.
                if (!freshestPeer || peer->commitCount > freshestPeer->commitCount) {
                    freshestPeer = peer;
                }
            }
        }

        // Keep searching until we connect to at least half our non-permafollowers peers OR timeout
        SINFO("Signed in to " << numLoggedInFullPeers << " of " << numFullPeers << " full peers (" << _peerList.size()
                              << " with permafollowers), timeout in " << (_stateTimeout - STimeNow()) / 1000
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
            _changeState(WAITING);
            return true; // Re-update
        }

        // How does our state compare with the freshest peer?
        SASSERT(freshestPeer);
        uint64_t freshestPeerCommitCount = freshestPeer->commitCount;
        if (freshestPeerCommitCount == _db.getCommitCount()) {
            // We're up to date
            SINFO("Synchronized with the freshest peer '" << freshestPeer->name << "', WAITING.");
            _changeState(WAITING);
            return true; // Re-update
        }

        // Are we fresher than the freshest peer?
        if (freshestPeerCommitCount < _db.getCommitCount()) {
            // Looks like we're the freshest peer overall
            SINFO("We're the freshest peer, WAITING.");
            _changeState(WAITING);
            return true; // Re-update
        }

        // It has a higher commit count than us, synchronize.
        SASSERT(freshestPeerCommitCount > _db.getCommitCount());
        SASSERTWARN(!_syncPeer);
        _updateSyncPeer();
        if (_syncPeer) {
            _sendToPeer(_syncPeer, SData("SYNCHRONIZE"));
        } else {
            SWARN("Updated to NULL _syncPeer when about to send SYNCHRONIZE. Going to WAITING.");
            _changeState(WAITING);
            return true; // Re-update
        }
        _changeState(SYNCHRONIZING);
        return true; // Re-update
    }

    /// - SYNCHRONIZING: We only stay in this state while waiting for
    ///     the SYNCHRONIZE_RESPONSE.  When we receive it, we'll enter
    ///     the WAITING state.  Alternately, give up waitng after a
    ///     period and go SEARCHING.
    ///
    case SYNCHRONIZING: {
        SASSERTWARN(_syncPeer);
        SASSERTWARN(!_leadPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Nothing to do but wait
        if (STimeNow() > _stateTimeout) {
            // Give up on synchronization; reconnect that peer and go searching
            SHMMM("Timed out while waiting for SYNCHRONIZE_RESPONSE, searching.");
            _reconnectPeer(_syncPeer);
            _syncPeer = nullptr;
            _changeState(SEARCHING);
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
    ///             - current leader (might be STANDINGUP or STANDINGDOWN)
    ///         if( no peers logged in )
    ///             goto SEARCHING
    ///         if( a higher-priority LEADING leader exists )
    ///             send SUBSCRIBE and go SUBSCRIBING
    ///         if( the freshest peer has more commits han us )
    ///             goto SEARCHING
    ///         if( no leader and we're the highest prioriy )
    ///             clear "StandupResponse" on all peers
    ///             goto STANDINGUP
    ///
    case WAITING: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_leadPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        SASSERTWARN(_escalatedCommandMap.empty());
        // If we're trying and ready to shut down, do nothing.
        if (_gracefulShutdown()) {
            // Do we have an outstanding command?
            if (1/* TODO: Commit in progress? */) {
                // Nope!  Let's just halt the FSM here until we shutdown so as to
                // avoid potential confusion.  (Technically it would be fine to continue
                // the FSM, but it makes the logs clearer to just stop here.)
                SINFO("Graceful shutdown underway and no queued commands, do nothing.");
                return false; // No fast update
            } else {
                // We do have outstanding commands, even though a graceful shutdown
                // has been requested.  This is probably due to us previously being a leader
                // to which commands had been sent directly -- we got the signal to shutdown,
                // and stood down immediately.  All the followers will re-escalate whatever
                // commands they were waiting on us to process, so they're fine.  But our own
                // commands still need to be processed.  We're no longer the leader, so we
                // can't do it.  Rather, even though we're trying to do a graceful shutdown,
                // we need to find and follower to the new leader, and have it process our
                // commands.  Once the new leader has processed our commands, then we can
                // shut down gracefully.
                SHMMM("Graceful shutdown underway but queued commands so continuing...");
            }
        }

        // Loop across peers and find the highest priority and leader
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        SQLitePeer* highestPriorityPeer = nullptr;
        SQLitePeer* freshestPeer = nullptr;
        SQLitePeer* currentLeader = nullptr;
        for (auto peer : _peerList) {
            // Make sure we're a full peer
            if (!peer->permaFollower) {
                // Verify we're logged in
                ++numFullPeers;
                if (peer->loggedIn) {
                    // Verify we're still fresh
                    ++numLoggedInFullPeers;
                    if (!freshestPeer || peer->commitCount > freshestPeer->commitCount)
                        freshestPeer = peer;

                    // See if it's the highest priority
                    if (!highestPriorityPeer || peer->priority > highestPriorityPeer->priority)
                        highestPriorityPeer = peer;

                    // See if it is currently the leader (or standing up/down)
                    if (peer->state == STANDINGUP || peer->state == LEADING || peer->state == STANDINGDOWN) {
                        // Found the current leader
                        if (currentLeader)
                            PHMMM("Multiple peers trying to stand up (also '" << currentLeader->name
                                                                              << "'), let's hope they sort it out.");
                        currentLeader = peer;
                    }
                }
            }
        }

        // If there are no logged in peers, then go back to SEARCHING.
        if (!highestPriorityPeer) {
            // Not connected to any other peers
            SHMMM("Configured to have peers but can't connect to any, re-SEARCHING.");
            _changeState(SEARCHING);
            return true; // Re-update
        }
        SASSERT(highestPriorityPeer);
        SASSERT(freshestPeer);

        SDEBUG("Dumping evaluated cluster state: numLoggedInFullPeers=" << numLoggedInFullPeers << " freshestPeer=" << freshestPeer->name << " highestPriorityPeer=" << highestPriorityPeer->name << " currentLeader=" << (currentLeader ? currentLeader->name : "none"));

        // If there is already a leader that is higher priority than us,
        // subscribe -- even if we're not in sync with it.  (It'll bring
        // us back up to speed while subscribing.)
        if (currentLeader && _priority < highestPriorityPeer->priority && currentLeader->state == LEADING) {
            // Subscribe to the leader
            SINFO("Subscribing to leader '" << currentLeader->name << "'");
            _leadPeer = currentLeader;
            _sendToPeer(currentLeader, SData("SUBSCRIBE"));
            _changeState(SUBSCRIBING);
            return true; // Re-update
        }

        // No leader to subscribe to, let's see if there's anybody else
        // out there with commits we don't have.  Might as well synchronize
        // while waiting.
        if (freshestPeer->commitCount > _db.getCommitCount()) {
            // Out of sync with a peer -- resynchronize
            SHMMM("Lost synchronization while waiting; re-SEARCHING.");
            _changeState(SEARCHING);
            return true; // Re-update
        }

        // No leader and we're in sync, perhaps everybody is waiting for us
        // to stand up?  If we're higher than the highest priority, are using 
        // a real priority and are not a permafollower, and are connected to 
        // enough full peers to achieve quorum, we should be leader.
        if (!currentLeader && numLoggedInFullPeers * 2 >= numFullPeers &&
            _priority > 0 && _priority > highestPriorityPeer->priority) {
            // Yep -- time for us to stand up -- clear everyone's
            // last approval status as they're about to send them.
            SINFO("No leader and we're highest priority (over " << highestPriorityPeer->name << "), STANDINGUP");
            for (auto peer : _peerList) {
                peer->standupResponse = SQLitePeer::Response::NONE;
            }
            _changeState(STANDINGUP);
            return true; // Re-update
        }

        // Otherwise, Keep waiting
        SDEBUG("Connected to " << numLoggedInFullPeers << " of " << numFullPeers << " full peers (" << _peerList.size()
                               << " with permafollowers), priority=" << _priority);
        break;
    }

    /// - STANDINGUP: We're waiting for peers to approve or deny our standup
    ///     request.  The logic for this state is:
    ///
    ///         if( at least one peer has denied standup )
    ///             goto SEARCHING
    ///         if( everybody has responded and approved )
    ///             goto LEADING
    ///         if( somebody hasn't responded but we're timing out )
    ///             goto SEARCHING
    ///
    case STANDINGUP: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_leadPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Wait for everyone to respond
        bool allResponded = true;
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        if (_gracefulShutdown()) {
            SINFO("Shutting down while standing up, setting state to SEARCHING");
            _changeState(SEARCHING);
            return true; // Re-update
        }
        for (auto peer : _peerList) {
            // Check this peer; if not logged in, tacit approval
            if (!peer->permaFollower) {
                ++numFullPeers;
                if (peer->loggedIn) {
                    // Connected and logged in.
                    numLoggedInFullPeers++;

                    // Has it responded yet?
                    if (peer->standupResponse == SQLitePeer::Response::NONE) {
                        // At least one logged in full peer hasn't responded
                        allResponded = false;
                    } else if (peer->standupResponse != SQLitePeer::Response::APPROVE) {
                        // It responeded, but didn't approve -- abort
                        PHMMM("Refused our STANDUP, cancel and RESEARCH");
                        _changeState(SEARCHING);
                        return true; // Re-update
                    }
                }
            }
        }

        // If everyone's responded with approval and we form a majority, then finish standup.
        bool majorityConnected = numLoggedInFullPeers * 2 >= numFullPeers;
        if (allResponded && majorityConnected) {
            // Complete standup
            SINFO("All peers approved standup, going LEADING.");
            _changeState(LEADING);
            return true; // Re-update
        }

        // See if we're taking too long
        if (STimeNow() > _stateTimeout) {
            // Timed out
            SHMMM("Timed out waiting for STANDUP approval; reconnect all and re-SEARCHING.");
            _reconnectAll();
            _changeState(SEARCHING);
            return true; // Re-update
        }
        break;
    }

    /// - LEADING / STANDINGDOWN : These are the states where the magic
    ///     happens.  In both states, the node will execute distributed
    ///     transactions.  However, new transactions are only
    ///     started in the LEADING state (while existing transactions are
    ///     concluded in the STANDINGDOWN) state.  The logic for this state
    ///     is as follows:
    ///
    ///         if( we're processing a transaction )
    ///             if( all subscribed followers have responded/approved )
    ///                 commit this transaction to the local DB
    ///                 broadcast COMMIT_TRANSACTION to all subscribed followers
    ///                 send a STATE to show we've committed a new transaction
    ///                 notify the caller that the command is complete
    ///         if( we're LEADING and not processing a command )
    ///             if( there is another LEADER )         goto STANDINGDOWN
    ///             if( there is a higher priority peer ) goto STANDINGDOWN
    ///             if( a command is queued )
    ///                 if( processing the command affects the database )
    ///                    clear the transactionResponse of all peers
    ///                    broadcast BEGIN_TRANSACTION to subscribed followers
    ///         if( we're standing down and all followers have unsubscribed )
    ///             goto SEARCHING
    ///
    case LEADING:
    case STANDINGDOWN: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_leadPeer);

        // NOTE: This block very carefully will not try and call _changeState() while holding SQLite::g_commitLock,
        // because that could cause a deadlock when called by an outside caller!

        // If there's no commit in progress, we'll send any outstanding transactions that exist. We won't send them
        // mid-commit, as they'd end up as nested transactions interleaved with the one in progress. However, there
        // should never be any commits in here while a commit is in progress anyway, since all commits except the one
        // running are blocked until that one finishes.
        if (!commitInProgress()) {
            _sendOutstandingTransactions();
        }

        // This means we've started a distributed transaction and need to decide if we should commit it, which can mean
        // waiting on peers to approve the transaction. We can do this even after we've begun standing down.
        if (_commitState == CommitState::COMMITTING) {
            // Loop across all peers configured to see how many are:
            int numFullPeers = 0;     // Num non-permafollowers configured
            int numFullFollowers = 0; // Num full peers that are "subscribed"
            int numFullResponded = 0; // Num full peers that have responded approve/deny
            int numFullApproved = 0;  // Num full peers that have approved
            int numFullDenied = 0;    // Num full peers that have denied
            for (auto peer : _peerList) {
                // Check this peer to see if it's full or a permafollower
                if (!peer->permaFollower) {
                    // It's a full peer -- is it subscribed, and if so, how did it respond?
                    ++numFullPeers;
                    if (peer->subscribed) {
                        // Subscribed, did it respond?
                        numFullFollowers++;
                        if (peer->transactionResponse == SQLitePeer::Response::NONE) {
                            continue;
                        }
                        numFullResponded++;
                        if (peer->transactionResponse == SQLitePeer::Response::APPROVE) {
                            SDEBUG("Peer '" << peer->name << "' has approved transaction.");
                            ++numFullApproved;
                        } else {
                            SWARN("Peer '" << peer->name << "' denied transaction.");
                            ++numFullDenied;
                        }
                    }
                }
            }

            // Did we get a majority? This is important whether or not our consistency level needs it, as it will
            // reset the checkpoint limit either way.
            bool majorityApproved = (numFullApproved * 2 >= numFullPeers);

            // Figure out if we have enough consistency
            bool consistentEnough = false;
            switch (_commitConsistency) {
                case ASYNC:
                    // Always consistent enough if we don't care!
                    consistentEnough = true;
                    break;
                case ONE:
                    // So long at least one full approved (if we have any peers, that is), we're good.
                    consistentEnough = !numFullPeers || (numFullApproved > 0);
                    break;
                case QUORUM:
                    // This one requires a majority
                    consistentEnough = majorityApproved;
                    break;
                default:
                    SERROR("Invalid write consistency.");
                    break;
            }

            // See if all active non-permafollowers have responded.
            // NOTE: This can be true if nobody responds if there are no full followers - this includes machines that
            // should be followers that are disconnected.
            bool everybodyResponded = numFullResponded >= numFullFollowers;

            // Record these for posterity
            SDEBUG(     "numFullPeers="           << numFullPeers
                   << ", numFullFollowers="       << numFullFollowers
                   << ", numFullResponded="       << numFullResponded
                   << ", numFullApproved="        << numFullApproved
                   << ", majorityApproved="       << majorityApproved
                   << ", writeConsistency="       << CONSISTENCY_LEVEL_NAMES[_commitConsistency]
                   << ", consistencyRequired="    << CONSISTENCY_LEVEL_NAMES[_commitConsistency]
                   << ", consistentEnough="       << consistentEnough
                   << ", everybodyResponded="     << everybodyResponded);

            // If anyone denied this transaction, roll this back. Alternatively, roll it back if everyone we're
            // currently connected to has responded, but that didn't generate enough consistency. This could happen, in
            // theory, if we were disconnected from enough of the cluster that we could no longer reach QUORUM, but
            // this should have been detected earlier and forced us out of leading.
            // TODO: we might want to remove the `numFullDenied` condition here. A single failure shouldn't cause the
            // entire cluster to break. Imagine a scenario where a follower disk was full, and every write operation
            // failed with an sqlite3 error.
            if (numFullDenied || (everybodyResponded && !consistentEnough)) {
                SINFO("Rolling back transaction because everybody currently connected responded "
                      "but not consistent enough. Num denied: " << numFullDenied << ". Follower write failure?");

                // Notify everybody to rollback
                SData rollback("ROLLBACK_TRANSACTION");
                rollback.set("ID", _lastSentTransactionID + 1);
                _sendToAllPeers(rollback, true); // true: Only to subscribed peers.
                _db.rollback();

                // Finished, but failed.
                _commitState = CommitState::FAILED;
            } else if (consistentEnough) {
                // Commit this distributed transaction. Either we have quorum, or we don't need it.
                SDEBUG("Committing current transaction because consistentEnough: " << _db.getUncommittedQuery());
                uint64_t beforeCommit = STimeNow();
                int result = _db.commit(stateName(_state));
                SINFO("SQLite::commit in SQLiteNode took " << ((STimeNow() - beforeCommit)/1000) << "ms.");

                // If this is the case, there was a commit conflict.
                if (result == SQLITE_BUSY_SNAPSHOT) {
                    // We already asked everyone to commit this (even if it was async), so we'll have to tell them to
                    // roll back.
                    SINFO("[performance] Conflict committing " << CONSISTENCY_LEVEL_NAMES[_commitConsistency]
                          << " commit, rolling back.");
                    SData rollback("ROLLBACK_TRANSACTION");
                    rollback.set("ID", _lastSentTransactionID + 1);
                    _sendToAllPeers(rollback, true); // true: Only to subscribed peers.
                    _db.rollback();

                    // Finished, but failed.
                    _commitState = CommitState::FAILED;
                } else {
                    // Hey, our commit succeeded! Record how long it took.
                    uint64_t beginElapsed, readElapsed, writeElapsed, prepareElapsed, commitElapsed, rollbackElapsed;
                    uint64_t totalElapsed = _db.getLastTransactionTiming(beginElapsed, readElapsed, writeElapsed,
                                                                         prepareElapsed, commitElapsed, rollbackElapsed);
                    SINFO("Committed leader transaction for '"
                          << (_lastSentTransactionID + 1) << " (" << _db.getCommittedHash() << "). "
                          << " (consistencyRequired=" << CONSISTENCY_LEVEL_NAMES[_commitConsistency] << "), "
                          << numFullApproved << " of " << numFullPeers << " approved (" << _peerList.size() << " total) in "
                          << totalElapsed / 1000 << " ms ("
                          << beginElapsed / 1000 << "+" << readElapsed / 1000 << "+"
                          << writeElapsed / 1000 << "+" << prepareElapsed / 1000 << "+"
                          << commitElapsed / 1000 << "+" << rollbackElapsed / 1000 << "ms)");

                    SINFO("[performance] Successfully committed " << CONSISTENCY_LEVEL_NAMES[_commitConsistency]
                          << " transaction. Sending COMMIT_TRANSACTION to peers.");

                    // Send our outstanding transactions. Note that this particular transaction will send a COMMIT
                    // only, although if any other transactions have completed since we released a commit lock, we will
                    // send those ass well.
                    _sendOutstandingTransactions({_lastSentTransactionID + 1});

                    // Done!
                    _commitState = CommitState::SUCCESS;
                }
            } else {
                // Not consistent enough, but not everyone's responded yet, so we'll wait.
                SINFO("Waiting to commit. consistencyRequired=" << CONSISTENCY_LEVEL_NAMES[_commitConsistency]);

                // We're going to need to read from the network to finish this.
                return false;
            }
        }

        // If there's a transaction that's waiting, we'll start it. We do this *before* we check to see if we should
        // stand down, and since we return true, we'll never stand down as long as we keep adding new transactions
        // here. It's up to the server to stop giving us transactions to process if it wants us to stand down.
        if (_commitState == CommitState::WAITING) {
            _commitState = CommitState::COMMITTING;
            SINFO("[performance] Beginning " << CONSISTENCY_LEVEL_NAMES[_commitConsistency] << " commit.");

            // We should already have locked the DB before getting here, we can safely clear out any outstanding
            // transactions, no new ones can be added until we release the lock.
            _sendOutstandingTransactions();

            // We'll send the commit count to peers.
            uint64_t commitCount = _db.getCommitCount();

            // There's no handling for a failed prepare. This should only happen if the DB has been corrupted or
            // something catastrophic like that.
            SASSERT(_db.prepare());

            // Begin the distributed transaction
            SData transaction("BEGIN_TRANSACTION");
            SINFO("beginning distributed transaction for commit #" << commitCount + 1 << " ("
                  << _db.getUncommittedHash() << ")");
            transaction.set("NewCount", commitCount + 1);
            transaction.set("NewHash", _db.getUncommittedHash());
            transaction.set("leaderSendTime", to_string(STimeNow()));
            transaction.set("dbCountAtStart", to_string(_db.getDBCountAtStart()));
            if (_commitConsistency == ASYNC) {
                transaction.set("ID", "ASYNC_" + to_string(_lastSentTransactionID + 1));
            } else {
                transaction.set("ID", _lastSentTransactionID + 1);
            }
            transaction.content = _db.getUncommittedQuery();

            for (auto peer : _peerList) {
                // Clear the response flag from the last transaction
                peer->transactionResponse = SQLitePeer::Response::NONE;
            }

            // And send it to everyone who's subscribed.
            uint64_t beforeSend = STimeNow();
            _sendToAllPeers(transaction, true);
            SINFO("[performance] SQLite::_sendToAllPeers in SQLiteNode took " << ((STimeNow() - beforeSend)/1000) << "ms.");

            // We return `true` here to immediately re-update and thus commit this transaction immediately if it was
            // asynchronous.
            return true;
        }

        // Check to see if we should stand down. We'll finish any outstanding commits before we actually do.
        if (_state == LEADING) {
            string standDownReason;
            if (_gracefulShutdown()) {
                // Graceful shutdown. Set priority 1 and stand down so we'll re-connect to the new leader and finish
                // up our commands.
                standDownReason = "Shutting down, setting priority 1 and STANDINGDOWN.";
                _priority = 1;
            } else {
                // Loop across peers
                for (auto peer : _peerList) {
                    // Check this peer
                    if (peer->state == LEADING) {
                        // Hm... somehow we're in a multi-leader scenario -- not good.
                        // Let's get out of this as soon as possible.
                        standDownReason = "Found another LEADER (" + peer->name + "), STANDINGDOWN to clean it up.";
                    } else if (peer->state == WAITING) {
                        // We have a WAITING peer; is it waiting to STANDUP?
                        if (peer->priority > _priority) {
                            // We've got a higher priority peer in the works; stand down so it can stand up.
                            standDownReason = "Found higher priority WAITING peer (" + peer->name
                                              + ") while LEADING, STANDINGDOWN";
                        } else if (peer->commitCount > _db.getCommitCount()) {
                            // It's got data that we don't, stand down so we can get it.
                            standDownReason = "Found WAITING peer (" + peer->name +
                                              ") with more data than us (we have " + SToStr(_db.getCommitCount()) +
                                              "/" + _db.getCommittedHash() + ", it has " + to_string(peer->commitCount) +
                                              "/" + peer->hash.load() + ") while LEADING, STANDINGDOWN";
                        }
                    }
                }
            }

            // Do we want to stand down, and can we?
            if (!standDownReason.empty()) {
                SHMMM(standDownReason);
                _changeState(STANDINGDOWN);
                SINFO("Standing down: " << standDownReason);
            }
        }

        // At this point, we're no longer committing. We'll have returned false above, or we'll have completed any
        // outstanding transaction, we can complete standing down if that's what we're doing.
        if (_state == STANDINGDOWN) {
            // See if we're done
            // We can only switch to SEARCHING if the server has no outstanding write work to do.
            if (_standDownTimeout.ringing()) {
                SWARN("Timeout STANDINGDOWN, giving up on server and continuing.");
            } else if (!_server.canStandDown()) {
                // Try again.
                SINFO("Can't switch from STANDINGDOWN to SEARCHING yet, server prevented state change.");
                return false;
            }
            // Standdown complete
            SINFO("STANDDOWN complete, SEARCHING");
            _changeState(SEARCHING);

            // We're no longer waiting on responses from peers, we can re-update immediately and start becoming a
            // follower node instead.
            return true;
        }
        break;
    }

    /// - SUBSCRIBING: We're waiting for a SUBSCRIPTION_APPROVED from the
    ///     leader.  When we receive it, we'll go FOLLOWING. Otherwise, if we
    ///     timeout, go SEARCHING.
    ///
    case SUBSCRIBING:
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(_leadPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Nothing to do but wait
        if (STimeNow() > _stateTimeout) {
            // Give up
            SHMMM("Timed out waiting for SUBSCRIPTION_APPROVED, reconnecting to leader and re-SEARCHING.");
            _reconnectPeer(_leadPeer);
            _leadPeer = nullptr;
            _changeState(SEARCHING);
            return true; // Re-update
        }
        break;

    /// - FOLLOWING: This is where the other half of the magic happens.  Most
    ///     nodes will (hopefully) spend 99.999% of their time in this state.
    ///     FOLLOWING nodes simply begin and commit transactions with the
    ///     following logic:
    ///
    ///         if( leader steps down or disconnects ) goto SEARCHING
    ///         if( new queued commands ) send ESCALATE to leader
    ///
    case FOLLOWING:
        SASSERTWARN(!_syncPeer);
        // If graceful shutdown requested, stop following once there is
        // nothing blocking shutdown.  We stop listening for new commands
        // immediately upon TERM.)
        if (_gracefulShutdown() && _isNothingBlockingShutdown()) {
            // Go searching so we stop following
            SINFO("Stopping FOLLOWING in order to gracefully shut down, SEARCHING.");
            _changeState(SEARCHING);
            return false; // Don't update
        }

        // If the leader stops leading (or standing down), we'll go SEARCHING, which allows us to look for a new
        // leader. We don't want to go searching before that, because we won't know when leader is done sending its
        // final transactions.
        SASSERT(_leadPeer);
        if (_leadPeer.load()->state != LEADING && _leadPeer.load()->state != STANDINGDOWN) {
            // Leader stepping down
            SHMMM("Leader stepping down, re-queueing commands.");

            // If there were escalated commands, give them back to the server to retry.
            {
                auto lock = _escalatedCommandMap.scopedLock();
                for (auto& cmd : _escalatedCommandMap) {
                    _server.acceptCommand(move(cmd.second), false);
                }
                _escalatedCommandMap.clear();
            }

            // Are we in the middle of a commit? This should only happen if we received a `BEGIN_TRANSACTION` without a
            // corresponding `COMMIT` or `ROLLBACK`, this isn't supposed to happen.
            if (!_db.getUncommittedHash().empty()) {
                SWARN("Leader stepped down with transaction in progress, rolling back.");
                _db.rollback();
            }
            _changeState(SEARCHING);
            return true; // Re-update
        }

        break;

    default:
        SERROR("Invalid state #" << _state);
    }

    // Don't update immediately
    return false;
}

// Messages
// Here are the messages that can be received, and how a cluster node will respond to each based on its state:
void SQLiteNode::_onMESSAGE(SQLitePeer* peer, const SData& message) {
    SASSERT(peer);
    SASSERTWARN(!message.empty());
    SDEBUG("Received sqlitenode message from peer " << peer->name << ": " << message.serialize());
    // Every message broadcasts the current state of the node
    if (!message.isSet("CommitCount")) {
        STHROW("missing CommitCount");
    }
    if (!message.isSet("Hash")) {
        STHROW("missing Hash");
    }
    if (message.isSet("commandAddress")) {
        peer->commandAddress = message["commandAddress"];
    }

    peer->setCommit(message.calcU64("CommitCount"), message["Hash"]);

    // Classify and process the message
    if (SIEquals(message.methodLine, "LOGIN")) {
        // LOGIN: This is the first message sent to and received from a new peer. It communicates the current state of
        // the peer (hash and commit count), as well as the peer's priority. Peers can connect in any state, so this
        // message can be sent and received in any state.
        if (peer->loggedIn) {
            STHROW("already logged in");
        }
        if (!message.isSet("Priority")) {
            STHROW("missing Priority");
        }
        if (!message.isSet("State")) {
            STHROW("missing State");
        }
        if (!message.isSet("Version")) {
            STHROW("missing Version");
        }
        if (peer->permaFollower && (message["Permafollower"] != "true" || message.calc("Priority") > 0)) {
            STHROW("you're supposed to be a 0-priority permafollower");
        }
        if (!peer->permaFollower && (message["Permafollower"] == "true" || message.calc("Priority") == 0)) {
            STHROW("you're *not* supposed to be a 0-priority permafollower");
        }

        // It's an error to have to peers configured with the same priority, except 0 and -1
        SASSERT(_priority == -1 || _priority == 0 || message.calc("Priority") != _priority);
        PINFO("Peer logged in at '" << message["State"] << "', priority #" << message["Priority"] << " commit #"
              << message["CommitCount"] << " (" << message["Hash"] << ")");
        peer->priority = message.calc("Priority");
        peer->loggedIn = true;
        peer->version = message["Version"];
        peer->state = stateFromName(message["State"]);

        // Let the server know that a peer has logged in.
        _server.onNodeLogin(peer);
    } else if (!peer->loggedIn) {
        STHROW("not logged in");
    }
    else if (SIEquals(message.methodLine, "STATE")) {
        // STATE: Broadcast to all peers whenever a node's state changes. Also sent whenever a node commits a new query
        // (and thus has a new commit count and hash). A peer can react or respond to a peer's state change as follows:
        if (!message.isSet("State")) {
            STHROW("missing State");
        }
        if (!message.isSet("Priority")) {
            STHROW("missing Priority");
        }
        const State from = peer->state;
        peer->priority = message.calc("Priority");
        peer->state = stateFromName(message["State"]);
        const State to = peer->state;
        if (from == to) {
            // No state change, just new commits?
            PINFO("Peer received new commit in state '" << stateName(from) << "', commit #" << message["CommitCount"] << " ("
                  << message["Hash"] << ")");
        } else {
            // State changed -- first see if it's doing anything unusual
            PINFO("Peer switched from '" << stateName(from) << "' to '" << stateName(to) << "' commit #" << message["CommitCount"]
                  << " (" << message["Hash"] << ")");
            if (from == UNKNOWN) {
                PWARN("Peer coming from unrecognized state '" << stateName(from) << "'");
            }
            if (to == UNKNOWN) {
                PWARN("Peer going to unrecognized state '" << stateName(to) << "'");
            }

            // Make sure transition states are an approved pair
            bool okTransition = false;
            switch (from) {
            case UNKNOWN:
                break;
            case SEARCHING:
                okTransition = (to == SYNCHRONIZING || to == WAITING || to == LEADING);
                break;
            case SYNCHRONIZING:
                okTransition = (to == SEARCHING || to == WAITING);
                break;
            case WAITING:
                okTransition = (to == SEARCHING || to == STANDINGUP || to == SUBSCRIBING);
                break;
            case STANDINGUP:
                okTransition = (to == SEARCHING || to == LEADING);
                break;
            case LEADING:
                okTransition = (to == SEARCHING || to == STANDINGDOWN);
                break;
            case STANDINGDOWN:
                okTransition = (to == SEARCHING);
                break;
            case SUBSCRIBING:
                okTransition = (to == SEARCHING || to == FOLLOWING);
                break;
            case FOLLOWING:
                okTransition = (to == SEARCHING);
                break;
            }
            if (!okTransition) {
                PWARN("Peer making invalid transition from '" << stateName(from) << "' to '" << stateName(to) << "'");
            }

            // Next, should we do something about it?
            if (to == SEARCHING) {
                // SEARCHING: If anything ever goes wrong, a node reverts to the SEARCHING state. Thus if we see a peer
                // go SEARCHING, we reset its accumulated state.  Specifically, we mark it is no longer being
                // "subscribed", and we clear its last transaction response.
                peer->transactionResponse = SQLitePeer::Response::NONE;
                peer->subscribed = false;
            } else if (to == STANDINGUP) {
                // STANDINGUP: When a peer announces it intends to stand up, we immediately respond with approval or
                // denial. We determine this by checking to see if there is any  other peer who is already leader or
                // also trying to stand up.
                //
                // **FIXME**: Should it also deny if it knows of a higher priority peer?
                SData response("STANDUP_RESPONSE");
                // Parrot back the node's attempt count so that it can differentiate stale responses.
                response["StateChangeCount"] = message["StateChangeCount"];

                // Reason we would deny, if we do.
                string reason;
                if (peer->permaFollower) {
                    // We think it's a permafollower, deny
                    PHMMM("Permafollower trying to stand up, denying.");
                    response["Response"] = "deny";
                    reason = "You're a permafollower";
                }

                // What's our state
                if (SWITHIN(STANDINGUP, _state, STANDINGDOWN)) {
                    // Oh crap, it's trying to stand up while we're leading. Who is higher priority?
                    if (peer->priority > _priority) {
                        // The other peer is a higher priority than us, so we should stand down (maybe it crashed, we
                        // came up as leader, and now it's been brought back up). We'll want to stand down here, but we
                        // do it gracefully so that we won't lose any transactions in progress.
                        if (_state == STANDINGUP) {
                            PWARN("Higher-priority peer is trying to stand up while we are STANDINGUP, SEARCHING.");
                            _changeState(SEARCHING);
                        } else if (_state == LEADING) {
                            PWARN("Higher-priority peer is trying to stand up while we are LEADING, STANDINGDOWN.");
                            _changeState(STANDINGDOWN);
                        } else {
                            PWARN("Higher-priority peer is trying to stand up while we are STANDINGDOWN, continuing.");
                        }
                    } else {
                        // Deny because we're currently in the process of leading and we're higher priority.
                        response["Response"] = "deny";
                        reason = "I am leading";

                        // Hmm, why is a lower priority peer trying to stand up? Is it possible we're no longer in
                        // control of the cluster? Let's see how many nodes are subscribed.
                        if (_majoritySubscribed()) {
                            // we have a majority of the cluster, so ignore this oddity.
                            PHMMM("Lower-priority peer is trying to stand up while we are " << stateName(_state)
                                  << " with a majority of the cluster; denying and ignoring.");
                        } else {
                            // We don't have a majority of the cluster -- maybe it knows something we don't?  For
                            // example, it could be that the rest of the cluster has forked away from us. This can
                            // happen if the leader hangs while processing a command: by the time it finishes, the
                            // cluster might have elected a new leader, forked, and be a thousand commits in the future.
                            // In this case, let's just reset everything anyway to be safe.
                            PWARN("Lower-priority peer is trying to stand up while we are " << stateName(_state)
                                  << ", but we don't have a majority of the cluster so reconnecting and SEARCHING.");
                            _reconnectAll();
                            // TODO: This puts us in an ambiguous state if we switch to SEARCHING from LEADING,
                            // without going through the STANDDOWN process. We'll need to handle it better, but it's
                            // unclear if this can ever happen at all. exit() may be a reasonable strategy here.
                            _changeState(SEARCHING);
                        }
                    }
                } else {
                    // Approve if nobody else is trying to stand up
                    response["Response"] = "approve"; // Optimistic; will override
                    for (auto otherPeer : _peerList) {
                        if (otherPeer != peer) {
                            // See if it's trying to be leader
                            if (otherPeer->state == STANDINGUP || otherPeer->state == LEADING || otherPeer->state == STANDINGDOWN) {
                                // We need to contest this standup
                                response["Response"] = "deny";
                                reason = "peer '" + otherPeer->name + "' is '" + stateName(otherPeer->state) + "'";
                                break;
                            }
                        }
                    }
                }

                // Send the response
                if (SIEquals(response["Response"], "approve")) {
                    PINFO("Approving standup request");
                } else {
                    PHMMM("Denying standup request because " << reason);
                }
                _sendToPeer(peer, response);
            } else if (from == STANDINGDOWN) {
                // STANDINGDOWN: When a peer stands down we double-check to make sure we don't have any outstanding
                // transaction (and if we do, we warn and rollback).
                if (!_db.getUncommittedHash().empty()) {
                    // Crap, we were waiting for a response that will apparently never come. I guess roll it back? This
                    // should never happen, however, as the leader shouldn't STANDOWN unless all subscribed followers
                    // (including us) have already unsubscribed, and we wouldn't do that in the middle of a
                    // transaction. But just in case...
                    SASSERTWARN(_state == FOLLOWING);
                    PWARN("Was expecting a response for transaction #"
                          << _db.getCommitCount() + 1 << " (" << _db.getUncommittedHash()
                          << ") but stood down prematurely, rolling back and hoping for the best.");
                    _db.rollback();
                }
            }
        }
    } else if (SIEquals(message.methodLine, "STANDUP_RESPONSE")) {
        // STANDUP_RESPONSE: Sent in response to the STATE message generated when a node enters the STANDINGUP state.
        // Contains a header "Response" with either the value "approve" or "deny".  This response is stored within the
        // peer for testing in the update loop.
        if (_state == STANDINGUP) {
            // We only verify this if it's present, which allows us to still receive valid STANDUP_RESPONSE
            // messages from peers on older versions. Once all nodes have been upgraded past the first version that
            // supports this, we can enforce that this count is present.
            if (message.isSet("StateChangeCount") && message.calc("StateChangeCount") != _stateChangeCount) {
                SHMMM("Received STANDUP_RESPONSE for old standup attempt (" << message.calc("StateChangeCount") << "), ignoring.");
                return;
            }
            if (!message.isSet("Response")) {
                STHROW("missing Response");
            }
            if (peer->standupResponse != SQLitePeer::Response::NONE) {
                PWARN("Already received standup response '" << peer->standupResponse << "', now receiving '"
                      << message["Response"] << "', odd -- multiple leaders competing?");
            }
            if (SIEquals(message["Response"], "approve")) {
                PINFO("Received standup approval");
                peer->standupResponse = SQLitePeer::Response::APPROVE;
            } else {
                PHMMM("Received standup denial: reason='" << message["Reason"] << "'");
                peer->standupResponse = SQLitePeer::Response::DENY;
            }
        } else {
            SINFO("Got STANDUP_RESPONSE but not STANDINGUP. Probably a late message, ignoring.");
        }
    } else if (SIEquals(message.methodLine, "SYNCHRONIZE")) {
        // If we're FOLLOWING, we'll let worker threads handle SYNCHRONIZATION messages. We don't on leader, because if
        // there's a backlog of commands, these can get stale, and by the time they reach the follower, it's already
        // behind, thus never catching up.
        if (_state == FOLLOWING) {
            // Attach all of the state required to populate a SYNCHRONIZE_RESPONSE to this message. All of this is
            // processed asynchronously, but that is fine, the final `SUBSCRIBE` message and its response will be
            // processed synchronously.
            SData request = message;
            uint64_t count = 0;
            string hash;
            peer->getCommit(count, hash);
            request["peerCommitCount"] = to_string(count);
            request["peerHash"] = hash;
            request["peerID"] = to_string(_getIDByPeer(peer));

            // The following properties are only used to expand out our log macros.
            request["name"] = _name;
            request["peerName"] = peer->name;

            // Create a command from this request and pass it on to the server to handle.
            auto command = make_unique<SQLiteCommand>(move(request));
            command->initiatingPeerID = peer->id;
            _server.acceptCommand(move(command), true);
        } else {
            // Otherwise we handle them immediately, as the server doesn't deliver commands to workers until we've
            // stood up.
            SData response("SYNCHRONIZE_RESPONSE");
            _queueSynchronize(this, peer, _db, response, false);
            _sendToPeer(peer, response);
        }
    } else if (SIEquals(message.methodLine, "SYNCHRONIZE_RESPONSE")) {
        // SYNCHRONIZE_RESPONSE: Sent in response to a SYNCHRONIZE request. Contains a payload of zero or more COMMIT
        // messages, all of which are immediately committed to the local database.
        if (_state != SYNCHRONIZING) {
            STHROW("not synchronizing");
        }
        if (!_syncPeer) {
            STHROW("too late, gave up on you");
        }
        if (peer != _syncPeer) {
            STHROW("sync peer mismatch");
        }
        PINFO("Beginning synchronization");
        try {
            // Received this synchronization response; are we done?
            _recvSynchronize(peer, message);
            uint64_t peerCommitCount = _syncPeer->commitCount;
            if (_db.getCommitCount() == peerCommitCount) {
                // All done
                SINFO("Synchronization complete, at commitCount #" << _db.getCommitCount() << " ("
                      << _db.getCommittedHash() << "), WAITING");
                _syncPeer = nullptr;
                _changeState(WAITING);
            } else if (_db.getCommitCount() > peerCommitCount) {
                // How did this happen?  Something is screwed up.
                SWARN("We have more data (" << _db.getCommitCount() << ") than our sync peer '" << _syncPeer->name
                      << "' (" << peerCommitCount << "), reconnecting and SEARCHING.");
                _reconnectPeer(_syncPeer);
                _syncPeer = nullptr;
                _changeState(SEARCHING);
            } else {
                // Otherwise, more to go
                SINFO("Synchronization underway, at commitCount #"
                      << _db.getCommitCount() << " (" << _db.getCommittedHash() << "), "
                      << peerCommitCount - _db.getCommitCount() << " to go.");
                _updateSyncPeer();
                if (_syncPeer) {
                    _sendToPeer(_syncPeer, SData("SYNCHRONIZE"));
                } else {
                    SWARN("No usable _syncPeer but syncing not finished. Going to SEARCHING.");
                    _changeState(SEARCHING);
                }

                // Also, extend our timeout so long as we're still alive
                _stateTimeout = STimeNow() + RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_S * 5;
            }
        } catch (const SException& e) {
            // Transaction failed
            SWARN("Synchronization failed '" << e.what() << "', reconnecting and re-SEARCHING.");
            _reconnectPeer(_syncPeer);
            _syncPeer = nullptr;
            _changeState(SEARCHING);
            throw e;
        }
    } else if (SIEquals(message.methodLine, "SUBSCRIBE")) {
        // SUBSCRIBE: Sent by a node in the WAITING state to the current leader to begin FOLLOWING. Respond
        // SUBSCRIPTION_APPROVED with any COMMITs that the subscribing peer lacks (for example, any commits that have
        // occurred after it completed SYNCHRONIZING but before this SUBSCRIBE was received). Tag this peer as
        // "subscribed" for use in the LEADING and STANDINGDOWN update loops. Finally, if there is an outstanding
        // distributed transaction being processed, send it to this new follower.
        if (_state != LEADING) {
            STHROW("not leading");
        }
        PINFO("Received SUBSCRIBE, accepting new follower");
        SData response("SUBSCRIPTION_APPROVED");
        _queueSynchronize(this, peer, _db, response, true); // Send everything it's missing
        _sendToPeer(peer, response);
        SASSERTWARN(!peer->subscribed);
        peer->subscribed = true;

        // New follower; are we in the midst of a transaction?
        if (_commitState == CommitState::COMMITTING) {
            // Invite the new peer to participate in the transaction
            SINFO("Inviting peer into distributed transaction already underway (" << _db.getUncommittedHash() << ")");

            // TODO: This duplicates code in `update()`, would be nice to refactor out the common code.
            uint64_t commitCount = _db.getCommitCount();
            SData transaction("BEGIN_TRANSACTION");
            SINFO("beginning distributed transaction for commit #" << commitCount + 1 << " ("
                  << _db.getUncommittedHash() << ")");
            transaction.set("NewCount", commitCount + 1);
            transaction.set("NewHash", _db.getUncommittedHash());
            transaction.set("leaderSendTime", to_string(STimeNow()));
            transaction.set("dbCountAtStart", to_string(_db.getDBCountAtStart()));
            transaction.set("ID", _lastSentTransactionID + 1);
            transaction.content = _db.getUncommittedQuery();
            _sendToPeer(peer, transaction);
        }
    } else if (SIEquals(message.methodLine, "SUBSCRIPTION_APPROVED")) {
        // SUBSCRIPTION_APPROVED: Sent by a follower's new leader to complete the subscription process. Includes zero or
        // more COMMITS that should be immediately applied to the database.
        if (_state != SUBSCRIBING) {
            STHROW("not subscribing");
        }
        if (_leadPeer != peer) {
            STHROW("not subscribing to you");
        }
        SINFO("Received SUBSCRIPTION_APPROVED, final synchronization.");
        try {
            // Done synchronizing
            _recvSynchronize(peer, message);
            SINFO("Subscription complete, at commitCount #" << _db.getCommitCount() << " (" << _db.getCommittedHash()
                  << "), FOLLOWING");
            _changeState(FOLLOWING);
        } catch (const SException& e) {
            // Transaction failed
            SWARN("Subscription failed '" << e.what() << "', reconnecting to leader and re-SEARCHING.");
            _reconnectPeer(_leadPeer);
            _changeState(SEARCHING);
            throw e;
        }
    } else if (SIEquals(message.methodLine, "BEGIN_TRANSACTION") || SIEquals(message.methodLine, "COMMIT_TRANSACTION") || SIEquals(message.methodLine, "ROLLBACK_TRANSACTION")) {
        if (_replicationThreadsShouldExit) {
            SINFO("Discarding replication message, stopping FOLLOWING");
        } else {
            auto threadID = _replicationThreadCount.fetch_add(1);
            SDEBUG("Spawning concurrent replicate thread (blocks until DB handle available): " << threadID);
            thread(&SQLiteNode::_replicate, this, peer, message, _dbPool->getIndex(false)).detach();
            SDEBUG("Done spawning concurrent replicate thread: " << threadID);
        }
    } else if (SIEquals(message.methodLine, "APPROVE_TRANSACTION") || SIEquals(message.methodLine, "DENY_TRANSACTION")) {
        // APPROVE_TRANSACTION: Sent to the leader by a follower when it confirms it was able to begin a transaction and
        // is ready to commit. Note that this peer approves the transaction for use in the LEADING and STANDINGDOWN
        // update loop.
        if (!message.isSet("ID")) {
            STHROW("missing ID");
        }
        if (!message.isSet("NewCount")) {
            STHROW("missing NewCount");
        }
        if (!message.isSet("NewHash")) {
            STHROW("missing NewHash");
        }
        if (_state != LEADING && _state != STANDINGDOWN) {
            STHROW("not leading");
        }
        SQLitePeer::Response response = SIEquals(message.methodLine, "APPROVE_TRANSACTION") ? SQLitePeer::Response::APPROVE : SQLitePeer::Response::DENY;
        try {
            // We ignore late approvals of commits that have already been finalized. They could have been committed
            // already, in which case `_lastSentTransactionID` will have incremented, or they could have been rolled
            // back due to a conflict, which would cuase them to have the wrong hash (the hash of the previous attempt
            // at committing the transaction with this ID).
            bool hashMatch = message["NewHash"] == _db.getUncommittedHash();
            if (hashMatch && to_string(_lastSentTransactionID + 1) == message["ID"]) {
                if (message.calcU64("NewCount") != _db.getCommitCount() + 1) {
                    STHROW("commit count mismatch. Expected: " + message["NewCount"] + ", but would actually be: "
                          + to_string(_db.getCommitCount() + 1));
                }
                if (peer->permaFollower) {
                    STHROW("permafollowers shouldn't approve/deny");
                }
                PINFO("Peer " << response << " transaction #" << message["NewCount"] << " (" << message["NewHash"] << ")");
                peer->transactionResponse = response;
            } else {
                // Old command.  Nothing to do.  We already sent a commit or rollback.
                PINFO("Peer '" << message.methodLine << "' transaction #" << message["NewCount"]
                      << " (" << message["NewHash"] << ") after " << (hashMatch ? "commit" : "rollback") << ".");
            }
        } catch (const SException& e) {
            // Doesn't correspond to the outstanding transaction not necessarily fatal. This can happen if, for
            // example, a command is escalated from/ one follower, approved by the second, but where the first follower dies
            // before the second's approval is received by the leader. In this case the leader will drop the command
            // when the initiating peer is lost, and thus won't have an outstanding transaction (or will be processing
            // a new transaction) when the old, outdated approval is received. Furthermore, in this case we will have
            // already sent a ROLLBACK, so it will already correct itself. If not, then we'll wait for the follower to
            // determine it's screwed and reconnect.
            SWARN("Received " << message.methodLine << " for transaction #"
                  << message.calc("NewCount") << " (" << message["NewHash"] << ", " << message["ID"] << ") but '"
                  << e.what() << "', ignoring.");
        }
    } else if (SIEquals(message.methodLine, "ESCALATE")) {
        // ESCALATE: Sent to the leader by a follower. Is processed like a normal command, except when complete an
        // ESCALATE_RESPONSE is sent to the follower that initiated the escalation.
        if (!message.isSet("ID")) {
            STHROW("missing ID");
        }
        if (_state != LEADING) {
            // Reject escalation because we're no longer leading
            if (_state != STANDINGDOWN) {
                // Don't warn if we're standing down, this is expected.
                PWARN("Received ESCALATE but not LEADING or STANDINGDOWN, aborting command.");
            }
            SData aborted("ESCALATE_ABORTED");
            aborted["ID"] = message["ID"];
            aborted["Reason"] = "not leading";
            _sendToPeer(peer, aborted);
        } else {
            // We're leading, make sure the rest checks out
            SData request;
            if (!request.deserialize(message.content)) {
                STHROW("malformed request");
            }
            if (!peer->subscribed) {
                STHROW("not subscribed");
            }
            if (!message.isSet("ID")) {
                STHROW("missing ID");
            }
            SAUTOPREFIX(request);
            PINFO("Received ESCALATE command for '" << message["ID"] << "' (" << request.methodLine << ")");

            // Create a new Command and send to the server.
            auto command = make_unique<SQLiteCommand>(move(request));
            command->initiatingPeerID = peer->id;
            command->id = message["ID"];
            _server.acceptCommand(move(command), true);
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_CANCEL")) {
        // ESCALATE_CANCEL: Sent to the leader by a follower. Indicates that the follower would like to cancel the escalated
        // command, such that it is not processed. For example, if the client that sent the original request
        // disconnects from the follower before an answer is returned, there is no value (and sometimes a negative value)
        // to the leader going ahead and completing it.
        if (!message.isSet("ID")) {
            STHROW("missing ID");
        }
        if (_state != LEADING) {
            // Reject escalation because we're no longer leading
            PWARN("Received ESCALATE_CANCEL but not LEADING, ignoring.");
        } else {
            // We're leading, make sure the rest checks out
            SData request;
            if (!request.deserialize(message.content)) {
                STHROW("malformed request");
            }
            if (!peer->subscribed) {
                STHROW("not subscribed");
            }
            if (!message.isSet("ID")) {
                STHROW("missing ID");
            }
            const string& commandID = SToLower(message["ID"]);
            PINFO("Received ESCALATE_CANCEL command for '" << commandID << "'");
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_RESPONSE")) {
        // ESCALATE_RESPONSE: Sent when the leader processes the ESCALATE.
        if (_state != FOLLOWING) {
            STHROW("not following");
        }
        if (!message.isSet("ID")) {
            STHROW("missing ID");
        }
        SData response;
        if (!response.deserialize(message.content)) {
            STHROW("malformed content");
        }

        // Go find the escalated command
        PINFO("Received ESCALATE_RESPONSE for '" << message["ID"] << "'");
        auto lock = _escalatedCommandMap.scopedLock();
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Process the escalated command response
            unique_ptr<SQLiteCommand>& command = commandIt->second;
            if (command->escalationTimeUS) {
                command->escalationTimeUS = STimeNow() - command->escalationTimeUS;
                SINFO("Total escalation time for command " << command->request.methodLine << " was "
                      << command->escalationTimeUS/1000 << "ms.");
            }
            command->response = response;
            command->complete = true;
            _server.acceptCommand(move(command), false);
            _escalatedCommandMap.erase(commandIt);
        } else {
            SHMMM("Received ESCALATE_RESPONSE for unknown command ID '" << message["ID"] << "', ignoring. ");
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_ABORTED")) {
        // ESCALATE_RESPONSE: Sent when the leader aborts processing an escalated command. Re-submit to the new leader.
        if (_state != FOLLOWING) {
            STHROW("not following");
        }
        if (!message.isSet("ID")) {
            STHROW("missing ID");
        }
        PINFO("Received ESCALATE_ABORTED for '" << message["ID"] << "' (" << message["Reason"] << ")");

        // Look for that command
        auto lock = _escalatedCommandMap.scopedLock();
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Re-queue this
            unique_ptr<SQLiteCommand>& command = commandIt->second;
            PINFO("Re-queueing command '" << message["ID"] << "' (" << command->request.methodLine << ") ("
                  << command->id << ")");
            _server.acceptCommand(move(command), false);
            _escalatedCommandMap.erase(commandIt);
        } else
            SWARN("Received ESCALATE_ABORTED for unescalated command " << message["ID"] << ", ignoring.");
    } else if (SIEquals(message.methodLine, "CRASH_COMMAND") || SIEquals(message.methodLine, "BROADCAST_COMMAND")) {
        // Create a new Command and send to the server.
        SData messageCopy = message;
        PINFO("Received " << message.methodLine << " command, forwarding to server.");
        _server.acceptCommand(make_unique<SQLiteCommand>(move(messageCopy)), true);
    } else {
        STHROW("unrecognized message");
    }
}

void SQLiteNode::_onConnect(SQLitePeer* peer) {
    SASSERT(peer);
    SASSERTWARN(!peer->loggedIn);
    // Send the LOGIN
    PINFO("Sending LOGIN");
    SData login("LOGIN");
    login["Priority"] = to_string(_priority);
    login["State"] = stateName(_state);
    login["Version"] = _version;
    login["Permafollower"] = _originalPriority ? "false" : "true";
    _sendToPeer(peer, login);
}

// --------------------------------------------------------------------------
// On Peer Disconnections
// --------------------------------------------------------------------------
// Whenever a peer disconnects, the following checks are made to verify no
// internal consistency has been lost:  (Technically these checks need only be
// made in certain states, but we'll check them in all states just to be sure.)
void SQLiteNode::_onDisconnect(SQLitePeer* peer) {
    SASSERT(peer);

    /// - Verify we didn't just lose contact with our leader.  This should
    ///   only be possible if we're SUBSCRIBING or FOLLOWING.  If we did lose our
    ///   leader, roll back any uncommitted transaction and go SEARCHING.
    ///
    if (peer == _leadPeer) {
        // We've lost our leader: make sure we aren't waiting for
        // transaction response and re-SEARCH
        PHMMM("Lost our LEADER, re-SEARCHING.");
        SASSERTWARN(_state == SUBSCRIBING || _state == FOLLOWING);
        {
            _leadPeer = nullptr;
        }
        if (!_db.getUncommittedHash().empty()) {
            // We're in the middle of a transaction and waiting for it to
            // approve or deny, but we'll never get its response.  Roll it
            // back and synchronize when we reconnect.
            PHMMM("Was expecting a response for transaction #" << _db.getCommitCount() + 1 << " ("
                                                               << _db.getUncommittedHash()
                                                               << ") but disconnected prematurely; rolling back.");
            _db.rollback();
        }

        // If there were escalated commands, give them back to the server to retry, unless it looks like they were in
        // progress when the leader died, in which case we say they completed with a 500 Error.
        {
            auto lock = _escalatedCommandMap.scopedLock();
            for (auto& cmd : _escalatedCommandMap) {
                _server.acceptCommand(move(cmd.second), false);
            }
            _escalatedCommandMap.clear();
        }
        _changeState(SEARCHING);
    }

    /// - Verify we didn't just lose contact with the peer we're synchronizing
    ///   with.  This should only be possible if we're SYNCHRONIZING.  If we did
    ///   lose our sync peer, give up and go back to SEARCHING.
    ///
    if (peer == _syncPeer) {
        // Synchronization failed
        PHMMM("Lost our synchronization peer, re-SEARCHING.");
        SASSERTWARN(_state == SYNCHRONIZING);
        _syncPeer = nullptr;
        _changeState(SEARCHING);
    }

    // If we're leader, but we've lost quorum, we can't commit anything, nor can worker threads. We need to drop out of
    // a state that implies we can perform commits, and cancel any outstanding commits.
    if (_state == LEADING || _state == STANDINGUP || _state == STANDINGDOWN) {
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        for (auto otherPeer : _peerList) {
            // Skip the current peer, it no longer counts.
            if (otherPeer == peer) {
                continue;
            }
            // Make sure we're a full peer
            if (!otherPeer->permaFollower) {
                // Verify we're logged in
                ++numFullPeers;
                if (otherPeer->loggedIn) {
                    // Verify we're still fresh
                    ++numLoggedInFullPeers;
                }
            }
        }

        // If we've fallen below the minimum amount of peers required to control the database, we need to stop
        // committing things.
        if (numLoggedInFullPeers * 2 < numFullPeers) {
            // This works for workers, as they block on the state mutex to finish commits, so they've either already
            // completed, or they won't be able to until after this changes, and then they'll see the wrong state.
            //
            // It works for the sync thread as well, as there's handling in _changeState to rollback a commit when
            // dropping out of leading or standing down (and there can't be commits in progress in other states).
            SWARN("We were " << stateName(_state) << " but lost quorum. Going to SEARCHING.");
            _changeState(SEARCHING);
        }
    }
}

SData SQLiteNode::_addPeerHeaders(SData message) {
    if (!message.isSet("CommitCount")) {
        message["CommitCount"] = SToStr(_db.getCommitCount());
    }
    if (!message.isSet("Hash")) {
        message["Hash"] = _db.getCommittedHash();
    }
    message["commandAddress"] = _commandAddress;
    return message;
}

void SQLiteNode::_sendToPeer(SQLitePeer* peer, const SData& message) {
    // We can treat this whole function as atomic and thread-safe as it sends data to a peer with it's own atomic
    // `sendMessage` and the peer itself (assuming it's something from _peerList, which, if not, don't do that) is
    // const and will exist without changing until destruction.
    peer->sendMessage(_addPeerHeaders(message).serialize());
}

void SQLiteNode::_sendToAllPeers(const SData& message, bool subscribedOnly) {
    const string serializedMessage = _addPeerHeaders(message).serialize();

    // Loop across all connected peers and send the message. _peerList is const so this is thread-safe.
    for (auto peer : _peerList) {
        // This check is strictly thread-safe, as SQLitePeer::subscribed is atomic, but there's still a race condition
        // around checking subscribed and then sending, as subscribed could technically change.
        if (subscribedOnly && peer->subscribed) {
            peer->sendMessage(_addPeerHeaders(serializedMessage).serialize());
        }
    }
}

void SQLiteNode::broadcast(const SData& message, SQLitePeer* peer) {
    // TODO: this gets called with the following broken stack:
    // std::__throw_system_error(int) [0x7f8781e4d73f]
    // SQLiteNode::broadcast(SData const&, SQLitePeer*) [0x55f16c8b295a]
    // BedrockServer::onNodeLogin(SQLitePeer*) [0x55f16c7f9f3d]
    // SQLiteNode::_onMESSAGE(SQLitePeer*, SData const&) [0x55f16c8fd448]
    // SQLiteNode::postPoll(std::map<int, pollfd, std::less<int>, std::allocator<std::pair<int const, pollfd> > >&, unsigned long&) [0x55f16c912381]

    // unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);
    if (peer) {
        SINFO("Sending broadcast: " << message.serialize() << " to peer: " << peer->name);
        _sendToPeer(peer, message);
    } else {
        SINFO("Sending broadcast: " << message.serialize());
        _sendToAllPeers(message, false);
    }
}

void SQLiteNode::_changeState(SQLiteNode::State newState) {
    SINFO("[NOTIFY] setting commit count to: " << _db.getCommitCount());
    _localCommitNotifier.notifyThrough(_db.getCommitCount());

    if (newState != _state) {
        // If we were following, and now we're not, we give up an any replications.
        if (_state == FOLLOWING) {
            _replicationThreadsShouldExit = true;
            uint64_t cancelAfter = _leaderCommitNotifier.getValue();
            SINFO("Replication threads should exit, canceling commits after current leader commit " << cancelAfter);
            _localCommitNotifier.cancel(cancelAfter);
            _leaderCommitNotifier.cancel(cancelAfter);

            // Polling wait for threads to quit. This could use a notification model such as with a condition_variable,
            // which would probably be "better" but introduces yet more state variables for a state that we're rarely
            // in, and so I've left it out for the time being.
            while (_replicationThreadCount) {
                usleep(10'000);
            }

            // Done exiting. Reset so that we can resume FOLLOWING in the future.
            _replicationThreadsShouldExit = false;

            // Guaranteed to be done right now.
            _localCommitNotifier.reset();
            _leaderCommitNotifier.reset();

            // We have no leader anymore.
            _leadPeer = nullptr;
        }

        // Depending on the state, set a timeout
        SINFO("Switching from '" << stateName(_state) << "' to '" << stateName(newState) << "'");
        uint64_t timeout = 0;
        if (newState == STANDINGUP) {
            // If two nodes try to stand up simultaneously, they can get in a conflicted state where they're waiting
            // for the other to respond, but neither sends a response. We want a short timeout on this state.
            // TODO: Maybe it would be better to re-send the message indicating we're standing up when we see someone
            // hasn't responded.
            timeout = STIME_US_PER_S * 5 + SRandom::rand64() % STIME_US_PER_S * 5;
        } else if (newState == SEARCHING || newState == SUBSCRIBING || newState == SYNCHRONIZING) {
            timeout = RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_S * 5;
        } else {
            timeout = 0;
        }
        SDEBUG("Setting state timeout of " << timeout / 1000 << "ms");
        _stateTimeout = STimeNow() + timeout;

        // Additional logic for some old states
        if (SWITHIN(LEADING, _state, STANDINGDOWN) && !SWITHIN(LEADING, newState, STANDINGDOWN)) {
            // We are no longer leading.  Are we processing a command?
            if (commitInProgress()) {
                // Abort this command
                SWARN("Stopping LEADING/STANDINGDOWN with commit in progress. Canceling.");
                _commitState = CommitState::FAILED;
                _db.rollback();
            }

            // Turn off commits. This prevents late commits coming in right after we call `_sendOutstandingTransactions`
            // below, which otherwise could get committed on leader and not replicated to followers.
            _db.setCommitEnabled(false);

            // We send any unsent transactions here before we finish switching states, we need to make sure these are
            // all sent to the new leader before we complete the transition.
            _sendOutstandingTransactions();
        }

        // Clear some state if we can
        if (newState < SUBSCRIBING) {
            // We're no longer SUBSCRIBING or FOLLOWING, so we have no leader
            _leadPeer = nullptr;
        }

        // Re-enable commits if they were disabled during a previous stand-down.
        if (newState != SEARCHING) {
            _db.setCommitEnabled(true);
        }

        // Additional logic for some new states
        if (newState == LEADING) {
            // Seed our last sent transaction.
            {
                // Clear these.
                _db.popCommittedTransactions();
                _lastSentTransactionID = _db.getCommitCount();
            }
        } else if (newState == STANDINGDOWN) {
            // start the timeout countdown.
            _standDownTimeout.alarmDuration = STIME_US_PER_S * 30; // 30s timeout before we give up
            _standDownTimeout.start();

            // Abort all remote initiated commands if no longer LEADING
            // TODO: No we don't, we finish it, as per other documentation in this file.
        } else if (newState == SEARCHING) {
            auto lock = _escalatedCommandMap.scopedLock();
            if (!_escalatedCommandMap.empty()) {
                // This isn't supposed to happen, though we've seen in logs where it can.
                // So what we'll do is try and correct the problem and log the state we're coming from to see if that
                // gives us any more useful info in the future.
                _escalatedCommandMap.clear();
                SWARN("Switching from '" << stateName(_state) << "' to '" << stateName(newState)
                      << "' but _escalatedCommandMap not empty. Clearing it and hoping for the best.");
            }
        } else if (newState == WAITING) {
            // The first time we enter WAITING, we're caught up and ready to join the cluster - use our real priority from now on
            _priority = _originalPriority;
        }

        // Send to everyone we're connected to, whether or not
        // we're "LoggedIn" (else we might change state after sending LOGIN,
        // but before we receive theirs, and they'll miss it).
        // Broadcast the new state
        _state = newState;
        SData state("STATE");
        state["StateChangeCount"] = to_string(++_stateChangeCount);
        state["State"] = stateName(_state);
        state["Priority"] = SToStr(_priority);
        _sendToAllPeers(state);
    }
}

void SQLiteNode::_queueSynchronize(SQLiteNode* node, SQLitePeer* peer, SQLite& db, SData& response, bool sendAll) {
    // We need this to check the state of the node, and we also need `name` to make the logging macros work in a static
    // function. However, if you pass a null pointer here, we can't set these, so we'll fail. We also can't log that,
    // so we are just going to rely on the signal handling for sigsegv to log that for you. Don't do that.
    auto _state = node->_state.load();
    auto _name = node->_name;

    uint64_t peerCommitCount = 0;
    string peerHash;
    peer->getCommit(peerCommitCount, peerHash);
    if (peerCommitCount > db.getCommitCount())
        STHROW("you have more data than me");
    if (peerCommitCount) {
        // It has some data -- do we agree on what we share?
        string myHash, ignore;
        if (!db.getCommit(peerCommitCount, ignore, myHash)) {
            PWARN("Error getting commit for peer's commit: " << peerCommitCount << ", my commit count is: " << db.getCommitCount());
            STHROW("error getting hash");
        }
        if (myHash != peerHash) {
            SWARN("Hash mismatch. Peer at commit:" << peerCommitCount << " with hash " << peerHash
                  << ", but we have hash: " << myHash << " for that commit.");
            STHROW("hash mismatch");
        }
        PINFO("Latest commit hash matches our records, beginning synchronization.");
    } else {
        PINFO("Peer has no commits, beginning synchronization.");
    }

    // We agree on what we share, do we need to give it more?
    SQResult result;

    // Because this is used for both SYNCHRONIZE_RESPONSE and SUBSCRIPTION_APPROVED messages, we need to be careful.
    // The commitCount can change at any time, and on LEADER, we need to make sure we don't send the same transaction
    // twice, where _lastSentTransactionID only changes in the sync thread. From followers serving SYNCHRONIZE
    // requests, they can always serve their entire DB, there's no point at which they risk double-sending data.
    uint64_t targetCommit = (_state == LEADING || _state == STANDINGDOWN) ? node->_lastSentTransactionID : db.getCommitCount();
    if (peerCommitCount == targetCommit) {
        // Already synchronized; nothing to send
        PINFO("Peer is already synchronized");
        response["NumCommits"] = "0";
    } else {
        // Figure out how much to send it
        uint64_t fromIndex = peerCommitCount + 1;
        uint64_t toIndex = targetCommit;
        if (!sendAll)
            toIndex = min(toIndex, fromIndex + 100); // 100 transactions at a time
        if (!db.getCommits(fromIndex, toIndex, result))
            STHROW("error getting commits");
        if ((uint64_t)result.size() != toIndex - fromIndex + 1)
            STHROW("mismatched commit count");

        // Wrap everything into one huge message
        PINFO("Synchronizing commits from " << peerCommitCount + 1 << "-" << targetCommit);
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

void SQLiteNode::_recvSynchronize(SQLitePeer* peer, const SData& message) {
    SASSERT(peer);
    // Walk across the content and commit in order
    if (!message.isSet("NumCommits"))
        STHROW("missing NumCommits");
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
            STHROW("expecting COMMIT");
        if (!commit.isSet("CommitIndex"))
            STHROW("missing CommitIndex");
        if (commit.calc64("CommitIndex") < 0)
            STHROW("invalid CommitIndex");
        if (!commit.isSet("Hash"))
            STHROW("missing Hash");
        if (commit.content.empty())
            SALERT("Synchronized blank query");
        if (commit.calcU64("CommitIndex") != _db.getCommitCount() + 1)
            STHROW("commit index mismatch");

        // This block repeats until we successfully commit, or throw out of it.
        // This allows us to retry in the event we're interrupted for a checkpoint. This should only happen once,
        // because the second try will be blocked on the checkpoint.
        while (true) {
            try {
                if (!_db.beginTransaction()) {
                    STHROW("failed to begin transaction");
                }

                // Inside a transaction; get ready to back out if an error
                if (!_db.writeUnmodified(commit.content)) {
                    STHROW("failed to write transaction");
                }
                if (!_db.prepare()) {
                    STHROW("failed to prepare transaction");
                }

                // Done, break out of `while (true)`.
                break;
            } catch (const SException& e) {
                // Transaction failed, clean up
                SERROR("Can't synchronize (" << e.what() << "); shutting down.");
                // **FIXME: Remove the above line once we can automatically handle?
                _db.rollback();
                throw e;
            }
        }

        // Transaction succeeded, commit and go to the next
        SDEBUG("Committing current transaction because _recvSynchronize: " << _db.getUncommittedQuery());
        _db.commit(stateName(_state));

        // Should work here.
        SINFO("[NOTIFY] setting commit count to: " << _db.getCommitCount());
        _localCommitNotifier.notifyThrough(_db.getCommitCount());

        if (_db.getCommittedHash() != commit["Hash"])
            STHROW("potential hash mismatch");
        --commitsRemaining;
    }

    // Did we get all our commits?
    if (commitsRemaining)
        STHROW("commits remaining at end");
}

void SQLiteNode::_updateSyncPeer()
{
    SQLitePeer* newSyncPeer = nullptr;
    uint64_t commitCount = _db.getCommitCount();
    for (auto peer : _peerList) {
        // If either of these conditions are true, then we can't use this peer.
        if (!peer->loggedIn || peer->commitCount <= commitCount) {
            continue;
        }

        // Any peer that makes it to here is a usable peer, so it's by default better than nothing.
        if (!newSyncPeer) {
            newSyncPeer = peer;
        }
        // If the previous best peer and this one have the same latency (meaning they're probably both 0), the best one
        // is the one with the highest commit count.
        else if (newSyncPeer->latency == peer->latency) {
            if (peer->commitCount > newSyncPeer->commitCount) {
                newSyncPeer = peer;
            }
        }
        // If the existing best has no latency, then this peer is faster (because we just checked if they're equal and
        // 0 is the slowest latency value).
        else if (newSyncPeer->latency == 0) {
            newSyncPeer = peer;
        }
        // Finally, if this peer is faster than the best, but not 0 itself, it's the new best.
        else if (peer->latency != 0 && peer->latency < newSyncPeer->latency) {
            newSyncPeer = peer;
        }
    }

    // Log that we've changed peers.
    if (_syncPeer != newSyncPeer) {
        string from, to;
        if (_syncPeer) {
            from = _syncPeer->name + " (commit count=" + to_string(_syncPeer->commitCount) + "), latency="
                                   + to_string(_syncPeer->latency/1000) + "ms";
        } else {
            from = "(NONE)";
        }
        if (newSyncPeer) {
            to = newSyncPeer->name + " (commit count=" + to_string(newSyncPeer->commitCount) + "), latency="
                                   + to_string(newSyncPeer->latency/1000) + "ms";
        } else {
            to = "(NONE)";
        }

        // We see strange behavior when choosing peers. Peers are being chosen from distant data centers rather than
        // peers on the same LAN. This is extra diagnostic info to try and see why we don't choose closer ones.
        list<string> nonChosenPeers;
        for (auto peer : _peerList) {
            if (peer == newSyncPeer || peer == _syncPeer) {
                continue; // These ones we're already logging.
            } else if (!peer->loggedIn) {
                nonChosenPeers.push_back(peer->name + ":!loggedIn");
            } else if (peer->commitCount <= commitCount) {
                nonChosenPeers.push_back(peer->name + ":commit=" + to_string(peer->commitCount));
            } else {
                nonChosenPeers.push_back(peer->name + ":" + to_string(peer->latency/1000) + "ms");
            }
        }
        SINFO("Updating SYNCHRONIZING peer from " << from << " to " << to << ". Not chosen: " << SComposeList(nonChosenPeers));

        // And save the new sync peer internally.
        _syncPeer = newSyncPeer;
    }
}

void SQLiteNode::_reconnectPeer(SQLitePeer* peer) {
    SHMMM("Reconnecting to '" << peer->name << "'");
    peer->loggedIn = false;
    peer->shutdownSocket();
}

void SQLiteNode::_reconnectAll() {
    // Loop across and reconnect
    for (auto peer : _peerList) {
        _reconnectPeer(peer);
    }
}

bool SQLiteNode::_majoritySubscribed() const {
    // Count up how may full and subscribed peers we have (A "full" peer is one that *isn't* a permafollower).
    int numFullPeers = 0;
    int numFullFollowers = 0;
    for (auto peer : _peerList) {
        if (!peer->permaFollower) {
            ++numFullPeers;
            if (peer->subscribed) {
                ++numFullFollowers;
            }
        }
    }

    // Done!
    return (numFullFollowers * 2 >= numFullPeers);
}

bool SQLiteNode::peekPeerCommand(shared_ptr<SQLiteNode> node, SQLite& db, SQLiteCommand& command)
{
    if (!node) {
        // Node deleted while trying to peek peer command, just pretend it worked.
        return true;
    }

    SQLitePeer* peer = nullptr;
    try {
        if (SIEquals(command.request.methodLine, "SYNCHRONIZE")) {
            peer = node->_getPeerByID(SToUInt64(command.request["peerID"]));
            if (!peer) {
                // There's nobody to send to, but this was a valid command that's been handled.
                return true;
            }
            command.response.methodLine = "SYNCHRONIZE_RESPONSE";

            // Because we hold a sharedPtr to the node, it can't delete any peers (because it only does at
            // destruction), and since our peers our thread-safe, we can run this just fine.
            // TODO: Consider the thread-safety of this.
            _queueSynchronize(node.get(), peer, db, command.response, false);

            // The following two lines are copied from `_sendToPeer`.
            command.response["CommitCount"] = to_string(db.getCommitCount());
            command.response["Hash"] = db.getCommittedHash();
            peer->sendMessage(command.response);
            return true;
        }
    } catch (const SException& e) {
        if (peer) {
            // Any failure causes the response to in initiate a reconnect, if we got a peer.
            command.response.methodLine = "RECONNECT";
            command.response["Reason"] = e.what();
            peer->sendMessage(command.response);
        }

        // If we even got here, then it must have been a peer command, so we'll call it complete.
        return true;
    }
    return false;
}

void SQLiteNode::_handleBeginTransaction(SQLite& db, SQLitePeer* peer, const SData& message, bool wasConflict) {
    // BEGIN_TRANSACTION: Sent by the LEADER to all subscribed followers to begin a new distributed transaction. Each
    // follower begins a local transaction with this query and responds APPROVE_TRANSACTION. If the follower cannot start
    // the transaction for any reason, it is broken somehow -- disconnect from the leader.
    // **FIXME**: What happens if LEADER steps down before sending BEGIN?
    // **FIXME**: What happens if LEADER steps down or disconnects after BEGIN?
    if (_state != FOLLOWING) {
        STHROW("not following");
    }
    if (!db.getUncommittedHash().empty()) {
        STHROW("already in a transaction");
    }

    // This block repeats until we successfully commit, or error out of it.
    // This allows us to retry in the event we're interrupted for a checkpoint. This should only happen once,
    // because the second try will be blocked on the checkpoint.
    while (true) {
        try {
            // If we are running this after a conflict, we'll grab an exclusive lock here. This makes no practical
            // difference in replication, as transactions must commit in order, thus if we've failed one commit, nobody
            // else can attempt to commit anyway, but this logs our time spent in the commit mutex in EXCLUSIVE rather
            // than SHARED mode.
            if (!db.beginTransaction(wasConflict ? SQLite::TRANSACTION_TYPE::EXCLUSIVE : SQLite::TRANSACTION_TYPE::SHARED)) {
                STHROW("failed to begin transaction");
            }

            // Inside transaction; get ready to back out on error
            if (!db.writeUnmodified(message.content)) {
                STHROW("failed to write transaction");
            }

            // Done, break out of `while (true)`.
            break;
        } catch (const SException& e) {
            // Something caused a write failure.
            SALERT(e.what());
            db.rollback();

            // This is a fatal error case.
            break;
        }
    }
}

void SQLiteNode::_handlePrepareTransaction(SQLite& db, SQLitePeer* peer, const SData& message) {
    // BEGIN_TRANSACTION: Sent by the LEADER to all subscribed followers to begin a new distributed transaction. Each
    // follower begins a local transaction with this query and responds APPROVE_TRANSACTION. If the follower cannot start
    // the transaction for any reason, it is broken somehow -- disconnect from the leader.
    // **FIXME**: What happens if LEADER steps down before sending BEGIN?
    // **FIXME**: What happens if LEADER steps down or disconnects after BEGIN?
    bool success = true;
    uint64_t leaderSentTimestamp = message.calcU64("leaderSendTime");
    uint64_t followerDequeueTimestamp = STimeNow();
    if (!message.isSet("ID")) {
        STHROW("missing ID");
    }
    if (!message.isSet("NewCount")) {
        STHROW("missing NewCount");
    }
    if (!message.isSet("NewHash")) {
        STHROW("missing NewHash");
    }
    if (_state != FOLLOWING) {
        STHROW("not following");
    }

    // This block repeats until we successfully commit, or error out of it.
    // This allows us to retry in the event we're interrupted for a checkpoint. This should only happen once,
    // because the second try will be blocked on the checkpoint.
    while (true) {
        try {
            // This will grab the commit lock and hold it until we commit or rollback.
            if (!db.prepare()) {
                STHROW("failed to prepare transaction");
            }

            // Done, break out of `while (true)`.
            break;
        } catch (const SException& e) {
            // Something caused a write failure.
            success = false;
            db.rollback();

            // This is a fatal error case.
            break;
        }
    }

    // Are we participating in quorum?
    if (_priority) {
        // If the ID is /ASYNC_\d+/, no need to respond, leader will ignore it anyway.
        string verb = success ? "APPROVE_TRANSACTION" : "DENY_TRANSACTION";
        if (!SStartsWith(message["ID"], "ASYNC_")) {
            // Not a permafollower, approve the transaction
            PINFO(verb << " #" << db.getCommitCount() + 1 << " (" << message["NewHash"] << ").");
            SData response(verb);
            response["NewCount"] = SToStr(db.getCommitCount() + 1);
            response["NewHash"] = success ? db.getUncommittedHash() : message["NewHash"];
            response["ID"] = message["ID"];
            if (!_leadPeer) {
                STHROW("no leader?");
            }
            _sendToPeer(_leadPeer, response);
        } else {
            SDEBUG("Skipping " << verb << " for ASYNC command.");
        }
    } else {
        PINFO("Would approve/deny transaction #" << db.getCommitCount() + 1 << " (" << db.getUncommittedHash()
              << "), but a permafollower -- keeping quiet.");
    }
    uint64_t transitTimeUS = followerDequeueTimestamp - leaderSentTimestamp;
    uint64_t applyTimeUS = STimeNow() - followerDequeueTimestamp;
    float transitTimeMS = (float)transitTimeUS / 1000.0;
    float applyTimeMS = (float)applyTimeUS / 1000.0;
    PINFO("Replicated transaction " << message.calcU64("NewCount") << ", sent by leader at " << leaderSentTimestamp
          << ", transit/dequeue time: " << transitTimeMS << "ms, applied in: " << applyTimeMS << "ms, should COMMIT next.");
}

int SQLiteNode::_handleCommitTransaction(SQLite& db, SQLitePeer* peer, const uint64_t commandCommitCount, const string& commandCommitHash) {
    // COMMIT_TRANSACTION: Sent to all subscribed followers by the leader when it determines that the current
    // outstanding transaction should be committed to the database. This completes a given distributed transaction.
    if (_state != FOLLOWING) {
        STHROW("not following");
    }
    if (db.getUncommittedHash().empty()) {
        STHROW("no outstanding transaction");
    }
    if (commandCommitCount != db.getCommitCount() + 1) {
        STHROW("commit count mismatch. Expected: " + to_string(commandCommitCount) + ", but would actually be: "
              + to_string(db.getCommitCount() + 1));
    }
    if (commandCommitHash != db.getUncommittedHash()) {
        STHROW("hash mismatch:" + commandCommitHash + "!=" + db.getUncommittedHash() + ";");
    }

    SDEBUG("Committing current transaction because COMMIT_TRANSACTION: " << db.getUncommittedQuery());
    int result = db.commit(stateName(_state));
    if (result == SQLITE_BUSY_SNAPSHOT) {
        // conflict, bail out early.
        return result;
    }

    // Clear the list of committed transactions. We're following, so we don't need to send these.
    db.popCommittedTransactions();

    // Log timing info.
    // TODO: This is obsolete and replaced by timing info in BedrockCommand. This should be removed.
    uint64_t beginElapsed, readElapsed, writeElapsed, prepareElapsed, commitElapsed, rollbackElapsed;
    uint64_t totalElapsed = db.getLastTransactionTiming(beginElapsed, readElapsed, writeElapsed, prepareElapsed,
                                                         commitElapsed, rollbackElapsed);
    SINFO("Committed follower transaction #" << to_string(commandCommitCount) << " (" << commandCommitHash << ") in "
          << totalElapsed / 1000 << " ms (" << beginElapsed / 1000 << "+"
          << readElapsed / 1000 << "+" << writeElapsed / 1000 << "+"
          << prepareElapsed / 1000 << "+" << commitElapsed / 1000 << "+"
          << rollbackElapsed / 1000 << "ms)");

    return result;
}

void SQLiteNode::_handleRollbackTransaction(SQLite& db, SQLitePeer* peer, const SData& message) {
    // ROLLBACK_TRANSACTION: Sent to all subscribed followers by the leader when it determines that the current
    // outstanding transaction should be rolled back. This completes a given distributed transaction.
    if (!message.isSet("ID")) {
        STHROW("missing ID");
    }
    if (_state != FOLLOWING) {
        STHROW("not following");
    }
    if (db.getUncommittedHash().empty()) {
        SINFO("Received ROLLBACK_TRANSACTION with no outstanding transaction.");
    }
    db.rollback();
}

SQLiteNode::State SQLiteNode::leaderState() const {
    shared_lock<decltype(_stateMutex)> sharedLock(_stateMutex);
    if (_leadPeer) {
        return _leadPeer.load()->state;
    }
    return State::UNKNOWN;
}

string SQLiteNode::leaderCommandAddress() const {
    shared_lock<decltype(_stateMutex)> sharedLock(_stateMutex);
    if (_leadPeer && _leadPeer.load()->state == State::LEADING) {
        return _leadPeer.load()->commandAddress;
    }
    return "";
}

bool SQLiteNode::hasQuorum() const {
    shared_lock<decltype(_stateMutex)> sharedLock(_stateMutex);
    if (_state != LEADING && _state != STANDINGDOWN) {
        return false;
    }
    int numFullPeers = 0;
    int numFullFollowers = 0;
    for (auto peer : _peerList) {
        if (!peer->permaFollower) {
            ++numFullPeers;
            if (peer->subscribed) {
                numFullFollowers++;
            }
        }
    }
    return (numFullFollowers * 2 >= numFullPeers);
}

void SQLiteNode::prePoll(fd_map& fdm) const {
    shared_lock<decltype(_stateMutex)> sharedLock(_stateMutex);
    if (_port) {
        SFDset(fdm, _port->s, SREADEVTS);
    }
    for (auto& s : _socketList) {
        STCPManager::prePoll(fdm, *s);
    }
    _commitsToSend.prePoll(fdm);
}

STCPManager::Socket* SQLiteNode::_acceptSocket() {
    // Initialize to 0 in case we don't accept anything. Note that this *does* overwrite the passed-in pointer.
    Socket* socket = nullptr;

    // Try to accept on the port and wrap in a socket
    sockaddr_in addr;
    int s = S_accept(_port->s, addr, false);
    if (s > 0) {
        // Received a socket, wrap
        SDEBUG("Accepting socket from '" << addr << "' on port '" << _port->host << "'");
        socket = new Socket(s, Socket::CONNECTED);
        socket->addr = addr;
        // Pretty sure these leak.
        _socketList.push_back(socket);

        // Try to read immediately
        S_recvappend(socket->s, socket->recvBuffer);
    }

    return socket;
}

void SQLiteNode::postPoll(fd_map& fdm, uint64_t& nextActivity) {
    unique_lock<decltype(_stateMutex)> uniqueLock(_stateMutex);
    // Process the sockets
    for (auto& s : _socketList) {
        STCPManager::postPoll(fdm, *s);
    }

    // Accept any new peers
    Socket* socket = nullptr;
    while ((socket = _acceptSocket())) {
        _acceptedSocketList.push_back(socket);
    }

    // Process the incoming sockets
    list<Socket*>::iterator nextSocketIt = _acceptedSocketList.begin();
    while (nextSocketIt != _acceptedSocketList.end()) {
        // See if we've logged in (we know we're already connected because
        // we're accepting an inbound connection)
        list<Socket*>::iterator socketIt = nextSocketIt++;
        Socket* socket = *socketIt;
        try {
            // Verify it's still alive
            if (socket->state.load() != Socket::CONNECTED)
                STHROW("premature disconnect");

            // Still alive; try to login
            SData message;
            int messageSize = message.deserialize(socket->recvBuffer);
            if (messageSize) {
                // What is it?
                socket->recvBuffer.consumeFront(messageSize);
                if (SIEquals(message.methodLine, "NODE_LOGIN")) {
                    // Got it -- can we associate with a peer?
                    bool foundIt = false;
                    for (SQLitePeer* peer : _peerList) {
                        // Just match any unconnected peer
                        // **FIXME: Authenticate and match by public key
                        if (peer->name == message["Name"]) {
                            // Found it!  Are we already connected?
                            if (peer->setSocket(socket)) {
                                // Attach to this peer and LOGIN
                                PINFO("Attached incoming socket");
                                _acceptedSocketList.erase(socketIt);
                                foundIt = true;

                                // Send our own PING back so we can estimate latency
                                _sendPING(peer);

                                // Let the child class do its connection logic
                                _onConnect(peer);
                                break;
                            } else
                                STHROW("already connected");
                        }
                    }

                    // Did we find it?
                    if (!foundIt) {
                        // This node wasn't expected
                        SWARN("Unauthenticated node '" << message["Name"] << "' attempted to connected, rejecting.");
                        STHROW("unauthenticated node");
                    }
                } else
                    STHROW("expecting NODE_LOGIN");
            }
        } catch (const SException& e) {
            // Died prematurely
            if (socket->recvBuffer.empty() && socket->sendBufferEmpty()) {
                SDEBUG("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), empty buffers");
            } else {
                SWARN("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), send='" << socket->sendBufferCopy() << "'");
            }
            _socketList.remove(socket);
            _acceptedSocketList.erase(socketIt);
            delete socket;
        }
    }

    // Try to establish connections with peers and process messages
    for (SQLitePeer* peer : _peerList) {
        // See if we're connected
        if (peer->socket) {
            // We have a socket; process based on its state
            switch (peer->socket->state.load()) {
            case Socket::CONNECTED: {
                // See if there is anything new.
                SData message;
                int messageSize = 0;
                try {
                    // peer->socket->lastRecvTime is always set, it's initialized to STimeNow() at creation.
                    if (peer->socket->lastRecvTime + RECV_TIMEOUT < STimeNow()) {
                        // Reset and reconnect.
                        SHMMM("Connection with peer '" << peer->name << "' timed out.");
                        STHROW("Timed Out!");
                    }

                    // Send PINGs 5s before the socket times out
                    if (STimeNow() - peer->socket->lastSendTime > RECV_TIMEOUT - 5 * STIME_US_PER_S) {
                        // Let's not delay on flushing the PING PONG exchanges
                        // in case we get blocked before we get to flush later.
                        SINFO("Sending PING to peer '" << peer->name << "'");
                        _sendPING(peer);
                    }

                    // Process all messages
                    while ((messageSize = message.deserialize(peer->socket->recvBuffer))) {
                        // Which message?
                        peer->socket->recvBuffer.consumeFront(messageSize);
                        if (peer->socket->recvBuffer.size() > 10'000) {
                            // Make in known if this buffer ever gets big.
                            PINFO("Received '" << message.methodLine << "'(size: " << messageSize << ") with " 
                                  << (peer->socket->recvBuffer.size()) << " bytes remaining in message buffer.");
                        } else {
                            PDEBUG("Received '" << message.methodLine << "'.");
                        }
                        if (SIEquals(message.methodLine, "PING")) {
                            // Let's not delay on flushing the PING PONG
                            // exchanges in case we get blocked before we
                            // get to flush later.  Pass back the remote
                            // timestamp of the PING such that the remote
                            // host can calculate latency.
                            SINFO("Received PING from peer '" << peer->name << "'. Sending PONG.");
                            SData pong("PONG");
                            pong["Timestamp"] = message["Timestamp"];
                            peer->socket->send(pong.serialize());
                        } else if (SIEquals(message.methodLine, "PONG")) {
                            // Recevied the PONG; update our latency estimate for this peer.
                            // We set a lower bound on this at 1, because even though it should be pretty impossible
                            // for this to be 0 (it's in us), we rely on it being non-zero in order to connect to
                            // peers.
                            peer->latency = max(STimeNow() - message.calc64("Timestamp"), (uint64_t)1);
                            SINFO("Received PONG from peer '" << peer->name << "' (" << peer->latency/1000 << "ms latency)");
                        } else {
                            // Not a PING or PONG; pass to the child class
                            _onMESSAGE(peer, message);
                        }
                    }
                } catch (const SException& e) {
                    // Warn if the message is set. Otherwise, the error is that we got no message (we timed out), just
                    // reconnect without complaining about it.
                    if (message.methodLine.size()) {
                        PWARN("Error processing message '" << message.methodLine << "' (" << e.what() << "), reconnecting.");
                    }
                    SData reconnect("RECONNECT");
                    reconnect["Reason"] = e.what();
                    peer->socket->send(reconnect.serialize());
                    peer->socket->shutdown();
                    break;
                }
                break;
            }

            case Socket::CLOSED: {
                // Done; clean up and try to reconnect
                uint64_t delay = SRandom::rand64() % (STIME_US_PER_S * 5);
                if (peer->socket->connectFailure) {
                    PINFO("SQLitePeer connection failed after " << (STimeNow() - peer->socket->openTime) / 1000
                                                          << "ms, reconnecting in " << delay / 1000 << "ms");
                } else {
                    PHMMM("Lost peer connection after " << (STimeNow() - peer->socket->openTime) / 1000
                                                        << "ms, reconnecting in " << delay / 1000 << "ms");
                }
                _onDisconnect(peer);
                _socketList.remove(peer->socket);
                peer->reset();
                peer->nextReconnect = STimeNow() + delay;
                nextActivity = min(nextActivity, peer->nextReconnect.load());
                break;
            }

            default:
                // Connecting or shutting down, wait
                // **FIXME: Add timeout here?
                break;
            }
        } else {
            // Not connected, is it time to try again?
            if (STimeNow() > peer->nextReconnect) {
                // Try again
                PINFO("Retrying the connection");
                peer->reset();
                try {
                    peer->socket = new Socket(peer->host);
                    _socketList.push_back(peer->socket);

                    // Try to log in now.  Send a PING immediately after so we
                    // can get a fast estimate of latency.
                    SData login("NODE_LOGIN");
                    login["Name"] = _name;
                    peer->socket->send(login.serialize());
                    _sendPING(peer);
                    _onConnect(peer);
                } catch (const SException& exception) {
                    // Failed to open -- try again later
                    SWARN(exception.what());
                    peer->nextReconnect = STimeNow() + STIME_US_PER_M;
                }
            } else {
                // Waiting to reconnect -- notify the caller
                nextActivity = min(nextActivity, peer->nextReconnect.load());
            }
        }
    }

    // Just clear this, it doesn't matter what the contents are.
    _commitsToSend.postPoll(fdm);
    _commitsToSend.clear();
}

void SQLiteNode::notifyCommit() const {
    // Note: this can skip locking because it only accesses a single atomic variable, which makes it safe to call in
    // private methods.
    _commitsToSend.push(true);
}

void SQLiteNode::_sendPING(SQLitePeer* peer) {
    // Send a PING message, including our current timestamp
    SASSERT(peer);
    SData ping("PING");
    ping["Timestamp"] = SToStr(STimeNow());
    peer->sendMessage(ping.serialize());
}

uint64_t SQLiteNode::_getIDByPeer(SQLitePeer* peer) const {
    uint64_t id = 1;
    for (auto p : _peerList) {
        if (p == peer) {
            return id;
        }
        id++;
    }
    return 0;
}

SQLitePeer* SQLiteNode::_getPeerByID(uint64_t id) const {
    if (id <= 0) {
        return nullptr;
    }
    try {
        return _peerList[id - 1];
    } catch (const out_of_range& e) {
        return nullptr;
    }
}

const string& SQLiteNode::stateName(SQLiteNode::State state) {
    static string placeholder = "";
    static map<State, string> lookup = {
        {UNKNOWN, "UNKNOWN"},
        {SEARCHING, "SEARCHING"},
        {SYNCHRONIZING, "SYNCHRONIZING"},
        {WAITING, "WAITING"},
        {STANDINGUP, "STANDINGUP"},
        {LEADING, "LEADING"},
        {STANDINGDOWN, "STANDINGDOWN"},
        {SUBSCRIBING, "SUBSCRIBING"},
        {FOLLOWING, "FOLLOWING"},
    };
    auto it = lookup.find(state);
    if (it == lookup.end()) {
        return placeholder;
    } else {
        return it->second;
    }
}

SQLiteNode::State SQLiteNode::stateFromName(const string& name) {
    const string normalizedName = SToUpper(name);
    static map<string, State> lookup = {
        {"SEARCHING", SEARCHING},
        {"SYNCHRONIZING", SYNCHRONIZING},
        {"WAITING", WAITING},
        {"STANDINGUP", STANDINGUP},
        {"LEADING", LEADING},
        {"STANDINGDOWN", STANDINGDOWN},
        {"SUBSCRIBING", SUBSCRIBING},
        {"FOLLOWING", FOLLOWING},
    };
    auto it = lookup.find(normalizedName);
    if (it == lookup.end()) {
        return UNKNOWN;
    } else {
        return it->second;
    }
}

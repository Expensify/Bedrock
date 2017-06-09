#include <libstuff/libstuff.h>
#include "SQLiteNode.h"
#include "SQLiteServer.h"
#include "SQLiteCommand.h"

// Introduction
// ------------
// SQLiteNode builds atop STCPNode and SQLite to provide a distributed transactional SQL database. The STCPNode base
// class establishes and maintains connections with all peers: if any connection fails, it forever attempts to
// re-establish. This frees the SQLiteNode layer to focus on the high-level distributed database state machine.
//
// FIXME: Handle the case where two nodes have conflicting databases. Should find where they fork, tag the affected
//        accounts for manual review, and adopt the higher-priority
//
// FIXME: Master should detect whether any slaves fall out of sync for any reason, identify/tag affected accounts, and
//        re-synchronize.
//
// FIXME: Add test to measure how long it takes for master to stabilize.
//
// FIXME: If master dies before sending ESCALATE_RESPONSE (or if slave dies before receiving it), then a command might
//        have been committed to the database without notifying whoever initiated it. Perhaps have the caller identify
//        each command with a unique command id, and verify inside the query that the command hasn't been executed yet?

#undef SLOGPREFIX
#define SLOGPREFIX "{" << name << "/" << SQLiteNode::stateNames[_state] << "} "

// Initializations for static vars.
const uint64_t SQLiteNode::SQL_NODE_DEFAULT_RECV_TIMEOUT = STIME_US_PER_M * 5;
const uint64_t SQLiteNode::SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT = STIME_US_PER_M;
atomic<bool> SQLiteNode::unsentTransactions(false);
uint64_t SQLiteNode::_lastSentTransactionID = 0;

const string SQLiteNode::stateNames[] = {"SEARCHING",
                                         "SYNCHRONIZING",
                                         "WAITING",
                                         "STANDINGUP",
                                         "MASTERING",
                                         "STANDINGDOWN",
                                         "SUBSCRIBING",
                                         "SLAVING"};

const string SQLiteNode::consistencyLevelNames[] = {"ASYNC",
                                                    "ONE",
                                                    "QUORUM"};

SQLiteNode::SQLiteNode(SQLiteServer& server, SQLite& db, const string& name, const string& host,
                       const string& peerList, int priority, uint64_t firstTimeout, const string& version,
                       int quorumCheckpoint)
    : STCPNode(name, host, max(SQL_NODE_DEFAULT_RECV_TIMEOUT, SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT)),
      _db(db), _commitState(CommitState::UNINITIALIZED), _server(server)
    {
    SASSERT(priority >= 0);
    _priority = priority;
    _state = SEARCHING;
    _syncPeer = nullptr;
    _masterPeer = nullptr;
    _stateTimeout = STimeNow() + firstTimeout;
    _version = version;
    _commitsSinceCheckpoint = 0;
    _quorumCheckpoint = quorumCheckpoint;

    // Get this party started
    _changeState(SEARCHING);

    // Add any peers.
    list<string> parsedPeerList = SParseList(peerList);
    for (const string& peer : parsedPeerList) {
        // Get the params from this peer, if any
        string host;
        STable params;
        SASSERT(SParseURIPath(peer, host, params));
        string name = SGetDomain(host);
        if (params.find("nodeName") != params.end()) {
            name = params["nodeName"];
        }
        addPeer(name, host, params);
    }
}

SQLiteNode::~SQLiteNode() {
    // Make sure it's a clean shutdown
    SASSERTWARN(_escalatedCommandMap.empty());
    SASSERTWARN(!commitInProgress());
}

void SQLiteNode::startCommit(ConsistencyLevel consistency)
{
    // Verify we're not already committing something, and then record that we have begun. This doesn't actually *do*
    // anything, but `update()` will pick up the state in its next invocation and start the actual commit.
    SASSERT(_commitState == CommitState::UNINITIALIZED ||
            _commitState == CommitState::SUCCESS       ||
            _commitState == CommitState::FAILED);
    _commitState = CommitState::WAITING;
    _commitConsistency = consistency;
}

void SQLiteNode::sendResponse(const SQLiteCommand& command)
{
    Peer* peer = getPeerByID(command.initiatingPeerID);
    SASSERT(peer);
    SData escalate("ESCALATE_RESPONSE");
    escalate["ID"] = command.id;
    escalate.content = command.response.serialize();
    _sendToPeer(peer, escalate);
}

void SQLiteNode::beginShutdown() {
    // Ignore redundant
    if (!gracefulShutdown()) {
        // Start graceful shutdown
        SINFO("Beginning graceful shutdown.");
        _gracefulShutdownTimeout.alarmDuration = STIME_US_PER_S * 30; // 30s timeout before we give up
        _gracefulShutdownTimeout.start();
    }
}

bool SQLiteNode::_isNothingBlockingShutdown() {
    // Don't shutdown if in the middle of a transaction
    if (_db.insideTransaction())
        return false;

    // If we're doing a commit, don't shut down.
    if (commitInProgress()) {
        return false;
    }

    // If we have non-"Connection: wait" commands escalated to master, not done
    if (!_escalatedCommandMap.empty()) {
        return false;
    }

    return true;
}

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
    if (_state > WAITING) {
        // Not in a shutdown state
        SINFO("Can't graceful shutdown yet because state="
              << SQLiteNode::stateNames[_state] << ", commitInProgress=" << commitInProgress()
              << ", escalated=" << _escalatedCommandMap.size());

        // If we end up with anything left in the escalated command map when we're trying to shut down, let's log it,
        // so we can try and diagnose what's happening.
        if (!_escalatedCommandMap.empty()) {
            for (auto& cmd : _escalatedCommandMap) {
                string name = cmd.first;
                SQLiteCommand& command = cmd.second;
                int64_t created = command.request.calcU64("commandExecuteTime");
                int64_t elapsed = STimeNow() - created;
                double elapsedSeconds = (double)elapsed / STIME_US_PER_S;
                SINFO("Escalated command remaining at shutdown(" << name << "): " << command.request.methodLine
                      << ". Created: " << command.request["commandExecuteTime"] << " (" << elapsedSeconds << "s ago)");
            }
        }
        return false;
    }

    // If we have unsent data, not done
    for (auto peer : peerList) {
        if (peer->s && !peer->s->sendBuffer.empty()) {
            // Still sending data
            SINFO("Can't graceful shutdown yet because unsent data to peer '" << peer->name << "'");
            return false;
        }
    }

    // Finally, make sure nothing is blocking shutdown
    if (_isNothingBlockingShutdown()) {
        // Yes!
        SINFO("Graceful shutdown is complete");
        return true;
    } else {
        // Not done yet
        SINFO("Can't graceful shutdown yet because waiting on commands: commitInProgress="
              << commitInProgress() << ", escalated=" << _escalatedCommandMap.size());
        return false;
    }
}

void SQLiteNode::_sendOutstandingTransactions() {
    SQLITE_COMMIT_AUTOLOCK;

    // Make sure we have something to do.
    if (!unsentTransactions.load()) {
        return;
    }
    auto transactions = _db.getCommittedTransactions();
    for (auto& i : transactions) {
        uint64_t id = i.first;
        if (id <= _lastSentTransactionID) {
            continue;
        }
        string& query = i.second.first;
        string& hash = i.second.second;
        SData transaction("BEGIN_TRANSACTION");
        transaction["Command"] = "ASYNC";
        transaction["NewCount"] = to_string(id);
        transaction["NewHash"] = hash;
        transaction["ID"] = "ASYNC_" + to_string(id);
        transaction.content = query;
        _sendToAllPeers(transaction, true); // subscribed only
        for (auto peer : peerList) {
            // Clear the response flag from the last transaction
            (*peer)["TransactionResponse"].clear();
        }
        SData commit("COMMIT_TRANSACTION");
        commit["ID"] = transaction["ID"];
        commit["CommitCount"] = transaction["NewCount"];
        commit["Hash"] = hash;
        _sendToAllPeers(commit, true); // subscribed only
        _lastSentTransactionID = id;

        // Commits made by other threads are implicitly not quorum commits. We'll update our counter.
        _commitsSinceCheckpoint++;
    }
    unsentTransactions.store(false);
}

void SQLiteNode::escalateCommand(SQLiteCommand&& command) {
    // If the master is currently standing down, we won't escalate, we'll give the command back to the caller.
    if((*_masterPeer)["State"] == "STANDINGDOWN") {
        SINFO("Asked to escalate command but master standing down, letting server retry.");
        _server.acceptCommand(move(command));
        return;
    }

    // Send this to the MASTER
    SASSERT(_masterPeer);
    SASSERTEQUALS((*_masterPeer)["State"], "MASTERING");
    uint64_t elapsed = STimeNow() - command.request.calcU64("commandExecuteTime");
    SINFO("Escalating '" << command.request.methodLine << "' (" << command.id << ") to MASTER '" << _masterPeer->name
          << "' after " << elapsed / STIME_US_PER_MS << " ms");

    // Create a command to send to our master.
    SData escalate("ESCALATE");
    escalate["ID"] = command.id;
    escalate.content = command.request.serialize();

    // Store the command as escalated.
    command.escalationTimeUS = STimeNow();
    _escalatedCommandMap.emplace(command.id, move(command));

    // And send to master.
    _sendToPeer(_masterPeer, escalate);
}

list<string> SQLiteNode::getEscalatedCommandRequestMethodLines() {
    list<string> returnList;
    for (auto& commandPair : _escalatedCommandMap) {
        returnList.push_back(commandPair.second.request.methodLine);
    }
    return returnList;
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
//              STANDINGUP                    SUBSCRIBING
//                   |                              |
//               MASTERING                       SLAVING
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
// the new "master".  All other nodes then SUBSCRIBE and become "slaves".  If the
// master "stands down", then all slaves unsubscribe and everybody goes back into
// the SEARCHING state and tries it all over again.
//
//
// State Transitions
// -----------------
// Each state transitions according to the following events and operates as follows:
bool SQLiteNode::update() {
    // Process the database state machine
    switch (_state) {
    /// - SEARCHING: Wait for a period and try to connect to all known
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
    case SEARCHING: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // If we're trying to shut down, just do nothing
        if (shutdownComplete())
            return false; // Don't re-update

        // If no peers, we're the master, unless we're shutting down.
        if (peerList.empty()) {
            // There are no peers, jump straight to mastering
            SHMMM("No peers configured, jumping to MASTERING");
            _changeState(MASTERING);
            return true; // Re-update immediately
        }

        // How many peers have we logged in to?
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        Peer* freshestPeer = nullptr;
        for (auto peer : peerList) {
            // Wait until all connected (or failed) and logged in
            bool permaSlave = peer->test("Permaslave");
            bool loggedIn = peer->test("LoggedIn");

            // Count how many full peers (non-permaslaves) we have
            numFullPeers += !permaSlave;

            // Count how many full peers are logged in
            numLoggedInFullPeers += (!permaSlave) && loggedIn;

            // Find the freshest peer
            if (loggedIn) {
                // The freshest peer is the one that has the most commits.
                if (!freshestPeer || peer->calcU64("CommitCount") > freshestPeer->calcU64("CommitCount")) {
                    freshestPeer = peer;
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
            _changeState(WAITING);
            return true; // Re-update
        }

        // How does our state compare with the freshest peer?
        SASSERT(freshestPeer);
        uint64_t freshestPeerCommitCount = freshestPeer->calcU64("CommitCount");
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
        SASSERTWARN(!_masterPeer);
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
    case WAITING: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        SASSERTWARN(_escalatedCommandMap.empty());
        // If we're trying and ready to shut down, do nothing.
        if (gracefulShutdown()) {
            // Do we have an outstanding command?
            if (1/* TODO: Commit in progress? */) {
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
        Peer* highestPriorityPeer = nullptr;
        Peer* freshestPeer = nullptr;
        Peer* currentMaster = nullptr;
        for (auto peer : peerList) {
            // Make sure we're a full peer
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
            _changeState(SEARCHING);
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
            _changeState(SUBSCRIBING);
            return true; // Re-update
        }

        // No master to subscribe to, let's see if there's anybody else
        // out there with commits we don't have.  Might as well synchronize
        // while waiting.
        if (freshestPeer->calcU64("CommitCount") > _db.getCommitCount()) {
            // Out of sync with a peer -- resynchronize
            SHMMM("Lost synchronization while waiting; re-SEARCHING.");
            _changeState(SEARCHING);
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
            for (auto peer : peerList) {
                peer->erase("StandupResponse");
            }
            _changeState(STANDINGUP);
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
    case STANDINGUP: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Wait for everyone to respond
        bool allResponded = true;
        int numFullPeers = 0;
        int numLoggedInFullPeers = 0;
        if (gracefulShutdown()) {
            SINFO("Shutting down while standing up, setting state to SEARCHING");
            _changeState(SEARCHING);
            return true; // Re-update
        }
        for (auto peer : peerList) {
            // Check this peer; if not logged in, tacit approval
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
            SINFO("All peers approved standup, going MASTERING.");
            _changeState(MASTERING);
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
    case MASTERING:
    case STANDINGDOWN: {
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(!_masterPeer);

        // NOTE: This block very carefully will not try and call _changeState() while holding SQLite::g_commitLock,
        // because that could cause a deadlock when called by an outside caller!

        // If there's no commit in progress, we'll send any outstanding transactions that exist. We won't send them
        // mid-commit, as they'd end up as nested transactions interleaved with the one in progress.
        if (!commitInProgress()) {
            _sendOutstandingTransactions();
        }

        // This means we've started a distributed transaction and need to decide if we should commit it, which can mean
        // waiting on peers to approve the transaction. We can do this even after we've begun standing down.
        if (_commitState == CommitState::COMMITTING) {
            // Loop across all peers configured to see how many are:
            int numFullPeers = 0;     // Num non-permaslaves configured
            int numFullSlaves = 0;    // Num full peers that are "subscribed"
            int numFullResponded = 0; // Num full peers that have responded approve/deny
            int numFullApproved = 0;  // Num full peers that have approved
            int numFullDenied = 0;    // Num full peers that have denied
            for (auto peer : peerList) {
                // Check this peer to see if it's full or a permaslave
                if (peer->params["Permaslave"] != "true") {
                    // It's a full peer -- is it subscribed, and if so, how did it respond?
                    ++numFullPeers;
                    if ((*peer)["Subscribed"] == "true") {
                        // Subscribed, did it respond?
                        numFullSlaves++;
                        const string& response = (*peer)["TransactionResponse"];
                        if (response.empty()) {
                            continue;
                        }
                        numFullResponded++;
                        numFullApproved += SIEquals(response, "approve");
                        if (!SIEquals(response, "approve")) {
                            SWARN("Peer '" << peer->name << "' denied transaction.");
                            ++numFullDenied;
                        } else {
                            SDEBUG("Peer '" << peer->name << "' has approved transaction.");
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

            // See if all active non-permaslaves have responded.
            // NOTE: This can be true if nobody responds if there are no full slaves.
            bool everybodyResponded = numFullResponded >= numFullSlaves;

            // Record these for posterity
            SDEBUG(     "numFullPeers="           << numFullPeers
                   << ", numFullSlaves="          << numFullSlaves
                   << ", numFullResponded="       << numFullResponded
                   << ", numFullApproved="        << numFullApproved
                   << ", majorityApproved="       << majorityApproved
                   << ", writeConsistency="       << consistencyLevelNames[_commitConsistency]
                   << ", consistencyRequired="    << consistencyLevelNames[_commitConsistency]
                   << ", consistentEnough="       << consistentEnough
                   << ", everybodyResponded="     << everybodyResponded
                   << ", commitsSinceCheckpoint=" << _commitsSinceCheckpoint);

            // If everyone's responded, but we didn't get the required number of approvals, roll this back. Or, if
            // *anyone* denied, we'll roll back without waiting for the rest of the cluster.
            if ((everybodyResponded && !consistentEnough) || numFullDenied > 0) {
                // Abort this distributed transaction. Happened either because the initiator disappeared, or everybody
                // has responded without enough consistency (indicating it'll never come). Failed, tell everyone to
                // rollback.
                SWARN("Rolling back transaction because everybody responded but not consistent enough."
                      << "(This transaction would likely have conflicted.)");
                _db.rollback();

                // Notify everybody to rollback
                SData rollback("ROLLBACK_TRANSACTION");
                rollback.set("ID", _lastSentTransactionID + 1);
                _sendToAllPeers(rollback, true); // true: Only to subscribed peers.

                // Finished, but failed.
                _commitState = CommitState::FAILED;
            } else if (consistentEnough) {
                // Commit this distributed transaction. Either we have quorum, or we don't need it.
                int result = _db.commit();

                // If this is the case, there was a commit conflict.
                if (result == SQLITE_BUSY_SNAPSHOT) {
                    _db.rollback();

                    // We already asked everyone to commit this (even if it was async), so we'll have to tell them to
                    // roll back.
                    SINFO("[performance] Conflict committing " << consistencyLevelNames[_commitConsistency]
                          << " commit, rolling back.");
                    SData rollback("ROLLBACK_TRANSACTION");
                    rollback.set("ID", _lastSentTransactionID + 1);
                    _sendToAllPeers(rollback, true); // true: Only to subscribed peers.

                    // Finished, but failed.
                    _commitState = CommitState::FAILED;
                } else {
                    // Hey, our commit succeeded! Record how long it took.
                    uint64_t beginElapsed, readElapsed, writeElapsed, prepareElapsed, commitElapsed, rollbackElapsed;
                    uint64_t totalElapsed = _db.getLastTransactionTiming(beginElapsed, readElapsed, writeElapsed,
                                                                         prepareElapsed, commitElapsed, rollbackElapsed);
                    SINFO("Committed master transaction for '"
                          << _db.getCommitCount() << " (" << _db.getCommittedHash() << "). "
                          << _commitsSinceCheckpoint << " commits since quorum (consistencyRequired="
                          << consistencyLevelNames[_commitConsistency] << "), " << numFullApproved << " of "
                          << numFullPeers << " approved (" << peerList.size() << " total) in "
                          << totalElapsed / STIME_US_PER_MS << " ms ("
                          << beginElapsed / STIME_US_PER_MS << "+" << readElapsed / STIME_US_PER_MS << "+"
                          << writeElapsed / STIME_US_PER_MS << "+" << prepareElapsed / STIME_US_PER_MS << "+"
                          << commitElapsed / STIME_US_PER_MS << "+" << rollbackElapsed / STIME_US_PER_MS << "ms)");

                    SINFO("[performance] Successfully committed " << consistencyLevelNames[_commitConsistency]
                          << " transaction. Sending COMMIT_TRANSACTION to peers.");
                    SData commit("COMMIT_TRANSACTION");
                    commit.set("ID", _lastSentTransactionID + 1);
                    _sendToAllPeers(commit, true); // true: Only to subscribed peers.
                    
                    // clear the unsent transactions, we've sent them all (including this one);
                    _db.getCommittedTransactions();

                    // Update the last sent transaction ID to reflect that this is finished.
                    _lastSentTransactionID = _db.getCommitCount();

                    // If this was a quorum commit, we'll reset our counter, otherwise, we'll update it.
                    if (_commitConsistency == QUORUM) {
                        _commitsSinceCheckpoint = 0;
                    } else {
                        _commitsSinceCheckpoint++;
                    }

                    // Done!
                    _commitState = CommitState::SUCCESS;
                }
            } else {
                // Not consistent enough, but not everyone's responded yet, so we'll wait.
                SINFO("Waiting to commit. consistencyRequired=" << consistencyLevelNames[_commitConsistency]
                      << ", commitsSinceCheckpoint=" << _commitsSinceCheckpoint);

                // We're going to need to read from the network to finish this.
                return false;
            }

            // We were committing, but now we're not. The only code path through here that doesn't lead to the point
            // is the 'return false' immediately above here, everything else completes the transaction (even if it was
            // a failed transaction), so we can safely unlock now.
            SQLite::g_commitLock.unlock();
        }

        // If there's a transaction that's waiting, we'll start it. We do this *before* we check to see if we should
        // stand down, and since we return true, we'll never stand down as long as we keep adding new transactions
        // here. It's up to the server to stop giving us transactions to process if it wants us to stand down.
        if (_commitState == CommitState::WAITING) {
            // Lock the database. We'll unlock it when we complete in a future update cycle.
            SQLite::g_commitLock.lock();
            _commitState = CommitState::COMMITTING;

            // Figure out how much consistency we need. Go with whatever the caller specified, unless we're over our
            // checkpoint limit.
            if (_commitsSinceCheckpoint >= _quorumCheckpoint) {
                _commitConsistency = QUORUM;
            }
            SINFO("[performance] Beginning " << consistencyLevelNames[_commitConsistency] << " commit.");

            // Now that we've grabbed the commit lock, we can safely clear out any outstanding transactions, no new
            // ones can be added until we release the lock.
            _sendOutstandingTransactions();

            // We'll send the commit count to peers.
            uint64_t commitCount = _db.getCommitCount();

            // If there was nothing changed, then we shouldn't have anything to commit.
            SASSERT(!_db.getUncommittedQuery().empty());

            // There's no handling for a failed prepare. This should only happen if the DB has been corrupted or
            // something catastrophic like that.
            SASSERT(_db.prepare());

            // Begin the distributed transaction
            SData transaction("BEGIN_TRANSACTION");
            SINFO("beginning distributed transaction for commit #" << commitCount + 1 << " ("
                  << _db.getUncommittedHash() << ")");
            transaction.set("NewCount", commitCount + 1);
            transaction.set("NewHash", _db.getUncommittedHash());
            if (_commitConsistency == ASYNC) {
                transaction["ID"] = "ASYNC_" + to_string(_lastSentTransactionID + 1);
            } else {
                transaction.set("ID", _lastSentTransactionID + 1);
            }
            transaction.content = _db.getUncommittedQuery();

            for (auto peer : peerList) {
                // Clear the response flag from the last transaction
                (*peer)["TransactionResponse"].clear();
            }

            // And send it to everyone who's subscribed.
            _sendToAllPeers(transaction, true);

            // We return `true` here to immediately re-update and thus commit this transaction immediately if it was
            // asynchronous.
            return true;
        }

        // Check to see if we should stand down. We'll finish any outstanding commits before we actually do.
        if (_state == MASTERING) {
            string standDownReason;
            if (gracefulShutdown()) {
                // Graceful shutdown. Set priority 1 and stand down so we'll re-connect to the new master and finish
                // up our commands.
                standDownReason = "Shutting down, setting priority 1 and STANDINGDOWN.";
                _priority = 1;
            } else {
                // Loop across peers
                for (auto peer : peerList) {
                    // Check this peer
                    if (SIEquals((*peer)["State"], "MASTERING")) {
                        // Hm... somehow we're in a multi-master scenario -- not good.
                        // Let's get out of this as soon as possible.
                        standDownReason = "Found another MASTER (" + peer->name + "), STANDINGDOWN to clean it up.";
                    } else if (SIEquals((*peer)["State"], "WAITING")) {
                        // We have a WAITING peer; is it waiting to STANDUP?
                        if (peer->calc("Priority") > _priority) {
                            // We've got a higher priority peer in the works; stand down so it can stand up.
                            standDownReason = "Found higher priority WAITING peer (" + peer->name
                                              + ") while MASTERING, STANDINGDOWN";
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
                SHMMM(standDownReason);

                // As we fall out of mastering, make sure no lingering transactions are left behind.
                SAUTOLOCK(stateMutex);
                _sendOutstandingTransactions();
                _changeState(STANDINGDOWN);
                SINFO("Standing down: " << standDownReason);
            }
        }

        // At this point, we're no longer committing. We'll have returned false above, or we'll have completed any
        // outstanding transaction, we can complete standing down if that's what we're doing.
        if (_state == STANDINGDOWN) {
            // See if we're done
            // We can only switch to SEARCHING if the server has no outstanding write work to do.
            // **FIXME: Add timeout?
            if (!_server.canStandDown()) {
                // Try again.
                SWARN("Can't switch from STANDINGDOWN to SEARCHING yet, server prevented state change.");
                return false;
            }
            // Standdown complete
            SINFO("STANDDOWN complete, SEARCHING");

            // As we fall out of mastering, make sure no lingering transactions are left behind.
            SAUTOLOCK(stateMutex);
            _sendOutstandingTransactions();
            _changeState(SEARCHING);

            // We're no longer waiting on responses from peers, we can re-update immediately and start becoming a
            // slave node instead.
            return true;
        }
        break;
    }

    /// - SUBSCRIBING: We're waiting for a SUBSCRIPTION_APPROVED from the
    ///     master.  When we receive it, we'll go SLAVING.  Otherwise, if we
    ///     timeout, go SEARCHING.
    ///
    case SUBSCRIBING:
        SASSERTWARN(!_syncPeer);
        SASSERTWARN(_masterPeer);
        SASSERTWARN(_db.getUncommittedHash().empty());
        // Nothing to do but wait
        if (STimeNow() > _stateTimeout) {
            // Give up
            SHMMM("Timed out waiting for SUBSCRIPTION_APPROVED, reconnecting to master and re-SEARCHING.");
            _reconnectPeer(_masterPeer);
            _masterPeer = nullptr;
            _changeState(SEARCHING);
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
    case SLAVING:
        SASSERTWARN(!_syncPeer);
        SASSERT(_masterPeer);
        // If graceful shutdown requested, stop slaving once there is
        // nothing blocking shutdown.  We stop listening for new commands
        // immediately upon TERM.)
        if (gracefulShutdown() && _isNothingBlockingShutdown()) {
            // Go searching so we stop slaving
            SINFO("Stopping SLAVING in order to gracefully shut down, SEARCHING.");
            _changeState(SEARCHING);
            return false; // Don't update
        }

        // If the master stops mastering (or standing down), we'll go SEARCHING, which allows us to look for a new
        // master. We don't want to go searching before that, because we won't know when master is done sending its
        // final transactions.
        if (!SIEquals((*_masterPeer)["State"], "MASTERING") && !SIEquals((*_masterPeer)["State"], "STANDINGDOWN")) {
            // Master stepping down
            SHMMM("Master stepping down, re-queueing commands.");

            // If there were escalated commands, give them back to the server to retry.
            for (auto& cmd : _escalatedCommandMap) {
                _server.acceptCommand(move(cmd.second));
            }
            _escalatedCommandMap.clear();

            // Are we in the middle of a commit? This should only happen if we received a `BEGIN_TRANSACTION` without a
            // corresponding `COMMIT` or `ROLLBACK`, this isn't supposed to happen.
            if (!_db.getUncommittedHash().empty()) {
                SWARN("Master stepped down with transaction in progress, rolling back.");
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
void SQLiteNode::_onMESSAGE(Peer* peer, const SData& message) {
    SASSERT(peer);
    SASSERTWARN(!message.empty());
    // Every message broadcasts the current state of the node
    if (!message.isSet("CommitCount")) {
        throw "missing CommitCount";
    }
    if (!message.isSet("Hash")) {
        throw "missing Hash";
    }
    (*peer)["CommitCount"] = message["CommitCount"];
    (*peer)["Hash"] = message["Hash"];

    // Classify and process the message
    if (SIEquals(message.methodLine, "LOGIN")) {
        // LOGIN: This is the first message sent to and received from a new peer. It communicates the current state of
        // the peer (hash and commit count), as well as the peer's priority. Peers can connect in any state, so this
        // message can be sent and received in any state.
        if ((*peer)["LoggedIn"] == "true") {
            throw "already logged in";
        }
        if (!message.isSet("Priority")) {
            throw "missing Priority";
        }
        if (!message.isSet("State")) {
            throw "missing State";
        }
        if (!message.isSet("Version")) {
            throw "missing Version";
        }
        if (peer->params["Permaslave"] == "true" && message.calc("Priority")) {
            throw "you're supposed to be a 0-priority permaslave";
        }
        if (peer->params["Permaslave"] != "true" && !message.calc("Priority")) {
            throw "you're *not* supposed to be a 0-priority permaslave";
        }
        // It's an error to have to peers configured with the same priority, except 0.
        SASSERT(!_priority || message.calc("Priority") != _priority);
        PINFO("Peer logged in at '" << message["State"] << "', priority #" << message["Priority"] << " commit #"
              << message["CommitCount"] << " (" << message["Hash"] << ")");
        peer->set("Priority", message["Priority"]);
        peer->set("State",    message["State"]);
        peer->set("LoggedIn", "true");
        peer->set("Version",  message["Version"]);
    } else if (!SIEquals((*peer)["LoggedIn"], "true")) {
        throw "not logged in";
    }
    else if (SIEquals(message.methodLine, "STATE")) {
        // STATE: Broadcast to all peers whenever a node's state changes. Also sent whenever a node commits a new query
        // (and thus has a new commit count and hash). A peer can react or respond to a peer's state change as follows:
        if (!message.isSet("State")) {
            throw "missing State";
        }
        if (!message.isSet("Priority")) {
            throw "missing Priority";
        }
        string oldState = (*peer)["State"];
        peer->set("Priority", message["Priority"]);
        peer->set("State",    message["State"]);
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
            for (from = SEARCHING; from <= SLAVING; from++) {
                if (SIEquals(oldState, stateNames[from])) {
                    break;
                }
            }
            for (to = SEARCHING; to <= SLAVING; to++) {
                if (SIEquals(newState, stateNames[to])) {
                    break;
                }
            }
            if (from > SLAVING) {
                PWARN("Peer coming from unrecognized state '" << oldState << "'");
            }
            if (to > SLAVING) {
                PWARN("Peer going to unrecognized state '" << newState << "'");
            }
            bool okTransition = false;
            switch (from) {
            case SEARCHING:
                okTransition = (to == SYNCHRONIZING || to == WAITING || to == MASTERING);
                break;
            case SYNCHRONIZING:
                okTransition = (to == SEARCHING || to == WAITING);
                break;
            case WAITING:
                okTransition = (to == SEARCHING || to == STANDINGUP || to == SUBSCRIBING);
                break;
            case STANDINGUP:
                okTransition = (to == SEARCHING || to == MASTERING);
                break;
            case MASTERING:
                okTransition = (to == SEARCHING || to == STANDINGDOWN);
                break;
            case STANDINGDOWN:
                okTransition = (to == SEARCHING);
                break;
            case SUBSCRIBING:
                okTransition = (to == SEARCHING || to == SLAVING);
                break;
            case SLAVING:
                okTransition = (to == SEARCHING);
                break;
            }
            if (!okTransition) {
                PWARN("Peer making invalid transition from '" << stateNames[from] << "' to '" << stateNames[to] << "'");
            }

            // Next, should we do something about it?
            if (to == SEARCHING) {
                // SEARCHING: If anything ever goes wrong, a node reverts to the SEARCHING state. Thus if we see a peer
                // go SEARCHING, we reset its accumulated state.  Specifically, we mark it is no longer being
                // "subscribed", and we clear its last transaction response.
                peer->erase("TransactionResponse");
                peer->erase("Subscribed");
            } else if (to == STANDINGUP) {
                // STANDINGUP: When a peer announces it intends to stand up, we immediately respond with approval or
                // denial. We determine this by checking to see if there is any  other peer who is already master or
                // also trying to stand up.
                //
                // **FIXME**: Should it also deny if it knows of a higher priority peer?
                SData response("STANDUP_RESPONSE");
                if (peer->params["Permaslave"] == "true") {
                    // We think it's a permaslave, deny
                    PHMMM("Permaslave trying to stand up, denying.");
                    response["Response"] = "deny";
                    response["Reason"] = "You're a permaslave";
                }

                // What's our state
                if (SWITHIN(STANDINGUP, _state, STANDINGDOWN)) {
                    // Oh crap, it's trying to stand up while we're mastering. Who is higher priority?
                    if (peer->calc("Priority") > _priority) {
                        // The other peer is a higher priority than us, so we should stand down (maybe it crashed, we
                        // came up as master, and now it's been brought back up). We'll want to stand down here, but we
                        // do it gracefully so that we won't lose any transactions in progress.
                        if (_state == STANDINGUP) {
                            PWARN("Higher-priority peer is trying to stand up while we are STANDINGUP, SEARCHING.");
                            _changeState(SEARCHING);
                        } else if (_state == MASTERING) {
                            PWARN("Higher-priority peer is trying to stand up while we are MASTERING, STANDINGDOWN.");
                            _changeState(STANDINGDOWN);
                        } else {
                            PWARN("Higher-priority peer is trying to stand up while we are STANDINGDOWN, continuing.");
                        }
                    } else {
                        // Deny because we're currently in the process of mastering and we're higher priority.
                        response["Response"] = "deny";
                        response["Reason"] = "I am mastering";

                        // Hmm, why is a lower priority peer trying to stand up? Is it possible we're no longer in
                        // control of the cluster? Let's see how many nodes are subscribed.
                        if (_majoritySubscribed()) {
                            // we have a majority of the cluster, so ignore this oddity.
                            PHMMM("Lower-priority peer is trying to stand up while we are " << stateNames[_state]
                                  << " with a majority of the cluster; denying and ignoring.");
                        } else {
                            // We don't have a majority of the cluster -- maybe it knows something we don't?  For
                            // example, it could be that the rest of the cluster has forked away from us. This can
                            // happen if the master hangs while processing a command: by the time it finishes, the
                            // cluster might have elected a new master, forked, and be a thousand commits in the future.
                            // In this case, let's just reset everything anyway to be safe.
                            PWARN("Lower-priority peer is trying to stand up while we are " << stateNames[_state]
                                  << ", but we don't have a majority of the cluster so reconnecting and SEARCHING.");
                            _reconnectAll();
                            // TODO: This puts us in an ambiguous state if we switch to SEARCHING from MASTERING,
                            // without going through the STANDDOWN process. We'll need to handle it better, but it's
                            // unclear if this can ever happen at all. exit() may be a reasonable strategy here.
                            _changeState(SEARCHING);
                        }
                    }
                } else {
                    // Approve if nobody else is trying to stand up
                    response["Response"] = "approve"; // Optimistic; will override
                    for (auto otherPeer : peerList) {
                        if (otherPeer != peer) {
                            // See if it's trying to be master
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
                }

                // Send the response
                if (SIEquals(response["Response"], "approve")) {
                    PINFO("Approving standup request");
                } else {
                    PHMMM("Denying standup request because " << response["Reason"]);
                }
                _sendToPeer(peer, response);
            } else if (from == STANDINGDOWN) {
                // STANDINGDOWN: When a peer stands down we double-check to make sure we don't have any outstanding
                // transaction (and if we do, we warn and rollback).
                if (!_db.getUncommittedHash().empty()) {
                    // Crap, we were waiting for a response that will apparently never come. I guess roll it back? This
                    // should never happen, however, as the master shouldn't STANDOWN unless all subscribed slaves
                    // (including us) have already unsubscribed, and we wouldn't do that in the middle of a
                    // transaction. But just in case...
                    SASSERTWARN(_state == SLAVING);
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
            if (!message.isSet("Response")) {
                throw "missing Response";
            }
            if (peer->isSet("StandupResponse")) {
                PWARN("Already received standup response '" << (*peer)["StandupResponse"] << "', now receiving '"
                      << message["Response"] << "', odd -- multiple masters competing?");
            }
            if (SIEquals(message["Response"], "approve")) {
                PINFO("Received standup approval");
            } else {
                PHMMM("Received standup denial: reason='" << message["Reason"] << "'");
            }
            (*peer)["StandupResponse"] = message["Response"];
        } else {
            SINFO("Got STANDUP_RESPONSE but not STANDINGUP. Probably a late message, ignoring.");
        }
    } else if (SIEquals(message.methodLine, "SYNCHRONIZE")) {
        // SYNCHRONIZE: Sent by a node in the SEARCHING state to a peer that has new commits. Respond with a
        // SYNCHRONIZE_RESPONSE containing all COMMITs the requesting peer lacks.
        SData response("SYNCHRONIZE_RESPONSE");
        _queueSynchronize(peer, response, false);
        _sendToPeer(peer, response);
    } else if (SIEquals(message.methodLine, "SYNCHRONIZE_RESPONSE")) {
        // SYNCHRONIZE_RESPONSE: Sent in response to a SYNCHRONIZE request. Contains a payload of zero or more COMMIT
        // messages, all of which are immediately committed to the local database.
        if (_state != SYNCHRONIZING) {
            throw "not synchronizing";
        }
        if (!_syncPeer) {
            throw "too late, gave up on you";
        }
        if (peer != _syncPeer) {
            throw "sync peer mismatch";
        }
        PINFO("Beginning synchronization");
        try {
            // Received this synchronization response; are we done?
            _recvSynchronize(peer, message);
            uint64_t peerCommitCount = _syncPeer->calcU64("CommitCount");
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
                _stateTimeout = STimeNow() + SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_M * 5;
            }
        } catch (const char* e) {
            // Transaction failed
            SWARN("Synchronization failed '" << e << "', reconnecting and re-SEARCHING.");
            _reconnectPeer(_syncPeer);
            _syncPeer = nullptr;
            _changeState(SEARCHING);
            throw e;
        }
    } else if (SIEquals(message.methodLine, "SUBSCRIBE")) {
        // SUBSCRIBE: Sent by a node in the WAITING state to the current master to begin SLAVING. Respond
        // SUBSCRIPTION_APPROVED with any COMMITs that the subscribing peer lacks (for example, any commits that have
        // occurred after it completed SYNCHRONIZING but before this SUBSCRIBE was received). Tag this peer as
        // "subscribed" for use in the MASTERING and STANDINGDOWN update loops. Finally, if there is an outstanding
        // distributed transaction being processed, send it to this new slave.
        if (_state != MASTERING) {
            throw "not mastering";
        }
        PINFO("Received SUBSCRIBE, accepting new slave");
        SData response("SUBSCRIPTION_APPROVED");
        _queueSynchronize(peer, response, true); // Send everything it's missing
        _sendToPeer(peer, response);
        SASSERTWARN(!SIEquals((*peer)["Subscribed"], "true"));
        (*peer)["Subscribed"] = "true";

        // New slave; are we in the midst of a transaction?
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
            transaction.set("ID", _lastSentTransactionID + 1);
            transaction.content = _db.getUncommittedQuery();
            _sendToPeer(peer, transaction);
        }
    } else if (SIEquals(message.methodLine, "SUBSCRIPTION_APPROVED")) {
        // SUBSCRIPTION_APPROVED: Sent by a slave's new master to complete the subscription process. Includes zero or
        // more COMMITS that should be immediately applied to the database.
        if (_state != SUBSCRIBING) {
            throw "not subscribing";
        }
        if (_masterPeer != peer) {
            throw "not subscribing to you";
        }
        SINFO("Received SUBSCRIPTION_APPROVED, final synchronization.");
        try {
            // Done synchronizing
            _recvSynchronize(peer, message);
            if (_db.getCommitCount() != _masterPeer->calcU64("CommitCount")) {
                throw "Incomplete synchronizationg";
            }
            SINFO("Subscription complete, at commitCount #" << _db.getCommitCount() << " (" << _db.getCommittedHash()
                  << "), SLAVING");
            _changeState(SLAVING);
        } catch (const char* e) {
            // Transaction failed
            SWARN("Subscription failed '" << e << "', reconnecting to master and re-SEARCHING.");
            _reconnectPeer(_masterPeer);
            _changeState(SEARCHING);
            throw e;
        }
    } else if (SIEquals(message.methodLine, "BEGIN_TRANSACTION")) {
        // BEGIN_TRANSACTION: Sent by the MASTER to all subscribed slaves to begin a new distributed transaction. Each
        // slave begins a local transaction with this query and responds APPROVE_TRANSACTION. If the slave cannot start
        // the transaction for any reason, it is broken somehow -- disconnect from the master.
        // **FIXME**: What happens if MASTER steps down before sending BEGIN?
        // **FIXME**: What happens if MASTER steps down or disconnects after BEGIN?
        bool success = true;
        if (!message.isSet("ID")) {
            throw "missing ID";
        }
        if (!message.isSet("NewCount")) {
            throw "missing NewCount";
        }
        if (!message.isSet("NewHash")) {
            throw "missing NewHash";
        }
        if (_state != SLAVING) {
            throw "not slaving";
        }
        if (!_masterPeer) {
            throw "no master?";
        }
        if (!_db.getUncommittedHash().empty()) {
            throw "already in a transaction";
        }
        if (_db.getCommitCount() + 1 != message.calcU64("NewCount")) {
            throw "commit count mismatch. Expected: " + message["NewCount"] + ", but would actually be: " + to_string(_db.getCommitCount() + 1);
        }
        if (!_db.beginTransaction()) {
            throw "failed to begin transaction";
        }
        try {
            // Inside transaction; get ready to back out on error
            if (!_db.write(message.content)) {
                throw "failed to write transaction";
            }
            if (!_db.prepare()) {
                throw "failed to prepare transaction";
            }
            // Successful commit; we in the right state?
            if (_db.getUncommittedHash() != message["NewHash"]) {
                // Something is screwed up
                PWARN("New hash mismatch: command='" << message["Command"] << "', commitCount=#" << _db.getCommitCount()
                      << "', committedHash='" << _db.getCommittedHash() << "', uncommittedHash='"
                      << _db.getUncommittedHash() << "', messageHash='" << message["NewHash"] << "', uncommittedQuery='"
                      << _db.getUncommittedQuery() << "'");
                throw "new hash mismatch";
            }
        } catch (const char* e) {
            // Ok, so we failed to commit. This probably means that master ran two transactions that each would have
            // succeeded on their own, but will end up conflicting. We should deny this transaction if we're
            // participating in quorum, though it will eventually conflict and get rolled back anyway.
            success = false;
            _db.rollback();
        }

        // Are we participating in quorum?
        if (_priority) {
            // If the ID is /ASYNC_\d+/, no need to respond, master will ignore it anyway.
            string verb = success ? "APPROVE_TRANSACTION" : "DENY_TRANSACTION";
            if (!SStartsWith(message["ID"], "ASYNC_")) {
                // Not a permaslave, approve the transaction
                PINFO(verb << " #" << _db.getCommitCount() + 1 << " (" << message["NewHash"] << ").");
                SData response(verb);
                response["NewCount"] = SToStr(_db.getCommitCount() + 1);
                response["NewHash"] = success ? _db.getUncommittedHash() : message["NewHash"];
                response["ID"] = message["ID"];
                _sendToPeer(_masterPeer, response);
            } else {
                PINFO("Skipping " << verb << " for ASYNC command.");
            }
        } else {
            PINFO("Would approve/deny transaction #" << _db.getCommitCount() + 1 << " (" << _db.getUncommittedHash()
                  << ") for command '" << message["Command"] << "', but a permaslave -- keeping quiet.");
        }

        // Check our escalated commands and see if it's one being processed
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this
            // so we know that this command might be corrupted if the master
            // crashes.
            SINFO("Master is processing our command " << message["ID"] << " (" << message["Command"] << ")");
            commandIt->second.transaction = message;
        }
    } else if (SIEquals(message.methodLine, "APPROVE_TRANSACTION") ||
               SIEquals(message.methodLine, "DENY_TRANSACTION")) {
        // APPROVE_TRANSACTION: Sent to the master by a slave when it confirms it was able to begin a transaction and
        // is ready to commit. Note that this peer approves the transaction for use in the MASTERING and STANDINGDOWN
        // update loop.
        if (!message.isSet("ID")) {
            throw "missing ID";
        }
        if (!message.isSet("NewCount")) {
            throw "missing NewCount";
        }
        if (!message.isSet("NewHash")) {
            throw "missing NewHash";
        }
        if (_state != MASTERING && _state != STANDINGDOWN) {
            throw "not mastering";
        }
        string response = SIEquals(message.methodLine, "APPROVE_TRANSACTION") ? "approve" : "deny";
        try {
            // We ignore late approvals of commits that have already been finalized. They could have been committed
            // already, in which case `_lastSentTransactionID` will have incremented, or they could have been rolled
            // back due to a conflict, which would cuase them to have the wrong hash (the hash of the previous attempt
            // at committing the transaction with this ID).
            bool hashMatch = message["NewHash"] == _db.getUncommittedHash();
            if (hashMatch && to_string(_lastSentTransactionID + 1) == message["ID"]) {
                if (message.calcU64("NewCount") != _db.getCommitCount() + 1) {
                    throw "commit count mismatch. Expected: " + message["NewCount"] + ", but would actually be: "
                          + to_string(_db.getCommitCount() + 1);
                }
                if (peer->params["Permaslave"] == "true") {
                    throw "permaslaves shouldn't approve/deny";
                }
                PINFO("Peer " << response << " transaction #" << message["NewCount"] << " (" << message["NewHash"] << ")");
                (*peer)["TransactionResponse"] = response;
            } else {
                // Old command.  Nothing to do.  We already sent a commit or rollback.
                PINFO("Peer '" << message.methodLine << "' transaction #" << message["NewCount"]
                      << " (" << message["NewHash"] << ") after " << (hashMatch ? "commit" : "rollback") << ".");
            }
        } catch (const char* e) {
            // Doesn't correspond to the outstanding transaction not necessarily fatal. This can happen if, for
            // example, a command is escalated from/ one slave, approved by the second, but where the first slave dies
            // before the second's approval is received by the master. In this case the master will drop the command
            // when the initiating peer is lost, and thus won't have an outstanding transaction (or will be processing
            // a new transaction) when the old, outdated approval is received. Furthermore, in this case we will have
            // already sent a ROLLBACK, so it will already correct itself. If not, then we'll wait for the slave to
            // determine it's screwed and reconnect.
            SWARN("Received " << message.methodLine << " for transaction #"
                  << message.calc("NewCount") << " (" << message["NewHash"] << ", " << message["ID"] << ") but '" << e
                  << "', ignoring.");
        }
    } else if (SIEquals(message.methodLine, "COMMIT_TRANSACTION")) {
        // COMMIT_TRANSACTION: Sent to all subscribed slaves by the master when it determines that the current
        // outstanding transaction should be committed to the database. This completes a given distributed transaction.
        if (_state != SLAVING) {
            throw "not slaving";
        }
        if (_db.getUncommittedHash().empty()) {
            throw "no outstanding transaction";
        }
        if (message.calcU64("CommitCount") != _db.getCommitCount() + 1) {
            throw "commit count mismatch. Expected: " + message["CommitCount"] + ", but would actually be: "
                  + to_string(_db.getCommitCount() + 1);
        }
        if (message["Hash"] != _db.getUncommittedHash()) {
            throw "hash mismatch";
        }

        _db.commit();
        uint64_t beginElapsed, readElapsed, writeElapsed, prepareElapsed, commitElapsed, rollbackElapsed;
        uint64_t totalElapsed = _db.getLastTransactionTiming(beginElapsed, readElapsed, writeElapsed, prepareElapsed,
                                                             commitElapsed, rollbackElapsed);
        SINFO("Committed slave transaction #" << message["CommitCount"] << " (" << message["Hash"] << ") in "
              << totalElapsed / STIME_US_PER_MS << " ms (" << beginElapsed / STIME_US_PER_MS << "+"
              << readElapsed / STIME_US_PER_MS << "+" << writeElapsed / STIME_US_PER_MS << "+"
              << prepareElapsed / STIME_US_PER_MS << "+" << commitElapsed / STIME_US_PER_MS << "+"
              << rollbackElapsed / STIME_US_PER_MS << "ms)");

        // Look up in our escalated commands and see if it's one being processed
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this so we know that this command might be
            // corrupted if the master crashes.
            SINFO("Master has committed in response to our command " << message["ID"]);
            commandIt->second.transaction = message;
        }
    } else if (SIEquals(message.methodLine, "ROLLBACK_TRANSACTION")) {
        // ROLLBACK_TRANSACTION: Sent to all subscribed slaves by the master when it determines that the current
        // outstanding transaction should be rolled back. This completes a given distributed transaction.
        if (!message.isSet("ID")) {
            throw "missing ID";
        }
        if (_state != SLAVING) {
            throw "not slaving";
        }
        if (_db.getUncommittedHash().empty()) {
            SINFO("Received ROLLBACK_TRANSACTION with no outstanding transaction.");
        }
        _db.rollback();

        // Look through our escalated commands and see if it's one being processed
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this so we know that this command might be
            // corrupted if the master crashes.
            SINFO("Master has rolled back in response to our command " << message["ID"]);
            commandIt->second.transaction = message;
        }
    } else if (SIEquals(message.methodLine, "ESCALATE")) {
        // ESCALATE: Sent to the master by a slave. Is processed like a normal command, except when complete an
        // ESCALATE_RESPONSE is sent to the slave that initiated the escalation.
        if (!message.isSet("ID")) {
            throw "missing ID";
        }
        if (_state != MASTERING) {
            // Reject escalation because we're no longer mastering
            PWARN("Received ESCALATE but not MASTERING, aborting.");
            SData aborted("ESCALATE_ABORTED");
            aborted["ID"] = message["ID"];
            aborted["Reason"] = "not mastering";
            _sendToPeer(peer, aborted);
        } else {
            // We're mastering, make sure the rest checks out
            SData request;
            if (!request.deserialize(message.content)) {
                throw "malformed request";
            }
            if ((*peer)["Subscribed"] != "true") {
                throw "not subscribed";
            }
            if (!message.isSet("ID")) {
                throw "missing ID";
            }
            PINFO("Received ESCALATE command for '" << message["ID"] << "' (" << request.methodLine << ")");

            // Create a new Command and send to the server.
            SQLiteCommand command(move(request));
            command.initiatingPeerID = peer->id;
            command.id = message["ID"];
            _server.acceptCommand(move(command));
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_CANCEL")) {
        // ESCALATE_CANCEL: Sent to the master by a slave. Indicates that the slave would like to cancel the escalated
        // command, such that it is not processed. For example, if the client that sent the original request
        // disconnects from the slave before an answer is returned, there is no value (and sometimes a negative value)
        // to the master going ahead and completing it.
        if (!message.isSet("ID")) {
            throw "missing ID";
        }
        if (_state != MASTERING) {
            // Reject escalation because we're no longer mastering
            PWARN("Received ESCALATE_CANCEL but not MASTERING, ignoring.");
        } else {
            // We're mastering, make sure the rest checks out
            SData request;
            if (!request.deserialize(message.content)) {
                throw "malformed request";
            }
            if ((*peer)["Subscribed"] != "true") {
                throw "not subscribed";
            }
            if (!message.isSet("ID")) {
                throw "missing ID";
            }
            const string& commandID = SToLower(message["ID"]);
            PINFO("Received ESCALATE_CANCEL command for '" << commandID << "'");

            // Pass it along to the server. We don't try and cancel a command that's currently being committed. It's
            // both super unlikely to happen (as it requires perfect timing), and not a deterministic operation anyway
            // (i.e., a few MS network latency would make it too late, anyway).
            _server.cancelCommand(commandID);
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_RESPONSE")) {
        // ESCALATE_RESPONSE: Sent when the master processes the ESCALATE.
        if (_state != SLAVING) {
            throw "not slaving";
        }
        if (!message.isSet("ID")) {
            throw "missing ID";
        }
        SData response;
        if (!response.deserialize(message.content)) {
            throw "malformed content";
        }

        // Go find the escalated command
        PINFO("Received ESCALATE_RESPONSE for '" << message["ID"] << "'");
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Process the escalated command response
            SQLiteCommand& command = commandIt->second;
            if (command.escalationTimeUS) {
                command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
                SINFO("[performance] Total escalation time for command " << command.request.methodLine << " was "
                      << command.escalationTimeUS << "us.");
            }
            command.response = response;
            command.complete = true;
            _server.acceptCommand(move(command));
            _escalatedCommandMap.erase(commandIt);
        } else {
            SHMMM("Received ESCALATE_RESPONSE for unknown command ID '" << message["ID"] << "', ignoring. " << message.serialize());
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_ABORTED")) {
        // ESCALATE_RESPONSE: Sent when the master aborts processing an escalated command. Re-submit to the new master.
        if (_state != SLAVING) {
            throw "not slaving";
        }
        if (!message.isSet("ID")) {
            throw "missing ID";
        }
        PINFO("Received ESCALATE_ABORTED for '" << message["ID"] << "' (" << message["Reason"] << ")");

        // Look for that command
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Re-queue this
            SQLiteCommand& command = commandIt->second;
            PINFO("Re-queueing command '" << message["ID"] << "' (" << command.request.methodLine << ") ("
                  << command.id << ")");
            _server.acceptCommand(move(command));
            _escalatedCommandMap.erase(commandIt);
        } else
            SWARN("Received ESCALATE_ABORTED for unescalated command " << message["ID"] << ", ignoring.");
    } else {
        throw "unrecognized message";
    }
}

void SQLiteNode::_onConnect(Peer* peer) {
    SASSERT(peer);
    SASSERTWARN(!SIEquals((*peer)["LoggedIn"], "true"));
    // Send the LOGIN
    PINFO("Sending LOGIN");
    SData login("LOGIN");
    login["Priority"] = to_string(_priority);
    login["State"] = stateNames[_state];
    login["Version"] = _version;
    _sendToPeer(peer, login);
}

// --------------------------------------------------------------------------
// On Peer Disconnections
// --------------------------------------------------------------------------
// Whenever a peer disconnects, the following checks are made to verify no
// internal consistency has been lost:  (Technically these checks need only be
// made in certain states, but we'll check them in all states just to be sure.)
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
        SASSERTWARN(_state == SUBSCRIBING || _state == SLAVING);
        _masterPeer = nullptr;
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
        // progress when the master died, in which case we say they completed with a 500 Error.
        for (auto& cmd : _escalatedCommandMap) {
            // If this isn't set, the master hadn't actually started processing this, and we can re-queue it.
            if (!cmd.second.transaction.methodLine.empty()) {
                PWARN("Aborting escalated command '" << cmd.second.request.methodLine << "' (" << cmd.second.id
                      << ") in transaction state '" << cmd.second.transaction.methodLine << "'");
                cmd.second.complete = true;
                cmd.second.response.methodLine = "500 Aborted";
            }
            _server.acceptCommand(move(cmd.second));
        }
        _escalatedCommandMap.clear();
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
}

void SQLiteNode::_sendToPeer(Peer* peer, const SData& message) {
    SASSERT(peer);
    SASSERT(!message.empty());

    // If a peer is currently disconnected, we can't send it a message.
    if (!peer->s) {
        PWARN("Can't send message to peer, no socket. Message '" << message.methodLine << "' will be discarded.");
        return;
    }
    // Piggyback on whatever we're sending to add the CommitCount/Hash
    SData messageCopy = message;
    messageCopy["CommitCount"] = to_string(_db.getCommitCount());
    messageCopy["Hash"] = _db.getCommittedHash();
    peer->s->send(messageCopy.serialize());
}

void SQLiteNode::_sendToAllPeers(const SData& message, bool subscribedOnly) {
    // Piggyback on whatever we're sending to add the CommitCount/Hash, but only serialize once before broadcasting.
    SData messageCopy = message;
    if (!messageCopy.isSet("CommitCount")) {
        messageCopy["CommitCount"] = SToStr(_db.getCommitCount());
    }
    if (!messageCopy.isSet("Hash")) {
        messageCopy["Hash"] = _db.getCommittedHash();
    }
    const string& serializedMessage = messageCopy.serialize();

    // Loop across all connected peers and send the message
    for (auto peer : peerList) {
        // Send either to everybody, or just subscribed peers.
        if (peer->s && (!subscribedOnly || SIEquals((*peer)["Subscribed"], "true"))) {
            // Send it now, without waiting for the outer event loop
            peer->s->send(serializedMessage);
        }
    }
}

void SQLiteNode::_changeState(SQLiteNode::State newState) {
    SAUTOLOCK(stateMutex);

    // Did we actually change _state?
    SQLiteNode::State oldState = _state;
    if (newState != oldState) {
        // Depending on the state, set a timeout
        SDEBUG("Switching from '" << stateNames[_state] << "' to '" << stateNames[newState] << "'");
        uint64_t timeout = 0;
        if (newState == STANDINGUP) {
            // If two nodes try to stand up simultaneously, they can get in a conflicted state where they're waiting
            // for the other to respond, but neither sends a response. We want a short timeout on this state.
            // TODO: Maybe it would be better to re-send the message indicating we're standing up when we see someone
            // hasn't responded.
            timeout = STIME_US_PER_S * 5 + SRandom::rand64() % STIME_US_PER_S * 5;
        } else if (newState == SEARCHING || newState == SUBSCRIBING) {
            timeout = SQL_NODE_DEFAULT_RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_S * 5;
        } else if (newState == SYNCHRONIZING) {
            timeout = SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT + SRandom::rand64() % STIME_US_PER_M * 5;
        } else {
            timeout = 0;
        }
        SDEBUG("Setting state timeout of " << timeout / STIME_US_PER_MS << "ms");
        _stateTimeout = STimeNow() + timeout;

        // Additional logic for some old states
        if (SWITHIN(MASTERING, _state, STANDINGDOWN) && !SWITHIN(MASTERING, newState, STANDINGDOWN)) {
            // We are no longer mastering.  Are we processing a command?
            if (commitInProgress()) {
                // Abort this command
                SWARN("Stopping MASTERING/STANDINGDOWN with commit in progress. Canceling.");
                _commitState = CommitState::FAILED;
                if (!_db.getUncommittedHash().empty()) {
                    _db.rollback();
                }
            }
        }

        // Clear some state if we can
        if (newState < SUBSCRIBING) {
            // We're no longer SUBSCRIBING or SLAVING, so we have no master
            _masterPeer = nullptr;
        }

        // Additional logic for some new states
        if (newState == MASTERING) {
            // Seed our last sent transaction.
            {
                SQLITE_COMMIT_AUTOLOCK;
                unsentTransactions.store(false);
                _lastSentTransactionID = _db.getCommitCount();
                // Clear these.
                _db.getCommittedTransactions();
            }
        } else if (newState == STANDINGDOWN) {
            // Abort all remote initiated commands if no longer MASTERING
            // TODO: No we don't, we finish it, as per other documentation in this file.
        } else if (newState == SEARCHING) {
            if (!_escalatedCommandMap.empty()) {
                // This isn't supposed to happen, though we've seen in logs where it can.
                // So what we'll do is try and correct the problem and log the state we're coming from to see if that
                // gives us any more useful info in the future.
                _escalatedCommandMap.clear();
                SWARN(
                    "Switching from '" << stateNames[_state] << "' to '" << stateNames[newState]
                                       << "' but _escalatedCommandMap not empty. Clearing it and hoping for the best.");
            }
        }

        // Send to everyone we're connected to, whether or not
        // we're "LoggedIn" (else we might change state after sending LOGIN,
        // but before we receive theirs, and they'll miss it).
        // Broadcast the new state
        _state = newState;
        SData state("STATE");
        state["State"] = stateNames[_state];
        state["Priority"] = SToStr(_priority);
        _sendToAllPeers(state);
    }
}

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
        if (!_db.getCommit(peerCommitCount, ignore, myHash)) {
            PWARN("Error getting commit for peer's commit: " << peerCommitCount << ", my commit count is: " << _db.getCommitCount());
            throw "error getting hash";
        }
        if (myHash != (*peer)["Hash"]) {
            SWARN("[TY5] Hash mismatch. Peer at commit:" << peerCommitCount << " with hash " << (*peer)["Hash"]
                  << ", but we have hash: " << myHash << " for that commit.");
            throw "hash mismatch";
        }
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
            toIndex = min(toIndex, fromIndex + 100); // 100 transactions at a time
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

void SQLiteNode::_updateSyncPeer()
{
    Peer* newSyncPeer = nullptr;
    uint64_t commitCount = _db.getCommitCount();
    for (auto peer : peerList) {
        // If either of these conditions are true, then we can't use this peer.
        if (!peer->test("LoggedIn") || peer->calcU64("CommitCount") <= commitCount) {
            continue;
        }

        // Any peer that makes it to here is a usable peer, so it's by default better than nothing.
        if (!newSyncPeer) {
            newSyncPeer = peer;
        }
        // If the previous best peer and this one have the same latency (meaning they're probably both 0), the best one
        // is the one with the highest commit count.
        else if (newSyncPeer->latency == peer->latency) {
            if (peer->calc64("CommitCount") > newSyncPeer->calc64("CommitCount")) {
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
            from = _syncPeer->name + " (commit count=" + (*_syncPeer)["CommitCount"] + "), latency="
                                   + to_string(_syncPeer->latency) + "us";
        } else {
            from = "(NONE)";
        }
        if (newSyncPeer) {
            to = newSyncPeer->name + " (commit count=" + (*newSyncPeer)["CommitCount"] + "), latency="
                                   + to_string(newSyncPeer->latency) + "us";
        } else {
            to = "(NONE)";
        }

        // We see strange behavior when choosing peers. Peers are being chosen from distant data centers rather than
        // peers on the same LAN. This is extra diagnostic info to try and see why we don't choose closer ones.
        list<string> nonChosenPeers;
        for (auto peer : peerList) {
            if (peer == newSyncPeer || peer == _syncPeer) {
                continue; // These ones we're already logging.
            } else if (!peer->test("LoggedIn")) {
                nonChosenPeers.push_back(peer->name + ":!loggedIn");
            } else if (peer->calcU64("CommitCount") <= commitCount) {
                nonChosenPeers.push_back(peer->name + ":commit=" + (*peer)["CommitCount"]);
            } else {
                nonChosenPeers.push_back(peer->name + ":" + to_string(peer->latency) + "us");
            }
        }
        SINFO("Updating SYNCHRONIZING peer from " << from << " to " << to << ". Not chosen: " << SComposeList(nonChosenPeers));

        // And save the new sync peer internally.
        _syncPeer = newSyncPeer;
    }
}

void SQLiteNode::_reconnectPeer(Peer* peer) {
    // If we're connected, just kill the connection
    if (peer->s) {
        // Reset
        SWARN("Reconnecting to '" << peer->name << "'");
        shutdownSocket(peer->s);
        (*peer)["LoggedIn"] = "false";
    }
}

void SQLiteNode::_reconnectAll() {
    // Loop across and reconnect
    for (auto peer : peerList) {
        _reconnectPeer(peer);
    }
}

bool SQLiteNode::_majoritySubscribed() {
    // Count up how may full and subscribed peers we have (A "full" peer is one that *isn't* a permaslave).
    int numFullPeers = 0;
    int numFullSlaves = 0;
    for (auto peer : peerList) {
        if (peer->params["Permaslave"] != "true") {
            ++numFullPeers;
            if (peer->test("Subscribed")) {
                ++numFullSlaves;
            }
        }
    }

    // Done!
    return (numFullSlaves * 2 >= numFullPeers);
}

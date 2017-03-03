#include <libstuff/libstuff.h>
#include "SQLiteNode.h"

// This is a hack so that we can support 'special' Status commands, but it really breaks encapsulation entirely.
#include <plugins/Status.h>

atomic<int> SQLiteNode::_commandCount(0);
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
/// **FIXME**: Add 'nextActivity' to update() [TYLER: Partially addressed?]
///
/// **FIXME**: If master dies before sending ESCALATE_RESPONSE (or if slave dies
///            before receiving it), then a command might have been committed to
///            the database without notifying whoever initiated it.  Perhaps have
///            the caller identify each command with a unique command guid, and
///            verify inside the query that the command hasn't been executed yet?
///
// --------------------------------------------------------------------------
#undef SLOGPREFIX
#define SLOGPREFIX "{" << name << "/" << SQLiteNode::stateNames[_state] << "} "

// Useful STL macros
// _CT_ : Container type
// _C_  : Container
// _I_  : Iterator
#define SFOREACH(_CT_, _C_, _I_) for (_CT_::iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)
#define SFOREACHREVERSE(_CT_, _C_, _I_) for (_CT_::reverse_iterator _I_ = (_C_).rbegin(); _I_ != (_C_).rend(); ++_I_)
#define SFOREACHCONST(_CT_, _C_, _I_) for (_CT_::const_iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)
#define SFOREACHMAP(_CT0_, _CT1_, _C_, _I_)                                                                            \
    for (map<_CT0_, _CT1_>::iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)
#define SFOREACHMAPREVERSE(_CT0_, _CT1_, _C_, _I_)                                                                     \
    for (map<_CT0_, _CT1_>::reverse_iterator _I_ = (_C_).rbegin(); _I_ != (_C_).rend(); ++_I_)
#define SFOREACHMAPCONST(_CT0_, _CT1_, _C_, _I_)                                                                       \
    for (map<_CT0_, _CT1_>::const_iterator _I_ = (_C_).begin(); _I_ != (_C_).end(); ++_I_)

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

// Initializations for static vars.
atomic<bool> SQLiteNode::unsentTransactions(false);
uint64_t SQLiteNode::_lastSentTransactionID = 0;

// --------------------------------------------------------------------------
SQLiteNode::SQLiteNode(SQLiteServer& server, SQLite& db, const string& name, const string& host,
                       const string& peerList, int priority, uint64_t firstTimeout, const string& version,
                       int quorumCheckpoint)
    : STCPNode(name, host, max(SQL_NODE_DEFAULT_RECV_TIMEOUT, SQL_NODE_SYNCHRONIZING_RECV_TIMEOUT)),
      _db(db),
      _processTimer("process()"), _commitTimer("COMMIT"), _server(server)
    {
    SASSERT(priority >= 0);
    // Initialize
    _priority = priority;
    _setState(SEARCHING);
    _currentCommand = nullptr;
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

// --------------------------------------------------------------------------
SQLiteNode::~SQLiteNode() {
    // Make sure it's a clean shutdown
    //SASSERTWARN(_isQueuedCommandMapEmpty());
    SASSERTWARN(_escalatedCommandMap.empty());
    //SASSERTWARN(processedCommandList.empty());
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

    // If we're doing a commit, don't shut down.
    if (_commitInProgress) {
        return false;
    }

    // If we have non-"Connection: wait" commands escalated to master, not done
    if (!_escalatedCommandMap.empty()) {
        return false;
    }

    // Finally, we can shut down if we have no open processed commands.
    return processedCommandList.empty();
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
    if (_state > WAITING) {
        // Not in a shutdown state
        SINFO("Can't graceful shutdown yet because state="
              << SQLiteNode::stateNames[_state] << ", commitInProgress=" << _commitInProgress
              << ", escalated=" << _escalatedCommandMap.size() << ", processed=" << processedCommandList.size());

        // If we end up with anything left in the escalated command map when we're trying to shut down, let's log it,
        // so we can try and diagnose what's happening.
        if (!_escalatedCommandMap.empty()) {
            for (auto& cmd : _escalatedCommandMap) {
                string name = cmd.first;
                SQLiteCommand& command = cmd.second;
                int64_t created = command.creationTimestamp;
                int64_t elapsed = STimeNow() - created;
                double elapsedSeconds = (double)elapsed / STIME_US_PER_S;
                SINFO("Escalated command remaining at shutdown("
                      << name << "): " << command.request.methodLine << ". Created: " << command.creationTimestamp
                      << " (" << elapsedSeconds << "s ago)");
            }
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
        SINFO("Can't graceful shutdown yet because waiting on commands: commitInProgress="
              << _commitInProgress << ", escalated=" << _escalatedCommandMap.size()
              << ", processed=" << processedCommandList.size());
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
        SFOREACH (list<Peer*>, peerList, peerIt) {
            // Clear the response flag from the last transaction
            Peer* peer = *peerIt;
            (*peer)["TransactionResponse"].clear();
        }

        SData commit("COMMIT_TRANSACTION");
        commit["ID"] = transaction["ID"];

        commit["CommitCount"] = transaction["NewCount"];
        commit["Hash"] = hash;

        _sendToAllPeers(commit, true); // subscribed only

        _lastSentTransactionID = id;
    }

    unsentTransactions.store(false);
}

void SQLiteNode::_escalateCommand(SQLiteCommand&& command) {
    // Send this to the MASTER
    SASSERT(_masterPeer);
    SASSERTEQUALS((*_masterPeer)["State"], "MASTERING");
    uint64_t elapsed = STimeNow() - command.creationTimestamp;
    SINFO("Escalating '" << command.request.methodLine << "' (" << command.id << ") to MASTER '" << _masterPeer->name
          << "' after " << elapsed / STIME_US_PER_MS << " ms");

    // Create a command to send to our master.
    SData escalate("ESCALATE");
    escalate["ID"] = command.id;
    escalate.content = command.request.serialize();

    // Store the command as escalated.
    _escalatedCommandMap.emplace(command.id, move(command));

    // And send to master.
    _sendToPeer(_masterPeer, escalate);
}
#if 0
// --------------------------------------------------------------------------
SQLiteNode::SQLiteCommand* SQLiteNode::_finishCommand(SQLiteCommand* command) {
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
        if (command->initiator->s) {
            SData escalate("ESCALATE_RESPONSE");
            escalate["ID"] = command->id;
            escalate.content = command->response.serialize();
            _sendToPeer(command->initiator, escalate);
        } else {
            SWARN("Peer socket died and trying to finish command. Handling disconnect.");
            _onDisconnect(command->initiator);
        }
        delete command;
        return nullptr;
    } else if (SIEquals(command->request.methodLine, "UpgradeDatabase")) {
        // Special command, just delete it.
        SINFO("Database upgrade complete");
        delete command;
        return nullptr;
    } else {
        // Locally-initiated command -- hold onto it until the caller cleans up.
        // TODO: Probably not for retried commands.
        processedCommandList.push_back(command);
        return command;
    }
}
#endif
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
        SFOREACH (list<Peer*>, peerList, peerIt) {
            // Wait until all connected (or failed) and logged in
            Peer* peer = *peerIt;
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
            SFOREACH (list<Peer*>, peerList, peerIt)
                (*peerIt)->erase("StandupResponse");
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

        // If there are outstanding transactions to send, then we'll do that, but only if we're not waiting on a
        // synchronous command to complete. However, it should be impossible for there to be any outstanding
        // transactions while we wait for a synchronous command to complete
        if (!_currentCommand) {
            _sendOutstandingTransactions();
        }

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
            ConsistencyLevel consistencyRequired =
                (_commitsSinceCheckpoint >= _quorumCheckpoint) ? QUORUM : _currentCommand->writeConsistency;

            // Figure out if we have enough consistency
            bool consistentEnough = false;
            switch (consistencyRequired) {
            case ASYNC:
                // Always consistent enough if we don't care!
                consistentEnough = true;
                break;

            case ONE:
                // So long at least one full approved (if we have any peers, that is), we're good
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
            SDEBUG("'" << _currentCommand->request.methodLine << "' (#" << _currentCommand->id << "): numFullPeers="
                       << numFullPeers << ", numFullSlaves="
                       << numFullSlaves << ", numFullResponded="
                       << numFullResponded << ", numFullApproved="
                       << numFullApproved << ", majorityApproved="
                       << majorityApproved << ", writeConsistency="
                       << consistencyLevelNames[_currentCommand->writeConsistency] << ", consistencyRequired="
                       << consistencyLevelNames[consistencyRequired] << ", consistentEnough="
                       << consistentEnough << ", everybodyResponded="
                       << everybodyResponded << ", initiatorSubscribed="
                       << initiatorSubscribed << ", commitsSinceCheckpoint="
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
                _commitTimer.stop();
                commandFinished = true;

                // Notify everybody to rollback
                SWARN("[TY2] ROLLBACK, not approved: " << _currentCommand->id);
                SData rollback("ROLLBACK_TRANSACTION");
                rollback["ID"] = _currentCommand->id;
                _sendToAllPeers(rollback, true); // subscribed only

                // Notify the caller that this command failed
                _currentCommand->response.clear();
                _currentCommand->response.methodLine = "500 Failed to get adequate consistency";
            } else if (consistentEnough) {
                // Commit this distributed transaction.  Either we have quorum, or we don't need it.
                uint64_t start = STimeNow();
                int result = _db.commit();

                if (result == SQLITE_BUSY_SNAPSHOT) {
                    _db.rollback();
                    _commitTimer.stop();

                    // Notify everybody to rollback
                    SWARN("ROLLBACK, conflicted on sync: " << _currentCommand->id << " : "
                          << _currentCommand->request.methodLine);
                    SData rollback("ROLLBACK_TRANSACTION");
                    rollback["ID"] = _currentCommand->id;
                    _sendToAllPeers(rollback, true); // subscribed only

                    // We're done with this, we'll re-open it, and then return from `update` indicating we need to run
                    // again. We specifically don't call `finishCommand`, as we don't want to send a response to the
                    // caller.
                    SQLite::commitLock.unlock();

                    // Make sure the response is empty, and re-open the command.
                    _currentCommand->response.clear();
                    // reopenCommand(_currentCommand);

                    _currentCommand = nullptr;
                    return true;
                } else {
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
                          << consistencyLevelNames[consistencyRequired] << "), " << numFullApproved << " of "
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
                    _lastSentTransactionID = getCommitCount();

                    // clear the unsent transactions, we've sent them all (including this one);
                    _db.getCommittedTransactions();

                    SINFO("Successfully committed: " << _currentCommand->id << ":"
                          << _currentCommand->request.methodLine
                          << ". Sending COMMIT_TRANSACTION to peers. Updated _lastSentTransactionID to "
                          << _lastSentTransactionID);
                    SData commit("COMMIT_TRANSACTION");
                    commit["ID"] = _currentCommand->id;
                    _sendToAllPeers(commit, true); // subscribed only
                }

                // OK, this might fail.
                _commitTimer.stop();
                _currentCommand->processingTime += STimeNow() - start;
                commandFinished = true;
            }

            // Did we finish the command:
            if (commandFinished) {
                // Either way, we're done with this command
                //_finishCommand(_currentCommand);
                _currentCommand = nullptr;

                // If we haven't received majority approval, increment how
                // many commits we've had without a "checkpoint"
                if (majorityApproved) {
                    // Safe!
                    SINFO("Commit checkpoint achieved, resetting uncheckpointed commit count to 0.");
                    _commitsSinceCheckpoint = 0;
                } else {
                    // We're going further out on a limb...
                    _commitsSinceCheckpoint++;
                }

                // Done with this.
                SQLite::commitLock.unlock();
            } else {
                // Still waiting
                SINFO("Waiting to commit: '"
                      << _currentCommand->request.methodLine << "' (#" << _currentCommand->id
                      << "): consistencyRequired=" << consistencyLevelNames[consistencyRequired]
                      << ", commitsSinceCheckpoint=" << _commitsSinceCheckpoint);
            }
        }

        // If we're the master, see if we're to stand down (and if not, start a new command)
        if (_state == MASTERING && !_currentCommand) {
            // See if it's time to stand down
            string standDownReason;
            bool shuttingDown = false;
            if (gracefulShutdown()) {
                // Graceful shutdown.  Set priority 1 and stand down so
                // we'll re-connect to the new master and finish up our
                // commands.
                standDownReason = "Shutting down, setting priority 1 and STANDINGDOWN.";
                shuttingDown = true;
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

            // Historically, if we're shutting down, everything's already processed and handled, but since we now
            // support other threads processing commands on our behalf, we can end up in the state where we're shutting
            // down, but we still have either queued or processed commands. In the case that we still have queued
            // commands to deal with at shutdown, we'll defer switching to STANDINGDOWN for another loop iteration to
            // handle those commands. And, once we do switch to STANDNGDOWN, we return `true` rather than `false` to
            // let the caller know it should clean up our command queues, as we may have processed some more commands.
            if (!shuttingDown || commitInProgress()) {
                // Do we want to stand down, and can we?
                if (!standDownReason.empty()) {
                    // Do it
                    SHMMM(standDownReason);
                    _changeState(STANDINGDOWN);
                    SINFO("Standing down: " << standDownReason);
                    // We might have processed more commands that the caller needs to respond to.
                    return false;
                }
            } else {
                SWARN("Shutting down but non-empty command queue, running another MASTERING loop.");
            }


            // Not standing down -- do we have any commands to start?  Only
            // dequeue if we either have no peers configured (meaning
            // remote commits are non-mandatory), or if at least half of
            // the peers are connected.  Otherwise we're in a live
            // environment but can't commit anything because we may cause a
            // split brain scenario.
            if (!commitInProgress() && _majoritySubscribed()) {
            #if 0
                // Have commands and a majority, so let's start a new one.
                SFOREACHMAPREVERSE(int, list<SQLiteCommand*>, _queuedCommandMap, it) {
                    // **XXX: Using pointer to list because STL containers copy on assignment.
                    list<SQLiteCommand*>* commandList = &it->second;
                    if (!commandList->empty()) {
                        // Find the first command that either has no httpsRequest,
                        // or has a completed one.
                        int64_t now = STimeNow();
                        list<SQLiteCommand*>::iterator nextIt = commandList->begin();
                        while (nextIt != commandList->end()) {
                            // See if this command has an outstanding https
                            // transaction.  If so, wait for it to complete.
                            list<SQLiteCommand*>::iterator commandIt = nextIt++;
                            SQLiteCommand* command = *commandIt;
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

                            // Determine if we need to force a synchronous commit due to excessive conflicts
                            bool unlock = false;
                            if (_currentCommand->processCount >= MAX_PROCESS_TRIES) {
                                SWARN("[concurrent] Command " << _currentCommand->id << " processed "
                                      << _currentCommand->processCount << " times, exceeded max of "
                                      << MAX_PROCESS_TRIES << ", forcing synchronous commit.");
                                unlock = true;
                                SQLite::commitLock.lock();
                            }

                            // Process the command itself and see if it needs to commit
                            bool needsCommit = _processCommandWrapper(_db, _currentCommand);
                            SASSERT(!_currentCommand->response.empty()); // Must set a response

                            // Anything to commit?
                            if (needsCommit) {

                                // We're about to send a transaction here, grab our commit mutex and send anything
                                // outstanding. This remains locked until we've finished this transaction.
                                SQLite::commitLock.lock();

                                // Clear anything outstanding before starting this one.
                                uint64_t commitCount = getCommitCount();
                                // This breaks with a non-recursive mutex.
                                _sendOutstandingTransactions();

                                // There's no handling for a failed prepare. This should only happen if the DB has been
                                // corrupted or something catastrophic like that.
                                SASSERT(_db.prepare());

                                _commitTimer.start();
                                // Begin the distributed transaction
                                SASSERT(!_db.getUncommittedQuery().empty());
                                SINFO("Finished processing command '"
                                      << _currentCommand->request.methodLine << "' (" << _currentCommand->id
                                      << "), beginning distributed transaction for commit #" << commitCount + 1
                                      << " (" << _db.getUncommittedHash() << ")");
                                _currentCommand->replicationStartTimestamp = STimeNow();
                                _currentCommand->transaction.methodLine = "BEGIN_TRANSACTION";
                                _currentCommand->transaction["Command"] = _currentCommand->request.methodLine;
                                _currentCommand->transaction["NewCount"] = SToStr(commitCount + 1);
                                _currentCommand->transaction["NewHash"] = _db.getUncommittedHash();
                                _currentCommand->transaction["ID"] = _currentCommand->id;
                                _currentCommand->transaction.content = _db.getUncommittedQuery();
                                _sendToAllPeers(_currentCommand->transaction, true); // subscribed only
                                SFOREACH (list<Peer*>, peerList, peerIt) {
                                    // Clear the response flag from the last transaction
                                    Peer* peer = *peerIt;
                                    (*peer)["TransactionResponse"].clear();
                                }

                                // If we forced a synchronous commit, unlock.
                                if (unlock) {
                                    // NOTE: This doesn't actually unlock anything, it just decrements our lock
                                    // counter, so that we'll unlock properly when we commit the transaction.
                                    SQLite::commitLock.unlock();
                                }

                                // By returning 'true', we update the FSM immediately, and thus evaluate whether or not
                                // we need to wait for quorum.  This keeps all the quorum logic in the same place.
                                if (STimeNow() > nextActivity) {
                                    // TODO: If we hit this, nobody can commit anything until we resolve that. Is that
                                    // an issue?
                                    SINFO("Timeout reached while processing (transaction) commands. Exceeded by "
                                          << (STimeNow() - nextActivity) << "us");
                                    return false;
                                }
                                return true;
                            } else {
                                // Doesn't need to commit anything; done processing.
                                SINFO("Finished processing command '" << _currentCommand->request.methodLine << "' ("
                                                                      << _currentCommand->id
                                                                      << "), nothing to commit.");
                                SASSERT(!_db.insideTransaction());
                                //_finishCommand(_currentCommand);
                                _currentCommand = nullptr;
                            }

                            // If we forced a synchronous commit, unlock.
                            if (unlock) {
                                SQLite::commitLock.unlock();
                            }

                            // **NOTE: This loops back and starts the next command of the same priority immediately
                            if (STimeNow() > nextActivity) {
                                SINFO("Timeout reached while processing commands. Exceeded by "
                                      << (STimeNow() - nextActivity) << "us");
                                return false;
                            }
                        }
                    }

                    // **NOTE: This loops back and starts the next command of the next lower priority immediately
                }

                // **NOTE: we've exhausted the current batch of queued commands and can continue
            #endif
            }
        }

        // We're standing down; wait until there are no more subscribed peers
        if (_state == STANDINGDOWN) {
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
                _changeState(SEARCHING);
                return true; // Re-update
            }
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

        // If the master stands down, stop slaving
        // **FIXME: Wait for all commands to finish
        if (!SIEquals((*_masterPeer)["State"], "MASTERING")) {
            // Master stepping down
            SHMMM("Master stepping down, re-queueing commands and re-SEARCHING.");

            // If there were escalated commands, give them back to the server to retry.
            for (auto& cmd : _escalatedCommandMap) {
                _server.acceptCommand(move(cmd.second));
            }
            _escalatedCommandMap.clear();
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
        /// - LOGIN: This is the first message sent to and received from a new
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
            for (from = SEARCHING; from <= SLAVING; from++)
                if (SIEquals(oldState, stateNames[from]))
                    break;
            for (to = SEARCHING; to <= SLAVING; to++)
                if (SIEquals(newState, stateNames[to]))
                    break;
            if (from > SLAVING)
                PWARN("Peer coming from unrecognized state '" << oldState << "'");
            if (to > SLAVING)
                PWARN("Peer going to unrecognized state '" << newState << "'");
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
            if (!okTransition)
                PWARN("Peer making invalid transition from '" << stateNames[from] << "' to '" << stateNames[to] << "'");

            // Next, should we do something about it?
            if (to == SEARCHING) {
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
            } else if (to == STANDINGUP) {
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
                if (SWITHIN(STANDINGUP, _state, STANDINGDOWN)) {
                    // Oh crap, it's trying to stand up while we're mastering.
                    // Who is higher priority?
                    if (peer->calc("Priority") > _priority) {
                        // Not good -- we're in the way.  Not sure how we got
                        // here, so just reconnect and start over.
                        PWARN("Higher-priority peer is trying to stand up while we are " << stateNames[_state]
                              << ", reconnecting and SEARCHING.");
                        _reconnectAll();
                        _changeState(SEARCHING);
                    } else {
                        // Deny because we're currently in the process of mastering
                        // and we're higher priority
                        response["Response"] = "deny";
                        response["Reason"] = "I am mastering";

                        // Hmm, why is a lower priority peer trying to stand up? Is it possible we're no longer in
                        // control of the cluster? Let's see how many nodes are subscribed.
                        if (_majoritySubscribed()) {
                            // we have a majority of the cluster, so ignore this oddity.
                            PHMMM("Lower-priority peer is trying to stand up while we are " << stateNames[_state]
                                  << " with a majority of the cluster; denying and ignoring.");
                        } else {
                            // We don't have a majority of the cluster -- maybe
                            // it knows something we don't?  For example, it
                            // could be that the rest of the cluster has forked
                            // away from us.  This can happen if the master
                            // hangs while processing a command: by the time it
                            // finishes, the cluster might have elected a new
                            // master, forked, and be a thousand commits in the
                            // future.  In this case, let's just reset
                            // everything anyway to be safe.
                            PWARN("Lower-priority peer is trying to stand up while we are " << stateNames[_state]
                                  << ", but we don't have a majority of the cluster so reconnecting and SEARCHING.");
                            _reconnectAll();
                            _changeState(SEARCHING);
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
                if (SIEquals(response["Response"], "approve")) {
                    PINFO("Approving standup request");
                } else {
                    PHMMM("Denying standup request because " << response["Reason"]);
                }
                _sendToPeer(peer, response);
            } else if (from == STANDINGDOWN) {
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
            }
            else {
                PHMMM("Received standup denial: reason='" << message["Reason"] << "'");
            }
            (*peer)["StandupResponse"] = message["Response"];
        } else {
            SINFO("Got STANDUP_RESPONSE but not STANDINGUP. Probably a late message, ignoring.");
        }
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
        if (_state != SYNCHRONIZING)
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
        /// - SUBSCRIBE: Sent by a node in the WAITING state to the current
        ///     master to begin SLAVING.  Respond SUBSCRIPTION_APPROVED with any
        ///     COMMITs that the subscribing peer lacks (for example, any commits
        ///     that have occured after it completed SYNCHRONIZING but before this
        ///     SUBSCRIBE was received).  Tag this peer as "subscribed" for use
        ///     in the MASTERING and STANDINGDOWN update loops.  Finally, if there
        ///     is an outstanding distributed transaction being processed, send it
        ///     to this new slave.
        ///
        if (_state != MASTERING)
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
        if (_state != SUBSCRIBING)
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
            _changeState(SLAVING);
        } catch (const char* e) {
            // Transaction failed
            SWARN("Subscription failed '" << e << "', reconnecting to master and re-SEARCHING.");
            _reconnectPeer(_masterPeer);
            _changeState(SEARCHING);
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
        if (_state != SLAVING)
            throw "not slaving";
        if (!_masterPeer)
            throw "no master?";
        if (!_db.getUncommittedHash().empty())
            throw "already in a transaction";
        if (_db.getCommitCount() + 1 != message.calcU64("NewCount")) {
            throw "commit count mismatch. Expected: " + message["NewCount"] + ", but would actually be: " + to_string(_db.getCommitCount() + 1);
        } if (!_db.beginTransaction())
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
        } catch (const string& e) {
            // TODO: Don't duplicate the above block.
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
                  << _db.getCommittedHash() << "', uncommittedHash='" << _db.getUncommittedHash() << "', messageHash='" << message["NewHash"]
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
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this
            // so we know that this command might be corrupted if the master
            // crashes.
            SINFO("Master is processing our command " << message["ID"] << " (" << _currentTransactionCommand << ")");
            commandIt->second.transaction = message;
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
        if (_state != MASTERING && _state != STANDINGDOWN)
            throw "not mastering";
        try {
            // Approval of current command, or an old one?
            if (_currentCommand && _currentCommand->id == message["ID"]) {
                // Current, make sure it all matches.
                if (message["NewHash"] != _db.getUncommittedHash()) {
                    throw "new hash mismatch";
                }
                if (message.calcU64("NewCount") != _db.getCommitCount() + 1) {
                    throw "commit count mismatch. Expected: " + message["NewCount"] + ", but would actually be: " + to_string(_db.getCommitCount() + 1);
                } if (peer->params["Permaslave"] == "true")
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
        if (_state != SLAVING)
            throw "not slaving";
        if (_db.getUncommittedHash().empty())
            throw "no outstanding transaction";
        if (message.calcU64("CommitCount") != _db.getCommitCount() + 1) {
            throw "commit count mismatch. Expected: " + message["CommitCount"] + ", but would actually be: " + to_string(_db.getCommitCount() + 1);
        }
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
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this
            // so we know that this command might be corrupted if the master
            // crashes.
            SINFO("Master has committed in response to our command " << message["ID"]);
            commandIt->second.transaction = message;
        }
    } else if (SIEquals(message.methodLine, "ROLLBACK_TRANSACTION")) {
        /// - ROLLBACK_TRANSACTION: Sent to all subscribed slaves by the master when
        ///     it determines that the current outstanding transaction should be
        ///     rolled back.  This completes a given distributed transaction.
        ///
        if (!message.isSet("ID"))
            throw "missing ID";
        if (_state != SLAVING)
            throw "not slaving";
        if (_db.getUncommittedHash().empty())
            throw "no outstanding transaction";
        SINFO("Rolling back slave transaction " << message["ID"]);
        SWARN("ROLLBACK received on slave for: " << message["ID"]);
        _db.rollback();

        // Look through our escalated commands and see if it's one being processed
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // We're starting the transaction for a given command; note this
            // so we know that this command might be corrupted if the master
            // crashes.
            SINFO("Master has rolled back in response to our command " << message["ID"]);
            commandIt->second.transaction = message;
        }
    } else if (SIEquals(message.methodLine, "ESCALATE")) {
        /// - ESCALATE: Sent to the master by a slave.  Is processed like a normal
        ///     command, except when complete an ESCALATE_RESPONSE is sent to the
        ///     slave that initiaed the escalation.
        ///
        if (!message.isSet("ID"))
            throw "missing ID";
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
            if (!request.deserialize(message.content))
                throw "malformed request";
            if ((*peer)["Subscribed"] != "true")
                throw "not subscribed";
            if (!message.isSet("ID"))
                throw "missing ID";
            PINFO("Received ESCALATE command for '" << message["ID"] << "' (" << request.methodLine << ")");

            // Create a new Command and send to the server.
            SQLiteCommand command(move(request));
            command.initiator = peer;
            command.id = message["ID"];
            _server.acceptCommand(move(command));
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
        if (_state != MASTERING) {
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
            if (_currentCommand && SIEquals(_currentCommand->id, commandID)) {
                SINFO("Canceling escalated command " << _currentCommand->id << " (" << _currentCommand->request.methodLine << ")");
                // TODO: ABORT
            } else {
                _server.cancelCommand(commandID);
            }
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_RESPONSE")) {
        /// - ESCALATE_RESPONSE: Sent when the master processes the ESCALATE.
        ///
        if (_state != SLAVING)
            throw "not slaving";
        if (!message.isSet("ID"))
            throw "missing ID";
        SData response;
        if (!response.deserialize(message.content))
            throw "malformed content";

        // Go find the escalated command
        PINFO("Received ESCALATE_RESPONSE for '" << message["ID"] << "'");
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Process the escalated command response
            SQLiteCommand& command = commandIt->second;
            _escalatedCommandMap.erase(command.id);
            command.response = response;
            command.complete = true;
            _server.acceptCommand(move(command));
        } else {
            SHMMM("Received ESCALATE_RESPONSE for unknown command ID '" << message["ID"] << "', ignoring. " << message.serialize());
        }
    } else if (SIEquals(message.methodLine, "ESCALATE_ABORTED")) {
        /// - ESCALATE_RESPONSE: Sent when the master aborts processing an
        ///     escalated command.  Re-submit to the new master.
        ///
        if (_state != SLAVING)
            throw "not slaving";
        if (!message.isSet("ID"))
            throw "missing ID";
        PINFO("Received ESCALATE_ABORTED for '" << message["ID"] << "' (" << message["Reason"] << ")");

        // Look for that command
        auto commandIt = _escalatedCommandMap.find(message["ID"]);
        if (commandIt != _escalatedCommandMap.end()) {
            // Re-queue this
            SQLiteCommand& command = commandIt->second;
            PINFO("Re-queueing command '" << message["ID"] << "' (" << command.request.methodLine << ") ("
                                          << command.id << ")");
            _server.acceptCommand(move(command));
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
    login["State"] = stateNames[_state];
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

    /// - Verify the current command we're processing wasn't initiated by this
    ///   peer.  This should only be possible if we're MASTERING or STANDINGDOWN,
    ///   and if it's SLAVING.  If this happens, drop the command (it'll resubmit)
    ///   and instruct all other subscribed slaves to roll-back the transaction.
    ///
    // **FIXME: Perhaps log if any initiator dies within a timeout of sending
    //          the response; they might be in jeopardy of not being sent out.  Or..
    //          we'll know which had responses go out at the 56K layer -- perhaps
    //          in a crash verify it went out... Or just always verify?
    if (_currentCommand && _currentCommand->initiator == peer) {
        // Clean up this command and notify everyone to roll it back
        PHMMM("Current command initiator disconnected, aborting '" << _currentCommand->request.methodLine << "' ("
                                                                   << _currentCommand->id << ") and rolling back.");
        SASSERTWARN(_state == MASTERING || _state == STANDINGDOWN);
        SASSERTWARN(SIEquals((*peer)["State"], "SLAVING"));
        if (_db.insideTransaction())
            _db.rollback();
        SData rollback("ROLLBACK_TRANSACTION");
        rollback["ID"] = _currentCommand->id;
        SINFO("ROLLBACK on disconnect: " << _currentCommand->id);
        SFOREACH (list<Peer*>, peerList, peerIt)
            if ((**peerIt)["Subscribed"] == "true") {
                // Send the rollback command
                Peer* peer = *peerIt;
                SASSERT(peer->s);
                _sendToPeer(peer, rollback);
            }
        delete _currentCommand;
        _currentCommand = nullptr;
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
    if (!messageCopy.isSet("CommitCount")) {
        messageCopy["CommitCount"] = SToStr(_db.getCommitCount());
    }
    if (!messageCopy.isSet("Hash")) {
        messageCopy["Hash"] = _db.getCommittedHash();
    }
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
void SQLiteNode::_changeState(SQLiteNode::State newState) {
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
        if (SWITHIN(MASTERING, _state, STANDINGDOWN) &&
            !SWITHIN(MASTERING, newState, STANDINGDOWN)) {
            // We are no longer mastering.  Are we processing a command?
            if (_currentCommand) {
                // Abort this command
                SWARN("No longer mastering, aborting current command");
                _currentCommand = nullptr;
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
            if (_currentCommand) {
                if (_currentCommand->initiator) {
                    // No need to wait, explicitly abort
                    SINFO("STANDINGDOWN and aborting " << _currentCommand->id);
                    SData aborted("ESCALATE_ABORTED");
                    aborted["ID"] = _currentCommand->id;
                    aborted["Reason"] = "Standing down";
                    _sendToPeer(_currentCommand->initiator, aborted);
                    delete _currentCommand;
                }
            }
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
        _setState(newState);
        SData state("STATE");
        state["State"] = stateNames[_state];
        state["Priority"] = SToStr(_priority);
        _sendToAllPeers(state);
    }

    // Verify some invariants with the new state
    SASSERT(!_currentCommand || (_state != MASTERING && _state != STANDINGDOWN));
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

// --------------------------------------------------------------------------
void SQLiteNode::_reconnectPeer(Peer* peer) {
    // If we're connected, just kill the connection
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

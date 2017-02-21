/// SQLiteTest.cpp
/// ==============
/// Automated test suite for SQLite and SQLiteNode
///
/// To Do
/// -----
/// - Add secondary httpsRequests
/// - Add pauses, rather than kills
///
#include <libstuff/libstuff.h>
#include "SQLite.h"
#include "SQLiteNode.h"

// Number of commands to run in a given test
#define NUM_COMMANDS 200

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
struct SQLiteTestWebServer : public STCPServer {
    // Simple constructor
    SQLiteTestWebServer(const string& host) : STCPServer(host) {}

    // Accept and respond to everything after a delay
    void postSelect(fd_map& fdm, uint64_t& nextActivity) {
        // Do the base class thing
        STCPServer::postSelect(fdm);

        // Just accept everything
        Socket* s = nullptr;
        while ((s = acceptSocket()))
            SDEBUG("Accepted socket from '" << s->addr << "'");

        // Respond to everything after a delay
        list<Socket*>::iterator nextIt = socketList.begin();
        while (nextIt != socketList.end()) {
            // Are we past its delay?
            list<Socket*>::iterator it = nextIt++;
            Socket* s = *it;
            if (s->state == STCP_CONNECTED) {
                // When will it timeout?
                uint64_t timeout = s->openTime + STIME_US_PER_S;
                if (STimeNow() > timeout) {
                    // Respond and shut down
                    SDEBUG("Responding to '" << s->addr << "'");
                    s->sendBuffer += SData("HTTP 200 OK").serialize();
                    shutdownSocket(s, SHUT_RD);
                } else
                    nextActivity = min(nextActivity, timeout);
            } else if (s->state == STCP_CLOSED) {
                // Close this socket
                SDEBUG("Done with socket from '" << s->addr << "'");
                closeSocket(s);
            }
        }
    }
};

struct SQLiteTestWebClient : public SHTTPSManager {
    // Open a basic command
    Transaction* send(const string& method) {
        // Just send a basic transaction
        SDEBUG("Sending '" << method << "'");
        return _httpsSend("http://localhost:12345", SData(method));
    }

    // Pass through the response
    virtual bool _onRecv(Transaction* transaction) {
        // Just pass through the response
        string ignore;
        if (!SParseResponseMethodLine(transaction->fullResponse.methodLine, ignore, transaction->response, ignore))
            transaction->response = 500;
        SDEBUG("Received '" << transaction->fullResponse.methodLine << "'");
        return false; // Hasn't been deleted
    }
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// A sample SQLiteNode that just increments a value, optionally with a delay.
struct SQLiteTestNode : public SQLiteNode {
    // Attributes
    uint64_t commandTimeout;
    SQLiteTestWebClient httpsClient;

    // Construct the base class
    SQLiteTestNode(const string& filename, const string& host, int priority)
        : SQLiteNode(filename, filename, host, priority, 1000, 0, STIME_US_PER_S * 2 + SRandom::rand64() % STIME_US_PER_S * 5,
                     "testversion", -1, -1, 100) {
        // Nothing to initialize
        commandTimeout = 0;
    }

    // Peek at this command
    virtual bool _peekCommand(SQLite& db, Command* command) {
        // See if it's peekable
        SINFO("Peeking '" << command->request.methodLine << "'");
        if (command->request.methodLine == "NOOP") {
            // Yep
            command->response.methodLine = "200 OK";
            return true; // Done
        } else if (getState() == SQLC_MASTERING && SEndsWith(command->request.methodLine, "EXTERNAL")) {
            // Open up a secondary transaction
            command->httpsRequest = httpsClient.send(command->request.methodLine);
            return false; // Not done
        } else {
            // Nope
            return false; // Not done
        }
    }

    // Begin processing a new command
    virtual bool _processCommand(SQLite& db, Command* command) {
        // See what we're starting
        bool needsCommit = false;
        SASSERT(db.beginTransaction());
        SDEBUG("Processing '" << command->request.methodLine << "'");
        if (SIEquals(command->request.methodLine, "UpgradeDatabase")) {
            // Verify the database
            bool created = false;
            SASSERT(db.verifyTable("test", "CREATE TABLE test ( value INTEGER )", created));
            if (created) {
                // Seed the database
                SASSERT(db.write("INSERT INTO test VALUES ( 0 );"));
                needsCommit = true;
            } else
                db.rollback();
            command->response.methodLine = "200 OK";
        } else {
            // Special test command -- verify the test command attributes came through
            SASSERTEQUALS(command->request["a"], "a");
            SASSERTEQUALS(command->request["n"], "n\n n\n");
            SASSERTEQUALS(command->request["r"], "t\t t\t");
            SASSERTEQUALS(command->request["t"], "t\t t\t");
            SASSERTEQUALS(command->request["z"], "z");

            // What is it?
            if (SIEquals(command->request.methodLine, "INCREMENT")) {
                // Just increment the value
                SASSERT(db.write("UPDATE test SET value=value+1;"));
                SASSERT(db.prepare());
                command->response.methodLine = "200 OK";

                // If we're holding something, also release that
                if (command->request.isSet("Holding"))
                    clearCommandHolds(command->request["Holding"]);
            } else if (SIEquals(command->request.methodLine, "NOOP_EXTERNAL")) {
                // No change, done
                db.rollback();
                command->response.methodLine = "200 OK";
                SASSERT(command->httpsRequest);
                httpsClient.closeTransaction(command->httpsRequest);
                command->httpsRequest = nullptr;
            } else if (SIEquals(command->request.methodLine, "INCREMENT_EXTERNAL")) {
                // Just increment the value
                SASSERT(db.write("UPDATE test SET value=value+1;"));
                SASSERT(db.prepare());
                command->response.methodLine = "200 OK";
                SASSERT(command->httpsRequest);
                httpsClient.closeTransaction(command->httpsRequest);
                command->httpsRequest = nullptr;
            } else
                SERROR("Unrecognized request '" << command->request.methodLine << "'");
        }
        return needsCommit;
    }

    // Cleanup pseudo-command
    virtual void _abortCommand(SQLite& db, Command* command) {
        // Note the failure in the response
        command->response.methodLine = "ABORTED";
        db.rollback();
    }

    // Test secondary
    virtual void _cleanCommand(Command* command) {
        // Clean the secondary request, if any
        if (command->httpsRequest)
            httpsClient.closeTransaction(command->httpsRequest);
        command->httpsRequest = nullptr;
    }

    // Override the base update
    bool update(uint64_t& nextActivity) {
        // If a timeout is set, use that
        bool result = SQLiteNode::update(nextActivity);
        if (commandTimeout)
            nextActivity = min(nextActivity, commandTimeout);
        return result;
    }
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// Creates a set of nodes for testing a SQLiteNode cluster
struct SQLiteTester {
    // Command
    struct Command {
        // Attributes
        SQLiteNode::Command* command;
        SQLiteNode* cluster;
    };

    // Attributes
    SQLiteTestWebServer _httpsServer;
    vector<SQLiteTestNode*> _nodeArray;
    uint64_t _nextActivity;
    list<Command> _commandList;
    int _commandsStarted;
    int _commandsFinished;
    int _commandsAborted;
    int _incrementsFinished;
    int _incrementsAborted;
    bool _abortsOk;

    // ---------------------------------------------------------------------
    SQLiteTester(int numDBs) : _httpsServer("localhost:12345") {
        // Initialize
        _nodeArray.resize(numDBs);
        _nextActivity = STimeNow() + STIME_US_PER_S;
        _commandsStarted = 0;
        _commandsFinished = 0;
        _commandsAborted = 0;
        _incrementsFinished = 0;
        _incrementsAborted = 0;
        _abortsOk = false;
    }
    ~SQLiteTester() {
        for (size_t c = 0; c < _nodeArray.size(); ++c)
            if (_nodeArray[c])
                delete _nodeArray[c];
    }

    // ---------------------------------------------------------------------
    bool slaving(size_t c) { return _nodeArray[c] && _nodeArray[c]->getState() == SQLC_SLAVING; }
    bool waiting(size_t c) { return _nodeArray[c] && _nodeArray[c]->getState() == SQLC_WAITING; }
    bool searching(size_t c) { return _nodeArray[c] && _nodeArray[c]->getState() == SQLC_SEARCHING; }
    bool mastering(size_t c) { return _nodeArray[c] && _nodeArray[c]->getState() == SQLC_MASTERING; }

    // ---------------------------------------------------------------------
    string getActivitySummary() {
        // Look up what activity remains
        list<string> activityList;
        for (size_t c = 0; c < _nodeArray.size(); ++c) {
            // Make sure it has no commands of any kind
            SQLiteTestNode* node = _nodeArray[c];
            if (!node->getQueuedCommandList().empty())
                activityList.push_back(node->name + ":queued");
            if (!node->getEscalatedCommandList().empty())
                activityList.push_back(node->name + ":escalated");
            if (!node->getProcessedCommandList().empty())
                activityList.push_back(node->name + ":processed");
        }

        // Return anything we found
        return SComposeList(activityList);
    }

    // ---------------------------------------------------------------------
    void unlink(size_t c) {
        SASSERT(!_nodeArray[c]);
        ::unlink(("test" + SToStr(c) + ".db").c_str());
    }
    void unlinkAll() {
        for (size_t c = 0; c < _nodeArray.size(); ++c)
            unlink(c);
    }

    // ---------------------------------------------------------------------
    int read(size_t c) {
        // Get the latest value of the database
        SQResult result;
        SASSERT(_nodeArray[c]->read("SELECT value FROM test", result));
        SASSERT(!result.empty() && !result[0].empty());
        return SToInt(result[0][0]);
    }

    // ---------------------------------------------------------------------
    void createCommand(int index, const SData& request) {
        // Create the command, and add a bunch of weirdly encoded attributes to make sure they go through
        SData requestCopy = request;
        requestCopy["a"] = "a";
        requestCopy["n"] = "n\n n\n";
        requestCopy["r"] = "t\t t\t";
        requestCopy["t"] = "t\t t\t";
        requestCopy["z"] = "z";
        requestCopy["writeConsistency"] = SQLCConsistencyLevelNames[_commandsStarted % SQLC_NUM_CONSISTENCY_LEVELS];
        Command command = {_nodeArray[index]->openCommand(requestCopy), _nodeArray[index]};
        _commandList.push_back(command);
        ++_commandsStarted;

        // We've created a new command, so let's skip the next select
        _nextActivity = STimeNow();
    }

    // ---------------------------------------------------------------------
    void createCommands(int index, uint64_t count) {
        // Create as many as are requested
        SASSERT(_nodeArray[index]);
        SASSERT(count >= 0);
        while (count--) {
            // Randomly choose a request
            SData request;
            switch (_commandsStarted % 4) {
            case 0:
                request.methodLine = "INCREMENT";
                break;
            case 1:
                request.methodLine = "NOOP";
                break;
            case 2:
                request.methodLine = "NOOP_EXTERNAL";
                break;
            case 3:
                request.methodLine = "INCREMENT_EXTERNAL";
                break;
            }

            // Add in some attributes and verify they come through ok
            createCommand(index, request);
        }
    }

    // ---------------------------------------------------------------------
    void toggle(size_t c, bool graceful = false) {
        SASSERT(SWITHIN(0, c, _nodeArray.size() - 1));
        // Add or remove a database, depending on current status
        if (_nodeArray[c]) {
            // Node already exists, shut it down. Graceful or hard kill?
            SHMMM("<<<<<<<<<<<<<<<<<<< " << (graceful ? "Stopping" : "Killing") << " server #" << c
                                         << "<<<<<<<<<<<<<<<<<<<<<");

            // Trigger a graceful shutdown, if requested
            if (graceful)
                _nodeArray[c]->beginShutdown();

            // Close any commands that haven't been processed.
            // **NOTE: For graceful shutdown, only close waiting commands, as the rest
            //         should get processed correctly
            list<Command>::iterator nextIt = _commandList.begin();
            while (nextIt != _commandList.end()) {
                // See if this is it, and if so, drop
                list<Command>::iterator commandIt = nextIt++;
                Command& command = *commandIt;
                if (!graceful || (command.command->request["Connection"] == "wait"))
                    if (command.cluster == _nodeArray[c]) {
                        // Close it
                        SINFO("Closing outstanding command '" << command.command->request.methodLine << " on node #"
                                                              << c);
                        _nodeArray[c]->closeCommand(command.command);
                        _commandList.erase(commandIt);
                    }
            }

            // Hard kill it, if not graceful
            if (!graceful) {
                delete _nodeArray[c];
                _nodeArray[c] = nullptr;
            }
        } else {
            // Add a new peer
            SHMMM(">>>>>>>>>>>>>>>>>>> Adding server #" << c << ">>>>>>>>>>>>>>>>>>>>>");
            const string& dbName = "test" + SToStr(c) + ".db";
            const string& host = "127.0.0.1:" + SToStr(10000 + c);
            _nodeArray[c] = new SQLiteTestNode(
                dbName, host, c < 3 ? 10 - (int)c : 0); // Nodes after the third are 0-priority permaslaves

            // Configure this node to connect to all the others (whether or not they exist yet).
            // Skip ourselves, as we don't want to connect to ourself.
            for (size_t d = 0; d < _nodeArray.size(); ++d)
                if (d != c) {
                    STable params;
                    if (d >= 3)
                        params["Permaslave"] = "true"; // Nodes after the third are 0-priority permaslaves
                    _nodeArray[c]->addPeer("test" + SToStr(d) + ".db", "127.0.0.1:" + SToStr(10000 + d), params);
                }
        }
    }

    // ---------------------------------------------------------------------
    void update(STestTimer& test) {
        // Collect all the sockets to select upon
        fd_map fdm;
        int maxS = _httpsServer.preSelect(fdm);
        for (size_t c = 0; c < _nodeArray.size(); ++c)
            if (_nodeArray[c]) {
                if (_nodeArray[c]->shutdownComplete()) {
                    // Graceful shutdown has completed
                    SINFO("<<<<<<<<<<<<<<<< Graceful shutdown complete #" << c << "<<<<<<<<<<<<<");
                    delete _nodeArray[c];
                    _nodeArray[c] = nullptr;
                } else {
                    // Alive, process
                    maxS = max(maxS, _nodeArray[c]->preSelect(fdm));
                    maxS = max(maxS, _nodeArray[c]->httpsClient.preSelect(fdm));
                }
            }

        // Wait for activity or timeout, and measure how long it took
        uint64_t now = STimeNow();
        uint64_t timeout = max(_nextActivity, now) - now;
        uint64_t before = STimeNow();
        int activeSockets = S_poll(fdm, timeout);
        uint64_t elapsed = STimeNow() - before;

        // If we waited out the full timeout, verify there are no non-held, non-externally-blocked commands
        int numNonHeldCommands = 0;
        int numHeldCommands = 0;
        int numOustandingExternalCommands = 0;
        for (auto& command : _commandList) {
            // See what they are
            numNonHeldCommands += !command.command->request.isSet("HeldBy");
            numHeldCommands += command.command->request.isSet("HeldBy");
            numOustandingExternalCommands += command.command->httpsRequest && !command.command->httpsRequest->response;
        }
        if (activeSockets == 0 && !numHeldCommands && timeout > STIME_US_PER_S * 9) {
            // This select shouldn't happen: we didn't have any activity and aren't waiting on any held commands, and
            // the state
            // machine didn't seem to be waiting for anything (because the timeout was large).
            STESTLOG("Idle select: activeSockets=" << activeSockets << ", timeout=" << timeout / STIME_US_PER_MS
                                                   << "ms, elapsed=" << elapsed / STIME_US_PER_MS << "ms, waiting on "
                                                   << numNonHeldCommands << " non-held commands (of "
                                                   << _commandList.size() << " total, " << numOustandingExternalCommands
                                                   << " external outstanding)" << endl);
        } else {
            // This is a valid select -- waiting for socket activity or timeout
            SDEBUG("Active select: activeSockets=" << activeSockets << ", timeout=" << timeout / STIME_US_PER_MS
                                                   << "ms, elapsed=" << elapsed / STIME_US_PER_MS << "ms, waiting on "
                                                   << numNonHeldCommands << " non-held commands (of "
                                                   << _commandList.size() << " total, " << numOustandingExternalCommands
                                                   << " external outstanding)" << endl);
        }

        // Process the cluster nodes and see when they will next be active
        _nextActivity = STimeNow() + 10 * STIME_US_PER_S; // Long so we notice
        _httpsServer.postSelect(fdm, _nextActivity);
        string status;
        for (size_t c = 0; c < _nodeArray.size(); ++c)
            if (_nodeArray[c]) {
                // Update and process
                _nodeArray[c]->postSelect(fdm, _nextActivity);
                _nodeArray[c]->httpsClient.postSelect(fdm, _nextActivity);
                while (_nodeArray[c]->update(_nextActivity))
                    ;
                status += " | #" + SToStr(c) + " " + (string)SQLCStateNames[_nodeArray[c]->getState()];

                // Clean up any completed commands
                SQLiteNode::Command* sqlCommand = nullptr;
                while (!_commandList.empty() && (sqlCommand = _nodeArray[c]->getProcessedCommand()))
                    for (auto commandIt = _commandList.begin(); commandIt != _commandList.end(); ++commandIt)
                        if (commandIt->command == sqlCommand) {
                            // Remove it
                            Command& command = *commandIt;
                            _commandsFinished++;
                            bool aborted = !SIEquals(sqlCommand->response.methodLine, "200 OK") &&
                                           !SIEquals(sqlCommand->response.methodLine, "303 Timeout");
                            bool incremented = SStartsWith(sqlCommand->request.methodLine, "INCREMENT");
                            _incrementsFinished += (incremented && !aborted);
                            _incrementsAborted += (incremented && aborted);
                            if (aborted) {
                                // Aborted, make sure a real abort and not some other failure
                                if (!_abortsOk)
                                    STESTEQUALS(sqlCommand->response.methodLine, "ABORTED");
                                _commandsAborted++;
                            }
                            command.cluster->closeCommand(command.command);
                            _commandList.erase(commandIt);
                            break;
                        }
            }

        // Show status
        SINFO("Status=" << status << " (" << _commandList.size() << " commands)");
    }
};

/////////////////////////////////////////////////////////////////////////////
void SQLiteTestBlinks(SQLiteTester& tester, const string& message, int initiator, int blinker) {
    STestTimer test(message);
    // Execute a continous stream of commands while blinking
    uint64_t start = STimeNow();
    uint64_t end = start + STIME_US_PER_M;
    uint64_t lastCommand = start;
    uint64_t lastBlink = start;
    int beforeDB = tester.read(0);
    int numBlinks = 0;
    SASSERT(tester._commandList.empty());
    tester._commandsStarted = 0;
    tester._commandsFinished = 0;
    tester._commandsAborted = 0;
    tester._incrementsFinished = 0;
    tester._incrementsAborted = 0;
    tester._abortsOk = true; // Blinking can create valid aborts so let's not fail if we see them
    for (size_t c = 1; c < tester._nodeArray.size(); ++c)
        SASSERT(tester.read(0) == tester.read(c));
    while (STimeNow() < end) {
        // Is it time for another blink?
        uint64_t now = STimeNow();
        if (now - lastBlink > STIME_US_PER_S * 10) {
            // Time for another blink
            tester.toggle(blinker, (numBlinks / 2) % 2); // Alternate clean dirty
            lastBlink = now;
            ++numBlinks;
        }

        // How many new commands to add for steady stream?
        uint64_t commands = (now - lastCommand) / STIME_HZ(4);
        if (tester._commandList.empty())
            commands++; // Always want something
        if (commands > 0 && tester._nodeArray[initiator] && !tester._nodeArray[initiator]->gracefulShutdown()) {
            // Time for a new command
            tester.createCommands(initiator, commands);
            lastCommand = now;
        }

        // Update everything
        tester.update(test);
    }

    // Bring the blinking node back up (if need be) and try to complete
    if (!tester._nodeArray[blinker])
        tester.toggle(blinker);
    while (true) {
        // Update
        tester.update(test);

        // Verify the master is mastering
        if (!tester.mastering(0))
            continue;

        // Make sure all slaves are slaving
        bool allSlaving = true;
        for (size_t c = 1; c < tester._nodeArray.size(); ++c)
            if (!tester.slaving(c))
                allSlaving = false;
        if (!allSlaving)
            continue;

        // Make sure all commands are done processing
        if (!tester._commandList.empty())
            continue;

        // And make sure everybody is in sync
        bool allInSync = true;
        for (size_t c = 1; c < tester._nodeArray.size(); ++c)
            if (tester.read(0) != tester.read(1))
                allInSync = false;
        if (!allInSync)
            continue;

        // If we get here, we're done
        break;
    }

    // Depending on the arrangement, get more strict
    STESTASSERT(tester._commandsStarted >= tester._commandsFinished);
    if (blinker == initiator) {
        // If we're blinking the initiator, we're going to drop a lot, but make sure
        // no more than some sanity-check percent.
        int dropped = tester._commandsStarted - tester._commandsFinished;
        const int PERCENT = 50;
        if (100 * dropped / tester._commandsStarted > PERCENT) {
            // Too many
            STESTLOG("Too many dropped commands: expected up to "
                     << PERCENT << "% but had " << 100 * dropped / tester._commandsStarted << "% (" << dropped << "/"
                     << tester._commandsStarted << ")");
            test.success = false;
        }
    } else {
        // We're not blinking the initiator so no command should be dropped.
        // However, some might get aborted.  Who's blinking?
        STESTEQUALS(tester._commandsStarted, tester._commandsFinished);
        int maxDB = beforeDB + tester._incrementsFinished;
        int minDB = maxDB - tester._incrementsAborted; // These might not have been commiteed
        if (!SWITHIN(minDB, tester.read(0), maxDB)) {
            // Out of range
            STESTLOG("Database value out of range: is " << tester.read(0) << ", should be within " << minDB << "-"
                                                        << maxDB);
        }

        // If the master is blinking while the slave initiates, the slave will usually re-submit, but will
        // abort if the master died while processing it.  On the other hand, if
        // the slave is blinking while the master initiates, the master might find
        // it has no slave and thus not be able achieve the necessary level of
        // consistency. Either way, there should be at most one abort per blink.
        if (tester._commandsAborted > numBlinks) {
            // Note the failure
            STESTLOG("Too many aborted commands: expected up to " << numBlinks << " but had "
                                                                  << tester._commandsAborted);
            test.success = false;
        }
    }

    // Clean up the tester
    tester._abortsOk = false;
}

/////////////////////////////////////////////////////////////////////////////
void SQLiteTest(const SData& args) {
    if (args.isSet("-all") || args.isSet("-1")) {
        SINFO("##############################################################");
        SINFO("                Testing standalone node");
        SINFO("##############################################################");
        SQLiteTester tester(1);
        tester.unlinkAll();
        {
            STestTimer test("Standing up one node");
            tester.toggle(0); // Turn on 0
            while (!tester.mastering(0))
                tester.update(test); // Wait for 0 to master
        }

        {
            STestTimer test("Testing standalone commands");
            tester.createCommands(0, NUM_COMMANDS); // Queue a bunch of commands
            while (!tester._commandList.empty())
                tester.update(test); // Wait until they're done
            STESTEQUALS(tester.read(0), tester._incrementsFinished);
        }
    }

    if (args.isSet("-all") || args.isSet("-2")) {
        SINFO("##############################################################");
        SINFO("                Testing two-node cluster");
        SINFO("##############################################################");
        SQLiteTester tester(2);
        tester.unlinkAll();

        // ---------------------------------------------------------------------
        {
            STestTimer test("Standing up two nodes");
            tester.toggle(0); // Turn on the master
            tester.toggle(1); // Turn on the slave
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test); // Wait until stable
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing two-node master commands");
            tester.createCommands(0, NUM_COMMANDS);      // Queue a bunch of commands on the master
            tester._incrementsFinished = 0;              // Reset the increment counter
            int before = tester.read(0);                 // Record the start value
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify both nodes have the same value
            while (!tester._commandList.empty())
                tester.update(test);                                          // Process the commands
            STESTEQUALS(tester.read(0), before + tester._incrementsFinished); // Verify it ads up
            STESTEQUALS(tester.read(0), tester.read(1));                      // Verify they're the same
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing two-node slave commands");
            tester.createCommands(1, NUM_COMMANDS);      // Queue a bunch of commands on the slave
            tester._incrementsFinished = 0;              // Reset the increment counter
            int before = tester.read(0);                 // Record the start value
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify both nodes have the same value
            while (!tester._commandList.empty())
                tester.update(test);                                          // Process the commands
            STESTEQUALS(tester.read(0), before + tester._incrementsFinished); // Verify it adds up
            STESTEQUALS(tester.read(0), tester.read(1));                      // Verify they're the same
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Test a master hold/release command");
            SData holdRequest("INCREMENT"); // Create a command that is held by another
            holdRequest["Connection"] = "wait";
            holdRequest["HeldBy"] = "Mr. Dude";
            tester.createCommand(0, holdRequest);                 // Queue the held master command
            uint64_t waitUntil = STimeNow() + STIME_US_PER_S * 5; // Wait 5 seconds
            while (STimeNow() < waitUntil)
                tester.update(test);
            STESTEQUALS(tester._commandList.size(), 1); // Verify not processed
            SData releaseRequest("INCREMENT");          // Create a command to release the first
            releaseRequest["Holding"] = "Mr. Dude";
            tester.createCommand(0, releaseRequest); // Release the first command
            while (!tester._commandList.empty())
                tester.update(test); // Verify both process
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Test a slave hold/release command");
            SData holdRequest("INCREMENT"); // Create a command that is held by another
            holdRequest["Connection"] = "wait";
            holdRequest["HeldBy"] = "Mr. Dude";
            tester.createCommand(1, holdRequest);                 // Queue the held slave command
            uint64_t waitUntil = STimeNow() + STIME_US_PER_S * 5; // Wait 5 seconds
            while (STimeNow() < waitUntil)
                tester.update(test);
            STESTEQUALS(tester._commandList.size(), 1); // Verify not processed
            SData releaseRequest("INCREMENT");          // Create a command to release the first
            releaseRequest["Holding"] = "Mr. Dude";
            tester.createCommand(1, releaseRequest); // Release the first command
            while (!tester._commandList.empty())
                tester.update(test); // Verify both process
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Test a slave hold/timeout command");
            SData holdRequest("INCREMENT"); // Create a command that is held by another
            holdRequest["Connection"] = "wait";
            holdRequest["HeldBy"] = "Mr. Dude";
            holdRequest["Timeout"] = "15000";     // 15s
            tester.createCommand(1, holdRequest); // Queue the held slave command
            while (!tester._commandList.empty())
                tester.update(test); // Verify it timesout without being released
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Test cancelling a slave hold/release command");
            SData holdRequest("INCREMENT"); // Create a command that is held by another
            holdRequest["Connection"] = "wait";
            holdRequest["HeldBy"] = "Mr. Dude";
            tester.createCommand(1, holdRequest); // Queue the held slave command
            while (tester._nodeArray[0]->getQueuedCommandList().empty())
                // Wait until escalated
                tester.update(test);
            STESTEQUALS(tester._commandList.size(), 1); // Verify not processed
            tester._nodeArray[1]->closeCommand(tester._commandList.front().command);
            tester._commandList.clear();
            while (!tester._nodeArray[0]->getQueuedCommandList().empty())
                // Wait for master to cancel escalated command
                tester.update(test);
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Test slave hold/release commands are cancelled on dirty slave shutdown");
            SData holdRequest("INCREMENT"); // Create a command that is held by another
            holdRequest["Connection"] = "wait";
            holdRequest["HeldBy"] = "Mr. Dude";
            tester.createCommand(1, holdRequest); // Queue the held slave command
            while (tester._nodeArray[0]->getQueuedCommandList().empty())
                // Wait until escalated
                tester.update(test);
            STESTEQUALS(tester._commandList.size(), 1); // Verify not processed
            tester.toggle(1, false); // Kill the slave in a non-graceful way, so it can't ESCALATE_CANCEL
            while (!tester._nodeArray[0]->getQueuedCommandList().empty())
                // Wait for master to cancel escalated command
                tester.update(test);
            tester.toggle(1); // Turn back on the slave
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test);                     // Wait to stabilize
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify they're in sync
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing graceful two-node recovery of the slave");
            tester.toggle(1, true); // Shut down 1
            while (tester._nodeArray[1])
                tester.update(test); // Wait for 1 to shut down
            tester.unlink(1);        // Clean up 1
            tester.toggle(1);        // Restart 1
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test);                     // Wait to stabilize
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify they're in sync
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing dirty two-node recovery of the slave");
            tester.toggle(1, false); // Kill 1
            tester.unlink(1);        // Clean up 1
            tester.toggle(1);        // Restart 1
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test);                     // Stabilize
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify they're in sync
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing graceful two-node recovery of the master");
            tester.toggle(0, true); // Shut down 0
            while (tester._nodeArray[0])
                tester.update(test); // Wait for 0 to shut down
            tester.unlink(0);        // Clean up 0
            tester.toggle(0);        // Restart 0
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test);                     // Stabilize
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify they're in sync
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing dirty two-node recovery of the master");
            tester.toggle(0, false); // Kill 0
            tester.unlink(0);        // Clean up 0
            tester.toggle(0);        // Restart 0
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test);                     // Stabilize
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify they're in sync
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing graceful two-node master shutdown with outstanding commands");
            tester.createCommands(0, NUM_COMMANDS); // Queue a bunch of commands on the master
            while (tester._commandList.size() > NUM_COMMANDS / 2)
                tester.update(test); // Process half
            tester.toggle(0, true);  // Shut down the master gracefully while 1/2 commands remain
            while (tester._nodeArray[0])
                tester.update(test); // Wait for it to shut down
            // Master has shut down, slave should still have ~1/2 commands ready to re-queue when master restarts
            tester.toggle(0); // Start 0 back up
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test);                     // Stabilize
            STESTEQUALS(tester.read(0), tester.read(1)); // Verify they're in sync
            while (!tester._commandList.empty())
                tester.update(test); // Wait for slave to re-escalate and process the rest
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Verify a HeldBy: command doesn't block a master from graceful shutdown");
            SData holdRequest("INCREMENT"); // Create a command that is held by another
            holdRequest["Connection"] = "wait";
            holdRequest["HeldBy"] = "Mr. Dude";
            tester.createCommand(1, holdRequest); // Queue the held command
            while (tester._nodeArray[0]->getQueuedCommandList().empty())
                tester.update(test); // Wait until master gets the command
            tester.toggle(0, true);  // Shut down master
            while (tester._nodeArray[0])
                tester.update(test); // Wait for master to shut down
            tester.unlink(0);        // Clean up master
            tester.toggle(0);        // Restart master
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test);           // Wait until stable
            SData releaseRequest("INCREMENT"); // Create a command to release the first
            releaseRequest["Holding"] = "Mr. Dude";
            tester.createCommand(1, releaseRequest); // Release the first command
            while (!tester._commandList.empty())
                tester.update(test); // Verify both process
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Verify a Connection:wait command doesn't block a slave from graceful shutdown");
            SData holdRequest("INCREMENT"); // Create a command that is held by another
            holdRequest["Connection"] = "wait";
            holdRequest["HeldBy"] = "Mr. Dude";
            tester.createCommand(1, holdRequest); // Queue the held command on the slave
            while (tester._nodeArray[0]->getQueuedCommandList().empty())
                tester.update(test); // Wait until master gets the command
            tester.toggle(1, true);  // Shut down slave
            while (tester._nodeArray[1])
                tester.update(test); // Wait for slave to shut down
            tester.unlink(1);        // Clean up slave
            tester.toggle(1);        // Restart slave
            while (!tester.mastering(0) || !tester.slaving(1))
                tester.update(test); // Wait until stable
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        // Test all combinations
        SQLiteTestBlinks(tester, "Testing two-node master commands with master blinks", 0, 0);
        SQLiteTestBlinks(tester, "Testing two-node master commands with slave blinks", 0, 1);
        SQLiteTestBlinks(tester, "Testing two-node slave commands with master blinks", 1, 0);
        SQLiteTestBlinks(tester, "Testing two-node slave commands with slave blinks", 1, 1);
    }

    if (args.isSet("-all") || args.isSet("-3")) {
        SINFO("##############################################################");
        SINFO("                Testing three-node cluster");
        SINFO("##############################################################");
        SQLiteTester tester(3);
        tester.unlinkAll();

        // ---------------------------------------------------------------------
        {
            STestTimer test("Standing up three nodes");
            tester.toggle(0);
            tester.toggle(1);
            tester.toggle(2);
            while (!tester.mastering(0) || !tester.slaving(1) || !tester.slaving(2))
                tester.update(test);
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing three-node master commands");
            tester.createCommands(0, NUM_COMMANDS);
            tester._incrementsFinished = 0;
            int before = tester.read(0);
            STESTEQUALS(tester.read(0), tester.read(1));
            while (!tester._commandList.empty())
                tester.update(test);
            STESTEQUALS(tester.read(0), before + tester._incrementsFinished);
            STESTEQUALS(tester.read(0), tester.read(1));
            STESTEQUALS(tester.read(0), tester.read(2));
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing three-node primary slave commands");
            tester.createCommands(1, NUM_COMMANDS);
            tester._incrementsFinished = 0;
            int before = tester.read(0);
            STESTEQUALS(tester.read(0), tester.read(1));
            while (!tester._commandList.empty())
                tester.update(test);
            STESTEQUALS(tester.read(0), before + tester._incrementsFinished);
            STESTEQUALS(tester.read(0), tester.read(1));
            STESTEQUALS(tester.read(0), tester.read(2));
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing three-node secondary slave commands");
            tester.createCommands(2, NUM_COMMANDS);
            tester._incrementsFinished = 0;
            int before = tester.read(0);
            STESTEQUALS(tester.read(0), tester.read(1));
            while (!tester._commandList.empty())
                tester.update(test);
            STESTEQUALS(tester.read(0), before + tester._incrementsFinished);
            STESTEQUALS(tester.read(0), tester.read(1));
            STESTEQUALS(tester.read(0), tester.read(2));
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing three-node failover/recovery of master");
            tester.toggle(0);
            while (!tester.mastering(1))
                tester.update(test);
            tester.unlink(0);
            tester.toggle(0);
            while (!tester.mastering(0) || !tester.slaving(1) || !tester.slaving(2))
                tester.update(test);
            STESTEQUALS(tester.read(0), tester.read(1));
            STESTEQUALS(tester.read(0), tester.read(2));
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing three-node failure/recovery of primary slave");
            tester.toggle(1);
            tester.unlink(1);
            tester.toggle(1);
            while (!tester.slaving(1))
                tester.update(test);
            STESTEQUALS(tester.read(0), tester.read(1));
            STESTEQUALS(tester.read(0), tester.read(2));
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Testing three-node failure/recovery of secondary slave");
            tester.toggle(2);
            tester.unlink(2);
            tester.toggle(2);
            while (!tester.slaving(2))
                tester.update(test);
            STESTEQUALS(tester.read(0), tester.read(1));
            STESTEQUALS(tester.read(0), tester.read(2));
            STESTEQUALS(tester.getActivitySummary(), "");
        }

        // ---------------------------------------------------------------------
        // Test all combinations of single-node failure
        SQLiteTestBlinks(tester, "Testing three-node master commands with master blinks", 0, 0);
        SQLiteTestBlinks(tester, "Testing three-node master commands with primary slave blinks", 0, 1);
        SQLiteTestBlinks(tester, "Testing three-node master commands with secondary slave blinks", 0, 1);
        SQLiteTestBlinks(tester, "Testing three-node primary slave commands with master blinks", 1, 0);
        SQLiteTestBlinks(tester, "Testing three-node primary slave commands with primary slave blinks", 1, 1);
        SQLiteTestBlinks(tester, "Testing three-node primary slave commands with secondary slave blinks", 1, 2);
        SQLiteTestBlinks(tester, "Testing three-node secondary slave commands with master blinks", 2, 0);
        SQLiteTestBlinks(tester, "Testing three-node secondary slave commands with primary slave blinks", 2, 1);
        SQLiteTestBlinks(tester, "Testing three-node secondary slave commands with secondary slave blinks", 2, 2);

        // ---------------------------------------------------------------------
        // Test total chaos
        {
            STestTimer test("Testing total chaos");
            // Execute a continous stream of commands while blinking randomly
            uint64_t start = STimeNow();
            uint64_t end = start + STIME_US_PER_M * 5;
            uint64_t lastCommand = start;
            uint64_t lastBlink = start;
            int numBlinks = 0;
            int initiator = 0;
            SASSERT(tester._commandList.empty());
            tester._commandsStarted = 0;
            tester._commandsFinished = 0;
            tester._commandsAborted = 0;
            for (size_t c = 1; c < tester._nodeArray.size(); ++c)
                SASSERT(tester.read(0) == tester.read(c));
            while (STimeNow() < end) {
                // Is it time for another blink?  (Do it right away if we have
                // no nodes.)
                int numAlive = 0;
                for (size_t c = 0; c < tester._nodeArray.size(); ++c)
                    if (tester._nodeArray[c])
                        ++numAlive;
                uint64_t now = STimeNow();
                if (!numAlive || now - lastBlink > STIME_US_PER_S * 20) {
                    // Time for another blink
                    uint64_t blinker = SRandom::rand64() % tester._nodeArray.size();
                    tester.toggle(blinker, SRandom::rand64() % 2); // Randomly choose dirty or clean
                    lastBlink = now;
                    ++numBlinks;
                }

                // How many new commands to add for steady stream?
                uint64_t commands = (now - lastCommand) / STIME_HZ(4);
                if (tester._commandList.empty())
                    commands++; // Always want something
                initiator = (initiator + 1) % tester._nodeArray.size();
                if (commands && tester._nodeArray[initiator] && !tester._nodeArray[initiator]->gracefulShutdown()) {
                    // Time for a new command
                    tester.createCommands(initiator, commands);
                    lastCommand = now;
                }

                // Update everything
                tester.update(test);
            }

            // Bring all nodes back up and try to complete
            for (size_t c = 0; c < tester._nodeArray.size(); ++c)
                if (!tester._nodeArray[c])
                    tester.toggle(c);
            while (true) {
                // Update
                tester.update(test);

                // Verify the master is mastering
                if (!tester.mastering(0))
                    continue;

                // Make sure all slaves are slaving
                bool allSlaving = true;
                for (size_t c = 1; c < tester._nodeArray.size(); ++c)
                    if (!tester.slaving(c))
                        allSlaving = false;
                if (!allSlaving)
                    continue;

                // Make sure all commands are done processing
                if (!tester._commandList.empty())
                    continue;

                // And make sure everybody is in sync
                bool allInSync = true;
                for (size_t c = 1; c < tester._nodeArray.size(); ++c)
                    if (tester.read(0) != tester.read(1))
                        allInSync = false;
                if (!allInSync)
                    continue;

                // If we get here, we're done
                break;
            }

            // Output the results
            STESTASSERT(tester._commandsStarted >= tester._commandsFinished);
            int dropped = tester._commandsStarted - tester._commandsFinished;
            STESTLOG("dropped=" << dropped << ", aborted=" << tester._commandsAborted);
        }
    }

    if (args.isSet("-all") || args.isSet("-5")) {
        SINFO("##############################################################");
        SINFO("                Testing three-node cluster + 2 permaslaves");
        SINFO("##############################################################");
        SQLiteTester tester(5);
        tester.unlinkAll();

        // ---------------------------------------------------------------------
        {
            STestTimer test("Standing up master and two permaslaves");
            tester.toggle(0);
            tester.toggle(3);
            tester.toggle(4);
            SStopwatch wait1;
            while (wait1.elapsed() < STIME_US_PER_S * 10)
                tester.update(test);
            STESTASSERT(tester.waiting(0) || tester.searching(0)); // Full
            STESTASSERT(tester.waiting(3) || tester.searching(3)); // Permaslave
            STESTASSERT(tester.waiting(4) || tester.searching(4)); // Permaslave
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Standing up master/slave and two permaslaves");
            tester.toggle(2); // Start 2nd full node; 2/3 is quorum
            while (!tester.mastering(0) && !tester.slaving(2) && !tester.slaving(3) && !tester.slaving(4))
                tester.update(test);
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Returning to master and two permaslaves");
            tester.toggle(2);            // Kill 2nd full node, 1/3 is not quorum
            tester.createCommands(0, 1); // Queue one command
            SStopwatch wait2;
            while (wait2.elapsed() < STIME_US_PER_S * 10)
                tester.update(test); // Confirm it's not processed
            STESTASSERT(!tester._commandList.empty());
        }

        // ---------------------------------------------------------------------
        {
            STestTimer test("Returning to master/slave and two permaslaves");
            tester.toggle(2); // Bring back 2nd full node, 2/3 is not quorum
            while (!tester._commandList.empty())
                tester.update(test); // Confirm it's processed with quorum
        }
    }
}

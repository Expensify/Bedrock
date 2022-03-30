#include <BedrockCommand.h>
#include <sqlitecluster/SQLiteClusterMessenger.h>
#include <sqlitecluster/SQLiteNode.h>

#include <unistd.h>
#include <fcntl.h>

SQLiteClusterMessenger::SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node)
 : _node(node)
{
}

void __setErrorResponse(BedrockCommand& command) {
    // TODO: Do we use this to say we couldn't escalate a command, or do we just clear everything and let the caller
    // figure it out?
    command.response.methodLine = "500 Internal Server Error";
    command.response.nameValueMap.clear();
    command.response.content.clear();
}


// Returns true on ready or false on error or timeout.
bool __waitForReady(pollfd& fdspec, int timeoutMS) {
    static const map <int, string> labels = {
        {POLLOUT, "send"},
        {POLLIN, "recv"},
    };
    string type = "UNKNOWN";
    try {
        type = labels.at(fdspec.events);
    } catch (const out_of_range& e) {}

    while (true) {
        int result = poll(&fdspec, 1, 100); // 100 is timeout in ms.
        if (!result) {
            // TODO: Check command timeout.
            SINFO("[HTTPESC] Socket waiting to be ready (" << type << ").");
        } else if (result == 1) {
            if (fdspec.revents & POLLERR || fdspec.revents & POLLHUP) {
                SINFO("[HTTPESC] Socket disconnected while waiting to be ready (" << type << ").");
                return false;
            } else if (fdspec.revents & POLLIN || fdspec.revents & POLLOUT) {
                // Expected case.
                return true;
            } else {
                SWARN("[HTTPESC] Neither error nor success?? (" << type << ").");
                return false;
            }
        } else if (result < 0) {
            if (errno == EAGAIN || errno == EINTR) {
                // might work on a second try.
                SWARN("[HTTPESC] poll error (" << type << "): " << errno << ", retrying.");
            } else {
                // Anything else should be fatal.
                SWARN("[HTTPESC] poll error (" << type << "): " << errno);
                return false;
            }
        } else {
            SERROR("We have more than 1 file ready????");
        }
    }
}

bool SQLiteClusterMessenger::sendToLeader(BedrockCommand& command) {
    string leaderAddress;
    auto _nodeCopy = atomic_load(&_node);
    if (_nodeCopy) {
        // peerList is const, so we can safely read from it in  multiple threads without locking, similarly,
        // peer->commandAddress is atomic.
        for (SQLiteNode::Peer* peer : _nodeCopy->peerList) {
            string peerCommandAddress = peer->commandAddress;
            if (peer->state == STCPNode::LEADING && !peerCommandAddress.empty()) {
                leaderAddress = peerCommandAddress;
                break;
            }
        }
    }

    // SParseURI expects a typical http or https scheme.
    string url = "http://" + leaderAddress;
    string host, path;
    if (!SParseURI(url, host, path) || !SHostIsValid(host)) {
        return false;
    }

    // Start our escalation timing.
    command.escalationTimeUS = STimeNow();

    SINFO("[HTTPESC] Socket opening.");
    // TODO: RAII delete this.
    Socket* s = nullptr;
    try {
        // TODO: Future improvement - socket pool so these are reused.
        // TODO: Also, allow S_socket to take a parsed address instead of redoing all the parsing above.
        s = new Socket(host, nullptr);
    } catch (const SException& exception) {
        // Finish our escalation.
        command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
        SINFO("[HTTPESC] Socket failed to open.");
        return false;
    }
    SINFO("[HTTPESC] Socket opened.");

    // This is what we need to send.
    SData request = command.request;
    request.nameValueMap["ID"] = command.id;
    SFastBuffer buf(request.serialize());

    // We only have one FD to poll.
    pollfd fdspec = {s->s, POLLOUT, 0};
    while (true) {
        if (!__waitForReady(fdspec, 0)) {
            // TODO: Delete socket.
            return false;
        }

        ssize_t bytesSent = send(s->s, buf.c_str(), buf.size(), 0);
        if (bytesSent == -1) {
            switch (errno) {
                case EAGAIN:
                case EINTR:
                    // these are ok. try again.
                    SINFO("Got error (send): " << errno << ", trying again.");
                    break;
                default:
                    SINFO("Got error (send): " << errno << ", fatal.");
                    __setErrorResponse(command);
                    return false;
            }
        } else {
            buf.consumeFront(bytesSent);
            if (buf.empty()) {
                // Everything has sent, we're done with this loop.
                break;
            }
        }
    }

    // Ok, now we need to receive the response.
    fdspec.events = POLLIN;
    string responseStr;
    char response[4096] = {0};
    while (true) {
        if (!__waitForReady(fdspec, 0)) {
            // TODO: Delete socket.
            return false;
        }

        ssize_t bytesRead = recv(s->s, response, 4096, 0);
        SINFO("[HTTPESC] Socket read bytes:" << bytesRead);
        // TODO: Check errors above.

        // Save the response.
        responseStr.append(response, bytesRead);

        // Are we done? We've only sent one command so we can only get one response.
        int size = SParseHTTP(responseStr, command.response.methodLine, command.response.nameValueMap, command.response.content);
        if (size) {
            SINFO("[HTTPESC] response size:" << size);
            break;
        }
    }

    delete s;

    SINFO("Command complete");
    command.complete = true;

    // Finish our escalation.
    command.escalationTimeUS = STimeNow() - command.escalationTimeUS;

    return true;
}


// TODO:: Add shuttingDown flag that BedrockServer can set.

bool SQLiteClusterMessenger::_onRecv(Transaction* transaction)
{
    // Bedrock returns responses like `200 OK` rather than `HTTP/1.1 200 OK`, so we don't use getHTTPResponseCode
    transaction->response = SToInt(transaction->fullResponse.methodLine);

    // TODO: Demote this to DEBUG after we're confident in this code.
    SINFO("Finished HTTP escalation of " << transaction->fullRequest.methodLine << " command with response " << transaction->response);
    lock_guard<mutex> lock(_transactionCommandMutex);
    auto cmdIt = _transactionCommands.find(transaction);
    if (cmdIt != _transactionCommands.end()) {
        BedrockCommand* command = cmdIt->second.first;
        command->response = transaction->fullResponse;
        command->response["escalationTime"] = to_string(STimeNow() - cmdIt->second.second);
        command->complete = true;
        _transactionCommands.erase(cmdIt);

        // Finish our escalation.
        command->escalationTimeUS = STimeNow() - command->escalationTimeUS;
    }
    return false;
}

bool SQLiteClusterMessenger::handleAllResponses() {
    return true;
}

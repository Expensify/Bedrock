#include <BedrockCommand.h>
#include <sqlitecluster/SQLiteClusterMessenger.h>
#include <sqlitecluster/SQLiteNode.h>

#include <unistd.h>
#include <fcntl.h>

SQLiteClusterMessenger::SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node)
 : _node(node)
{
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

    // Create a new transaction. This can throw if `validate` fails. We explicitly do this *before* creating a socket.
    //Transaction* transaction = new Transaction(*this);

    //{
    //    lock_guard<mutex> lock(_transactionCommandMutex);
    //    _transactionCommands[transaction] = make_pair(&command, STimeNow());
    //}

    // Start our escalation.
    command.escalationTimeUS = STimeNow();

    SINFO("[HTTPESC] Socket opening.");
    Socket* s = nullptr;
    try {
        // TODO: Future improvement - socket pool so these are reused.
        // TODO: Also, allow S_socket to take a parsed address instead of redoing all the parsing above.
        s = new Socket(host, nullptr);
    } catch (const SException& exception) {
        // Finish our escalation.
        command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
        //lock_guard<mutex> lock(_transactionCommandMutex);
        //_transactionCommands.erase(transaction);
        //delete transaction;
        return false;
    }
    SINFO("[HTTPESC] Socket opened.");

    SData request = command.request;
    request.nameValueMap["ID"] = command.id;
    //transaction->s = s;
    //transaction->fullRequest = request.serialize();
    //command.httpsRequests.push_back(transaction);

    // Ship it.
    // TODO: remove transaction->s->send(transaction->fullRequest.serialize());

    pollfd fdspec = {s->s, POLLOUT, 0};
    
    // This is what we need to send.
    SFastBuffer buf(request.serialize());

    while (true) {
        // First we wait until we can send (connect is complete).
        while (true) {
            int result = poll(&fdspec, 1, 100); // 100 is timeout in ms.
            if (!result) {
                // still waiting to be able to write.
                // TODO: Check timeout and errors.
                SINFO("[HTTPESC] Socket waiting to write.");
            } else if (result == 1) {
                SINFO("[HTTPESC] 1 file ready (send)");
                break;
            } else if (result < 0) {
                SWARN("[HTTPESC] poll error (send): " << errno);
                break;
            }
        }
        SINFO("[HTTPESC] Socket writeable.");

        // Now we know we can send, so let's do so.
        ssize_t bytesSent = send(s->s, buf.c_str(), buf.size(), 0);
        // TODO: Check errors above.
        buf.consumeFront(bytesSent);
        SINFO("[HTTPESC] Socket sent bytes:" << bytesSent);
        if (buf.empty()) {
            // Everything has sent, we're done with this loop.
            break;
        }
    }

    // Ok, now we need to receive the response.
    fdspec.events = POLLIN;
    string responseStr;
    char response[4096] = {0};
    while (true) {
        // First we wait until we can receive (there's some data to read)
        while (true) {
            int result = poll(&fdspec, 1, 100); // 100 is timeout in ms.
            if (!result) {
                // still waiting to be able to read.
                // TODO: Check timeout and errors.
                SINFO("[HTTPESC] Socket waiting to read.");
            } else if (result == 1) {
                SINFO("[HTTPESC] 1 file ready (recv)");
                break;
            } else if (result < 0) {
                SWARN("[HTTPESC] poll error (recv): " << errno);
                break;
            }
        }
        SINFO("[HTTPESC] Socket readable.");

        // Now we know we can recv, so let's do so.
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

    return true;
}

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

#include <BedrockCommand.h>
#include <sqlitecluster/SQLiteClusterMessenger.h>
#include <sqlitecluster/SQLiteNode.h>

SQLiteClusterMessenger::SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node)
 : _node(node)
{
}

bool SQLiteClusterMessenger::sendToLeader(BedrockCommand& command) {
    string leaderAddress;
    auto _nodeCopy = atomic_load(&_node);
    if (_nodeCopy) {
        for (SQLiteNode::Peer* peer : _nodeCopy->peerList) {
            if (peer->state == STCPNode::LEADING && !peer->commandAddress.load().empty()) {
                leaderAddress = peer->commandAddress;
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
    Transaction* transaction = new Transaction(*this);

    // I don't trust this not to ever leak currently, but for the moment, this is OK.
    _transactionCommands[transaction] = make_pair(&command, STimeNow());

    Socket* s = nullptr;
    try {
        s = new Socket(host, nullptr);
    } catch (const SException& exception) {
        _transactionCommands.erase(transaction);
        delete transaction;
        return false;
    }

    transaction->s = s;
    transaction->fullRequest = command.request.serialize();

    command.httpsRequests.push_back(transaction);

    // Ship it.
    transaction->s->send(command.request.serialize());

    return true;
}

bool SQLiteClusterMessenger::_onRecv(Transaction* transaction)
{
    transaction->response = getHTTPResponseCode(transaction->fullResponse.methodLine);
    auto cmdIt = _transactionCommands.find(transaction);
    if (cmdIt != _transactionCommands.end()) {
        BedrockCommand* command = cmdIt->second.first;
        command->response = transaction->fullResponse;
        command->response["escalationTime"] = to_string(STimeNow() - cmdIt->second.second);
        command->complete = true;
        _transactionCommands.erase(cmdIt);
    }
    return false;
}

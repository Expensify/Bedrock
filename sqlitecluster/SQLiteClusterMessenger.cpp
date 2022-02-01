#include <BedrockCommand.h>
#include <sqlitecluster/SQLiteClusterMessenger.h>
#include <sqlitecluster/SQLiteNode.h>

SQLiteClusterMessenger::SQLiteClusterMessenger(shared_ptr<SQLiteNode>& node)
 : _node(node)
{
}

SStandaloneHTTPSManager::Transaction* SQLiteClusterMessenger::sendToLeader(BedrockCommand& command) {

    // Parse leader from this.
    // TODO: What if we're leader??
    string leaderAddress;
    auto _nodeCopy = atomic_load(&_node);
    if (_nodeCopy) {
        for (SQLiteNode::Peer* peer : _nodeCopy->peerList) {
            auto data = peer->getData();
            auto stateIt = data.find("state");
            auto hostIt = data.find("host");
            auto serverHostIt = data.find("serverHost");
            if (stateIt != data.end() && hostIt != data.end() && stateIt->second == STCPNode::stateName(STCPNode::LEADING)) {
                leaderAddress = serverHostIt->second;
                break;
            }
        }
    }

    // SParseURI expects a typical http or https scheme.
    string url = "http://" + leaderAddress;
    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    // Create a new transaction. This can throw if `validate` fails. We explicitly do this *before* creating a socket.
    Transaction* transaction = new Transaction(*this);

    // I don't trust this not to ever leak currently, but for the moment, this is OK.
    _transactionCommands[transaction] = make_pair(&command, STimeNow());

    Socket* s = nullptr;
    try {
        s = new Socket(host, nullptr);
    } catch (const SException& exception) {
        delete transaction;
        return _createErrorTransaction();
    }

    transaction->s = s;
    transaction->fullRequest = command.request.serialize();

    command.httpsRequests.push_back(transaction);

    // Ship it.
    transaction->s->send(command.request.serialize());

    // Keep track of the transaction.
    return transaction;
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

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
    list<STable> peerData;
    auto _nodeCopy = atomic_load(&_node);
    if (_nodeCopy) {
        for (SQLiteNode::Peer* peer : _nodeCopy->peerList) {
            peerData.emplace_back(peer->getData());
        }
    }

    string url = "http://" + "some address from above"s;

    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    // Create a new transaction. This can throw if `validate` fails. We explicitly do this *before* creating a socket.
    Transaction* transaction = new Transaction(*this);

    Socket* s = nullptr;
    try {
        s = new Socket(host, nullptr);
    } catch (const SException& exception) {
        delete transaction;
        return _createErrorTransaction();
    }

    transaction->s = s;
    transaction->fullRequest = command.request.serialize();

    // Ship it.
    transaction->s->send(command.request.serialize());

    // Keep track of the transaction.
    return transaction;
}

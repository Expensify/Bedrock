#include "SHTTPSManager.h"

#include <BedrockPlugin.h>
#include <BedrockServer.h>
#include <libstuff/libstuff.h>
#include <libstuff/SX509.h>
#include <sqlitecluster/SQLiteNode.h>

SHTTPSManager::SHTTPSManager(BedrockPlugin& plugin_) : plugin(plugin_)
{
    plugin.httpsManagers.push_back(this);
}

SHTTPSManager::SHTTPSManager(BedrockPlugin& plugin_, const string& pem, const string& srvCrt, const string& caCrt)
  : SStandaloneHTTPSManager(pem, srvCrt, caCrt), plugin(plugin_)
{
    plugin.httpsManagers.push_back(this);
}

void SHTTPSManager::validate() {
    // These can only be created on a leader node.
    if (plugin.server.getState() != SQLiteNode::LEADING && plugin.server.getState() != SQLiteNode::STANDINGDOWN) {
        throw NotLeading();
    }
}

SStandaloneHTTPSManager::SStandaloneHTTPSManager()
{
}

SStandaloneHTTPSManager::SStandaloneHTTPSManager(const string& pem, const string& srvCrt, const string& caCrt)
  : _pem(pem), _srvCrt(srvCrt), _caCrt(caCrt)
{
}

SStandaloneHTTPSManager::~SStandaloneHTTPSManager() {
}

void SStandaloneHTTPSManager::closeTransaction(Transaction* transaction) {
    if (transaction == nullptr) {
        return;
    }

    transaction->s = nullptr;
    delete transaction;
}

int SStandaloneHTTPSManager::getHTTPResponseCode(const string& methodLine) {
    // This code looks for the first space in the methodLine, and then for the first non-space
    // after that, and *then* parses the response code. If we fail to find such a code, or can't parse it as an
    // integer, we default to 400.
    size_t offset = methodLine.find_first_of(' ', 0);
    offset = methodLine.find_first_not_of(' ', offset);
    if (offset != string::npos) {
        int status = SToInt(methodLine.substr(offset));
        if (status) {
            return status;
        }
    }

    // Default case, return 400
    return 400;
}

void SStandaloneHTTPSManager::prePoll(fd_map& fdm, SStandaloneHTTPSManager::Transaction& transaction) {
    if (!transaction.s || transaction.finished) {
        // If there's no socket, or we're done, skip.
        return;
    }
    STCPManager::prePoll(fdm, *transaction.s);
}

void SStandaloneHTTPSManager::postPoll(fd_map& fdm, SStandaloneHTTPSManager::Transaction& transaction, uint64_t& nextActivity, uint64_t timeoutMS) {
    if (!transaction.s || transaction.finished) {
        // If there's no socket, or we're done, skip.
        return;
    }
    STCPManager::postPoll(fdm, *transaction.s);

    uint64_t timeout = timeoutMS * 1000;
    SAUTOPREFIX(transaction.requestID);
    if (transaction.isDelayedSend && !transaction.sentTime) {
        // This transaction was created, queued, and then meant to be sent later.
        // As such we'll use STimeNow() as it's "created" time for time.
        SINFO("Transaction is marked for delayed sending, setting sentTime for timeout.");
        transaction.sentTime = STimeNow();
    }
    uint64_t timeoutFromTime = transaction.sentTime ? transaction.sentTime : transaction.created;
    uint64_t now = STimeNow();
    uint64_t elapsed = now - timeoutFromTime;
    int size = transaction.fullResponse.deserialize(transaction.s->recvBuffer);
    bool specificallyTimedOut = transaction.timeoutAt < now;
    if (size) {
        // Consume how much we read.
        transaction.s->recvBuffer.consumeFront(size);

        // 200OK or any content?
        transaction.finished = now;
        if (SContains(transaction.fullResponse.methodLine, " 200 ") || transaction.fullResponse.content.size()) {
            // Pass the transaction down to the subclass.
            if (_onRecv(&transaction)) {
                // If true, then the transaction was closed in onRecv.
                return;
            }
            SASSERT(transaction.response);
        } else {
            // Error, failed to authenticate or receive a valid server response.
            SWARN("Message failed: '" << transaction.fullResponse.methodLine << "'");
            transaction.response = 500;
        }
    } else if (transaction.s->state.load() > Socket::CONNECTED || elapsed > timeout || specificallyTimedOut) {
        // Net problem. Did this transaction end in an inconsistent state?
        SWARN("Connection " << (elapsed > timeout ? "timed out" : "died prematurely") << " after " << elapsed / 1000 << "ms");
        transaction.response = transaction.s->sendBufferEmpty() ? 501 : 500;
        if (transaction.response == 501) {
            SHMMM("SStandaloneHTTPSManager: '" << transaction.fullRequest.methodLine
                  << "' timed out receiving response in " << (elapsed / 1000) << "ms.");
        }
    } else {
        // Haven't timed out yet, let the caller know how long until we do.
        nextActivity = min(nextActivity, timeoutFromTime + timeout);
    }

    // If we're done, remove from the active and add to completed
    if (transaction.response) {
        // Switch lists
        SINFO("Completed request '" << transaction.fullRequest.methodLine << "' to '" << transaction.fullRequest["Host"]
              << "' with response '" << transaction.response << "' in '" << elapsed / 1000 << "'ms");
    }
}

SStandaloneHTTPSManager::Transaction::Transaction(SStandaloneHTTPSManager& manager_) :
    s(nullptr),
    created(STimeNow()),
    finished(0),
    timeoutAt(0),
    response(0),
    manager(manager_),
    isDelayedSend(0),
    sentTime(0),
    requestID(SThreadLogPrefix)
{
    manager.validate();
}

SStandaloneHTTPSManager::Transaction::~Transaction() {
    SASSERT(!s);
}

SStandaloneHTTPSManager::Transaction* SStandaloneHTTPSManager::_createErrorTransaction() {
    // Sometimes we have to create transactions without an attempted connect. This could happen if we don't have the
    // host or service id yet.
    SWARN("We had to create an error transaction instead of attempting a real one.");
    Transaction* transaction = new Transaction(*this);
    transaction->response = 503;
    transaction->finished = STimeNow();
    return transaction;
}

SStandaloneHTTPSManager::Transaction* SStandaloneHTTPSManager::_httpsSend(const string& url, const SData& request) {
    // Open a connection, optionally using SSL (if the URL is HTTPS). If that doesn't work, then just return a
    // completed transaction with an error response.
    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    // Create a new transaction. This can throw if `validate` fails. We explicitly do this *before* creating a socket.
    Transaction* transaction = new Transaction(*this);

    // If this is going to be an https transaction, create a certificate and give it to the socket.
    SX509* x509 = SStartsWith(url, "https://") ? SX509Open(_pem, _srvCrt, _caCrt) : nullptr;
    Socket* s = nullptr;
    try {
        s = new Socket(host, x509);
    } catch (const SException& exception) {
        delete transaction;
        delete x509;
        return _createErrorTransaction();
    }

    transaction->s = s;
    transaction->fullRequest = request;

    // Ship it.
    transaction->s->send(request.serialize());

    // Keep track of the transaction.
    return transaction;
}

bool SStandaloneHTTPSManager::_onRecv(Transaction* transaction)
{
    transaction->response = getHTTPResponseCode(transaction->fullResponse.methodLine);
    return false;
}

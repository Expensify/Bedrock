#include "libstuff.h"

SHTTPSManager::SHTTPSManager()
{ }

SHTTPSManager::SHTTPSManager(const string& pem, const string& srvCrt, const string& caCrt)
  : _pem(pem), _srvCrt(srvCrt), _caCrt(caCrt)
{ }

SHTTPSManager::~SHTTPSManager() {
    lock_guard<decltype(socketSetMutex)> lock(socketSetMutex);

    // Clean up outstanding transactions
    SASSERTWARN(_activeTransactionList.empty());
    while (!_activeTransactionList.empty()) {
        closeTransaction(_activeTransactionList.front());
    }
    SASSERTWARN(_completedTransactionList.empty());
    while (!_completedTransactionList.empty()) {
        closeTransaction(_completedTransactionList.front());
    }
}

void SHTTPSManager::closeTransaction(Transaction* transaction) {
    if (transaction == nullptr) {
        return;
    }
    lock_guard<decltype(socketSetMutex)> lock(socketSetMutex);

    // Clean up the socket and done
    _activeTransactionList.remove(transaction);
    _completedTransactionList.remove(transaction);
    if (transaction->s) {
        closeSocket(transaction->s);
    }
    transaction->s = nullptr;
    delete transaction;
}

int SHTTPSManager::getHTTPResponseCode(const string& methodLine) {
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

void SHTTPSManager::postPoll(fd_map& fdm, uint64_t& nextActivity) {
    list<SHTTPSManager::Transaction*> completedRequests;
    map<Transaction*, uint64_t> transactionTimeouts;
    postPoll(fdm, nextActivity, completedRequests, transactionTimeouts);
}

void SHTTPSManager::postPoll(fd_map& fdm, uint64_t& nextActivity, list<SHTTPSManager::Transaction*>& completedRequests) {
    map<Transaction*, uint64_t> transactionTimeouts;
    postPoll(fdm, nextActivity, completedRequests, transactionTimeouts);
}

void SHTTPSManager::postPoll(fd_map& fdm, uint64_t& nextActivity, list<SHTTPSManager::Transaction*>& completedRequests, map<Transaction*, uint64_t>& transactionTimeouts, uint64_t timeoutMS) {
    lock_guard<decltype(socketSetMutex)> lock(socketSetMutex);

    // Let the base class do its thing
    STCPManager::postPoll(fdm);

    // Update each of the active requests
    uint64_t timeout = timeoutMS * 1000;
    list<Transaction*>::iterator nextIt = _activeTransactionList.begin();
    while (nextIt != _activeTransactionList.end()) {
        // Did we get any responses?
        list<Transaction*>::iterator activeIt = nextIt++;
        Transaction* active = *activeIt;
        if (active->isDelayedSend && !active->sentTime) {
            // This transaction was created, queued, and then meant to be sent later.
            // As such we'll use STimeNow() as it's "created" time for time.
            SINFO("Transaction is marked for delayed sending, setting sentTime for timeout.");
            active->sentTime = STimeNow();
        }
        uint64_t timeoutFromTime = active->sentTime ? active->sentTime : active->created;
        uint64_t now = STimeNow();
        uint64_t elapsed = now - timeoutFromTime;
        int size = active->fullResponse.deserialize(active->s->recvBuffer);
        auto timeoutIt = transactionTimeouts.find(active);
        bool specificallyTimedOut = timeoutIt != transactionTimeouts.end() && timeoutIt->second < now;
        if (size) {
            // Consume how much we read.
            SConsumeFront(active->s->recvBuffer, size);

            // 200OK or any content?
            active->finished = now;
            if (SContains(active->fullResponse.methodLine, " 200 ") || active->fullResponse.content.size()) {
                // Pass the transaction down to the subclass.
                if (_onRecv(active)) {
                    // If true, then the transaction was closed in onRecv.
                    continue;
                }
                SASSERT(active->response);
            } else {
                // Error, failed to authenticate or receive a valid server response.
                SWARN("Message failed: '" << active->fullResponse.methodLine << "'");
                active->response = 500;
            }
        } else if (active->s->state.load() > Socket::CONNECTED || elapsed > timeout || specificallyTimedOut) {
            // Net problem. Did this transaction end in an inconsistent state?
            SWARN("Connection " << (elapsed > timeout ? "timed out" : "died prematurely") << " after " << elapsed / 1000 << "ms");
            active->response = active->s->sendBufferEmpty() ? 501 : 500;
            if (active->response == 501) {
                SHMMM("SHTTPSManager: '" << active->fullRequest.methodLine
                      << "' timed out receiving response in " << (elapsed / 1000) << "ms.");
            }
        } else {
            // Haven't timed out yet, let the caller know how long until we do.
            nextActivity = min(nextActivity, timeoutFromTime + timeout);
        }

        // If we're done, remove from the active and add to completed
        if (active->response) {
            // Switch lists
            SINFO("Completed request '" << active->fullRequest.methodLine << "' to '" << active->fullRequest["Host"]
                  << "' with response '" << active->response << "' in '" << elapsed / 1000 << "'ms");
            _activeTransactionList.erase(activeIt);
            _completedTransactionList.push_back(active);
            completedRequests.push_back(active);
        }
    }
}

SHTTPSManager::Transaction::Transaction(SHTTPSManager& owner_) :
    s(nullptr),
    created(STimeNow()),
    finished(0),
    response(0),
    owner(owner_),
    isDelayedSend(0),
    sentTime(0)
{ }

SHTTPSManager::Transaction::~Transaction() {
    SASSERT(!s);
}

SHTTPSManager::Transaction* SHTTPSManager::_createErrorTransaction() {
    // Sometimes we have to create transactions without an attempted connect. This could happen if we don't have the
    // host or service id yet.
    SWARN("We had to create an error transaction instead of attempting a real one.");
    Transaction* transaction = new Transaction(*this);
    transaction->response = 503;
    transaction->finished = STimeNow();
    lock_guard<decltype(socketSetMutex)> lock(socketSetMutex);
    _completedTransactionList.push_front(transaction);
    return transaction;
}

SHTTPSManager::Transaction* SHTTPSManager::_httpsSend(const string& url, const SData& request) {
    // Open a connection, optionally using SSL (if the URL is HTTPS). If that doesn't work, then just return a
    // completed transaction with an error response.
    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    // If this is going to be an https transaction, create a certificate and give it to the socket.
    SX509* x509 = SStartsWith(url, "https://") ? SX509Open(_pem, _srvCrt, _caCrt) : nullptr;
    shared_ptr<Socket> s = openSocket(host, x509);
    if (!s) {
        return _createErrorTransaction();
    }

    // Wrap in a transaction
    Transaction* transaction = new Transaction(*this);
    transaction->s = s;
    transaction->fullRequest = request;

    // Ship it.
    transaction->s->send(request.serialize());

    // Keep track of the transaction.
    lock_guard<decltype(socketSetMutex)> lock(socketSetMutex);
    _activeTransactionList.push_front(transaction);
    return transaction;
}

bool SHTTPSManager::_onRecv(Transaction* transaction)
{
    transaction->response = getHTTPResponseCode(transaction->fullResponse.methodLine);
    return false;
}

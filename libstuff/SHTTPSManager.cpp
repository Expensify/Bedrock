#include "libstuff.h"

// --------------------------------------------------------------------------
SHTTPSManager::SHTTPSManager()
    : _x509(SX509Open()) { // Generate the x509 certificate.
    SASSERT(_x509);

    SASSERT(0 == pipe(_pipeFD));
    int flags = fcntl(_pipeFD[0], F_GETFL, 0);
    fcntl(_pipeFD[0], F_SETFL, flags | O_NONBLOCK);
}

// --------------------------------------------------------------------------
SHTTPSManager::SHTTPSManager(const string& pem, const string& srvCrt, const string& caCrt) {
    // Generate the x509 certificate.
    _x509 = SX509Open(pem, srvCrt, caCrt);
    SASSERT(_x509);
}

// --------------------------------------------------------------------------
SHTTPSManager::~SHTTPSManager() {
    // Clean up outstanding transactions
    SASSERTWARN(_activeTransactionList.empty());
    while (!_activeTransactionList.empty())
        closeTransaction(_activeTransactionList.front());
    SASSERTWARN(_completedTransactionList.empty());
    while (!_completedTransactionList.empty())
        closeTransaction(_completedTransactionList.front());

    // Clean up certificate.
    SX509Close(_x509);

    if (_pipeFD[0] != -1) {
        close(_pipeFD[0]);
    }
    if (_pipeFD[1] != -1) {
        close(_pipeFD[0]);
    }
}

// --------------------------------------------------------------------------
void SHTTPSManager::closeTransaction(Transaction* transaction) {
    // Clean up the socket and done
    _activeTransactionList.remove(transaction);
    _completedTransactionList.remove(transaction);
    if (transaction->s)
        closeSocket(transaction->s);
    transaction->s = nullptr;
    delete transaction;
}

int SHTTPSManager::preSelect(fd_map& fdm)
{
    SFDset(fdm, _pipeFD[0], SREADEVTS);
    return STCPManager::preSelect(fdm);
}


// --------------------------------------------------------------------------
void SHTTPSManager::postSelect(fd_map& fdm, uint64_t& nextActivity) {
    // Let the base class do its thing
    STCPManager::postSelect(fdm);

    if (pollKicked) {
        char readbuffer[1];
        read(_pipeFD[0], readbuffer, sizeof(readbuffer));
        pollKicked = false;
    }

    // Update each of the active requests
    uint64_t now = STimeNow();
    list<Transaction*>::iterator nextIt = _activeTransactionList.begin();
    while (nextIt != _activeTransactionList.end()) {
        // Did we get any responses?
        list<Transaction*>::iterator activeIt = nextIt++;
        Transaction* active = *activeIt;
        uint64_t elapsed = now - active->created;
        const uint64_t TIMEOUT = STIME_US_PER_S * 300;
        int size = active->fullResponse.deserialize(active->s->recvBuffer);
        if (size) {
            // Consume how much we read.
            SConsumeFront(active->s->recvBuffer, size);

            // 200OK or any content?
            active->finished = now;
            if (SContains(active->fullResponse.methodLine, " 200 ") || active->fullResponse.content.size()) {
                // Pass the transaction down to the subclass.
                if (_onRecv(active))
                    continue; // If true, then the transaction was closed in onRecv.
                SASSERT(active->response);
            } else {
                // Error, failed to authenticate or receive a valid server response.
                SWARN("Message failed: '" << active->fullResponse.methodLine << "'");
                active->response = 500;
            }
        } else if (active->s->state > STCP_CONNECTED || elapsed > TIMEOUT) {
            // Net problem. Did this transaction end in an inconsistent state?
            SWARN("Connection " << (elapsed > TIMEOUT ? "timed out" : "died prematurely") << " after "
                                << elapsed / STIME_US_PER_MS << "ms");
            active->response = active->s->sendBuffer.empty() ? 501 : 500;
            if (active->response == 501) {
                // This is pretty serious.  Let us know.
                SHMMM("SHTTPSManager: '" << active->fullRequest.methodLine
                                         << "' sent with no response. We don't know if they processed it!");
            }
        } else {
            // Haven't timed out yet, let the caller know how long until we do.
            nextActivity = min(nextActivity, active->created + TIMEOUT);
        }

        // If we're done, remove from the active and add to completd
        if (active->response) {
            // Switch lists
            SINFO("Completed request '" << active->fullRequest.methodLine << "' to '" << active->fullRequest["Host"]
                                        << "' with response '" << active->response << "' in '"
                                        << elapsed / STIME_US_PER_MS << "'ms");
            _activeTransactionList.erase(activeIt);
            _completedTransactionList.push_back(active);
        }
    }
}

// --------------------------------------------------------------------------
SHTTPSManager::Transaction* SHTTPSManager::_createErrorTransaction() {
    // Sometimes we have to create transactions without an attempted
    // connect.  This could happen if we dont have the host or service id yet.
    SWARN("We had to create an error transaction instead of attempting a real one.");
    Transaction* transaction = new Transaction(*this);
    transaction->response = 503;
    transaction->finished = STimeNow();
    _completedTransactionList.push_front(transaction);
    return transaction;
}

// --------------------------------------------------------------------------
SHTTPSManager::Transaction* SHTTPSManager::_httpsSend(const string& url, const SData& request) {
    // Open a connection, optionally using SSL (if the URL is HTTPS).  If that
    // doesnt't work, then just return a completed transaction with an error
    // response.
    string host, path;
    if (!SParseURI(url, host, path))
        return _createErrorTransaction();
    if (!SContains(host, ":"))
        host += ":443";
    Socket* s = openSocket(host, SStartsWith(url, "https://") ? _x509 : 0);
    if (!s)
        return _createErrorTransaction();

    // Wrap in a transaction
    Transaction* transaction = new Transaction(*this);
    transaction->s = s;
    transaction->fullRequest = request;

    // Ship it.
    transaction->s->sendBuffer = request.serialize();

    // Keep track of the transaction.
    // There is a very good reason for push_front and not back.
    // If this transaction is added in the postSelect loop, it
    // would instantly timeout if pushed to the back.
    _activeTransactionList.push_front(transaction);

    // We currently sit and do nothing at when we've written, but we're probably wrapped in a `poll` loop somewhere,
    // and if it doesn't know it's supposed to return, we'll wait for it to timeout before we try and handle the
    // response here.

    // Write arbitrary buffer to the pipe so any subscribers will
    // be awoken.
    // **NOTE: 1 byte so write is atomic.
    if (!pollKicked) {
        pollKicked = true;
        SASSERT(write(_pipeFD[1], "A", 1));
    }

    return transaction;
}

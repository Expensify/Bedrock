#include "SHTTPSManager.h"

#include <BedrockPlugin.h>
#include <BedrockServer.h>
#include <libstuff/libstuff.h>
#include <libstuff/SX509.h>
#include <sqlitecluster/SQLiteNode.h>
#include <iostream>
SHTTPSManager::SHTTPSManager(BedrockPlugin& plugin_) : plugin(plugin_)
{
}

SHTTPSManager::SHTTPSManager(BedrockPlugin& plugin_, const string& pem, const string& srvCrt, const string& caCrt)
  : SStandaloneHTTPSManager(pem, srvCrt, caCrt), plugin(plugin_)
{
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

    delete transaction->s;
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
        // If there's no socket, or we're done, skip. Because we call poll on commands, we may poll transactions that
        // have finished (because commands may finish one transaction but not another), or transactions that have not
        // started yet and have no socket (for instance, Stripe's rate limiting means sockets are built asynchronously
        // after the commands are created).
        //
        // TODO: We may want to be able to time out these transactions regardless, for instance at shutdown. We wont
        // hit the command timeouts in peek and process until there's no outstanding network requests for the command.
        return;
    }
    SAUTOPREFIX(transaction.requestID);

    // Do the postPoll on the socket
    STCPManager::postPoll(fdm, *transaction.s);

    //See if we got a response.
    uint64_t now = STimeNow();
    int size = transaction.fullResponse.deserialize(transaction.s->recvBuffer);
    cout << "Did we get a content-length? " << transaction.fullResponse["Content-Length"] << endl; 
    if (size) {
        // Consume how much we read.
        transaction.s->recvBuffer.consumeFront(size);
        transaction.finished = now;

        // Shut down the socket, we're done with it.
        transaction.s->shutdown(Socket::CLOSED);

        // This is supposed to check for a "200" response, which it does very poorly. It also checks for message
        // content. Why this is the what constitutes a valid response is lost to time. Any well-formed response should
        // be valid here, and this should get cleaned up. However, this requires testing anything that might rely on
        // the existing behavior, which is an exercise for later.
        if (SContains(transaction.fullResponse.methodLine, " 200") || transaction.fullResponse.content.size()) {
            // Pass the transaction down to the subclass.
            _onRecv(&transaction);
        } else {
            // Coercing anything that's not 200 to 500 makes no sense, and should be abandoned with the above.
            SWARN("Message failed: '" << transaction.fullResponse.methodLine << "'");
            transaction.response = 500;
        }
    } else {
        // If we don't have a response, we need to check for a timeout, or a disconnection.
        // The disconnection check is straightforward, we just check the socket state.
        // The timeout is a little less so, because we have two different ways to time out:
        //
        // 1. If it's been more than `timeoutMS` since the last time we sent any data on our socket. This number can
        //    change with each call to this function, because we shorten our timeouts at shutdown to avoid holding up
        //    the whole server on a single stuck network request.
        // 2. If the transaction's timeout (which is likely it's associated command's timeout) has passed.
        if ((transaction.s->state.load() > Socket::CONNECTED) ||
            (now > transaction.s->lastSendTime + timeoutMS * 1000) || 
            (now > transaction.timeoutAt)) {
            SWARN("Connection " << ((transaction.s->state.load() > Socket::CONNECTED) ? "died prematurely" : "timed out"));
            transaction.response = transaction.s->sendBufferEmpty() ? 501 : 500;
        } else {
            // No timeout yet, set nextActivity short enough that it'll catch the next timeout.
            uint64_t remainingUntilTimeoutMS = (timeoutMS * 1000) - (now - transaction.s->lastSendTime);
            uint64_t remainingUntilTimeoutAt = transaction.timeoutAt - now;
            nextActivity = min(nextActivity, min(remainingUntilTimeoutMS, remainingUntilTimeoutAt));
        }
    }
}

SStandaloneHTTPSManager::Transaction::Transaction(SStandaloneHTTPSManager& manager_, const string& requestID) :
    s(nullptr),
    created(STimeNow()),
    finished(0),
    timeoutAt(0),
    response(0),
    manager(manager_),
    sentTime(0),
    requestID(requestID.empty() ? SThreadLogPrefix : requestID)
{
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

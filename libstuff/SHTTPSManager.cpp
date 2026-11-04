#include "SHTTPSManager.h"
#include "SHTTPSProxySocket.h"
#include "libstuff/STCPManager.h"

#include <BedrockPlugin.h>
#include <BedrockServer.h>
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>

const string SStandaloneHTTPSManager::proxyAddressHTTPS = initProxyAddressHTTPS();

string SStandaloneHTTPSManager::initProxyAddressHTTPS()
{
    const char* proxyString = getenv("HTTPS_PROXY");
    if (proxyString != nullptr) {
        return proxyString;
    }
    return "";
}

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

SStandaloneHTTPSManager::~SStandaloneHTTPSManager()
{
}

int SStandaloneHTTPSManager::getHTTPResponseCode(const string& methodLine, const int defaultStatusCode)
{
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

    return defaultStatusCode;
}

void SStandaloneHTTPSManager::prePoll(fd_map& fdm, SStandaloneHTTPSManager::Transaction& transaction)
{
    if (!transaction.s || transaction.finished) {
        // If there's no socket, or we're done, skip.
        return;
    }
    STCPManager::prePoll(fdm, *transaction.s);
}

void SStandaloneHTTPSManager::postPoll(fd_map& fdm, SStandaloneHTTPSManager::Transaction& transaction, uint64_t& nextActivity, uint64_t timeoutMS)
{
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

    const uint64_t now = STimeNow();
    const Socket::State state = transaction.s->state.load();
    const SFastBuffer& received = transaction.s->recvBuffer;
    auto result = SHTTPResponseParser::Result::NEED_MORE;
    if (received.size()) {
        // The proxy socket uses this buffer for CONNECT until its tunnel opens. A rejected CONNECT
        // closes the socket and leaves the proxy response available to report to the caller.
        const auto* proxySocket = dynamic_cast<const SHTTPSProxySocket*>(transaction.s);
        const bool isTargetResponse = !proxySocket || proxySocket->tunnelEstablished();
        if (isTargetResponse || state == Socket::CLOSED) {
            SData& response = transaction.shouldStreamResponse ? transaction.fullResponse : transaction._bufferedResponse;
            const bool isHeadRequest = isTargetResponse && SStartsWith(transaction.fullRequest.methodLine, "HEAD ");
            result = transaction._responseParser.parse(string_view(received.c_str(), received.size()), response, state == Socket::CLOSED, isHeadRequest);
            if (!isTargetResponse && result == SHTTPResponseParser::Result::COMPLETE) {
                const int proxyStatusCode = SHTTPResponseParser::getStatusCode(response.methodLine);
                if (proxyStatusCode < 300 || proxyStatusCode >= 600) {
                    // Only a CONNECT rejection can be reported before a tunnel is established.
                    response.clear();
                    result = SHTTPResponseParser::Result::NEED_MORE;
                }
            }
        }
    }

    const uint64_t timeoutAt = min(transaction.s->lastSendTime + timeoutMS * 1000, transaction.timeoutAt);
    if (result == SHTTPResponseParser::Result::NEED_MORE && state <= Socket::CONNECTED && now <= timeoutAt) {
        nextActivity = min(nextActivity, timeoutAt - now);
        return;
    }

    transaction.finished = now;
    if (result == SHTTPResponseParser::Result::COMPLETE) {
        if (!transaction.shouldStreamResponse) {
            transaction.fullResponse = move(transaction._bufferedResponse);
        }

        // A receive callback can return the socket to a pool. Consume only this response first.
        transaction.s->recvBuffer.consumeFront(transaction._responseParser.consumed());

        // Callbacks handle responses with a body and the common empty success responses.
        if (
            SContains(transaction.fullResponse.methodLine, " 200") ||
            SContains(transaction.fullResponse.methodLine, "204") ||
            SContains(transaction.fullResponse.methodLine, "202") ||
            transaction.fullResponse.content.size()
        ) {
            _onRecv(transaction);
        } else {
            SHMMM("Message failed: '" << transaction.fullResponse.methodLine << "'");
            transaction.response = getHTTPResponseCode(transaction.fullResponse.methodLine, 500);
        }
    } else if (result == SHTTPResponseParser::Result::INVALID) {
        SHMMM("Invalid HTTP response, responding 502");
        transaction.response = 502;
    } else {
        // Callers use these transport statuses to decide whether and when to retry.
        transaction.response = transaction.s->sendBufferEmpty() ? 501 : 500;
        const string progress = _describeRequestProgress(transaction);
        const string outcome = (state > Socket::CONNECTED) ? "died prematurely" : "timed out";
        const string stage = _describeFailureStage(transaction, state, progress == "sent");
        const string target = transaction.targetHost.empty() ? "an unknown host" : transaction.targetHost;
        const string via = transaction.proxyHost.empty() ? "" : " via proxy " + transaction.proxyHost;
        SHMMM("Connection " << outcome << " " << stage << " to " << target << via
              << "; request " << progress << ", responding " << transaction.response);
    }

    transaction._bufferedResponse = SData();
    delete transaction.s;
    transaction.s = nullptr;
}

SStandaloneHTTPSManager::Transaction::Transaction(SStandaloneHTTPSManager& manager_, const string& requestID) :
    s(nullptr),
    created(STimeNow()),
    finished(0),
    timeoutAt(0),
    response(0),
    manager(manager_),
    requestID(requestID.empty() ? SThreadLogPrefix : requestID)
{
    // The blockingCommit thread runs commands under an exclusive transaction, serialized against the whole blocking
    // queue. Starting an HTTPS request there would block that thread for the full network round-trip, stalling
    // every command behind it. Every outbound request builds exactly one Transaction before opening its
    // socket, so refusing here covers every manager uniformly, and the throw propagates out of peek to a clean failure.
    // Reconstructing already-completed requests in BedrockCommand::deserializeHTTPSRequests happens on the
    // command-reception thread, never worker 0, so this does not reject those.
    if (isBlockingCommitThread) {
        STHROW("500 Refused - HTTPS request attempted on blockingCommit thread");
    }
}

SStandaloneHTTPSManager::Transaction::~Transaction()
{
    delete s;
}

unique_ptr<SStandaloneHTTPSManager::Transaction> SStandaloneHTTPSManager::_createErrorTransaction()
{
    // Sometimes we have to create transactions without an attempted connect. This could happen if we don't have the
    // host or service id yet.
    SHMMM("We had to create an error transaction instead of attempting a real one.");
    unique_ptr<Transaction> transaction = make_unique<Transaction>(*this);
    transaction->response = 503;
    transaction->finished = STimeNow();
    return transaction;
}

unique_ptr<SStandaloneHTTPSManager::Transaction> SStandaloneHTTPSManager::_httpsSend(const string& url, const SData& request, bool allowProxy)
{
    return _httpsSend(url, request, nullptr, allowProxy);
}

unique_ptr<SStandaloneHTTPSManager::Transaction> SStandaloneHTTPSManager::_httpsSend(const string& url, const SData& request, shared_ptr<const MTLSConnection> connection, bool allowProxy)
{
    // Open a connection, optionally using SSL (if the URL is HTTPS). If that doesn't work, then just return a
    // completed transaction with an error response.
    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    const bool isHttps = SStartsWith(url, "https://");
    if (connection && (!isHttps || !SIEquals(host, connection->hostname))) {
        SWARN("mTLS requires an HTTPS URL matching the connection hostname");
        return _createErrorTransaction();
    }

    unique_ptr<Transaction> transaction = make_unique<Transaction>(*this);
    transaction->targetHost = host;

    // Open a socket, optionally using a client certificate.
    Socket* s = nullptr;
    bool usingProxy = false;
    try {
        // If a proxy is set, and it's allowed to use it, go through the proxy.
        if (isHttps && allowProxy && proxyAddressHTTPS.size()) {
            string proxyHost, path;
            SParseURI(proxyAddressHTTPS, proxyHost, path);
            SINFO("Proxying " << url << " through " << proxyHost);
            transaction->proxyHost = proxyHost;
            s = connection
                ? new SHTTPSProxySocket(proxyHost, move(connection), transaction->requestID)
                : new SHTTPSProxySocket(proxyHost, host, transaction->requestID);
            usingProxy = true;
        } else {
            s = connection ? new Socket(move(connection)) : new Socket(host, isHttps);
        }
    } catch (const SException& exception) {
        return _createErrorTransaction();
    }

    // When using a proxy with CONNECT tunnels, remove "Connection: close" header to prevent
    // premature connection termination during TLS data transfer, which causes SSL EOF errors.
    // HTTP CONNECT establishes a persistent tunnel that must remain open for the entire TLS session.
    // HTTP/1.1 uses persistent connections by default, and letting the client/server negotiate
    // keep-alive naturally is more robust.
    SData modifiedRequest = request;
    if (usingProxy) {
        modifiedRequest.nameValueMap.erase("Connection");
    }
    transaction->s = s;
    transaction->fullRequest = modifiedRequest;

    // Ship it.
    transaction->s->send(modifiedRequest.serialize());

    // Keep track of the transaction.
    return transaction;
}

string SStandaloneHTTPSManager::_describeFailureStage(const Transaction& transaction, Socket::State state, bool requestSent)
{
    const bool viaProxy = !transaction.proxyHost.empty();

    // A failed lookup closes the socket without ever filling in its address.
    const bool lookupFailed = transaction.s->connectFailure && transaction.s->addr.sin_addr.s_addr == 0;
    if (state == Socket::RESOLVING || lookupFailed) {
        return viaProxy ? "resolving the proxy" : "resolving DNS";
    }
    if (transaction.s->connectFailure || state == Socket::CONNECTING) {
        return viaProxy ? "connecting to the proxy" : "connecting";
    }
    const SHTTPSProxySocket* proxySocket = dynamic_cast<const SHTTPSProxySocket*>(transaction.s);
    if (proxySocket && !proxySocket->tunnelEstablished()) {
        return "opening the proxy tunnel";
    }
    return requestSent ? "waiting for a response" : "sending the request";
}

string SStandaloneHTTPSManager::_describeRequestProgress(const Transaction& transaction)
{
    // An empty send buffer doesn't prove the request went out: a socket that closed before the request was queued
    // never had it in its buffer. Bytes only count as reaching the target once a direct socket has sent something, or
    // once a proxy socket's tunnel is open (the bytes before that are the CONNECT to the proxy).
    const SHTTPSProxySocket* proxySocket = dynamic_cast<const SHTTPSProxySocket*>(transaction.s);
    const bool reachedTarget = proxySocket ? proxySocket->tunnelEstablished() : transaction.s->lastSendTime > transaction.s->openTime;
    if (!reachedTarget) {
        return "not sent";
    }
    return transaction.s->sendBufferEmpty() ? "sent" : "not fully sent";
}

bool SStandaloneHTTPSManager::_onRecv(Transaction& transaction)
{
    transaction.response = getHTTPResponseCode(transaction.fullResponse.methodLine);
    return false;
}

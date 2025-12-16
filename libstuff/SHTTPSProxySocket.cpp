#include "SHTTPSProxySocket.h"
#include "libstuff/SData.h"
#include "libstuff/STCPManager.h"
#include "libstuff/libstuff.h"
#include <libstuff/SSSLState.h>
#include <mutex>

SHTTPSProxySocket::SHTTPSProxySocket(const string& proxyAddress, const string& host, const string& requestID)
 : STCPManager::Socket::Socket(0, STCPManager::Socket::State::CONNECTING, true),
   proxyAddress(proxyAddress),
   hostname(host),
   requestID(requestID)
{
    SASSERT(SHostIsValid(proxyAddress));
    s = S_socket(proxyAddress, true, false, false);
    if (s < 0) {
        STHROW("Couldn't open socket to " + host);
    }
}

SHTTPSProxySocket::SHTTPSProxySocket(SHTTPSProxySocket&& from)
: STCPManager::Socket(move(from))
{
    proxyAddress = move(from.proxyAddress);
    from.proxyAddress = "";
    requestID = move(from.requestID);
    from.requestID = "";
}

SHTTPSProxySocket::~SHTTPSProxySocket() {
}

bool SHTTPSProxySocket::send(size_t* bytesSentCount) {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    bool result = false;
    size_t oldSize = sendBuffer.size();
    size_t oldPreSendSize = preSendBuffer.size();
    if (oldPreSendSize) {
        result = S_sendconsume(s, preSendBuffer);
        size_t bytesSent = oldPreSendSize - preSendBuffer.size();
        if (bytesSent) {
            lastSendTime = STimeNow();
            if (bytesSentCount) {
                *bytesSentCount = bytesSent;
            }
        }
    } else if (proxyNegotiationComplete) {
        result = ssl->sendConsume(sendBuffer);
    } else {
        // Waiting for proxy negotiation to complete before sending more.
        return true;
    }
    size_t bytesSent = oldSize - sendBuffer.size();
    if (bytesSent) {
        lastSendTime = STimeNow();
        if (bytesSentCount) {
            *bytesSentCount = bytesSent;
        }
    }
    return result;
}

bool SHTTPSProxySocket::send(const string& buffer, size_t* bytesSentCount) {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    if (state.load() < Socket::State::SHUTTINGDOWN) {
        if (!filledPreSendBuffer) {
            SData connectMessage("CONNECT " + hostname + " HTTP/1.1");
            connectMessage["Host"] = hostname;
            connectMessage["X-Request-ID"] = requestID;

            string serialized = connectMessage.serialize();
            preSendBuffer.append(serialized.c_str(), serialized.size());
            filledPreSendBuffer = true;
        }

        sendBuffer += buffer;
    } else if (!sendBuffer.empty()) {
        SWARN("Not appending to sendBuffer in socket state " << state.load());
    }

    return send(bytesSentCount);
}

bool SHTTPSProxySocket::recv() {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    bool result = false;
    if (s > 0) {
        const size_t oldSize = recvBuffer.size();
        if (!proxyNegotiationComplete) {
            result = S_recvappend(s, recvBuffer);
            if (recvBuffer.size()) {
                SData connectionEstablished;
                connectionEstablished.deserialize(recvBuffer);
                if (!connectionEstablished.empty()) {
                    // Basic checking that we got back a 200.
                    if (SContains(connectionEstablished.methodLine, " 200 ")) {
                        proxyNegotiationComplete = true;
                        recvBuffer.clear();

                        // We create this here, rather than in the constructor or somewhere that seems more reasonable, because
                        // STCPManager::prePoll will start the TLS handshake if the Socket object's `ssl` field is set. Since the
                        // `CONNECT` message is plain HTTP, we want to skip the handshake until that is all completed, and then set
                        // the ssl field so that the handsahke begins.
                        // Technically, it's feasible to begin the TLS handshake as soon as the CONNECT message has sent, even without
                        // waiting for the response, but this was causing issues debugging in wireshark, which couldn't reassemble the
                        // stream of packets in a way that really made sense. It's also just sort of strange looking, so we just
                        // wait to start the TLS handshake until the CONNECT message is complete and its response is received.
                        ssl = new SSSLState(hostname, s);
                    } else {
                        SWARN("Proxy server " << proxyAddress << " returned methodLine: " << connectionEstablished.methodLine);
                        close(s);
                        s = -1;
                        state = STCPManager::Socket::CLOSED;
                    }
                }
            }
        } else  {
            result = ssl->recvAppend(recvBuffer);
        }

        if (oldSize != recvBuffer.size()) {
            lastRecvTime = STimeNow();
        }
    }

    return result;
}

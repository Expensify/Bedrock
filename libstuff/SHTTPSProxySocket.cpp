#include "SHTTPSProxySocket.h"
#include "libstuff/SData.h"
#include "libstuff/STCPManager.h"
#include "libstuff/libstuff.h"
#include <libstuff/SSSLState.h>
#include <mutex>

SHTTPSProxySocket::SHTTPSProxySocket(const string& proxyAddress, const string& host)
 : STCPManager::Socket::Socket(0, STCPManager::Socket::State::CONNECTING, true),
   proxyAddress(proxyAddress),
   hostname(host)
{
    SASSERT(SHostIsValid(proxyAddress));
    s = S_socket(proxyAddress, true, false, false);
    if (s < 0) {
        STHROW("Couldn't open socket to " + host);
    }

    string domain;
    if (https) {
        uint16_t port;
        SParseHost(hostname, domain, port);
    }

    ssl = new SSSLState(s, domain);
}

SHTTPSProxySocket::SHTTPSProxySocket(SHTTPSProxySocket&& from)
: STCPManager::Socket(move(from))
{
    proxyAddress = move(from.proxyAddress);
    from.proxyAddress = "";
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
            connectMessage["Host"] = proxyAddress;
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

#include "SHTTPSProxySocket.h"
#include "libstuff/SData.h"
#include "libstuff/STCPManager.h"
#include "libstuff/libstuff.h"
#include <libstuff/SSSLState.h>
#include <mutex>
#include <iostream>
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
    uint16_t port;
    SParseHost(hostname, domain, port);

    //ssl = new SSSLState(domain, s);
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
        cout << "Presending: " << preSendBuffer.c_str() << endl;
        result = S_sendconsume(s, preSendBuffer);
        size_t bytesSent = oldPreSendSize - preSendBuffer.size();
        if (preSendBuffer.size() == 0) {
            cout << "Present all " << bytesSent << "bytes" << endl;
        }
        if (bytesSent) {
            lastSendTime = STimeNow();
            if (bytesSentCount) {
                *bytesSentCount = bytesSent;
            }
        }
    } else if (proxyNegotiationComplete) {
        SINFO("TYLER starting send buffer: " << sendBuffer.c_str());
        cout << "TYLER starting send buffer: " << sendBuffer.c_str() << endl;
        result = ssl->sendConsume(sendBuffer);
        SINFO("TYLER remaining send buffer: " << sendBuffer.c_str());
        cout << "TYLER remaining send buffer: " << sendBuffer.c_str() << endl;
    } else {
        // Waiting for proxy negotiation to complete before sending more.
        cout << "WAITING ON PROXY" << endl;
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
            connectMessage["Proxy-Connection"] = "Keep-Alive";
            connectMessage["User-Agent"] = "bedrock/3.0";

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
    cout << "SHTTPSProxySocket recv called" << endl;

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
                        SINFO("TYLER finished proxy negotiation");
                        cout << "TYLER finished proxy negotiation: " << recvBuffer.c_str() << endl;

                        recvBuffer.clear();

                        string domain;
                        uint16_t port;
                        SParseHost(hostname, domain, port);
                        ssl = new SSSLState(domain, s);

                    } else {
                        cout << "TYLER BROKEN" << endl;
                        SWARN("Proxy server " << proxyAddress << " returned methodLine: " << connectionEstablished.methodLine);
                        close(s);
                        s = -1;
                        state = STCPManager::Socket::CLOSED;
                    }
                }
            }
        } else  {
            SINFO("TYLER recvBuffer is: " << recvBuffer.c_str());
            cout << "TYLER recvBuffer is: " << recvBuffer.c_str() << endl;
            result = ssl->recvAppend(recvBuffer);
        }

        if (oldSize != recvBuffer.size()) {
            lastRecvTime = STimeNow();
        }
    }

    return result;
}

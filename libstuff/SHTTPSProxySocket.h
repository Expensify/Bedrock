/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SHTTPSProxySocket.h
 * Path:    libstuff/SHTTPSProxySocket.h
 * Pair:    SHTTPSProxySocket.cpp
 *
 * INTENT
 *   An STCPManager::Socket that first speaks plain-HTTP CONNECT to
 *   negotiate a tunnel through an HTTPS forward proxy, then switches to a
 *   normal TLS connection to the real destination once the tunnel is up.
 *
 * OBJECTS
 *   SHTTPSProxySocket (class, : STCPManager::Socket) - proxyAddress/hostname/
 *     requestID; proxyNegotiationComplete/filledPreSendBuffer flags;
 *     preSendBuffer (SFastBuffer, holds the CONNECT request); overrides of
 *     send(size_t*)/send(string,size_t*)/recv().
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; a natural peer of STCPManager/SSSLState in libstuff.
 *
 * NAMING QUALITY
 *   [CANDIDATE] Private members (proxyAddress, hostname, requestID,
 *     proxyNegotiationComplete, filledPreSendBuffer, preSendBuffer) lack the
 *     `_` prefix the repo otherwise uses for private/protected members (e.g.
 *     SStandaloneHTTPSManager's _pem/_srvCrt/_caCrt, SSynchronizedQueue's
 *     _queue/_queueMutex).
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include "libstuff/SFastBuffer.h"
#include <libstuff/STCPManager.h>

class SHTTPSProxySocket : public STCPManager::Socket {
public:
    // Implement all the same constructors as the base class.
    SHTTPSProxySocket(const string& proxyAddress, const string& host, const string& requestID);
    SHTTPSProxySocket(SHTTPSProxySocket&& from);

    ~SHTTPSProxySocket();

    // Allow us to send and receive without SSL at the start.
    virtual bool send(size_t* bytesSentCount = nullptr) override;
    virtual bool send(const string& buffer, size_t* bytesSentCount = nullptr) override;
    virtual bool recv() override;

private:

    // These should contain the address and port, i.e.:
    // www.proxy.com:443
    // or:
    // 127.0.0.1:443
    string proxyAddress;
    string hostname;
    string requestID;

    // Before we can send real HTTPS data, we need to establish the connecton to the proxy.
    bool proxyNegotiationComplete = false;
    bool filledPreSendBuffer = false;
    SFastBuffer preSendBuffer;
};

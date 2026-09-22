#pragma once

#include "libstuff/SFastBuffer.h"
#include <libstuff/STCPManager.h>

class SHTTPSProxySocket : public STCPManager::Socket {
public:
    // Establishes a plaintext CONNECT tunnel before starting TLS to the target host.
    SHTTPSProxySocket(const string& proxyAddress, const string& host, const string& requestID);

    // Requires a non-null connection, whose hostname is the CONNECT target.
    SHTTPSProxySocket(const string& proxyAddress, shared_ptr<const STCPManager::MTLSConnection> connection, const string& requestID);
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

#include "libstuff/SFastBuffer.h"
#include "libstuff/SSSLState.h"
#include <libstuff/STCPManager.h>

class SHTTPSProxySocket : public STCPManager::Socket {
  public:
    // Implement all the same constructors as the base class.
    SHTTPSProxySocket(const string& proxyAddress, const string& host);
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

    // Before we can send real HTTPS data, we need to establish the connecton to the proxy.
    bool proxyNegotiationComplete = false;
    bool filledPreSendBuffer = false;
    SFastBuffer preSendBuffer;

    //SSSLState* tempSSL;
};

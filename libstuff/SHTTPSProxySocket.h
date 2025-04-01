#include <libstuff/STCPManager.h>

class SHTTPSProxySocket : public STCPManager::Socket {
  public:
    // Implement all the same constructors as the base class.
    SHTTPSProxySocket(const string& proxyAddress, const string& host, bool https = false);
    SHTTPSProxySocket(const string& proxyAddress, int sock = 0, State state_ = CONNECTING, bool https = false);
    SHTTPSProxySocket(SHTTPSProxySocket&& from);

    ~SHTTPSProxySocket();

    private:

    // This should contain the address and port, i.e.:
    // www.proxy.com:443
    // or:
    // 127.0.0.1:443
    string proxyAddress;
};
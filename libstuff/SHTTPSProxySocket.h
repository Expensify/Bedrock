#include <libstuff/STCPManager.h>

class SHTTPSProxySocket : public STCPManager::Socket {
    // Implement all the same constructors as the base class.
    SHTTPSProxySocket(const string& host, bool https = false);
    SHTTPSProxySocket(int sock = 0, State state_ = CONNECTING, bool https = false);
    SHTTPSProxySocket(SHTTPSProxySocket&& from);

    ~SHTTPSProxySocket();

};
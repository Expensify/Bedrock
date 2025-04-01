#include "SHTTPSProxySocket.h"
#include "libstuff/STCPManager.h"

SHTTPSProxySocket::SHTTPSProxySocket(const string& proxyAddress, const string& host, bool https)
 : STCPManager::Socket(host, https), proxyAddress(proxyAddress)
{
}

SHTTPSProxySocket::SHTTPSProxySocket(const string& proxyAddress, int sock, State state_, bool https)
 : STCPManager::Socket(sock, state_, https), proxyAddress(proxyAddress)
{
}

SHTTPSProxySocket::SHTTPSProxySocket(SHTTPSProxySocket&& from)
: STCPManager::Socket(move(from))
{
    proxyAddress = move(from.proxyAddress);
    from.proxyAddress = "";
}

SHTTPSProxySocket::~SHTTPSProxySocket() {
    STCPManager::Socket::~Socket();
}
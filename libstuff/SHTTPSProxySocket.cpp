#include "SHTTPSProxySocket.h"
#include "libstuff/STCPManager.h"

SHTTPSProxySocket::SHTTPSProxySocket(const string& host, bool https)
 : STCPManager::Socket(host, https)
{
}

SHTTPSProxySocket::SHTTPSProxySocket(int sock, State state_, bool https)
 : STCPManager::Socket(sock, state_, https)
{
}

SHTTPSProxySocket::SHTTPSProxySocket(SHTTPSProxySocket&& from)
: STCPManager::Socket(move(from))
{
}

SHTTPSProxySocket::~SHTTPSProxySocket() {
    STCPManager::Socket::~Socket();
}
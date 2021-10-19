#include "STCPServer.h"

#include <unistd.h>

#include <libstuff/libstuff.h>

STCPServer::STCPServer(const string& host) : port(nullptr)
{
    // Initialize
    if (!host.empty()) {
        port = openPort(host);
    }
}

STCPServer::~STCPServer() {
}

unique_ptr<STCPServer::Port> STCPServer::openPort(const string& host) {
    // Open a port on the requested host
    SASSERT(SHostIsValid(host));
    int s = S_socket(host, true, true, false);
    SASSERT(s >= 0);
    return make_unique<Port>(s, host);
}

STCPManager::Socket* STCPServer::acceptSocket(unique_ptr<Port>& port) {
    // Initialize to 0 in case we don't accept anything. Note that this *does* overwrite the passed-in pointer.
    Socket* socket = nullptr;

    // Try to accept on the port and wrap in a socket
    sockaddr_in addr;
    int s = S_accept(port->s, addr, false);
    if (s > 0) {
        // Received a socket, wrap
        SDEBUG("Accepting socket from '" << addr << "' on port '" << port->host << "'");
        socket = new Socket(s, Socket::CONNECTED);
        socket->addr = addr;
        // Pretty sure these leak.
        socketList.push_back(socket);

        // Try to read immediately
        S_recvappend(socket->s, socket->recvBuffer);
    }

    return socket;
}

void STCPServer::prePoll(fd_map& fdm) {
    // Call the base class
    for (auto& s : socketList) {
        STCPManager::prePoll(fdm, *s);
    }

    // Add the ports
    if (port) {
        SFDset(fdm, port->s, SREADEVTS);
    }
}

void STCPServer::postPoll(fd_map& fdm) {
    // Process all the existing sockets.
    // FIXME: Detect port failure
    for (auto& s : socketList) {
        STCPManager::postPoll(fdm, *s);
    }
}

STCPServer::Port::Port(int _s, string _host) : s(_s), host(_host)
{
}

STCPServer::Port::~Port()
{
    if (s != -1) {
        ::close(s);
    }
}

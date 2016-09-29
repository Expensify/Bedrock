#include "libstuff.h"

// --------------------------------------------------------------------------
STCPServer::STCPServer(const string& host) {
    // Initialize
    port = -1;
    if (!host.empty()) {
        openPort(host);
    }
}

// --------------------------------------------------------------------------
STCPServer::~STCPServer() {
    // Close the port
    if (port >= 0) {
        ::closesocket(port);
    }
}

// --------------------------------------------------------------------------
void STCPServer::openPort(const string& host) {
    // Open a port on the requested host
    SASSERT(SHostIsValid(host));
    SASSERT(port < 0);
    SASSERT((port = S_socket(host, true, true, false)) >= 0);
}

// --------------------------------------------------------------------------
void STCPServer::closePort() {
    // Close the port
    if (port >= 0) {
        ::closesocket(port);
        port = -1;
    } else {
        SWARN("Port already closed.");
    }
}

// --------------------------------------------------------------------------
STCPManager::Socket* STCPServer::acceptSocket() {
    // Ignore if no socket
    if (port < 0) {
        return 0;
    }

    // Try to accept on the port and wrap in a socket
    sockaddr_in addr;
    int s = S_accept(port, addr, false);
    if (s <= 0) {
        return 0;
    }

    // Received a socket, wrap
    SDEBUG("Accepting socket from '" << addr << "'");
    Socket* socket = new Socket;
    socket->s = s;
    socket->addr = addr;
    socket->state = STCP_CONNECTED;
    socket->connectFailure = false;
    socket->openTime = STimeNow();
    socket->ssl = 0;
    socket->data = 0; // Used by caller, not libstuff
    socketList.push_back(socket);

    // Try to read immediately
    S_recvappend(socket->s, socket->recvBuffer);
    return socket;
}

// --------------------------------------------------------------------------
int STCPServer::preSelect(fd_map& fdm) {
    // Do the base class
    int maxS = STCPManager::preSelect(fdm);

    // If no port, stop there
    if (port < 0) {
        return maxS;
    }

    // Add the port
    SFDset(fdm, port, SREADEVTS);
    return (SMax(port, maxS));
}

// --------------------------------------------------------------------------
void STCPServer::postSelect(fd_map& fdm) {
    // Process all the existing sockets;
    STCPManager::postSelect(fdm);
    // **FIXME: Detect port failure
}

#include "libstuff.h"

// --------------------------------------------------------------------------
STCPServer::STCPServer(const string& host) {
    // Initialize
    if (!host.empty()) {
        openPort(host);
    }
}

// --------------------------------------------------------------------------
STCPServer::~STCPServer() {
    // Close all ports
    closePorts();
}

// --------------------------------------------------------------------------
STCPServer::Port* STCPServer::openPort(const string& host) {
    // Open a port on the requested host
    SASSERT(SHostIsValid(host));
    Port port;
    port.host = host;
    port.s = S_socket(host, true, true, false);
    SASSERT(port.s >= 0);
    list<Port>::iterator portIt = portList.insert(portList.end(), port);
    return &*portIt;
}

// --------------------------------------------------------------------------
void STCPServer::closePorts() {
    // Are there any ports to close?
    if (!portList.empty()) {
        // Loop across and close all ports
        for (Port& port : portList) {
            // Close this port
            ::close(port.s);
        }
        portList.clear();
    } else {
        SHMMM("Ports already closed.");
    }
}

// --------------------------------------------------------------------------
STCPManager::Socket* STCPServer::acceptSocket(Port*& portOut) {
    // Initialize to 0 in case we don't accept anything. Note that this *does* overwrite the passed-in pointer.
    portOut = 0;
    Socket* socket = 0;

    // See if we can accept on any port
    for (Port& port : portList) {
        // Try to accept on the port and wrap in a socket
        sockaddr_in addr;
        int s = S_accept(port.s, addr, false);
        if (s > 0) {
            // Received a socket, wrap
            SDEBUG("Accepting socket from '" << addr << "' on port '" << port.host << "'");
            socket = new Socket;
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

            // Record what port it was accepted on
            portOut = &port;
        }
    }

    return socket;
}

// --------------------------------------------------------------------------
int STCPServer::preSelect(fd_map& fdm) {
    // Do the base class
    STCPManager::preSelect(fdm);

    // Add the ports
    for (Port port : portList) {
        SFDset(fdm, port.s, SREADEVTS);
    }
    // Done!
    return 0;
}

// --------------------------------------------------------------------------
void STCPServer::postSelect(fd_map& fdm) {
    // Process all the existing sockets;
    STCPManager::postSelect(fdm);
    // **FIXME: Detect port failure
}

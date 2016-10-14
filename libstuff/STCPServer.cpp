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
        SFOREACH (list<Port>, portList, portIt) {
            // Close this port
            ::closesocket(portIt->s);
        }
        portList.clear();
    } else {
        SWARN("Ports already closed.");
    }
}

// --------------------------------------------------------------------------
STCPManager::Socket* STCPServer::acceptSocket(Port*& portOut) {
    // See if we can accept on any port
    for_each(portList.begin(), portList.end(), [&](Port port) {
        // Try to accept on the port and wrap in a socket
        sockaddr_in addr;
        int s = S_accept(port.s, addr, false);
        if (s > 0) {
            // Received a socket, wrap
            SDEBUG("Accepting socket from '" << addr << "' on port '" << port.host << "'");
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

            // Record what port it was accepted on
            portOut = &port;

            // Done
            return socket;
        }
    });

    // Didn't accept anything
    portOut = 0;
    return 0;
}

// --------------------------------------------------------------------------
int STCPServer::preSelect(fd_map& fdm) {
    // Do the base class
    STCPManager::preSelect(fdm);

    // Add the ports
    for_each(portList.begin(), portList.end(), [&](Port port) { SFDset(fdm, port.s, SREADEVTS); });
    // Done!
    return 0;
}

// --------------------------------------------------------------------------
void STCPServer::postSelect(fd_map& fdm) {
    // Process all the existing sockets;
    STCPManager::postSelect(fdm);
    // **FIXME: Detect port failure
}

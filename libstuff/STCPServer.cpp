#include "STCPServer.h"

#include <unistd.h>

#include <libstuff/libstuff.h>

STCPServer::STCPServer(const string& host) {
    // Initialize
    if (!host.empty()) {
        openPort(host);
    }
}

STCPServer::~STCPServer() {
    // Close all ports
    closePorts();
}

STCPServer::Port* STCPServer::openPort(const string& host) {
    // Open a port on the requested host
    SASSERT(SHostIsValid(host));
    Port port;
    port.host = host;
    port.s = S_socket(host, true, true, false);
    SASSERT(port.s >= 0);
    lock_guard <decltype(portListMutex)> lock(portListMutex);
    list<Port>::iterator portIt = portList.insert(portList.end(), port);
    return &*portIt;
}

void STCPServer::closePorts(list<Port*> except) {
    // Are there any ports to close?
    lock_guard <decltype(portListMutex)> lock(portListMutex);
    if (!portList.empty()) {
        // Loop across and close all ports not excepted.
        auto it = portList.begin();
        while (it != portList.end()) {
            if  (find(except.begin(), except.end(), &(*it)) == except.end()) {
                // Close this port
                ::close(it->s);
                SINFO("Close ports closing " << it->host << ".");
                it = portList.erase(it);
            } else {
                SINFO("Close ports skipping " << it->host << ": in except list.");
                it++;
            }
        }
    } else {
        SHMMM("Ports already closed.");
    }
}

STCPManager::Socket* STCPServer::acceptSocket(Port*& portOut) {
    // Initialize to 0 in case we don't accept anything. Note that this *does* overwrite the passed-in pointer.
    portOut = 0;
    Socket* socket = nullptr;

    // See if we can accept on any port
    lock_guard <decltype(portListMutex)> lock(portListMutex);
    for (Port& port : portList) {
        // Try to accept on the port and wrap in a socket
        sockaddr_in addr;
        int s = S_accept(port.s, addr, false);
        if (s > 0) {
            // Received a socket, wrap
            SDEBUG("Accepting socket from '" << addr << "' on port '" << port.host << "'");
            socket = new Socket(s, Socket::CONNECTED);
            socket->addr = addr;
            // Pretty sure these leak.
            socketList.push_back(socket);

            // Try to read immediately
            S_recvappend(socket->s, socket->recvBuffer);

            // Record what port it was accepted on
            portOut = &port;
        }
    }

    return socket;
}

void STCPServer::prePoll(fd_map& fdm) {
    // Call the base class
    for (auto& s : socketList) {
        STCPManager::prePoll(fdm, *s);
    }

    // Add the ports
    lock_guard <decltype(portListMutex)> lock(portListMutex);
    for (Port& port : portList) {
        SFDset(fdm, port.s, SREADEVTS);
    }
}

void STCPServer::postPoll(fd_map& fdm) {
    // Process all the existing sockets.
    // FIXME: Detect port failure
    for (auto& s : socketList) {
        STCPManager::postPoll(fdm, *s);
    }
}

#include "libstuff.h"

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
    port.isHost = true;
    SDEBUG("MB OPENING PORT " << port.host);
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


            // SSL?
            if(port.isHost) {
                SDEBUG("MB SERVER ACCEPT HOST SOCKET " << socket->state.load() << " " << SToStr(socket->addr));

                SX509* x509;

                x509 = SX509Open();

                socket->ssl = SSSLOpen(s, x509, true);
                SDEBUG("MB SSL object for peer client created"); 
                // SERVER HELLO?
                
                int tries = 0;
                int ret = 0;
                do {
                    ret = mbedtls_ssl_handshake(&socket->ssl->ssl);
                    SDEBUG("MB Server SSL Handshake " << ret << " : " << SSSLError(ret));
                    sleep(1);
                    if(tries++ > 100) {
                        // give it up as a bad job.
                        ret = -1;
                        SDEBUG("XXXXXXXXXXXXXXXX MB Server Handshake ABORTING.");
                    }
                } while(ret != 0);
                
                //SDEBUG("MB NON-LOOP SERVER Handshake");
                //int ret = mbedtls_ssl_handshake(&socket->ssl->ssl);
                
                if(ret == 0) {
                    SDEBUG("XXXXXXXXXXXXXXXXXXXXXXXXXX MB Server Handshake Completed!" << ret);
                    socket->useSSL = true;
                } else {
                    sleep(1);
                    mbedtls_ssl_session_reset( &socket->ssl->ssl );
                }
                

            } else {
                SDEBUG("MB Accepted non-host socket. ");
            }
            
            socket->addr = addr;
            socketList.push_back(socket);

            // Try to read immediately
            if(socket->useSSL) {
                SSSLRecvAppend(socket->ssl,socket->recvBuffer);
            } else {
                S_recvappend(socket->s, socket->recvBuffer);
            }

            // Record what port it was accepted on
            portOut = &port;
        }
    }

    return socket;
}

void STCPServer::prePoll(fd_map& fdm) {
    // Call the base class
    STCPManager::prePoll(fdm);

    // Add the ports
    lock_guard <decltype(portListMutex)> lock(portListMutex);
    for (Port& port : portList) {
        SFDset(fdm, port.s, SREADEVTS);
    }
}

void STCPServer::postPoll(fd_map& fdm) {
    // Process all the existing sockets.
    // FIXME: Detect port failure
    STCPManager::postPoll(fdm);
}

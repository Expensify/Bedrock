#include "libstuff.h"

atomic<uint64_t> STCPManager::Socket::socketCount(1);

STCPManager::~STCPManager() {
    SASSERTWARN(socketList.empty());
}

void STCPManager::prePoll(fd_map& fdm) {
    // Add all the sockets
    for (Socket* socket : socketList) {
        // Make sure it's not closed
        if (socket->state.load() != Socket::CLOSED) {
            // Check and see if it looks like we're still valid.
            if (socket->s < 0) {
                SWARN("Invalid FD number("
                      << socket->s << "), we're probably about to corrupt stack memory. FD_SETSIZE=" << FD_SETSIZE);
            }
            // Add this socket. First, we always want to read, and we always want to learn of exceptions.
            SFDset(fdm, socket->s, SREADEVTS);

            // However, we only want to write in some states. No matter what, we want to send if we're not yet
            // connected. And if we're not using SSL, then we want to send only when we have something buffered for
            // sending. But if we *are* using SSL, it's a bit more complex. If we've completed the handshake, then we
            // only want to send when we have data. But if we're inside the handshake, leave it up to the SSL engine
            // to decide if it wants to send.
            if (socket->state.load() == Socket::CONNECTING) {
                // We haven't yet connected -- send regardless of SSL
                SFDset(fdm, socket->s, SWRITEEVTS);
            } else if (!socket->ssl) {
                // No SSL, just send if we have anything buffered
                if (!socket->sendBufferEmpty()) {
                    SFDset(fdm, socket->s, SWRITEEVTS);
                }
            } else {
                // Have we completed the handshake?
                SASSERT(socket->ssl);
                SSSLState* sslState = socket->ssl;
                if (sslState->ssl.state == MBEDTLS_SSL_HANDSHAKE_OVER) {
                    // Handshake done -- send if we have anything buffered
                    if (!socket->sendBufferEmpty()) {
                        SFDset(fdm, socket->s, SWRITEEVTS);
                    }
                } else {
                    // Handshake isn't done -- send if SSL wants to
                    bool write;
                    switch (sslState->ssl.state) {
                    case MBEDTLS_SSL_HELLO_REQUEST:
                    case MBEDTLS_SSL_CLIENT_HELLO:
                    case MBEDTLS_SSL_CLIENT_CERTIFICATE:
                    case MBEDTLS_SSL_CLIENT_KEY_EXCHANGE:
                    case MBEDTLS_SSL_CERTIFICATE_VERIFY:
                    case MBEDTLS_SSL_CLIENT_CHANGE_CIPHER_SPEC:
                    case MBEDTLS_SSL_CLIENT_FINISHED:
                        // In these cases, SSL is waiting to write already.
                        // @see https://www.mail-archive.com/list@xyssl.org/msg00041.html
                        write = true;
                        break;
                    default:
                        write = false;
                        break;
                    }
                    if (write) {
                        SFDset(fdm, socket->s, SWRITEEVTS);
                    }
                }
            }
        }
    }
}

void STCPManager::postPoll(fd_map& fdm) {
    // Walk across the sockets
    for (Socket* socket : socketList) {
        // Update this socket
        switch (socket->state.load()) {
        case Socket::CONNECTING: {
            // See if it connected or failed
            if (!SFDAnySet(fdm, socket->s, SWRITEEVTS | POLLHUP | POLLERR)) {
                // Keep waiting for asynchronous connect result
                break;
            }

            // Mark any sockets that the other end disconnected as closed.
            if (SFDAnySet(fdm, socket->s, POLLHUP)) {
                socket->state.store(Socket::CLOSED);
                ::shutdown(socket->s, SHUT_RDWR);
            }

            // Tagged as writable; check SO_ERROR to see if the connect failed
            int result = 0;
            socklen_t size = sizeof(result);
            SASSERTWARN(!getsockopt(socket->s, SOL_SOCKET, SO_ERROR, &result, &size));
            if (result) {
                // Asynchronous connect failed; close socket
                SDEBUG("Connect to '" << socket->addr << "' failed with SO_ERROR #" << result << ", closing.");
                socket->state.store(Socket::CLOSED);
                socket->connectFailure = true;
                break;
            }

            // Asynchronous connect succeeded
            SDEBUG("Connect to '" << socket->addr << "' succeeded.");
            SASSERTWARN(SFDAnySet(fdm, socket->s, SWRITEEVTS));
            socket->state.store(Socket::CONNECTED);
            // **NOTE: Intentionally fall through to the connected state
        }

        case Socket::CONNECTED: {
            // Connected -- see if we're ready to send
            bool aliveAfterRecv = true;
            bool aliveAfterSend = true;
            if (socket->ssl) {
                // If the socket is ready to send or receive, do both: SSL has its own internal traffic, so even if we
                // only want to receive, SSL might need to send (and vice versa)
                //
                // **NOTE: SSL can receive data for a while before giving any back, so if this gets called many times
                //         in a row it might just be filling an internal buffer (and not due to some busy loop)
                SDEBUG("sslState=" << SSSLGetState(socket->ssl) << ", canrecv=" << SFDAnySet(fdm, socket->s, SREADEVTS)
                                   << ", recvsize=" << socket->recvBuffer.size()
                                   << ", cansend=" << SFDAnySet(fdm, socket->s, SWRITEEVTS)
                                   << ", sendsize=" << socket->sendBufferCopy().size());
                if (SFDAnySet(fdm, socket->s, SREADEVTS | SWRITEEVTS)) {
                    // Do both
                    aliveAfterRecv = socket->recv();
                    aliveAfterSend = socket->send();
                }
            } else {
                // Only send/recv if the socket is ready
                if (SFDAnySet(fdm, socket->s, SREADEVTS)) {
                    aliveAfterRecv = socket->recv();
                }
                if (SFDAnySet(fdm, socket->s, SWRITEEVTS)) {
                    aliveAfterSend = socket->send();
                }
            }

            // If we died, update
            if (!aliveAfterRecv || !aliveAfterSend) {
                // How did we die?
                SDEBUG("Connection to '" << socket->addr << "' died (recv=" << aliveAfterRecv << ", send="
                       << aliveAfterSend << ")");
                socket->state.store(Socket::CLOSED);
            }
            break;
        }

        case Socket::SHUTTINGDOWN:
            // Is this a SSL socket?
            if (socket->ssl) {
                // Always send/recv (see Socket::CONNECTED, above)
                // **FIXME: Add timeout.
                bool aliveAfterRecv = socket->recv();
                bool aliveAfterSend = socket->send();
                if (!aliveAfterSend || (!aliveAfterRecv && socket->sendBufferEmpty())) {

                    // Did we send everything?  (Technically this the send buffer could be empty and we still haven't
                    // sent everything -- SSL buffers internally, so we should check that buffer.  But odds are it sent fine.)
                    if (socket->sendBufferEmpty()) {
                        SDEBUG("Graceful shutdown of SSL socket '" << socket->addr << "'");
                    } else {
                        SWARN("Dirty shutdown of SSL socket '" << socket->addr << "' (" << socket->sendBufferCopy().size()
                                                               << " bytes remain)");
                    }
                    socket->state.store(Socket::CLOSED);
                    ::shutdown(socket->s, SHUT_RDWR);
                }
            } else {
                // Not SSL -- only send if we have something to send
                if (!socket->sendBufferEmpty()) {
                    // Still have something to send -- try to send it.
                    if (!socket->send()) {
                        // Done trying to send
                        SHMMM("Unable to finish sending to '" << socket->addr << "' on shutdown, clearing.");
                        ::shutdown(socket->s, SHUT_RDWR);
                        socket->setSendBuffer("");
                    }
                }

                // Are we done sending?
                // **FIXME: Add timeout
                if (socket->sendBufferEmpty()) {
                    // Wait for the other side to shut down
                    if (!socket->recv()) {
                        // Done shutting down
                        SDEBUG("Graceful shutdown of socket '" << socket->addr << "'");
                        socket->state.store(Socket::CLOSED);
                        ::shutdown(socket->s, SHUT_RDWR);
                    }
                }
            }
            break;
        case Socket::CLOSED:
            // Ignore
            break;
        default:
            SERROR("Unknown socket state");
        }
    }
}

void STCPManager::shutdownSocket(Socket* socket, int how) {
    // Send the shutdown and note
    SASSERT(socket);
    SDEBUG("Shutting down socket '" << socket->addr << "' (" << how << ")");
    ::shutdown(socket->s, how);
    socket->state.store(Socket::SHUTTINGDOWN);
}

void STCPManager::closeSocket(Socket* socket) {
    // Clean up this socket
    SASSERT(socket);
    SDEBUG("Closing socket '" << socket->addr << "'");
    socketList.remove(socket);

    delete socket;
}

STCPManager::Socket::Socket(int sock, STCPManager::Socket::State state_, SX509* x509)
  : s(sock), addr{}, state(state_), connectFailure(false), openTime(STimeNow()), lastSendTime(openTime),
    lastRecvTime(openTime), ssl(nullptr), data(nullptr), id(STCPManager::Socket::socketCount++), _x509(x509)
{ }

STCPManager::Socket::~Socket() {
    ::close(s);
    if (ssl) {
        SSSLClose(ssl);
    }
    if (_x509) {
        SX509Close(_x509);
    }
}

STCPManager::Socket* STCPManager::openSocket(const string& host, SX509* x509, recursive_mutex* listMutexPtr) {
    // Try to open the socket
    SASSERT(SHostIsValid(host));
    int s = S_socket(host, true, false, false);
    if (s < 0) {
        return 0;
    }

    // Create a new socket
    Socket* socket = new Socket(s, Socket::CONNECTING, x509);
    socket->ssl = x509 ? SSSLOpen(socket->s, x509) : 0;
    SASSERT(!x509 || socket->ssl);

    if (listMutexPtr) {
        lock_guard<recursive_mutex> lock(*listMutexPtr);
        socketList.push_back(socket);
    } else {
        socketList.push_back(socket);
    }
    return socket;
}

bool STCPManager::Socket::send() {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    // Send data
    bool result = false;
    if (ssl) {
        SDEBUG("MB socket send SSL");
        result = SSSLSendConsume(ssl, sendBuffer);
    } else if (s > 0) {
        SDEBUG("MB socket send non-SSL");
        result = S_sendconsume(s, sendBuffer);
    }
    lastSendTime = STimeNow();
    return result;
}

bool STCPManager::Socket::send(const string& buffer) {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    // If the socket's in a valid state for sending, append to the sendBuffer, otherwise warn
    if (state.load() < Socket::State::SHUTTINGDOWN) {
        sendBuffer += buffer;
    } else if (!sendBuffer.empty()) {
        SWARN("Not appending to sendBuffer in socket state " << state.load() << ", tried to send: " << buffer);
    }

    // Send anything we've got.
    return send();
}

bool STCPManager::Socket::sendBufferEmpty() {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    return sendBuffer.empty();
}

string STCPManager::Socket::sendBufferCopy() {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    return sendBuffer;
}

void STCPManager::Socket::setSendBuffer(const string& buffer) {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    sendBuffer = buffer;
}

bool STCPManager::Socket::recv() {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    // Read data
    bool result = false;
    const size_t oldSize = recvBuffer.size();
    if (ssl) {
        SDEBUG("MB SSL RECV");
        result = SSSLRecvAppend(ssl, recvBuffer);
    } else if (s > 0) {
        SDEBUG("MB NON-SSL RECV");
        result = S_recvappend(s, recvBuffer);
    }

    // We've received new data
    if (oldSize != recvBuffer.size()) {
        lastRecvTime = STimeNow();
    }
    return result;
}

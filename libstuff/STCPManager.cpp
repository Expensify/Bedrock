#include "STCPManager.h"

#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SSSLState.h>

atomic<uint64_t> STCPManager::Socket::socketCount(1);

void STCPManager::prePoll(fd_map& fdm, Socket& socket) {
    // Make sure it's not closed
    if (socket.state.load() != Socket::CLOSED) {
        // Check and see if it looks like we're still valid.
        if (socket.s < 0) {
            SWARN("Invalid FD number("
                  << socket.s << "), we're probably about to corrupt stack memory. FD_SETSIZE=" << FD_SETSIZE);
        }
        // Add this socket. First, we always want to read, and we always want to learn of exceptions.
        SFDset(fdm, socket.s, SREADEVTS);

        // However, we only want to write in some states. No matter what, we want to send if we're not yet
        // connected. And if we're not using SSL, then we want to send only when we have something buffered for
        // sending. But if we *are* using SSL, it's a bit more complex. If we've completed the handshake, then we
        // only want to send when we have data. But if we're inside the handshake, leave it up to the SSL engine
        // to decide if it wants to send.
        if (socket.state.load() == Socket::CONNECTING) {
            // We haven't yet connected -- send regardless of SSL
            SFDset(fdm, socket.s, SWRITEEVTS);
        } else if (!socket.ssl) {
            // No SSL, just send if we have anything buffered
            if (!socket.sendBufferEmpty()) {
                SFDset(fdm, socket.s, SWRITEEVTS);
            }
        } else {
            // Have we completed the handshake?
            SASSERT(socket.ssl);
            SSSLState* sslState = socket.ssl;
            if (mbedtls_ssl_is_handshake_over(&sslState->ssl)) {
                // Handshake done -- send if we have anything buffered
                if (!socket.sendBufferEmpty()) {
                    SFDset(fdm, socket.s, SWRITEEVTS);
                }
            } else {
                int ret = mbedtls_ssl_handshake(&sslState->ssl);
                if (ret == MBEDTLS_ERR_SSL_WANT_WRITE) {
                    SFDset(fdm, socket.s, SWRITEEVTS);
                } else if (ret == MBEDTLS_ERR_SSL_WANT_READ) {
                    // This is expected, but is already set.
                } else if (ret) {
                    SWARN("SSL ERROR");
                }
            }
        }
    }
}

void STCPManager::postPoll(fd_map& fdm, Socket& socket) {
    // Update this socket
    switch (socket.state.load()) {
    case Socket::CONNECTING: {
        // See if it connected or failed
        if (!SFDAnySet(fdm, socket.s, SWRITEEVTS | POLLHUP | POLLERR)) {
            // Keep waiting for asynchronous connect result
            break;
        }

        // Mark any sockets that the other end disconnected as closed.
        if (SFDAnySet(fdm, socket.s, POLLHUP)) {
            socket.shutdown(Socket::CLOSED);
        }

        // Tagged as writable; check SO_ERROR to see if the connect failed
        int result = 0;
        socklen_t size = sizeof(result);
        SASSERTWARN(!getsockopt(socket.s, SOL_SOCKET, SO_ERROR, &result, &size));
        if (result) {
            // Asynchronous connect failed; close socket
            SDEBUG("Connect to '" << socket.addr << "' failed with SO_ERROR #" << result << ", closing.");
            socket.state.store(Socket::CLOSED);
            socket.connectFailure = true;
            break;
        }

        // Asynchronous connect succeeded
        SDEBUG("Connect to '" << socket.addr << "' succeeded.");
        SASSERTWARN(SFDAnySet(fdm, socket.s, SWRITEEVTS));
        socket.state.store(Socket::CONNECTED);
        // **NOTE: Intentionally fall through to the connected state
    }

    case Socket::CONNECTED: {
        // Connected -- see if we're ready to send
        bool aliveAfterRecv = true;
        bool aliveAfterSend = true;
        if (socket.ssl) {
            // If the socket is ready to send or receive, do both: SSL has its own internal traffic, so even if we
            // only want to receive, SSL might need to send (and vice versa)
            //
            // **NOTE: SSL can receive data for a while before giving any back, so if this gets called many times
            //         in a row it might just be filling an internal buffer (and not due to some busy loop)
            if (SFDAnySet(fdm, socket.s, SREADEVTS | SWRITEEVTS)) {
                // Do both
                aliveAfterRecv = socket.recv();
                aliveAfterSend = socket.send();
            }
        } else {
            // Only send/recv if the socket is ready
            if (SFDAnySet(fdm, socket.s, SREADEVTS)) {
                aliveAfterRecv = socket.recv();
            }
            if (SFDAnySet(fdm, socket.s, SWRITEEVTS)) {
                aliveAfterSend = socket.send();
            }
        }

        // If we died, update
        if (!aliveAfterRecv || !aliveAfterSend) {
            // How did we die?
            SDEBUG("Connection to '" << socket.addr << "' died (recv=" << aliveAfterRecv << ", send="
                   << aliveAfterSend << ")");
            socket.state.store(Socket::CLOSED);
        }
        break;
    }

    case Socket::SHUTTINGDOWN:
        // Is this a SSL socket?
        if (socket.ssl) {
            // Always send/recv (see Socket::CONNECTED, above)
            // **FIXME: Add timeout.
            bool aliveAfterRecv = socket.recv();
            bool aliveAfterSend = socket.send();
            if (!aliveAfterSend || (!aliveAfterRecv && socket.sendBufferEmpty())) {

                // Did we send everything?  (Technically this the send buffer could be empty and we still haven't
                // sent everything -- SSL buffers internally, so we should check that buffer.  But odds are it sent fine.)
                if (socket.sendBufferEmpty()) {
                    SDEBUG("Graceful shutdown of SSL socket '" << socket.addr << "'");
                } else {
                    SWARN("Dirty shutdown of SSL socket '" << socket.addr << "' (" << socket.sendBufferCopy().size()
                                                           << " bytes remain)");
                }
                socket.shutdown(Socket::CLOSED);
            }
        } else {
            // Not SSL -- only send if we have something to send
            if (!socket.sendBufferEmpty()) {
                // Still have something to send -- try to send it.
                if (!socket.send()) {
                    // Done trying to send
                    SHMMM("Unable to finish sending to '" << socket.addr << "' on shutdown, clearing.");
                    socket.shutdown();
                    socket.setSendBuffer("");
                }
            }

            // Are we done sending?
            // **FIXME: Add timeout
            if (socket.sendBufferEmpty()) {
                // Wait for the other side to shut down
                if (!socket.recv()) {
                    // Done shutting down
                    SDEBUG("Graceful shutdown of socket '" << socket.addr << "'");
                    socket.shutdown(Socket::CLOSED);
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

void STCPManager::Socket::shutdown(Socket::State toState) {
    SDEBUG("Shutting down socket '" << addr << "'");
    ::shutdown(s, SHUT_RDWR);
    state.store(toState);
}

STCPManager::Socket::Socket(int sock, STCPManager::Socket::State state_, bool https)
  : s(sock), addr{}, state(state_), connectFailure(false), openTime(STimeNow()), lastSendTime(openTime),
    lastRecvTime(openTime), ssl(nullptr), data(nullptr), id(STCPManager::Socket::socketCount++), currentCommand(nullptr), https(https)
{ }

STCPManager::Socket::Socket(const string& host, bool https)
  : s(0), addr{}, state(State::CONNECTING), connectFailure(false), openTime(STimeNow()), lastSendTime(openTime),
    lastRecvTime(openTime), ssl(nullptr), data(nullptr), id(STCPManager::Socket::socketCount++), currentCommand(nullptr), https(https)
{
    SASSERT(SHostIsValid(host));
    if (https) {
        ssl = new SSSLState(host);
        s = ssl->net_ctx.fd;
    } else {
        ssl = nullptr;
        s = S_socket(host, true, false, false);
    }

    if (s < 0) {
        STHROW("Couldn't open socket to " + host);
    }
}

STCPManager::Socket::Socket(Socket&& from)
  : s(from.s),
    addr(from.addr),
    state(from.state.load()),
    connectFailure(from.connectFailure),
    openTime(from.openTime),
    lastSendTime(from.lastSendTime),
    lastRecvTime(from.lastRecvTime),
    ssl(from.ssl),
    data(from.data),
    id(from.id),
    currentCommand(from.currentCommand),
    https(from.https)
{
    from.s = -1;
    from.ssl = nullptr;
    from.data = nullptr;
    from.currentCommand = nullptr;
}

STCPManager::Socket::~Socket() {
    if (ssl) {
        delete ssl;
    } else {
        if (s >= 0) {
            ::shutdown(s, SHUT_RDWR);
            ::close(s);
        }
    }
}

bool STCPManager::Socket::send(size_t* bytesSentCount) {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    // Send data
    bool result = false;
    size_t oldSize = sendBuffer.size();
    if (ssl) {
        result = ssl->sendConsume(sendBuffer);
    } else if (s > 0) {
        result = S_sendconsume(s, sendBuffer);
    }
    size_t bytesSent = oldSize - sendBuffer.size();
    if (bytesSent) {
        lastSendTime = STimeNow();
        if (bytesSentCount) {
            *bytesSentCount = bytesSent;
        }
    }
    return result;
}

bool STCPManager::Socket::send(const string& buffer, size_t* bytesSentCount) {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    // If the socket's in a valid state for sending, append to the sendBuffer, otherwise warn
    if (state.load() < Socket::State::SHUTTINGDOWN) {
        sendBuffer += buffer;
    } else if (!sendBuffer.empty()) {
        SWARN("Not appending to sendBuffer in socket state " << state.load());
    }

    // Send anything we've got.
    return send(bytesSentCount);
}

bool STCPManager::Socket::sendBufferEmpty() {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    return sendBuffer.empty();
}

string STCPManager::Socket::sendBufferCopy() {
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    return string(sendBuffer.c_str(), sendBuffer.size());
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
        result = ssl->recvAppend(recvBuffer);
    } else if (s > 0) {
        result = S_recvappend(s, recvBuffer);
    }

    // We've received new data
    if (oldSize != recvBuffer.size()) {
        lastRecvTime = STimeNow();
    }
    return result;
}

unique_ptr<STCPManager::Port> STCPManager::openPort(const string& host, int remainingTries) {
    // Open a port on the requested host
    SASSERT(SHostIsValid(host));
    int s;
    while (remainingTries--) {
        s = S_socket(host, true, true, false);
        if (s == -1) {
            SWARN("Couldn't open port " << host << " with " << remainingTries << " retries remaining.");

            // If we have any tries left, sleep for a second. We skip the sleep after the last try.
            if (remainingTries) {
                sleep(1);
            }
        } else {
            // Socket succeeded.
            break;
        }
    }

    if (s == -1) {
        // If we don't return in the while loop, we die.
        SERROR("Failed to open port " << host << " and no more retries.");
    }

    return make_unique<Port>(s, host);
}

STCPManager::Port::Port(int _s, const string& _host) : s(_s), host(_host)
{
}

STCPManager::Port::~Port()
{
    if (s != -1) {
        ::shutdown(s, SHUT_RDWR);
        ::close(s);
    }
}

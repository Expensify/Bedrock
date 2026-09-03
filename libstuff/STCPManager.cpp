#include "STCPManager.h"

#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SSSLState.h>

#include <mbedtls/error.h>

atomic<uint64_t> STCPManager::Socket::socketCount(1);

void STCPManager::prePoll(fd_map& fdm, Socket& socket)
{
    // Make sure it's not closed
    if (socket.state.load() != Socket::CLOSED) {
        // There's no socket fd yet while we're waiting on DNS, so poll the resolution object's pipe instead.
        if (socket.state.load() == Socket::RESOLVING) {
            SFDset(fdm, socket.dnsResolution->getFD(), SREADEVTS);
            return;
        }

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
                    char errorBuffer[100] = {0};
                    mbedtls_strerror(ret, errorBuffer, sizeof(errorBuffer));
                    SWARN("SSL handshake error #" << ret << " (" << errorBuffer << ")");
                }
            }
        }
    }
}

void STCPManager::postPoll(fd_map& fdm, Socket& socket)
{
    // Update this socket
    switch (socket.state.load()) {
        case Socket::RESOLVING: {
            if (!SFDAnySet(fdm, socket.dnsResolution->getFD(), SREADEVTS)) {
                // Lookup still running, nothing to do.
                break;
            }

            // The lookup finished. Open the fd and set up SSL now that we have an address.
            //
            // Deliberately no fall-through: the fd we just opened isn't in this fd_map, and its number
            // could belong to a socket closed earlier in this same pass, whose stale revents would read
            // as a completed connection.
            socket._connectAfterDNSResolution();
            break;
        }

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

void STCPManager::Socket::shutdown(Socket::State toState)
{
    SDEBUG("Shutting down socket '" << addr << "'");

    // There's no fd to shut down and no way to flush what's buffered while we're still waiting on
    // DNS, so there's nothing a graceful shutdown could do. Close instead of leaving the socket in
    // a state that prePoll would try to register a missing fd for.
    if (state.load() == State::RESOLVING) {
        state.store(State::CLOSED);
        return;
    }

    // There may still be no fd: opening one can fail after the address is known.
    if (s > 0) {
        ::shutdown(s, SHUT_RDWR);
    }
    state.store(toState);
}

STCPManager::Socket::Socket(int sock, STCPManager::Socket::State state_, bool https)
    : s(sock), addr{}, state(state_), connectFailure(false), openTime(STimeNow()), lastSendTime(openTime),
    lastRecvTime(openTime), ssl(nullptr), data(nullptr), id(STCPManager::Socket::socketCount++), https(https)
{
}

shared_ptr<SResolution> STCPManager::Socket::_startResolution(const string& host)
{
    SASSERT(SHostIsValid(host));
    return SResolve(host);
}

STCPManager::Socket::Socket(const string& host, bool https, int resolveGraceMS)
    : s(-1), addr{}, state(State::CONNECTING), connectFailure(false), openTime(STimeNow()), lastSendTime(openTime),
    lastRecvTime(openTime), ssl(nullptr), data(nullptr), id(STCPManager::Socket::socketCount++), https(https),
    dnsResolution(_startResolution(host)), hostToResolve(host)
{
    // We give DNS a couple milliseconds to resolve. If it succeeds, we'll create a socket.
    pollfd pfd = {dnsResolution->getFD(), POLLIN, 0};
    poll(&pfd, 1, resolveGraceMS);

    // If it's not done yet, set it pending and let it get resolved in our poll() loop later.
    if (dnsResolution->getState() == SResolution::PENDING) {
        state.store(State::RESOLVING);
    } else {
        // It's finished, either way. This creates the socket, or closes it if the lookup failed.
        _connectAfterDNSResolution();
    }
}

bool STCPManager::Socket::_openSocket()
{
    s = S_socket(addr, true, false, false);
    if (s < 0) {
        state.store(State::CLOSED);
        connectFailure = true;
        return false;
    }

    if (https) {
        // SSSLState only closes the fd in its destructor, so if its constructor throws, the fd is still ours
        // to close. Reporting failure by return value rather than letting the exception out also keeps it out
        // of the poll loop, which reaches here by way of _connectAfterDNSResolution.
        try {
            ssl = new SSSLState(hostToResolve, s);
        } catch (const SException& e) {
            SWARN("Couldn't set up SSL for '" << hostToResolve << "' (" << addr << "): " << e.what());
            S_close(&s);
            state.store(State::CLOSED);
            connectFailure = true;
            return false;
        }
    }

    return true;
}

STCPManager::Socket::Socket(const sockaddr_in& addr, bool https, const string& hostname)
    : s(-1), addr(addr), state(State::CONNECTING), connectFailure(false), openTime(STimeNow()), lastSendTime(openTime),
    lastRecvTime(openTime), ssl(nullptr), data(nullptr), id(STCPManager::Socket::socketCount++), https(https),
    hostToResolve(hostname)
{
    if (!_openSocket()) {
        STHROW("Couldn't open socket to " + SToStr(addr));
    }
}

void STCPManager::Socket::_connectAfterDNSResolution()
{
    if (dnsResolution->getState() != SResolution::RESOLVED) {
        SINFO("Failed to resolve '" << hostToResolve << "', closing socket.");
        state.store(State::CLOSED);
        connectFailure = true;
        return;
    }
    addr = dnsResolution->getAddr();

    if (!_openSocket()) {
        return;
    }

    state.store(State::CONNECTING);
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
    https(from.https),
    dnsResolution(from.dnsResolution),
    hostToResolve(move(from.hostToResolve))
{
    from.s = -1;
    from.ssl = nullptr;
    from.data = nullptr;
}

STCPManager::Socket::~Socket()
{
    if (ssl) {
        delete ssl;
    } else {
        S_close(&s);
    }
}

bool STCPManager::Socket::send(size_t* bytesSentCount)
{
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    // Still waiting on DNS, so there's nowhere to send it yet. Whatever's buffered stays buffered
    // and goes out once postPoll finishes the connection.
    if (state.load() == State::RESOLVING) {
        return true;
    }

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

bool STCPManager::Socket::send(const string& buffer, size_t* bytesSentCount)
{
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

bool STCPManager::Socket::sendBufferEmpty()
{
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    return sendBuffer.empty();
}

string STCPManager::Socket::sendBufferCopy()
{
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    return string(sendBuffer.c_str(), sendBuffer.size());
}

void STCPManager::Socket::setSendBuffer(const string& buffer)
{
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);
    sendBuffer = buffer;
}

bool STCPManager::Socket::recv()
{
    lock_guard<decltype(sendRecvMutex)> lock(sendRecvMutex);

    // Still waiting on DNS, so there's nothing that could have arrived yet.
    if (state.load() == State::RESOLVING) {
        return true;
    }

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

unique_ptr<STCPManager::Port> STCPManager::openPort(const string& host)
{
    // Open a port on the requested host
    SASSERT(SHostIsValid(host));
    int s;
    s = S_socket(host, true, true, false);
    if (s == -1) {
        SHMMM("Couldn't open port " << host << " caller must retry.");
        return nullptr;
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

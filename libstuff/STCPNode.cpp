#include "libstuff.h"
#include <execinfo.h> // for backtrace
#undef SLOGPREFIX
#define SLOGPREFIX "{" << name << "} "

STCPNode::STCPNode(const string& name_, const string& host, const uint64_t recvTimeout_)
    : STCPServer(host), name(name_), recvTimeout(recvTimeout_) {
}

STCPNode::~STCPNode() {
    // Clean up all the sockets and peers
    for (Socket* socket : acceptedSocketList) {
        closeSocket(socket);
    }
    acceptedSocketList.clear();
    for (Peer* peer : peerList) {
        // Shut down the peer
        peer->closeSocket(*this);
        delete peer;
    }
    peerList.clear();
}

void STCPNode::addPeer(const string& peerName, const string& host, const STable& params) {
    // Create a new peer and ready it for connection
    SASSERT(SHostIsValid(host));
    SINFO("Adding peer #" << peerList.size() << ": " << peerName << " (" << host << "), " << SComposeJSONObject(params));
    Peer* peer = new Peer(peerName, host, params, peerList.size() + 1);

    // Wait up to 2s before trying the first time
    peer->nextReconnect = STimeNow() + SRandom::rand64() % (STIME_US_PER_S * 2);
    peerList.push_back(peer);
}

STCPNode::Peer* STCPNode::getPeerByID(uint64_t id) {
    if (id && id <= peerList.size()) {
        return peerList[id - 1];
    }
    return nullptr;
}

uint64_t STCPNode::getIDByPeer(STCPNode::Peer* peer) {
    uint64_t id = 1;
    for (auto p : peerList) {
        if (p == peer) {
            return id;
        }
        id++;
    }
    return 0;
}

void STCPNode::prePoll(fd_map& fdm) {
    // Let the base class do its thing
    return STCPServer::prePoll(fdm);
}

void STCPNode::postPoll(fd_map& fdm, uint64_t& nextActivity) {
    // Process the sockets
    STCPServer::postPoll(fdm);

    // Accept any new peers
    Socket* socket = nullptr;
    while ((socket = acceptSocket()))
        acceptedSocketList.push_back(socket);

    // Process the incoming sockets
    list<Socket*>::iterator nextSocketIt = acceptedSocketList.begin();
    while (nextSocketIt != acceptedSocketList.end()) {
        // See if we've logged in (we know we're already connected because
        // we're accepting an inbound connection)
        list<Socket*>::iterator socketIt = nextSocketIt++;
        Socket* socket = *socketIt;
        try {
            // Verify it's still alive
            if (socket->state != Socket::CONNECTED)
                STHROW("premature disconnect");

            // Still alive; try to login
            SData message;
            int messageSize = message.deserialize(socket->recvBuffer);
            if (messageSize) {
                // What is it?
                SConsumeFront(socket->recvBuffer, messageSize);
                if (SIEquals(message.methodLine, "NODE_LOGIN")) {
                    // Got it -- can we asssociate with a peer?
                    bool foundIt = false;
                    for (Peer* peer : peerList) {
                        // Just match any unconnected peer
                        // **FIXME: Authenticate and match by public key
                        if (peer->name == message["Name"]) {
                            // Found it!  Are we already connected?
                            if (!peer->hasSocket()) {
                                // Attach to this peer and LOGIN
                                PINFO("Attaching incoming socket");
                                peer->setSocket(socket);
                                peer->failedConnections = 0;
                                acceptedSocketList.erase(socketIt);
                                foundIt = true;

                                // Send our own PING back so we can estimate latency
                                _sendPING(peer);

                                // Let the child class do its connection logic
                                _onConnect(peer);
                                break;
                            } else
                                STHROW("already connected");
                        }
                    }

                    // Did we find it?
                    if (!foundIt) {
                        // This node wasn't expected
                        SWARN("Unauthenticated node '" << message["Name"] << "' attempted to connected, rejecting.");
                        STHROW("unauthenticated node");
                    }
                } else
                    STHROW("expecting NODE_LOGIN");
            }
        } catch (const SException& e) {
            // Died prematurely
            if (socket->recvBuffer.empty() && socket->sendBuffer.empty()) {
                SDEBUG("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), empty buffers");
            } else {
                SWARN("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), recv='"
                      << socket->recvBuffer << "', send='" << socket->sendBuffer << "'");
            }
            closeSocket(socket);
            acceptedSocketList.erase(socketIt);
        }
    }

    // Try to establish connections with peers and process messages
    for (Peer* peer : peerList) {
        // See if we're connected
        if (peer->hasSocket()) {
            // We have a socket; process based on its state
            switch (peer->socketState()) {
            case Socket::CONNECTED: {
                // See if there is anything new.
                peer->failedConnections = 0; // Success; reset failures
                SData message;
                int messageSize = 0;
                try {
                    // peer->s->lastRecvTime is always set, it's initialized to STimeNow() at creation.
                    if (peer->socketLastRecvTime() + recvTimeout < STimeNow()) {
                        // Reset and reconnect.
                        SWARN("Connection with peer '" << peer->name << "' timed out.");
                        STHROW("Timed Out!");
                    }

                    // Send PINGs 5s before the socket times out
                    if (STimeNow() - peer->socketLastSendTime() > recvTimeout - 5 * STIME_US_PER_S) {
                        // Let's not delay on flushing the PING PONG exchanges
                        // in case we get blocked before we get to flush later.
                        SINFO("Sending PING to peer '" << peer->name << "'");
                        _sendPING(peer);
                    }

                    // Process all messages
                    while ((messageSize = message.deserialize(peer->socketRecvBuffer()))) {
                        // Which message?
                        peer->socketRecvBufferConsumeFront(messageSize);
                        PDEBUG("Received '" << message.methodLine << "': " << message.serialize());
                        if (SIEquals(message.methodLine, "PING")) {
                            // Let's not delay on flushing the PING PONG
                            // exchanges in case we get blocked before we
                            // get to flush later.  Pass back the remote
                            // timestamp of the PING such that the remote
                            // host can calculate latency.
                            SINFO("Received PING from peer '" << peer->name << "'. Sending PONG.");
                            SData pong("PONG");
                            pong["Timestamp"] = message["Timestamp"];
                            peer->socketSend(pong.serialize());
                        } else if (SIEquals(message.methodLine, "PONG")) {
                            // Recevied the PONG; update our latency estimate for this peer.
                            // We set a lower bound on this at 1, because even though it should be pretty impossible
                            // for this to be 0 (it's in us), we rely on it being non-zero in order to connect to
                            // peers.
                            peer->latency = max(STimeNow() - message.calc64("Timestamp"), 1ul);
                            SINFO("Received PONG from peer '" << peer->name << "' (" << peer->latency << "us latency)");
                        } else {
                            // Not a PING or PONG; pass to the child class
                            _onMESSAGE(peer, message);
                        }
                    }
                } catch (const SException& e) {
                    // Error -- reconnect
                    PWARN("Error processing message '" << message.methodLine << "' (" << e.what()
                                                       << "), reconnecting:" << message.serialize());
                    SData reconnect("RECONNECT");
                    reconnect["Reason"] = e.what();
                    peer->socketSend(reconnect.serialize());
                    peer->shutdownSocket(*this);
                    break;
                }
                break;
            }

            case Socket::CLOSED: {
                // Done; clean up and try to reconnect
                uint64_t delay = SRandom::rand64() % (STIME_US_PER_S * 5);
                if (peer->socketConnectFailure()) {
                    PINFO("Peer connection failed after " << (STimeNow() - peer->socketOpenTime()) / STIME_US_PER_MS
                                                          << "ms, reconnecting in " << delay / STIME_US_PER_MS << "ms");
                } else {
                    PHMMM("Lost peer connection after " << (STimeNow() - peer->socketOpenTime()) / STIME_US_PER_MS
                                                        << "ms, reconnecting in " << delay / STIME_US_PER_MS << "ms");
                }
                _onDisconnect(peer);
                if (peer->socketConnectFailure())
                    peer->failedConnections++;
                peer->closeSocket(*this);
                peer->reset();
                peer->nextReconnect = STimeNow() + delay;
                nextActivity = min(nextActivity, peer->nextReconnect);
                break;
            }

            default:
                // Connecting or shutting down, wait
                // **FIXME: Add timeout here?
                break;
            }
        } else {
            // Not connected, is it time to try again?
            if (STimeNow() > peer->nextReconnect) {
                // Try again
                PINFO("Retrying the connection");
                peer->reset();
                peer->setSocket(openSocket(peer->host));
                if (peer->hasSocket()) {
                    // Try to log in now.  Send a PING immediately after so we
                    // can get a fast estimate of latency.
                    SData login("NODE_LOGIN");
                    login["Name"] = name;
                    peer->socketSend(login.serialize());
                    _sendPING(peer);
                    _onConnect(peer);
                } else {
                    // Failed to open -- try again later
                    SWARN("Failed to open socket '" << peer->host << "', trying again in 60s");
                    peer->failedConnections++;
                    peer->nextReconnect = STimeNow() + STIME_US_PER_M;
                }
            } else {
                // Waiting to reconnect -- notify the caller
                nextActivity = min(nextActivity, peer->nextReconnect);
            }
        }
    }
}

void STCPNode::_sendPING(Peer* peer) {
    // Send a PING message, including our current timestamp
    SASSERT(peer);
    SData ping("PING");
    ping["Timestamp"] = SToStr(STimeNow());
    peer->socketSend(ping.serialize());
}

bool STCPNode::Peer::socketSendBufferEmpty()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->sendBuffer.empty();
    } else {
        SWARN("Peer " << name << " has no socket.");
        return true;
    }
}

string STCPNode::Peer::socketSendBuffer()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->sendBuffer;
    } else {
        SWARN("Peer " << name << " has no socket.");
        return "";
    }
}

string STCPNode::Peer::socketRecvBuffer()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->recvBuffer;
    } else {
        SWARN("Peer " << name << " has no socket.");
        return "";
    }
}

bool STCPNode::Peer::hasSocket()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return true;
    } else {
        return false;
    }
}

void STCPNode::Peer::socketSend(const string& message)
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        s->send(message);
    } else {
        SWARN("Peer " << name << " has no socket.");
    }
}

void STCPNode::Peer::shutdownSocket(STCPManager& manager)
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        manager.shutdownSocket(s);
    } else {
        SWARN("Peer " << name << " has no socket.");
    }
}

void STCPNode::Peer::closeSocket(STCPManager& manager)
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        manager.closeSocket(s);
        s = nullptr;
    } else {
        SWARN("Peer " << name << " has no socket.");
    }
}

void STCPNode::Peer::setSocket(Socket* socket)
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        SWARN("Peer " << name << " already has socket, leaking.");
    }
    s = socket;
}

STCPManager::Socket::State STCPNode::Peer::socketState()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->state;
    } else {
        SWARN("Peer " << name << " has no socket.");
        return STCPManager::Socket::State::CLOSED;
    }
}

uint64_t STCPNode::Peer::socketLastRecvTime()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->lastRecvTime;
    } else {
        SWARN("Peer " << name << " has no socket.");
        return 0;
    }
}

uint64_t STCPNode::Peer::socketLastSendTime()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->lastSendTime;
    } else {
        SWARN("Peer " << name << " has no socket.");
        return 0;
    }
}

void STCPNode::Peer::socketRecvBufferConsumeFront(size_t size)
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        SConsumeFront(s->recvBuffer, size);
    } else {
        SWARN("Peer " << name << " has no socket.");
    }
}

bool STCPNode::Peer::socketConnectFailure()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->connectFailure;
    } else {
        SWARN("Peer " << name << " has no socket.");
        return false;
    }
}

uint64_t STCPNode::Peer::socketOpenTime()
{
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        return s->openTime;
    } else {
        SWARN("Peer " << name << " has no socket.");
        return 0;
    }
}

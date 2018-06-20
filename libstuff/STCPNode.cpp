#include "libstuff.h"
#include <execinfo.h> // for backtrace
#include <mbedtls/error.h>
#include <mbedtls/net.h>

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
        peer->closeSocket(this);
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
            SDEBUG("MB SOCKET LOOP " << socket->state.load() << " " << SToStr(socket->addr));
            // Verify it's still alive
            if (socket->state.load() != Socket::CONNECTED)
                STHROW("premature disconnect");


            // Still alive; try to login
            SData message;
            int messageSize = message.deserialize(socket->recvBuffer);
            if (messageSize) {
                SDEBUG("MB RECEIVED " << message.methodLine);
                // What is it?
                SConsumeFront(socket->recvBuffer, messageSize);
                if (SIEquals(message.methodLine, "NODE_LOGIN")) {
                    // Got it -- can we asssociate with a peer?
                    bool foundIt = false;
                    for (Peer* peer : peerList) {
                        SDEBUG("MB PeerList " << peer->name << " vs " << message["Name"] << " vs " << peer->host );
                        // Just match any unconnected peer
                        // **FIXME: Authenticate and match by public key
                        if (peer->name == message["Name"]) {
                            // Found it!  Are we already connected?
                            if (!peer->s) {
                                // Attach to this peer and LOGIN
                                PINFO("Attaching incoming socket");
                                peer->s = socket;
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
            SDEBUG("MB Exception Catch" << e.what());
            // Died prematurely
            if (socket->recvBuffer.empty() && socket->sendBufferEmpty()) {
                SDEBUG("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), empty buffers");
            } else {
                SWARN("Incoming connection failed from '" << socket->addr << "' (" << e.what() << "), recv='"
                      << socket->recvBuffer << "', send='" << socket->sendBufferCopy() << "'");
            }
            closeSocket(socket);
            acceptedSocketList.erase(socketIt);
        }
    }

    SDEBUG("MY PEERS");
    for (Peer* peer : peerList) {
        if(peer->s) {
            if(peer->s->ssl) {
                SDEBUG("Peer " << peer->name << " in SSL state " << SSSLGetState(peer->s->ssl));
            } else {
                SDEBUG("Peer " << peer->name << " in Non-SSL state " << peer->s->state.load());
            }
        } else {
            SDEBUG("Peer " << peer->name << " disconnected. " );
        }

    }

    // Try to establish connections with peers and process messages
    for (Peer* peer : peerList) {
        // See if we're connected
        
        if (peer->s) {
            // We have a socket; process based on its state
            SDEBUG("MB SOCKET STATE " << peer->s->state.load());
            switch (peer->s->state.load()) {
            case Socket::CONNECTED: {
                // See if there is anything new.
                peer->failedConnections = 0; // Success; reset failures
                SData message;
                int messageSize = 0;
                try {
                    // peer->s->lastRecvTime is always set, it's initialized to STimeNow() at creation.
                    if (peer->s->lastRecvTime + recvTimeout < STimeNow()) {
                        // Reset and reconnect.
                        SHMMM("Connection with peer '" << peer->name << "' timed out.");
                        STHROW("Timed Out!");
                    }

                    // Send PINGs 5s before the socket times out
                    if (STimeNow() - peer->s->lastSendTime > recvTimeout - 5 * STIME_US_PER_S) {
                        // Let's not delay on flushing the PING PONG exchanges
                        // in case we get blocked before we get to flush later.
                        SINFO("Sending PING to peer '" << peer->name << "'");
                        _sendPING(peer);
                    }

                    // Process all messages
                    while ((messageSize = message.deserialize(peer->s->recvBuffer))) {
                        // Which message?
                        SConsumeFront(peer->s->recvBuffer, messageSize);
                        PDEBUG("Received '" << message.methodLine << "'.");
                        if (SIEquals(message.methodLine, "PING")) {
                            // Let's not delay on flushing the PING PONG
                            // exchanges in case we get blocked before we
                            // get to flush later.  Pass back the remote
                            // timestamp of the PING such that the remote
                            // host can calculate latency.
                            SINFO("Received PING from peer '" << peer->name << "'. Sending PONG.");
                            SData pong("PONG");
                            pong["Timestamp"] = message["Timestamp"];
                            peer->s->send(pong.serialize());
                        } else if (SIEquals(message.methodLine, "PONG")) {
                            // Recevied the PONG; update our latency estimate for this peer.
                            // We set a lower bound on this at 1, because even though it should be pretty impossible
                            // for this to be 0 (it's in us), we rely on it being non-zero in order to connect to
                            // peers.
                            peer->latency = max(STimeNow() - message.calc64("Timestamp"), 1ul);
                            SINFO("Received PONG from peer '" << peer->name << "' (" << peer->latency/1000 << "ms latency)");
                        } else {
                            // Not a PING or PONG; pass to the child class
                            SDEBUG("RECEIVED MESSAGE " << peer->name << " : " << message.methodLine);
                            _onMESSAGE(peer, message);
                        }
                    }
                } catch (const SException& e) {
                    // Warn if the message is set. Otherwise, the error is that we got no message (we timed out), just
                    // reconnect without complaining about it.
                    if (message.methodLine.size()) {
                        PWARN("Error processing message '" << message.methodLine << "' (" << e.what()
                                                           << "), reconnecting:" << message.serialize());
                    }
                    SData reconnect("RECONNECT");
                    reconnect["Reason"] = e.what();
                    peer->s->send(reconnect.serialize());
                    shutdownSocket(peer->s);
                    break;
                }
                break;
            }

            case Socket::CLOSED: {
                // Done; clean up and try to reconnect
                uint64_t delay;
                if(peer->s->ssl) {
                    delay = SRandom::rand64() % (STIME_US_PER_S * 5) + 5000;
                } else {
                    delay = SRandom::rand64() % (STIME_US_PER_S * 5);
                }
                
                if (peer->s->connectFailure) {
                    PINFO("Peer connection failed after " << (STimeNow() - peer->s->openTime) / 1000
                                                          << "ms, reconnecting in " << delay / 1000 << "ms");
                } else {
                    PHMMM("Lost peer connection after " << (STimeNow() - peer->s->openTime) / 1000
                                                        << "ms, reconnecting in " << delay / 1000 << "ms");
                }
                _onDisconnect(peer);
                if (peer->s->connectFailure)
                    peer->failedConnections++;
                if(peer->s->ssl) {
                    SDEBUG("MB SSL Closed");
                    SSSLClose(peer->s->ssl);
                    // Cleanup
                    int ret;
                    do ret = mbedtls_ssl_close_notify( &peer->s->ssl->ssl );
                    while( ret == MBEDTLS_ERR_SSL_WANT_WRITE );
                }
                peer->closeSocket(this);
                peer->reset();
                

                peer->nextReconnect = STimeNow() + delay;
                nextActivity = min(nextActivity, peer->nextReconnect);
                break;
            }

            default:
                // Connecting or shutting down, wait
                // **FIXME: Add timeout here?
                SDEBUG("MB Socket in strange state " << peer->s->state.load()); 
                break;
            }
        } else {
            // Not connected, is it time to try again?
            if (STimeNow() > peer->nextReconnect) {
                // Try again
                PINFO("Retrying the connection");
                peer->reset();
                peer->s = openSocket(peer->host);
                if (peer->s) {
                    if(!peer->s->ssl) {
                            
                        /*********** TLS NODE CLIENT ***********/

                        // Escalate to SSL?
                        SX509* x509 = new SX509;
                        SDEBUG("MB Opening TLS Client");
                        x509 = SX509Open("", "", "", false, "");
                        peer->ssl = SSSLOpen(peer->s->s,x509,false);
                        string domain;
                        uint16_t serverport = 0;
                        if (!SParseHost(peer->host, domain, serverport)) {
                            STHROW("invalid host: " + peer->host);
                        }
                        // TODO put our local server name here.
                        // mbedtls_ssl_set_hostname( &peer->ssl->ssl, "mbed TLS Client 1" );

                        // TODO re-use our existing socket? This feels like duplicated work.

                        mbedtls_net_init( &peer->ssl->ctx );

                        mbedtls_net_connect( &peer->ssl->ctx, domain.c_str(), (char *)&serverport, MBEDTLS_NET_PROTO_TCP );
                        
                        int ret = 0;
                        int tries = 0;
                        do {
                            tries++;
                            ret = mbedtls_ssl_handshake(&peer->ssl->ssl);
                            SDEBUG("MB Client SSL Handshake " << ret << " : " << SSSLError(ret));
                            // remove me after debugging.
                            sleep(1);
                            if(tries > 100) {
                                // reset this show and let's loop around again.
                                ret = -1;
                                SDEBUG("XXXXXXXXXXXXXXXX MB Client Handshake ABORTING.");
                            }
                        } while(ret != 0);

                        //SDEBUG("MB NON LOOP HANDSHAKE " << ret);
                        //ret = mbedtls_ssl_handshake(&peer->ssl->ssl);
                        if(ret == 0) {
                            SDEBUG("XXXXXXXXXXXXXXXX MB Client Handshake Done. Connected to " << &peer->name << " at " << peer->host);
                            sleep(1); // let the server catch up?
                            //&peer->s->useSSL = true;

                            SData login("NODE_LOGIN");
                            login["Name"] = name;
                            //peer->s->send(login.serialize());
                            SDEBUG("MB Send NODE_LOGIN");
                            //static unsigned char buf[1024];
                            //sprintf( (char *) buf,  "%s", login.serialize().c_str());
                            static const char* data = login.serialize().c_str();
                            SDEBUG("MB Serialized string data " << data);
                            // SSSLSend(peer->s->ssl, data);
                            SSSLSendAll(peer->s->ssl,data);
                            //SSSLSend(peer->s->ssl, &buf);
                            SDEBUG("MB Send PING");
                            //_sendPING(peer);
                            //_onConnect(peer);
                        } else {
                            sleep(1);
                            mbedtls_ssl_session_reset( &peer->ssl->ssl );
                            peer->s->state.store(Socket::CLOSED);
                            _onDisconnect(peer);
                            if (peer->s->connectFailure)
                                peer->failedConnections++;
                            if(peer->s->ssl) {
                                SDEBUG("MB SSL Closed");
                                SSSLClose(peer->s->ssl);
                                // Cleanup
                                int ret;
                                do ret = mbedtls_ssl_close_notify( &peer->s->ssl->ssl );
                                while( ret == MBEDTLS_ERR_SSL_WANT_WRITE );
                            }
                            peer->closeSocket(this);
                            peer->reset();
                        }
                        

                        /******* END TLS ************/
                    } 
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
    peer->s->send(ping.serialize());
    //SSSLSend(peer->s->ssl,ping.serialize());
}

void STCPNode::Peer::sendMessage(const SData& message) {
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        s->send(message.serialize());
        
        
    } else {
        SWARN("Tried to send " << message.methodLine << " to peer, but not available.");
    }
}

void STCPNode::Peer::closeSocket(STCPManager* manager) {
    lock_guard<decltype(socketMutex)> lock(socketMutex);
    if (s) {
        manager->closeSocket(s);
        s = nullptr;
    } else {
        SWARN("Peer " << name << " has no socket.");
    }
}

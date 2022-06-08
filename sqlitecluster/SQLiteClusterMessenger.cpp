#include <BedrockCommand.h>
#include <sqlitecluster/SQLiteClusterMessenger.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLitePeer.h>

#include <unistd.h>
#include <fcntl.h>

SQLiteClusterMessenger::SQLiteClusterMessenger(const shared_ptr<const SQLiteNode> node)
 : _node(node)
{
}

void SQLiteClusterMessenger::setErrorResponse(BedrockCommand& command) {
    command.response.methodLine = "500 Internal Server Error";
    command.response.nameValueMap.clear();
    command.response.content.clear();
    command.complete = true;
}

void SQLiteClusterMessenger::shutdownBy(uint64_t shutdownTimestamp) {
    // If we haven't set a shutdown flag before, set one now.
    // If it was already set, we don't do anything.
    if(!_shutdownSet.test_and_set()) {
        _shutDownBy = shutdownTimestamp;
    }
}

// Returns true on ready or false on error or timeout.
SQLiteClusterMessenger::WaitForReadyResult SQLiteClusterMessenger::waitForReady(pollfd& fdspec, uint64_t timeoutTimestamp) {
    static const map <int, string> labels = {
        {POLLOUT, "send"},
        {POLLIN, "recv"},
    };
    string type = "UNKNOWN";
    try {
        type = labels.at(fdspec.events);
    } catch (const out_of_range& e) {}

    while (true) {
        int result = poll(&fdspec, 1, 100); // 100 is timeout in ms.
        if (!result) {
            // Because _shutDownBy can only change from 0 to non-zero once, there's no race condition here. If it's
            // true in the non-zero check, it will always be true after that.
            if (_shutDownBy && STimeNow() > _shutDownBy) {
                SINFO("[HTTPESC] Giving up because shutting down.");
                return WaitForReadyResult::SHUTTING_DOWN;
            } else if (timeoutTimestamp && timeoutTimestamp < STimeNow()) {
                SINFO("[HTTPESC] Timeout waiting for socket.");
                return WaitForReadyResult::TIMEOUT;
            }
        } else if (result == 1) {
            if (fdspec.revents & POLLERR || fdspec.revents & POLLHUP || fdspec.revents & POLLNVAL) {
                SINFO("[HTTPESC] Socket disconnected while waiting to be ready (" << type << ").");
                return fdspec.events == POLLIN ? WaitForReadyResult::DISCONNECTED_IN : WaitForReadyResult::DISCONNECTED_OUT;
            } else if ((fdspec.events & POLLIN && fdspec.revents & POLLIN) || (fdspec.events & POLLOUT && fdspec.revents & POLLOUT)) {
                // Expected case.
                return WaitForReadyResult::OK;
            } else {
                SWARN("[HTTPESC] Neither error nor success?? (" << type << ").");
                return WaitForReadyResult::UNSPECIFIED;
            }
        } else if (result < 0) {
            if (errno == EAGAIN || errno == EINTR) {
                // might work on a second try.
                SWARN("[HTTPESC] poll error (" << type << "): " << errno << ", retrying.");
            } else {
                // Anything else should be fatal.
                SWARN("[HTTPESC] poll error (" << type << "): " << errno);
                return WaitForReadyResult::POLL_ERROR;
            }
        } else {
            SERROR("[HTTPESC] We have more than 1 file ready????");
        }
    }
}

void SQLiteClusterMessenger::runOnAll(BedrockCommand& command) {
    //SINFO("Sending broadcast: " << message.serialize());

    //auto start = chrono::steady_clock::now();
    //bool sent = false;

    //unique_ptr<SHTTPSManager::Socket> s;
    //while (chrono::steady_clock::now() < (start + 5s) && !sent) {
        //for (auto data : _node->getPeerInfo()) {
            // do something with each peer data
            /*
             *
    STable result({
        {"name", name},
        {"host", host},
        {"state", (SQLiteNode::stateName(state) + (connected() ? "" : " (DISCONNECTED)"))},
        {"latency", to_string(latency)},
        {"nextReconnect", to_string(nextReconnect)},
        {"id", to_string(id)},
        {"loggedIn", (loggedIn ? "true" : "false")},
        {"priority", to_string(priority)},
        {"version", version},
        {"hash", hash},
        {"commitCount", to_string(commitCount)},
        {"standupResponse", responseName(standupResponse)},
        {"transactionResponse", responseName(transactionResponse)},
        {"subscribed", (subscribed ? "true" : "false")},
    });
             *
             */
        //}
    //}



    // use logic from SQLiteNode::_sendToAllPeers
    // add peer headers to the message
    // iterate over peer list
    // send over private command port, not node port

    //return false;
}

bool SQLiteClusterMessenger::runOnPeer(BedrockCommand& command, string peerName) {
    unique_ptr<SHTTPSManager::Socket> s;

    SQLitePeer* peer = _node->getPeerByName(peerName);
    if (!peer) {
        return false;
    }

    string peerCommandAddress = peer->commandAddress;
    s = _getSocketForAddress(peerCommandAddress);

    if (!s) {
        return false;
    }

    return _sendCommandOnSocket(move(s), command);
}

// TODO: writeme
bool SQLiteClusterMessenger::_sendCommandOnSocket(unique_ptr<SHTTPSManager::Socket> socket, BedrockCommand& command) {
    size_t sleepsDueToFailures = 0;
    auto start = chrono::steady_clock::now();
    bool sent = false;

    // This is what we need to send.
    SData request = command.request;
    request.nameValueMap["ID"] = command.id;
    SFastBuffer buf(request.serialize());

    // We only have one FD to poll.
    pollfd fdspec = {socket->s, POLLOUT, 0};
    while (true) {
        WaitForReadyResult result = waitForReady(fdspec, command.timeout());
        // TODO: Does this apply to general sending
        if (result == WaitForReadyResult::DISCONNECTED_OUT) {
            // This is the case we're likely to get if the leader's port is closed.
            // We break this loop and let the top loop (with the timer) start over.
            // But first we sleep 1/2 second to make this not spam a million times.
            sleepsDueToFailures++;
            usleep(500'000);
            break;
        } else if (result != WaitForReadyResult::OK) {
            return false;
        }

        ssize_t bytesSent = send(socket->s, buf.c_str(), buf.size(), 0);
        if (bytesSent == -1) {
            switch (errno) {
                case EAGAIN:
                case EINTR:
                    // these are ok. try again.
                    SINFO("[HTTPESC] Got error (send): " << errno << ", trying again.");
                    break;
                default:
                    SINFO("[HTTPESC] Got error (send): " << errno << ", fatal.");
                    return false;
            }
        } else {
            buf.consumeFront(bytesSent);
            if (buf.empty()) {
                // Everything has sent, we're done with this loop.
                sent = true;
                break;
            }
        }
    }

    if (!sent) {
        SINFO("[HTTPESC] Failed to send to leader after timeout establishing connnection.");
        return false;
    }
    if (sleepsDueToFailures) {
        auto msElapsed = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
        SINFO("[HTTPESC] Problems connecting for escalation but succeeded in " << msElapsed << "ms.");
    }

    // If we fail before here, we can try again. If we fail after here, we should return an error.

    // Ok, now we need to receive the response.
    fdspec = {socket->s, POLLIN, 0};
    string responseStr;
    char response[4096] = {0};
    while (true) {
        if (waitForReady(fdspec, command.timeout()) != WaitForReadyResult::OK) {
            setErrorResponse(command);
            return false;
        }

        ssize_t bytesRead = recv(socket->s, response, 4096, 0);
        if (bytesRead == -1) {
            switch (errno) {
                case EAGAIN:
                case EINTR:
                    // these are ok. try again.
                    SINFO("[HTTPESC] Got error (recv): " << errno << ", trying again.");
                    break;
                default:
                    SINFO("[HTTPESC] Got error (recv): " << errno << ", fatal.");
                    setErrorResponse(command);
                    return false;
            }
        } else if (bytesRead == 0) {
            SINFO("[HTTPESC] disconnected.");
            setErrorResponse(command);
            return false;
        } else {
            // Save the response.
            responseStr.append(response, bytesRead);

            // Are we done? We've only sent one command so we can only get one response.
            int size = SParseHTTP(responseStr, command.response.methodLine, command.response.nameValueMap, command.response.content);
            if (size) {
                break;
            }
        }
    }

    // If we got here, the command is complete.
    command.complete = true;

    return true;
}

unique_ptr<SHTTPSManager::Socket> SQLiteClusterMessenger::_getSocketForAddress(string address) {
    unique_ptr<SHTTPSManager::Socket> s;

    // SParseURI expects a typical http or https scheme.
    string url = "http://" + address;
    string host, path;
    if (!SParseURI(url, host, path) || !SHostIsValid(host)) {
        return nullptr;
    }

    // TODO: make this not reset the pool every time we send something to a different host
    // Get a socket from the pool.
    {
        lock_guard<mutex> lock(_socketPoolMutex);
        if (!_socketPool || _socketPool->host != host) {
            _socketPool = make_unique<SSocketPool>(host);
        }
        s = _socketPool->getSocket();
    }

    if (s == nullptr) {
        // Finish our escalation.
        SINFO("[HTTPESC] Socket failed to open.");
        return nullptr;
    }

    return s;
}

bool SQLiteClusterMessenger::runOnLeader(BedrockCommand& command) {
    auto start = chrono::steady_clock::now();
    bool sent = false;
    size_t sleepsDueToFailures = 0;
    string leaderAddress;

    unique_ptr<SHTTPSManager::Socket> s;
    while (chrono::steady_clock::now() < (start + 5s) && !sent) {
        leaderAddress = _node->leaderCommandAddress();
        if (leaderAddress.empty()) {
            // If there's no leader, it's possible we're supposed to be the leader. In this case, we can exit early.
            auto myState = _node->getState();
            if (myState == SQLiteNode::LEADING || myState == SQLiteNode::STANDINGUP) {
                SINFO("[HTTPESC] I'm the leader now! Exiting early.");
                return false;
            }

            // Otherwise, just wait until there is a leader.
            SINFO("[HTTPESC] No leader address.");
            sleepsDueToFailures++;
            usleep(500'000);
            continue;
        }

        // Start our escalation timing
        command.escalationTimeUS = STimeNow();

        s = _getSocketForAddress(leaderAddress);
        if (!s) {
            command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
            return false;
        }

        sent = _sendCommandOnSocket(move(s), command);
        if (!sent) {
            command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
            return false;
        }
    }
    if (sleepsDueToFailures) {
        auto msElapsed = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
        SINFO("[HTTPESC] Problems connecting for escalation but succeeded in " << msElapsed << "ms.");
    }

    // If we got here, the command is complete.
    command.escalated = true;

    // Finish our escalation timing.
    // FIXME: Do we need this both here and in `if (!sent)` or can we set it
    // once after _sendCommandOnSocket regardless of the outcome?
    command.escalationTimeUS = STimeNow() - command.escalationTimeUS;

    // Since everything went fine with this command, we can save its socket, unless it's being closed.
    if (!commandWillCloseSocket(command)) {
        lock_guard<mutex> lock(_socketPoolMutex);
        if (_socketPool && _socketPool->host == leaderAddress) {
            _socketPool->returnSocket(move(s));
        }
    }

    return true;
}

bool SQLiteClusterMessenger::commandWillCloseSocket(BedrockCommand& command) {
    // See if either the client or the leader specified `Connection: close`.
    // Technically, we shouldn't need to care if the client wants to close the connection, we could still re-use the connection from this server to leader, except that we've already sent
    // it a command with `Connection: close` on this socket so we should expect that it will honor that and close this socket.
    for (const auto& message : {command.request.nameValueMap, command.response.nameValueMap}) {
        auto connectionHeader = message.find("Connection");
        if (connectionHeader != message.end() && connectionHeader->second == "close") {
            return true;
        }
    }

    return false;
}

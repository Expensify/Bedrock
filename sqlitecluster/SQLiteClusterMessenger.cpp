#include <BedrockCommand.h>
#include <sqlitecluster/SQLiteClusterMessenger.h>
#include <sqlitecluster/SQLiteNode.h>
#include <sqlitecluster/SQLitePeer.h>

#include <unistd.h>
#include <fcntl.h>

SQLiteClusterMessenger::SQLiteClusterMessenger(const shared_ptr<const SQLiteNode> node)
 : _node(node), _socketPool()
{ }

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
SQLiteClusterMessenger::WaitForReadyResult SQLiteClusterMessenger::waitForReady(pollfd& fdspec, uint64_t timeoutTimestamp) const {
    static const map <int, string> labels = {
        {POLLOUT, "send"},
        {POLLIN, "recv"},
    };
    string type = "UNKNOWN";
    try {
        type = labels.at(fdspec.events);
    } catch (const out_of_range& e) {}

    // If we're trying to send, we need to also check that the socket is not disconnected by the other end.
    // We don't check this when receiving because it's possible the other end of the socket is closed but we still have
    // data to read from it, so we want to read the data first and notice the close later, which seems to work fine.
    if (fdspec.events & POLLOUT) {
        fdspec.events |= POLLRDHUP;
    }

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
            if (fdspec.revents & POLLERR || fdspec.revents & POLLHUP || fdspec.revents & POLLRDHUP || fdspec.revents & POLLNVAL) {
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

vector<SData> SQLiteClusterMessenger::runOnAll(const SData& cmd) {
    list<thread> threads;
    const list<STable> peerInfo = _node->getPeerInfo();
    vector<SData> results(peerInfo.size());
    atomic<size_t> index = 0;

    for (const auto& data : peerInfo) {
        string name = data.at("name");
        threads.emplace_back([this, &cmd, name, &results, &index](){
            BedrockCommand command(SQLiteCommand(SData(cmd)), nullptr);
            runOnPeer(command, name);
            size_t i = index.fetch_add(1);
            results[i] = command.response;
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    return results;
}

bool SQLiteClusterMessenger::runOnPeer(BedrockCommand& command, const string& peerName) {
    unique_ptr<SHTTPSManager::Socket> s;

    const SQLitePeer* peer = _node->getPeerByName(peerName);
    if (!peer) {
        setErrorResponse(command);
        return false;
    }

    s = _getSocketForAddress(peer->commandAddress);
    if (!s) {
        setErrorResponse(command);
        return false;
    }

    // _sendCommandOnSocket doesn't always call setErrorResponse - if the
    // command is intended for leader, we don't always want to set
    // command.complete = true because that prevents it from being retried. If
    // the command failed because leader was not available, but it will be
    // again soon, let the command be retried. In this case, we will let the
    // caller to runOnPeer determine how to handle the failed command.
    const bool result = _sendCommandOnSocket(*s, command);
    if (!result) {
        setErrorResponse(command);
    }

    return result;
}

bool SQLiteClusterMessenger::_sendCommandOnSocket(SHTTPSManager::Socket& socket, BedrockCommand& command) const {
    bool sent = false;

    // This is what we need to send.
    SData request = command.request;
    request.nameValueMap["ID"] = command.id;
    SFastBuffer buf(request.serialize());

    // We only have one FD to poll.
    pollfd fdspec = {socket.s, POLLOUT, 0};
    while (true) {
        WaitForReadyResult result = waitForReady(fdspec, command.timeout());
        if (result != WaitForReadyResult::OK) {
            return false;
        }

        ssize_t bytesSent = send(socket.s, buf.c_str(), buf.size(), 0);
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

    // If we fail before here, we can try again. If we fail after here, we should return an error.

    // Ok, now we need to receive the response.
    fdspec = {socket.s, POLLIN, 0};
    string responseStr;
    char response[4096] = {0};
    while (true) {
        if (waitForReady(fdspec, command.timeout()) != WaitForReadyResult::OK) {
            setErrorResponse(command);
            return false;
        }

        ssize_t bytesRead = recv(socket.s, response, 4096, 0);
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

    s = _socketPool.getSocket(host);
    if (s == nullptr) {
        SINFO("[HTTPESC] Socket failed to open.");
        return nullptr;
    }

    return s;
}

bool SQLiteClusterMessenger::runOnValidFollowerPeer(BedrockCommand& command) {
    auto start = chrono::steady_clock::now();
    bool sent = false;
    size_t sleepsDueToFailures = 0;
    string peerAddress;

    unique_ptr<SHTTPSManager::Socket> s;
    while (chrono::steady_clock::now() < (start + 5s) && !sent) {
        const string peerName = _node->getEligibleFollowerForForwarding();
        if (peerName.empty()) {
            // Otherwise, just wait until there is a peer available.
            SINFO("[HTTPPEERESC] No peers available.");
            sleepsDueToFailures++;
            usleep(500'000);
            continue;
        }
        peerAddress = _node->getPeerByName(peerName)->commandAddress.load();

        // Start our escalation timing
        command.escalationTimeUS = STimeNow();

        s = _getSocketForAddress(peerAddress);
        if (!s) {
            command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
            return false;
        }

        sent = _sendCommandOnSocket(*s, command);
        if (!sent) {
            command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
            return false;
        }
    }

    // If we fell out of the loop simply because we did not get a peer address in time, we can return false and retry later.
    if (peerAddress.empty()) {
        SINFO("[HTTPPEERESC] Could not get peer address in 5s, will retry later.");
        return false;
    }

    // If we succeeded but were delayed, log that and continue.
    if (sleepsDueToFailures) {
        auto msElapsed = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
        SINFO("[HTTPPEERESC] Problems connecting for escalation to peer but succeeded in " << msElapsed << "ms.");
    }

    // If we got here, the command is complete.
    command.escalated = true;

    // Finish our escalation timing.
    command.escalationTimeUS = STimeNow() - command.escalationTimeUS;

    // Since everything went fine with this command, we can save its socket, unless it's being closed.
    if (!commandWillCloseSocket(command)) {
        _socketPool.returnSocket(move(s), peerAddress);
    }
    return false;
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
            if (myState == SQLiteNodeState::LEADING || myState == SQLiteNodeState::STANDINGUP) {
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

        sent = _sendCommandOnSocket(*s, command);
        if (!sent) {
            command.escalationTimeUS = STimeNow() - command.escalationTimeUS;
            return false;
        }
    }

    // If we fell out of the loop simply because we did not get a leader address in time, we can return false and retry later.
    if (leaderAddress.empty()) {
        SINFO("[HTTPESC] Could not get leader address in 5s, will retry later.");
        return false;
    }

    // If we succeeded but were delayed, log that and continue.
    if (sleepsDueToFailures) {
        auto msElapsed = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
        SINFO("[HTTPESC] Problems connecting for escalation but succeeded in " << msElapsed << "ms.");
    }

    // If we got here, the command is complete.
    command.escalated = true;

    // Finish our escalation timing.
    command.escalationTimeUS = STimeNow() - command.escalationTimeUS;

    // Since everything went fine with this command, we can save its socket, unless it's being closed.
    if (!commandWillCloseSocket(command)) {
        _socketPool.returnSocket(move(s), leaderAddress);
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

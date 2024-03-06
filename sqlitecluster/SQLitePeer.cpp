#include "SQLitePeer.h"

#include <libstuff/SData.h>
#include <libstuff/SRandom.h>

#undef SLOGPREFIX
#define SLOGPREFIX "{" << name << "} "

SQLitePeer::SQLitePeer(const string& name_, const string& host_, const STable& params_, uint64_t id_)
  : commitCount(0),
    host(host_),
    id(id_),
    name(name_),
    params(params_),
    permaFollower(isPermafollower(params)),
    latency(0),
    loggedIn(false),
    nextReconnect(0),
    priority(0),
    state(SQLiteNodeState::SEARCHING),
    standupResponse(Response::NONE),
    subscribed(false),
    transactionResponse(Response::NONE),
    version(),
    hash()
{ }

SQLitePeer::~SQLitePeer() {
    delete socket;
}

bool SQLitePeer::connected() const {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    return (socket && socket->state.load() == STCPManager::Socket::CONNECTED);
}

void SQLitePeer::reset() {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    latency = 0;
    loggedIn = false;
    priority = 0;
    delete socket;
    socket = nullptr;
    state = SQLiteNodeState::SEARCHING;
    standupResponse = Response::NONE;
    subscribed = false;
    transactionResponse = Response::NONE;
    version = "";
    setCommit(0, "");
}

void SQLitePeer::shutdownSocket() {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket) {
        socket->shutdown();
    }
}

void SQLitePeer::prePoll(fd_map& fdm) const {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket) {
        STCPManager::prePoll(fdm, *socket);
        _lastRecvTime = socket->lastRecvTime;
    }
}

SQLitePeer::PeerPostPollStatus SQLitePeer::postPoll(fd_map& fdm, uint64_t& nextActivity) {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket) {
        STCPManager::postPoll(fdm, *socket);

        // We have a socket; process based on its state
        switch (socket->state.load()) {
            case STCPManager::Socket::CONNECTED: {
                if (SQLiteNode::IS_DB2_RNO && state != SQLiteNodeState::LEADING && _lastRecvTime != socket->lastRecvTime) {
                    SINFO("Updated last recv time from peer " << name);
                }
                // socket->lastRecvTime is always set, it's initialized to STimeNow() at creation.
                if (socket->lastRecvTime + SQLiteNode::RECV_TIMEOUT < STimeNow()) {
                    SHMMM("Connection with peer '" << name << "' timed out.");
                    return PeerPostPollStatus::SOCKET_ERROR;
                }

                break;
            }
            case STCPManager::Socket::CLOSED: {
                // Done; clean up and try to reconnect
                uint64_t delay = SRandom::rand64() % (STIME_US_PER_S * 5);
                if (socket->connectFailure) {
                    SINFO("SQLitePeer connection failed after " << (STimeNow() - socket->openTime) / 1000 << "ms, reconnecting in " << delay / 1000 << "ms");
                } else {
                    SHMMM("Lost peer connection after " << (STimeNow() - socket->openTime) / 1000 << "ms, reconnecting in " << delay / 1000 << "ms");
                }
                reset();
                nextReconnect = STimeNow() + delay;
                nextActivity = min(nextActivity, nextReconnect.load());
                return PeerPostPollStatus::SOCKET_CLOSED;
                break;
            }
            default:
                // Connecting or shutting down, wait
                // **FIXME: Add timeout here?
                SINFO("Peer connection to " << name << " in state " << socket->state.load() << ", waiting for it to stabilize.");
                break;
        }
    } else {
        // Not connected, is it time to try again?
        if (STimeNow() > nextReconnect) {
            // Try again
            SINFO("Retrying the connection");
            reset();
            try {
                socket = new STCPManager::Socket(host);
                return PeerPostPollStatus::JUST_CONNECTED;
            } catch (const SException& exception) {
                // Failed to open -- try again later
                SWARN(exception.what());
                nextReconnect = STimeNow() + STIME_US_PER_M;
            }
        } else {
            // Waiting to reconnect -- notify the caller
            nextActivity = min(nextActivity, nextReconnect.load());
        }
    }
    return PeerPostPollStatus::OK;
}

uint64_t SQLitePeer::lastRecvTime() const {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket) {
        return socket->lastRecvTime;
    }
    return 0;
}

uint64_t SQLitePeer::lastSendTime() const {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket) {
        return socket->lastSendTime;
    }
    return 0;
}

SData SQLitePeer::popMessage() {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket) {
        SData message;
        size_t size = message.deserialize(socket->recvBuffer);
        if (size) {
            socket->recvBuffer.consumeFront(size);
            return message;
        }
    }
    throw out_of_range("no messages");
}

bool SQLitePeer::setSocket(STCPManager::Socket* newSocket, bool onlyIfNull) {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket && onlyIfNull) {
        return false;
    }
    if (socket) {
        SWARN("Overwriting existing peer socket. Is it leaking?");
    }
    socket = newSocket;
    return true;
}

string SQLitePeer::responseName(Response response) {
    switch (response) {
        case Response::NONE:
            return "NONE";
            break;
        case Response::APPROVE:
            return "APPROVE";
            break;
        case Response::DENY:
            return "DENY";
            break;
        case Response::ABSTAIN:
            return "ABSTAIN";
            break;
        default:
            return "";
    }
}

void SQLitePeer::setCommit(uint64_t count, const string& hashString) {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    const_cast<atomic<uint64_t>&>(commitCount) = count;
    hash = hashString;
}

void SQLitePeer::getCommit(uint64_t& count, string& hashString) const {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    count = commitCount.load();
    hashString = hash.load();
}

STable SQLitePeer::getData() const {
    // Add all of our standard stuff.
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

    // And anything from the params (note: doesn't overwrite our standard stuff).
    for (auto& p : params) {
        result.emplace(p);
    }

    result["commandAddress"] = commandAddress;

    return result;
}

bool SQLitePeer::isPermafollower(const STable& params) {
    auto it = params.find("Permafollower");
    if (it != params.end() && it->second == "true") {
        return true;
    }
    return false;
}

void SQLitePeer::sendMessage(const SData& message) {
    lock_guard<decltype(peerMutex)> lock(peerMutex);
    if (socket) {
        uint64_t lastSendTime = socket->lastSendTime;
        size_t bytesSent = 0;
        if (socket->send(message.serialize(), &bytesSent)) {
            SINFO("No error sending " << message.methodLine << " to peer " << name << " (" << bytesSent << " bytes actually sent).");
        } else {
            SHMMM("Error sending " << message.methodLine << " to peer " << name << ".");
        }
        if (SQLiteNode::IS_DB2_RNO && state != SQLiteNodeState::LEADING && lastSendTime != socket->lastSendTime) {
            SINFO("Updated last send time to peer " << name);
        }
    } else {
        SINFO("Tried to send " << message.methodLine << " to peer " << name << ", but not available.");
    }
}

ostream& operator<<(ostream& os, const atomic<SQLitePeer::Response>& response)
{
    os << SQLitePeer::responseName(response.load());
    return os;
}

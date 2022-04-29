#include "SQLitePeer.h"

#include <libstuff/SData.h>

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
    state(SQLiteNode::SEARCHING),
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
    state = SQLiteNode::SEARCHING;
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
        socket = nullptr;
    }
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
        socket->send(message.serialize());
    } else {
        SWARN("Tried to send " << message.methodLine << " to peer, but not available.");
    }
}

ostream& operator<<(ostream& os, const atomic<SQLitePeer::Response>& response)
{
    os << SQLitePeer::responseName(response.load());
    return os;
}

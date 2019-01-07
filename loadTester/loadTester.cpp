#include "loadTester.h"

SimpleHTTPSManager::SimpleHTTPSManager() {}
SimpleHTTPSManager::~SimpleHTTPSManager() {}

bool SimpleHTTPSManager::_onRecv(Transaction* transaction) {
    string methodLine = transaction->fullResponse.methodLine;
    transaction->response = 0;
    // Just need to parse bedrock style method lines
    if (!methodLine.empty()) {
        transaction->response = stoi(SBefore(methodLine, " "));
    }
    if (!transaction->response) {
        transaction->response = 400;
        cout << "[WARN] Failed to parse method line from request: " << methodLine << endl;
    }

    return false;
}

SimpleHTTPSManager::Transaction* SimpleHTTPSManager::send(const string& url, const SData& request) {
    // Open a non https socket, bedrock doesn't use https
    // Socket* s = openSocket(url, nullptr);
    // if (!s) {
    //     cout << "[ALRT] Whoa failed to open a socket to " << url << endl;
    //     return _createErrorTransaction();
    // }

    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    // If this is going to be an https transaction, create a certificate and give it to the socket.
    SX509* x509 = SStartsWith(url, "https://") ? SX509Open(_pem, _srvCrt, _caCrt) : nullptr;
    Socket* s = openSocket(host, x509);
    if (!s) {
        return _createErrorTransaction();
    }

    // Wrap in a transaction
    Transaction* transaction = new Transaction(*this);
    transaction->s = s;
    transaction->fullRequest = request;

    // Ship it.
    transaction->s->send(request.serialize());

    // Keep track of the transaction.
    SAUTOLOCK(_listMutex);
    _activeTransactionList.push_front(transaction);
    return transaction;
}

void _poll(SimpleHTTPSManager& httpsManager, SHTTPSManager::Transaction* request)
{
    while (!request->response) {
        // Our fdm holds a list of all sockets we could need to read or write to
        fd_map fdm;
        const uint64_t& nextActivity = STimeNow();
        _prePoll(fdm, httpsManager);
        S_poll(fdm, 1'000);
        _postPoll(fdm, nextActivity, httpsManager);
    }
}

void _postPoll(fd_map& fdm, uint64_t nextActivity, SimpleHTTPSManager& httpsManager)
{
    list<SHTTPSManager::Transaction*> completedHTTPSRequests;
    httpsManager.postPoll(fdm, nextActivity, completedHTTPSRequests);
}

void _prePoll(fd_map& fdm, SimpleHTTPSManager& httpsManager)
{
    httpsManager.prePoll(fdm);
}

void _sendQueryRequest(string host, SimpleHTTPSManager& httpsManager, string& identifier) {
    SData request("GET ?SQF_EMAIL=cole+test+" + identifier + "@expensify.com HTTP/1.1");
    // request["query"] = "SELECT 1;";
    request["Host"] = "exops.io";
    SHTTPSManager::Transaction* transaction = httpsManager.send(host, request);
    _poll(httpsManager, transaction);
    SINFO("Received " << transaction->response);

    // Close and free the transaction.
    httpsManager.closeTransaction(transaction);
}

int main(int argc, char *argv[]) {
    // Init and set log level so we can get system logging from libraries
    SInitialize("main");
    SLogLevel(LOG_WARNING);

    // Parse our command line for easy adding of options
    SData args = SParseCommandLine(argc, argv);

    // Init arg values
    uint64_t threads = 1;
    uint64_t queryCount = 1;
    bool noop = false;
    bool verbose = false;

    // Change our default values if their CLI counterpart is set
    if (args.isSet("-threads")) {
        threads = stoi(args["-threads"]);
    }
    if (args.isSet("-queryCount")) {
        queryCount = stoi(args["-queryCount"]);
    }
    if (args.test("-v")) {
        verbose = true;
        SLogLevel(0);
    }

    list<thread> threadList;
    uint64_t startTime = STimeNow();
    for (size_t i = 0; i < threads; i++) {
        threadList.emplace_back([&, i]() {
            SInitialize("thread" + to_string(i));
            // Create our https manager, it's name is a misnomer because it can't
            // actually send https anything, it only sends to hosts that don't actually
            // start with http or https
            SimpleHTTPSManager httpsManager;

            for (size_t z = 0; z < queryCount; z++) {
                string identifier = to_string(i) + "+" + to_string(z);
                _sendQueryRequest("https://exops.io/mobile-signup", httpsManager, identifier);
                if (verbose) {
                    cout << "[INFO] Sent query!" << endl;
                }
            }
        });
    }

    int threadId = 0;
    for (auto& thread : threadList) {
        // cout << "[INFO] Joining thread " << threadId << endl;;
        threadId++;
        thread.join();
    }
    cout << "Run time: " << (STimeNow() - startTime) / 1'000 << "ms" << endl;
    return 0;
}

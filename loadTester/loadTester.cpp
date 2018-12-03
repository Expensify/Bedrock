#include "loadTester.h"

TestHTTPSMananager::TestHTTPSMananager() {}
TestHTTPSMananager::~TestHTTPSMananager() {}

bool TestHTTPSMananager::_onRecv(Transaction* transaction) {
    string methodLine = transaction->fullResponse.methodLine;
    transaction->response = 0;
    size_t offset = methodLine.find_first_of(' ', 0);
    offset = methodLine.find_first_not_of(' ', offset);
    if (offset != string::npos) {
        int status = SToInt(methodLine.substr(offset));
        if (status) {
            transaction->response = status;
        }
    }
    if (!transaction->response) {
        transaction->response = 400;
        cout << "[WARN] Failed to parse method line from request: " << methodLine << endl;
    }

    return false;
}

TestHTTPSMananager::Transaction* TestHTTPSMananager::send(const string& url, const SData& request) {
    // Open a non https socket, bedrock doesn't use https
    Socket* s = openSocket(url, nullptr);
    if (!s) {
        cout << "[ALRT] Whoa failed to open a socket to " << url << endl;
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

void _poll(TestHTTPSMananager& httpsManager, SHTTPSManager::Transaction* request)
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

void _postPoll(fd_map& fdm, uint64_t nextActivity, TestHTTPSMananager& httpsManager)
{
    list<SHTTPSManager::Transaction*> completedHTTPSRequests;
    httpsManager.postPoll(fdm, nextActivity, completedHTTPSRequests);
}

void _prePoll(fd_map& fdm, TestHTTPSMananager& httpsManager)
{
    httpsManager.prePoll(fdm);
}

void _sendQueryRequest(string host) {
    TestHTTPSMananager httpsManager;
    SData request("Query: SELECT 1;");
    SHTTPSManager::Transaction* transaction = httpsManager.send(host, request);
    _poll(httpsManager, transaction);

    int httpsResponseCode = transaction->response;
    cout << "[INFO] Received " << httpsResponseCode << endl;

    // Close and free the transaction.
    httpsManager.closeTransaction(transaction);
}

int main(int argc, char *argv[]) {
    SData args = SParseCommandLine(argc, argv);

    _sendQueryRequest("bedrock1:8888");
    cout << "[INFO] Sent query!" << endl;

    return 0;
}

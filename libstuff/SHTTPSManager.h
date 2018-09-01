#pragma once

class SHTTPSManager : public STCPManager {
  public:
    struct Transaction {
        // Constructor/Destructor
        Transaction(SHTTPSManager& owner_);
        ~Transaction();

        // Attributes
        STCPManager::Socket* s;
        uint64_t created;
        uint64_t finished;
        uint64_t sentTime;
        SData fullRequest;
        SData fullResponse;
        int response;
        STable values;
        SHTTPSManager& owner;
        bool isDelayedSend;
    };

    // Constructor/Destructor
    SHTTPSManager();
    SHTTPSManager(const string& pem, const string& srvCrt, const string& caCrt);
    virtual ~SHTTPSManager();

    // STCPServer API. Except for postPoll, these are just threadsafe wrappers around base class functions.
    void prePoll(fd_map& fdm);
    void postPoll(fd_map& fdm, uint64_t& nextActivity);

    // Default timeout for HTTPS requests is 5 minutes.This can be changed on any call to postPoll.
    void postPoll(fd_map& fdm, uint64_t& nextActivity, list<Transaction*>& completedRequests, uint64_t timeoutMS = (5 * 60 * 1000));
    Socket* openSocket(const string& host, SX509* x509 = nullptr);
    void closeSocket(Socket* socket);

    // Close a transaction and remove it from our internal lists.
    void closeTransaction(Transaction* transaction);

    static int getHTTPResponseCode(const string& methodLine);

  protected: // Child API

    // Used to create the signing certificate.
    const string _pem;
    const string _srvCrt;
    const string _caCrt;

    // Methods
    Transaction* _httpsSend(const string& url, const SData& request);
    Transaction* _createErrorTransaction();
    virtual bool _onRecv(Transaction* transaction);

    list<Transaction*> _activeTransactionList;
    list<Transaction*> _completedTransactionList;

    // SHTTPSManager operations are thread-safe, we lock around any accesses to our transaction lists, so that
    // multiple threads can add/remove from them.
    recursive_mutex _listMutex;
};

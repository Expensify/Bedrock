#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLiteNode.h>
class BedrockPlugin;

class SStandaloneHTTPSManager : public STCPManager {
  public:
    struct Transaction {
        // Constructor/Destructor
        Transaction(SStandaloneHTTPSManager& manager_);
        ~Transaction();

        // Attributes
        STCPManager::Socket* s;
        uint64_t created;
        uint64_t finished;
        SData fullRequest;
        SData fullResponse;
        int response;
        STable values;
        SStandaloneHTTPSManager& manager;
        bool isDelayedSend;
        uint64_t sentTime;
    };

    // Constructor/Destructor
    SStandaloneHTTPSManager();
    SStandaloneHTTPSManager(const string& pem, const string& srvCrt, const string& caCrt);
    virtual ~SStandaloneHTTPSManager();

    // STCPServer API. Except for postPoll, these are just threadsafe wrappers around base class functions.
    void prePoll(fd_map& fdm);
    void postPoll(fd_map& fdm, uint64_t& nextActivity);
    void postPoll(fd_map& fdm, uint64_t& nextActivity, list<Transaction*>& completedRequests);


    // Default timeout for HTTPS requests is 5 minutes.This can be changed on any call to postPoll.
    void postPoll(fd_map& fdm, uint64_t& nextActivity, list<Transaction*>& completedRequests, map<Transaction*, uint64_t>& transactionTimeouts, uint64_t timeoutMS = (5 * 60 * 1000));
    Socket* openSocket(const string& host, SX509* x509 = nullptr);
    void closeSocket(Socket* socket);

    // Close a transaction and remove it from our internal lists.
    void closeTransaction(Transaction* transaction);

    static int getHTTPResponseCode(const string& methodLine);

    virtual void validate() {
        // The constructor for a transaction needs to call this on it's manager. It can then throw in cases where this
        // manager should not be allowed to create transactions. This lets us have different validation behavior for
        // different managers without needing to subclass Transaction.
        // The default implementation does nothing.
    }

  protected: // Child API

    // Used to create the signing certificate.
    const string _pem;
    const string _srvCrt;
    const string _caCrt;

    // Methods
    Transaction* _httpsSend(const string& url, const SData& request);
    Transaction* _createErrorTransaction();
    virtual bool _onRecv(Transaction* transaction);

    list<Transaction*> _httpsSendMultiple(const string& url, vector<SData>& sendRequests);

    list<Transaction*> _activeTransactionList;
    list<Transaction*> _completedTransactionList;
    list<STCPManager::Socket*> _closedSockets;

    // SStandaloneHTTPSManager operations are thread-safe, we lock around any accesses to our transaction lists, so that
    // multiple threads can add/remove from them.
    recursive_mutex _listMutex;
};

class SHTTPSManager : public SStandaloneHTTPSManager {
    public:
    SHTTPSManager(BedrockPlugin& plugin_);
    SHTTPSManager(BedrockPlugin& plugin_, const string& pem, const string& srvCrt, const string& caCrt);

    class NotLeading : public exception {
        const char* what() {
            return "Can't create SHTTPSManager::Transaction when not leading";
        }
    };

    void validate();

    protected:
    // Reference to the plugin that owns this object.
    BedrockPlugin& plugin;
};

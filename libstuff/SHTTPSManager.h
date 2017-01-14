#pragma once

class SHTTPSManager : public STCPManager {
  public: // External API

    class Notifiable {
    public:
        virtual ~Notifiable() { }
        virtual void notifyActivity() = 0;
    };

    // Transaction
    struct Transaction {
        // Constructor/Destructor
        Transaction(SHTTPSManager& owner_)
          : s(nullptr), created(STimeNow()), finished(0), response(0), owner(owner_) {
        }
        ~Transaction() { SASSERT(!s); }

        // Attributes
        STCPManager::Socket* s;
        uint64_t created;
        uint64_t finished;
        SData fullRequest;
        SData fullResponse;
        int response;
        STable values;
        SHTTPSManager& owner;
    };

    // Constructor/Destructor
    SHTTPSManager();
    SHTTPSManager(const string& pem, const string& srvCrt, const string& caCrt);
    virtual ~SHTTPSManager();

    // Methods
    void closeTransaction(Transaction* transaction);

  public: // STCPServer API
    void postSelect(fd_map& fdm, uint64_t& nextActivity);
    int preSelect(fd_map& fdm);

    SHTTPSManager::Notifiable* notifyTarget = 0;

  protected: // Child API
    // Methods
    Transaction* _httpsSend(const string& url, const SData& request);
    Transaction* _createErrorTransaction();
    virtual bool _onRecv(Transaction* transaction) = 0;

  private: // Internal API
    // Attriubutes
    list<Transaction*> _activeTransactionList;
    list<Transaction*> _completedTransactionList;
    SX509* _x509;

    // We use this to kick `poll` to wake up when we send a request.
    int _pipeFD[2] = {-1, -1};
    bool pollKicked = false;

    // Multiple write threads can call these, so we syncronize access to them.
    recursive_mutex _transactionListMutex;
};

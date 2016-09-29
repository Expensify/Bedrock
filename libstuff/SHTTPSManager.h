#pragma once

class SHTTPSManager : public STCPManager {
public: // External API
  // Transaction
  struct Transaction {
    // Constructor/Destructor
    Transaction() {
      s = 0;
      created = STimeNow();
      finished = 0;
      response = 0;
    }
    ~Transaction() { SASSERT(!s); }

    // Attributes
    STCPManager::Socket *s;
    uint64_t created;
    uint64_t finished;
    SData fullRequest;
    SData fullResponse;
    int response;
    STable values;
    SHTTPSManager *
        owner; // pointer to the object that owns this transaction. Can be null.
    vector<vector<string>> transactionList; // **FIXME: move this into values.
  };

  // Constructor/Destructor
  SHTTPSManager();
  SHTTPSManager(const string &pem, const string &srvCrt, const string &caCrt);
  virtual ~SHTTPSManager();

  // Methods
  void closeTransaction(Transaction *transaction);

public: // STCPServer API
  void postSelect(fd_map &fdm, uint64_t &nextActivity);

protected: // Child API
  // Methods
  Transaction *_httpsSend(const string &url, const SData &request);
  Transaction *_createErrorTransaction();
  virtual bool _onRecv(Transaction *transaction) = 0;

private: // Internal API
  // Attriubutes
  list<Transaction *> _activeTransactionList;
  list<Transaction *> _completedTransactionList;
  SX509 *_x509;
};

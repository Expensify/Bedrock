#pragma once

#include <libstuff/SData.h>
#include <libstuff/STCPManager.h>

class BedrockPlugin;

class SStandaloneHTTPSManager : public STCPManager {
  public:
    struct Transaction {
        // Constructor/Destructor
        Transaction(SStandaloneHTTPSManager& manager_, const string& requestID = "");
        ~Transaction();

        // Attributes
        STCPManager::Socket* s;
        uint64_t created;
        uint64_t finished;
        uint64_t timeoutAt;
        SData fullRequest;
        SData fullResponse;
        int response;
        SStandaloneHTTPSManager& manager;
        uint64_t sentTime;
        const string requestID;
    };

    // Constructor/Destructor
    SStandaloneHTTPSManager();
    SStandaloneHTTPSManager(const string& pem, const string& srvCrt, const string& caCrt);
    virtual ~SStandaloneHTTPSManager();

    void prePoll(fd_map& fdm, Transaction& transaction);

    // Default timeout for HTTPS requests is 5 minutes.This can be changed on any call to postPoll.
    // This is a total amount of milliseconds of idle activity since the last send on a socket before killing it.
    // The purpose of this is to be able to shut down when no activity is happening.
    void postPoll(fd_map& fdm, Transaction& transaction, uint64_t& nextActivity, uint64_t timeoutMS = (5 * 60 * 1000));

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
};

class SHTTPSManager : public SStandaloneHTTPSManager {
    public:
    SHTTPSManager(BedrockPlugin& plugin_);
    SHTTPSManager(BedrockPlugin& plugin_, const string& pem, const string& srvCrt, const string& caCrt);

    // TODO: Remove this once Auth no longer checks for it.
    class NotLeading : public exception {
        using exception::what;
        const char* what() {
            return "Can't create SHTTPSManager::Transaction when not leading";
        }
    };

    protected:
    // Reference to the plugin that owns this object.
    BedrockPlugin& plugin;
};

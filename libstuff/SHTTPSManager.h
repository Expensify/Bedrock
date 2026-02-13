#pragma once

#include <libstuff/SData.h>
#include <libstuff/STCPManager.h>

class BedrockPlugin;

class SStandaloneHTTPSManager : public STCPManager {
public:
    struct Transaction
    {
        // Constructor/Destructor
        Transaction(SStandaloneHTTPSManager& manager_, const string& requestID = "");
        virtual ~Transaction();

        // Attributes
        STCPManager::Socket* s;
        uint64_t created;
        uint64_t finished;
        uint64_t timeoutAt;
        SData fullRequest;
        SData fullResponse;
        int response;
        SStandaloneHTTPSManager& manager;
        const string requestID;

        // Allow a transaction to be scheduled to start in the future.
        // If it is scheduled to start in the future, we will call `startFunc` at the timestamp scheduled.
        uint64_t scheduledStart = 0;
        function <void(Transaction*)> startFunc;
    };

    static const string proxyAddressHTTPS;

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

    // Allows the manager to clean up any references it has to this transaction. Note that this shoud *not* actually delete the
    // passed pointer.
    virtual void remove(Transaction* t)
    {
    }

protected:   // Child API

    // Used to create the signing certificate.
    const string _pem;
    const string _srvCrt;
    const string _caCrt;

    // Methods
    Transaction* _httpsSend(const string& url, const SData& request, bool allowProxy = false);
    Transaction* _createErrorTransaction();
    virtual bool _onRecv(Transaction* transaction);

    static string initProxyAddressHTTPS();
};

class SHTTPSManager : public SStandaloneHTTPSManager {
public:
    SHTTPSManager(BedrockPlugin& plugin_);
    SHTTPSManager(BedrockPlugin& plugin_, const string& pem, const string& srvCrt, const string& caCrt);

protected:
    // Reference to the plugin that owns this object.
    BedrockPlugin& plugin;
};

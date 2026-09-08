/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SHTTPSManager.h
 * Path:    libstuff/SHTTPSManager.h
 * Pair:    SHTTPSManager.cpp
 *
 * INTENT
 *   Defines the non-blocking base classes used to make outbound HTTPS (or
 *   plain HTTP) requests driven from a poll() loop, tracking each request
 *   as a Transaction with its own socket, timing, and response.
 *
 * OBJECTS
 *   SStandaloneHTTPSManager            - owns STCPManager sockets and drives
 *                                         Transactions through prePoll/postPoll;
 *                                         usable with no Bedrock plugin context.
 *   SStandaloneHTTPSManager::Transaction - one outbound request/response: socket,
 *                                         full request/response SData, timestamps,
 *                                         response code, optional scheduled start.
 *   SHTTPSManager                      - SStandaloneHTTPSManager bound to an
 *                                         owning BedrockPlugin.
 *
 * OUT OF PLACE
 *   [CANDIDATE] SHTTPSManager - exists only to attach a BedrockPlugin& to the
 *     manager, so this header forward-declares BedrockPlugin and the pairing
 *     .cpp depends on BedrockPlugin.h/BedrockServer.h. That makes a libstuff
 *     unit depend on the Bedrock application layer rather than the reverse.
 *
 * NAME/LOCATION FIT
 *   SStandaloneHTTPSManager fits libstuff (peer of STCPManager). SHTTPSManager,
 *   being plugin-bound, sits less naturally here than beside BedrockPlugin.
 *
 * NAMING QUALITY
 *   Consistent overall. SStandaloneHTTPSManager's protected members use the
 *   repo's `_` prefix (_pem, _srvCrt, _caCrt) but SHTTPSManager's protected
 *   `plugin` member does not - a small local inconsistency.
 * ─────────────────────────────────────────────────────────────────────*/

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
        function<void(Transaction&)> startFunc;
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

    static int getHTTPResponseCode(const string& methodLine, const int defaultStatusCode = 400);

protected:   // Child API

    // Used to create the signing certificate.
    const string _pem;
    const string _srvCrt;
    const string _caCrt;

    // Methods
    unique_ptr<Transaction> _httpsSend(const string& url, const SData& request, bool allowProxy = false);
    unique_ptr<Transaction> _createErrorTransaction();
    virtual bool _onRecv(Transaction& transaction);

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

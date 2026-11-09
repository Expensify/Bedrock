#pragma once

#include <libstuff/SData.h>
#include <libstuff/SHTTPResponseParser.h>
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

        // Set before polling to expose decoded body bytes while the HTTP response is still pending.
        bool shouldStreamResponse = false;

        SStandaloneHTTPSManager& manager;
        const string requestID;

        // Where the request is going (host:port), and the proxy it goes through, if any. Used to describe failures in logs.
        string targetHost;
        string proxyHost;

        // What's needed to open a fresh connection for this request, and how many times it may be resent on one.
        // Only `_httpsSend` sets `maxReconnectAttempts`; other managers build their own sockets, so a failed
        // connection for them is final.
        bool isHttps = false;
        shared_ptr<const STCPManager::MTLSConnection> mtlsConnection;
        int maxReconnectAttempts = 0;

        // How many times the request has been resent on a new connection after failing before it reached the target
        // host, and when the next attempt is due (0 if none is pending).
        int reconnectAttempts = 0;
        uint64_t reconnectAt = 0;

        // Allow a transaction to be scheduled to start in the future.
        // If it is scheduled to start in the future, we will call `startFunc` at the timestamp scheduled.
        uint64_t scheduledStart = 0;
        function<void(Transaction&)> startFunc;

private:
        friend class SStandaloneHTTPSManager;
        SHTTPResponseParser _responseParser;
        SData _bufferedResponse;
    };

    static const string proxyAddressHTTPS;

    // By default, a request sent with `_httpsSend` that fails before reaching its target host (DNS, connect, or the
    // proxy refusing the tunnel) is retried on a new connection up to this many times, waiting
    // RECONNECT_BASE_DELAY_US, then twice that, and so on. That spans a few seconds of lost connectivity, such as a
    // firewall failover.
    static constexpr int MAX_RECONNECT_ATTEMPTS = 4;
    static constexpr uint64_t RECONNECT_BASE_DELAY_US = 500'000;

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
    // `maxReconnectAttempts` limits how many times a request that never reached its target host is resent on a new
    // connection. Pass 0 to fail on the first connection failure.
    unique_ptr<Transaction> _httpsSend(const string& url, const SData& request, bool allowProxy = false, int maxReconnectAttempts = MAX_RECONNECT_ATTEMPTS);

    // A non-null connection requires an HTTPS URL with a matching host:port.
    unique_ptr<Transaction> _httpsSend(const string& url, const SData& request, shared_ptr<const MTLSConnection> connection, bool allowProxy = false, int maxReconnectAttempts = MAX_RECONNECT_ATTEMPTS);

    unique_ptr<Transaction> _createErrorTransaction();

    // Names the step a failed transaction got to (resolving, connecting, opening the proxy tunnel, sending, or waiting
    // for a response), so a failure log shows whether the request ever reached the target host.
    static string _describeFailureStage(const Transaction& transaction, Socket::State state, bool requestSent);

    // Says whether a failed transaction's request reached the target host: "sent", "not fully sent", or "not sent".
    static string _describeRequestProgress(const Transaction& transaction);

    // Opens a socket to the transaction's target host, through its proxy if it has one. Returns null if the socket
    // can't be created.
    static Socket* _openSocket(Transaction& transaction);

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

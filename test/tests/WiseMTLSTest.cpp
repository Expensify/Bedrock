#include <cstdlib>
#include <iostream>
#include <sstream>

#include <libstuff/JSON/Value.h>
#include <libstuff/SData.h>
#include <libstuff/SHTTPSManager.h>
#include <libstuff/SHTTPSProxySocket.h>
#include <libstuff/STCPManager.h>
#include <test/lib/tpunit++.hpp>

// From test/: WISE_SANDBOX_CREDENTIALS_FILE=../credentials.txt ./test -only WiseMTLS
// WISE_SANDBOX_ACCESS_TOKEN optionally supplies OAuth authentication for the playground request.
// WISE_SANDBOX_PROXY optionally specifies a CONNECT proxy as host:port, for example 127.0.0.1:3128.
struct WiseMTLSTest : tpunit::TestFixture
{
    WiseMTLSTest() : tpunit::TestFixture("WiseMTLS", TEST(WiseMTLSTest::request))
    {
    }

    void request()
    {
        const char* credentialsPath = getenv("WISE_SANDBOX_CREDENTIALS_FILE");
        if (!credentialsPath || !*credentialsPath) {
            cout << "Skipping Wise sandbox request: set WISE_SANDBOX_CREDENTIALS_FILE to enable it." << endl;
            return;
        }

        string fileContents;
        ASSERT_TRUE(SFileLoad(credentialsPath, fileContents));

        // Parse the JSON directly so its PEM newline escapes are decoded only once.
        const string prefix = "wiseSandboxClientCredentials:";
        istringstream lines(fileContents);
        string line;
        string credentialsJSON;
        while (getline(lines, line)) {
            if (SStartsWith(line, prefix)) {
                credentialsJSON = line.substr(prefix.size());
                break;
            }
        }
        ASSERT_FALSE(credentialsJSON.empty());

        const JSON::Value credentials = JSON::Value::parse(credentialsJSON);
        const auto connection = make_shared<const STCPManager::MTLSConnection>(STCPManager::MTLSConnection{
            "api-mtls.wise-sandbox.com:443",
            credentials["certificate"].getString(),
            credentials["privateKey"].getString(),
            true,
        });

        SStandaloneHTTPSManager manager;
        SData request("GET /v1/authenticated/playground HTTP/1.1");
        request["Host"] = "api-mtls.wise-sandbox.com";
        request["Connection"] = "close";
        const char* accessToken = getenv("WISE_SANDBOX_ACCESS_TOKEN");
        if (accessToken && *accessToken) {
            request["Authorization"] = "Bearer " + string(accessToken);
        }

        SData printableRequest = request;
        if (printableRequest.isSet("Authorization")) {
            printableRequest["Authorization"] = "Bearer <redacted>";
        }
        cout << "Wise sandbox request:\n" << printableRequest.serialize() << endl;

        auto transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager, "WiseMTLS");
        const char* proxy = getenv("WISE_SANDBOX_PROXY");
        if (proxy && *proxy) {
            cout << "Wise sandbox proxy: " << proxy << endl;
            transaction->s = new SHTTPSProxySocket(proxy, connection, transaction->requestID);
        } else {
            transaction->s = new STCPManager::Socket(connection);
        }
        transaction->fullRequest = request;
        transaction->timeoutAt = STimeNow() + 30'000'000;
        transaction->s->send(request.serialize());

        while (!transaction->response) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.prePoll(fdm, *transaction);
            S_poll(fdm, 100'000);
            manager.postPoll(fdm, *transaction, nextActivity);
        }

        cout << "Wise sandbox response (manager status " << transaction->response << "):\n"
        << transaction->fullResponse.serialize() << endl;

        // An HTTP error is useful diagnostic output too; a transport failure has no HTTP response.
        EXPECT_TRUE(SStartsWith(transaction->fullResponse.methodLine, "HTTP/"));
    }
} __WiseMTLSTest;

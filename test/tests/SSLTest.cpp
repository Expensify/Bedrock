#include "libstuff/SHTTPSManager.h"
#include "libstuff/STCPManager.h"
#include "test/lib/tpunit++.hpp"
#include <unistd.h>

#include <libstuff/libstuff.h>
#include <libstuff/SData.h>
#include <sqlitecluster/SQLiteNode.h>
#include <test/lib/BedrockTester.h>
#include <libstuff/SHTTPSProxySocket.h>
#include <libstuff/SSSLState.h>
#include <libstuff/SX509.h>
#include <sys/socket.h>

struct SSLTest : tpunit::TestFixture
{
    SSLTest()
        : tpunit::TestFixture("SSL",
                              BEFORE_CLASS(SSLTest::setup),
                              TEST(SSLTest::test),
                              TEST(SSLTest::proxyTest),
                              TEST(SSLTest::clientCertificateTest),
                              TEST(SSLTest::invalidClientCertificateTest),
                              AFTER_CLASS(SSLTest::teardown))
    {
    }

    BedrockTester* tester;

    // Throwaway self-signed pair generated for this test; it is its own CA.
    static constexpr auto TEST_CLIENT_CERT_PEM = R"(-----BEGIN CERTIFICATE-----
MIIDHTCCAgWgAwIBAgIUJ4J1PkU8UU2PbYVb9TdNXn+8GjowDQYJKoZIhvcNAQEL
BQAwHjEcMBoGA1UEAwwTYmVkcm9jay10ZXN0LWNsaWVudDAeFw0yNjA5MDgxMjIx
NTdaFw0zNjA5MDUxMjIxNTdaMB4xHDAaBgNVBAMME2JlZHJvY2stdGVzdC1jbGll
bnQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDIGXvyZ7bU6eWSWt0B
CGTEmGKgnSCErQg5g0kEIk0aY6RtdfNINSQAh5T3egh78Gj5590yYyuHCmlvOhC8
PdzcaGczubcBPblqromS6lqa8QdJNoFLCeSsyhm5idu/FEQUn3jZQAV6voBRz3/Q
J8ctIil3MXDK4aqLFSW7Xx7sIQvC2VkFXmMuVDmztwHqZLfMhi0kxLRyFJOwNpT4
G7cTNrVQmPWkC05ser69dD2IRN2ybCNiGDcAUw1bIdQKznZqbvD0v+KYzduOsq1O
fGim7+uF/+PtMSJwpcgDBxgSgpfGEneTz/Sa18dvAVnqBxfLQEFQSy/BAXbBx0og
9CjPAgMBAAGjUzBRMB0GA1UdDgQWBBT4rSD0kHqlu3V+h5CBv0NM+w42xDAfBgNV
HSMEGDAWgBT4rSD0kHqlu3V+h5CBv0NM+w42xDAPBgNVHRMBAf8EBTADAQH/MA0G
CSqGSIb3DQEBCwUAA4IBAQBP3A7PrUdaQIDSiuTK0hyAIfJ4SAMmeCPWMt6X5P72
C5djp91edcGKyihzLYR0brZP/ByOFoSrjsIgO8cmNK8MiTcd1u1WZqv0IWcLGj3l
J8n423Am7lmiOCeUBXhCaoJC1dwLQrQtYe+3QlIDZcRUf3StLpYGz2ybcjtgEsb6
rp/mbAwASyLihcIICr8vgcKnb6NrXcM47T+JGwoxvcJb1vdMCMfbb9o2CUAvcfl1
S7CpjAKCseecISAp3Osv8sV8d8L5XUOFhdazNuq7baFsYVdSBk/uLGM5R6dGYbe7
yjDLq3XCkLX046g5TyL7kPTCgHnmBXgL0VrGdCdbwvR9
-----END CERTIFICATE-----
)";

    static constexpr auto TEST_CLIENT_KEY_PEM = R"(-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDIGXvyZ7bU6eWS
Wt0BCGTEmGKgnSCErQg5g0kEIk0aY6RtdfNINSQAh5T3egh78Gj5590yYyuHCmlv
OhC8PdzcaGczubcBPblqromS6lqa8QdJNoFLCeSsyhm5idu/FEQUn3jZQAV6voBR
z3/QJ8ctIil3MXDK4aqLFSW7Xx7sIQvC2VkFXmMuVDmztwHqZLfMhi0kxLRyFJOw
NpT4G7cTNrVQmPWkC05ser69dD2IRN2ybCNiGDcAUw1bIdQKznZqbvD0v+KYzduO
sq1OfGim7+uF/+PtMSJwpcgDBxgSgpfGEneTz/Sa18dvAVnqBxfLQEFQSy/BAXbB
x0og9CjPAgMBAAECggEAL+p2VVJDpFGMMVND1+L4KmGpQHfP2SWMMiV/fxdtqjOR
JMsZYkGjaOMOp4JX9MHZiXpd7Bp2SmSSYdmgN0uDD16K1AaVTZ1HwMWh2QX4g2DT
U1NTe7IXSc8tgNIDxopmUR/s8u0TQtTKaozLOP/FgMfM8SdsMJEAY0TBn9BD2dwV
bdydqiX4O0QLa+lncLaYh9VV0QOK8niBx0DxV0doXfRt96QHS3Z9Qr8c24fFlgNU
ynba6CwwkKF790BF2fOXPlp29Vxab6J33jJoMwjtlJ464PIpC+icLJjVr4Z8h/XN
vI7UjUu6J+uNVf5LAPOBh4KrRhi9CsUqV6tNVntYZQKBgQDtUZ7aLrx9E7n93737
mjK5wlUqgNFfc2bzfIEQLh8DhhetnpSJ1ovfNyyuoizQtNLDjxuqI9EFOJ4K9QY3
qoJewtXmsx41I4sG3VlqD1EY4QrT6Rwa1N0qzxzCgh3bgS4KJMg54w1sOA60OgD1
gO4k2YevWXbubhN+phbbB4pEnQKBgQDX2dTickBYDyFa6YQoATnXw4L6ZZ1ZewVZ
q7M5qpkLx5J5jBc/LAH4r542bi9jAmT3MRiUurdhNBQJQzg+6P0Yk8ZVskGLEk+i
1tBgzlE8e5Yhty9UKn9fnI9OK3i8kpjiDrzPV7SaSVOQWPftFKnzdTJF4vXXHxGc
MKPl0SFJWwKBgQDsb23w97Eod9fMe/YeTkENWvRYtSBjlWjTWo6HHTwe2aCLhDt+
nDacO68TiVVW1WBKHzCzsJ1VM1QZnIYGPaVHXZuYDYoh7Phc7Xhgt0PXopWUDGHI
xZxXQyLnEpVGlIvW5VBqg7BiyfK0UjmUXlBkfCi3ZU2dPCPGARkyPG/f7QKBgH5F
tY8/bTrWpxmrEB/jD7aUbpQTS+ij2i5qDrAGh//nIV4vwQ24rfKQskp/TQNrzCr7
sriXPl4D/FCq3UWYQS/Wiylo3YUBEktdxYtMRyPN3LcelfZz4g3J5d/B+KDAVeWw
322gjkvP563DDk7ITt/YyK8vpcfSKhgoMElqfUGPAoGAUXnr6ly2WXGAOp7ulyt+
ungGbMAm0gz7I0HF0P/4IeDyYZ4pz9GzRaM0Y+/crIN4fCFGi9cSXAIOvD9L5YdH
Bw0e/YI3bEHRPd5BG8X7esZLFwr0UCNZzHFav3+Wh2aaLDxS/oAj9agKcIoekXEV
BrwVvK3xNiqukrGtNd3vG6k=
-----END PRIVATE KEY-----
)";

    void setup()
    {
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            STHROW("Couldn't get CWD");
        }

        tester = new BedrockTester({
            {"-plugins", string(cwd) + "/clustertest/testplugin/testplugin.so"},
        });
    }

    void teardown()
    {
        delete tester;
    }

    void test()
    {
        for (auto& url : (map<string, string>){
            // Verify we get some HTTP response from google. We don't care what it is, just that it's valid
            // HTTP. We want to notice that our fake URL, fails, though.
            {"www.google.com", "HTTP/1.1"},
            {"www.notarealplaceforsure.com.fake", "NO_RESPONSE"},
        }) {
            SData request("sendrequest");
            request["Host"] = url.first;
            request["Connection"] = "Close";
            request["passthrough"] = "true";
            // Note: the fake URL is known to periodically time out. (after 60s) Give it enough time for one retry.
            tester->executeWaitVerifyContent({request}, url.second, false, 70'000'000);
        }
    }

    void proxyTest()
    {
        // This is a generic HTTPS manager.
        SStandaloneHTTPSManager manager;

        const string host = "example.com:443";
        SData request("GET / HTTP/1.1");
        request["host"] = host;

        // Note: this works with a default squid install, which Github actions doesn't currently have.
        const string proxy = "127.0.0.1:3128";

        // Create a transaction with a socket, send the above request.
        unique_ptr<SStandaloneHTTPSManager::Transaction> transaction = make_unique<SStandaloneHTTPSManager::Transaction>(manager, "proxyTest");

        // Verify requestID is set correctly on the transaction
        EXPECT_EQUAL(transaction->requestID, "proxyTest");

        transaction->s = new SHTTPSProxySocket(proxy, host, transaction->requestID);
        transaction->timeoutAt = STimeNow() + 5'000'000;
        transaction->s->send(request.serialize());

        // Wait for a response.
        while (!transaction->response) {
            fd_map fdm;
            uint64_t nextActivity = STimeNow();
            manager.prePoll(fdm, *transaction);
            S_poll(fdm, 1'000'000);
            manager.postPoll(fdm, *transaction, nextActivity);
        }

        // Validate that the response is reasonable
        EXPECT_EQUAL(transaction->response, 200);

        // Make sure that the response has a body. This differentiates it from the response to a CONNECT message
        // So that we can test we're looking at the actual proxied response and not just the response from the proxy itself.
        EXPECT_TRUE(transaction->fullResponse.content.size());
    }

    void clientCertificateTest()
    {
        SX509* withCA = SX509Open(TEST_CLIENT_KEY_PEM, TEST_CLIENT_CERT_PEM, TEST_CLIENT_CERT_PEM);
        EXPECT_TRUE(withCA->hasCA);
        EXPECT_NO_THROW((SSSLState("example.com:443", socket(AF_INET, SOCK_STREAM, 0), withCA)));
        SX509Close(withCA);

        SX509* systemCA = SX509Open(TEST_CLIENT_KEY_PEM, TEST_CLIENT_CERT_PEM, "");
        EXPECT_FALSE(systemCA->hasCA);
        EXPECT_NO_THROW((SSSLState("example.com:443", socket(AF_INET, SOCK_STREAM, 0), systemCA)));
        SX509Close(systemCA);

        EXPECT_NO_THROW((SSSLState("example.com:443", socket(AF_INET, SOCK_STREAM, 0))));
    }

    void invalidClientCertificateTest()
    {
        const string garbageCert = "-----BEGIN CERTIFICATE-----\nbm90IGEgY2VydA==\n-----END CERTIFICATE-----\n";
        const string garbageKey = "-----BEGIN PRIVATE KEY-----\nbm90IGEga2V5\n-----END PRIVATE KEY-----\n";
        EXPECT_THROW(SX509Open(garbageKey, TEST_CLIENT_CERT_PEM, TEST_CLIENT_CERT_PEM), SException);
        EXPECT_THROW(SX509Open(TEST_CLIENT_KEY_PEM, garbageCert, TEST_CLIENT_CERT_PEM), SException);
        EXPECT_THROW(SX509Open(TEST_CLIENT_KEY_PEM, TEST_CLIENT_CERT_PEM, garbageCert), SException);
    }
} __SSLTest;

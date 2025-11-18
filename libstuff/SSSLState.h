#pragma once

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/ssl.h>
#include <mbedtls/net_sockets.h>
#include <mbedtls/x509_crt.h>
#include <string>

using namespace std;
class SFastBuffer;

class SSSLState {
public:
    SSSLState(const string& hostname, int socket);
    ~SSSLState();

    static void initConfig();
    static void freeConfig();

    int send(const char* buffer, int length);
    int send(const SFastBuffer& buffer);
    bool sendConsume(SFastBuffer& sendBuffer);
    int recv(char* buffer, int length);
    bool recvAppend(SFastBuffer& recvBuffer);

    mbedtls_ssl_context ssl;
    mbedtls_net_context net_ctx;

private:
    static mbedtls_entropy_context _ec;
    static mbedtls_ctr_drbg_context _ctr_drbg;
    static mbedtls_ssl_config _conf;
    static mbedtls_x509_crt _cacert;
};

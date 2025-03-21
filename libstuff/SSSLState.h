#pragma once

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/ssl.h>
#include <string>
#include <mbedtls/net_sockets.h>

using namespace std;

struct SX509;
class SFastBuffer;

struct SSSLState {
    // Attributes
    mbedtls_entropy_context ec;
    mbedtls_ctr_drbg_context ctr_drbg;
    mbedtls_ssl_config conf;
    mbedtls_ssl_context ssl;
    mbedtls_net_context net_ctx;

    SSSLState();
    ~SSSLState();
};

// SSL helpers
extern SSSLState* SSSLOpen(const string& hostname = "");
extern int SSSLSend(SSSLState* ssl, const char* buffer, int length);
extern int SSSLSend(SSSLState* ssl, const SFastBuffer& buffer);
extern bool SSSLSendConsume(SSSLState* ssl, SFastBuffer& sendBuffer);
extern bool SSSLSendAll(SSSLState* ssl, const string& buffer);
extern int SSSLRecv(SSSLState* ssl, char* buffer, int length);
extern bool SSSLRecvAppend(SSSLState* ssl, SFastBuffer& recvBuffer);
extern void SSSLShutdown(SSSLState* ssl);
extern void SSSLClose(SSSLState* ssl);

/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SSSLState.h
 * Path:    libstuff/SSSLState.h
 * Pair:    SSSLState.cpp
 *
 * INTENT
 *   Thin wrapper around mbedTLS providing per-connection TLS handshake,
 *   send/recv over a raw socket fd, and process-wide mbedTLS setup
 *   (entropy/DRBG/CA-cert store) shared across all connections.
 *
 * OBJECTS
 *   SSSLState (class) - ssl (mbedtls_ssl_context), net_ctx (mbedtls_net_context);
 *     initConfig/freeConfig (static, process-wide setup/teardown);
 *     send/recv (raw and SFastBuffer-based); sendConsume/recvAppend
 *     (buffer-integrated variants); static _ec/_ctr_drbg/_conf/_cacert
 *     (shared mbedTLS state, initConfig'd once for the whole process).
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; a natural peer of STCPManager/SHTTPSProxySocket in libstuff.
 *
 * NAMING QUALITY
 *   Consistent with repo convention.
 * ─────────────────────────────────────────────────────────────────────*/

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

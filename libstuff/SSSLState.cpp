#include "SSSLState.h"
#include "mbedtls/ssl.h"

#include <mbedtls/error.h>
#include <mbedtls/net_sockets.h>

#include <libstuff/libstuff.h>
#include <libstuff/SFastBuffer.h>

SSSLState::SSSLState() {
    mbedtls_ssl_init(&ssl);
    mbedtls_ssl_config_init(&conf);
    mbedtls_ctr_drbg_init(&ctr_drbg);
    mbedtls_entropy_init(&ec);
}

SSSLState::~SSSLState() {
    mbedtls_entropy_free(&ec);
    mbedtls_ctr_drbg_free(&ctr_drbg);
    mbedtls_ssl_config_free(&conf);
    mbedtls_ssl_free(&ssl);
}

// --------------------------------------------------------------------------
SSSLState* SSSLOpen(int s) {
    // Initialize the SSL state
    SASSERT(s >= 0);
    SSSLState* state = new SSSLState;
    state->s = s;

    // All new code here.
    mbedtls_ssl_init(&state->ssl);
    mbedtls_ssl_config_init(&state->conf);
    mbedtls_net_init(&state->net_ctx);
    state->net_ctx.fd = s;

    mbedtls_entropy_init(&state->ec);
    mbedtls_ctr_drbg_init(&state->ctr_drbg);
    mbedtls_ctr_drbg_seed(&state->ctr_drbg, mbedtls_entropy_func, &state->ec, nullptr, 0);

    if (mbedtls_ssl_config_defaults(&state->conf, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT)) {
        STHROW("ssl config defaults failed");
    }

    mbedtls_ssl_conf_authmode(&state->conf, MBEDTLS_SSL_VERIFY_OPTIONAL);
    mbedtls_ssl_conf_rng(&state->conf, mbedtls_ctr_drbg_random, &state->ctr_drbg);

    if (mbedtls_ssl_setup(&state->ssl, &state->conf)) {
        STHROW("ssl setup failed");
    }

    /* We don't verify hostnames, and it's also why aboe we set MBEDTLS_SSL_VERIFY_OPTIONAL instead of MBEDTLS_SSL_VERIFY_REQUIRED.
     * This could be a possible securiy improvement.
    if (mbedtls_ssl_set_hostname(&state->ssl, "your.server.hostname")) {
        STHROW("ssl set hostname failed");
    }
    */

    mbedtls_net_init(&state->net_ctx);
    state->net_ctx.fd = state->s;

    mbedtls_ssl_set_bio(&state->ssl, &state->net_ctx, mbedtls_net_send, mbedtls_net_recv, nullptr);

    return state;
}

// --------------------------------------------------------------------------
int SSSLSend(SSSLState* sslState, const char* buffer, int length) {
    // Send as much as possible and report what happened
    SASSERT(sslState && buffer);
    const int numSent = mbedtls_ssl_write(&sslState->ssl, (unsigned char*)buffer, length);
    if (numSent > 0) {
        return numSent;
    }

    // Handle the result
    switch (numSent) {
    case MBEDTLS_ERR_SSL_WANT_READ:
    case MBEDTLS_ERR_SSL_WANT_WRITE:
    case MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY:
        return 0; // retry

    default:
        // Error
        char errStr[100];
        mbedtls_strerror(numSent, errStr, 100);
        SINFO("SSL reports send error #" << numSent << " (" << errStr << ")");
        return -1;
    }
}

// --------------------------------------------------------------------------
int SSSLRecv(SSSLState* sslState, char* buffer, int length) {
    // Receive as much as we can and report what happened
    SASSERT(sslState && buffer);
    const int numRecv = mbedtls_ssl_read(&sslState->ssl, (unsigned char*)buffer, length);
    if (numRecv > 0) {
        return numRecv;
    }

    // Handle the response
    switch (numRecv) {
    case MBEDTLS_ERR_SSL_WANT_READ:
    case MBEDTLS_ERR_SSL_WANT_WRITE:
        // retry
        return 0;

    case MBEDTLS_ERR_NET_CONN_RESET:
        // connection reset by peer
        SINFO("SSL reports MBEDTLS_ERR_NET_CONN_RESET");
        return -1;

    case MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY:
        // the connection is about to be closed
        SINFO("SSL reports MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY");
        return -1;

    default:
        // Error
        char errStr[100];
        mbedtls_strerror(numRecv, errStr, 100);
        SINFO("SSL reports recv error #" << numRecv << " (" << errStr << ")");
        return -1;
    }
}

// --------------------------------------------------------------------------
void SSSLShutdown(SSSLState* ssl) {
    // Just clean up
    SASSERT(ssl);
    mbedtls_ssl_close_notify(&ssl->ssl);
}

// --------------------------------------------------------------------------
void SSSLClose(SSSLState* ssl) {
    // Just clean up
    SASSERT(ssl);
    mbedtls_net_free(&ssl->net_ctx);
    mbedtls_ssl_free(&ssl->ssl);
    mbedtls_ssl_config_free(&ssl->conf);
    mbedtls_ctr_drbg_free(&ssl->ctr_drbg);
    mbedtls_entropy_free(&ssl->ec);

    delete ssl;
}

// --------------------------------------------------------------------------
int SSSLSend(SSSLState* ssl, const SFastBuffer& buffer) {
    // Unwind the buffer
    return SSSLSend(ssl, buffer.c_str(), (int)buffer.size());
}

// --------------------------------------------------------------------------
bool SSSLSendConsume(SSSLState* ssl, SFastBuffer& sendBuffer) {
    // Send as much as we can and return whether the socket is still alive
    if (sendBuffer.empty()) {
        return true;
    }

    // Nothing to send, assume we're alive
    int numSent = SSSLSend(ssl, sendBuffer);
    if (numSent > 0) {
        sendBuffer.consumeFront(numSent);
    }

    // Done!
    return (numSent != -1);
}

// --------------------------------------------------------------------------
bool SSSLSendAll(SSSLState* ssl, const string& buffer) {
    // Keep sending until there is an error or we're done
    SASSERT(ssl);
    int totalSent = 0;
    while (totalSent < (int)buffer.size()) {
        int numSent = SSSLSend(ssl, &buffer[totalSent], (int)buffer.size() - totalSent);
        if (numSent == -1) {
            return false;
        }
        totalSent += numSent;
    }
    return true;
}

// --------------------------------------------------------------------------
bool SSSLRecvAppend(SSSLState* ssl, SFastBuffer& recvBuffer) {
    // Keep trying to receive as long as we can
    SASSERT(ssl);
    char buffer[1024 * 16];
    int numRecv = 0;
    while ((numRecv = SSSLRecv(ssl, buffer, sizeof(buffer))) > 0) {
        // Got some more data
        recvBuffer.append(buffer, numRecv);
    }

    // Return whether or not the socket is still alive
    return (numRecv != -1);
}

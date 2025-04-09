#include "SSSLState.h"

#include <mbedtls/error.h>
#include <mbedtls/net.h>

#include <libstuff/libstuff.h>
#include <libstuff/SFastBuffer.h>

SSSLState::SSSLState(int s, const string& hostname) : socket(s) {
    mbedtls_ssl_init(&ssl);
    mbedtls_ssl_config_init(&conf);
    mbedtls_ctr_drbg_init(&ctr_drbg);
    mbedtls_entropy_init(&ec);

    SASSERT(s >= 0);

    mbedtls_ctr_drbg_seed(&ctr_drbg, mbedtls_entropy_func, &ec, 0, 0);
    mbedtls_ssl_config_defaults(&conf, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, 0);

    mbedtls_ssl_setup(&ssl, &conf);

    mbedtls_ssl_conf_authmode(&conf, MBEDTLS_SSL_VERIFY_OPTIONAL);
    mbedtls_ssl_conf_rng(&conf, mbedtls_ctr_drbg_random, &ctr_drbg);
    mbedtls_ssl_set_bio(&ssl, &socket, mbedtls_net_send, mbedtls_net_recv, 0);

    if (hostname.size()) {
        if (mbedtls_ssl_set_hostname(&ssl, hostname.c_str())) {
            STHROW("ssl set hostname failed");
        }
    }
}

SSSLState::~SSSLState() {
    mbedtls_entropy_free(&ec);
    mbedtls_ctr_drbg_free(&ctr_drbg);
    mbedtls_ssl_config_free(&conf);
    mbedtls_ssl_free(&ssl);
}

// --------------------------------------------------------------------------
int SSSLState::send(const char* buffer, int length) {
    // Send as much as possible and report what happened
    SASSERT(buffer);
    const int numSent = mbedtls_ssl_write(&ssl, (unsigned char*)buffer, length);
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

int SSSLState::recv(char* buffer, int length) {
    // Receive as much as we can and report what happened
    SASSERT(buffer);
    const int numRecv = mbedtls_ssl_read(&ssl, (unsigned char*)buffer, length);
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

int SSSLState::send(const SFastBuffer& buffer) {
    // Unwind the buffer
    return send(buffer.c_str(), (int)buffer.size());
}

bool SSSLState::sendConsume(SFastBuffer& sendBuffer) {
    // Send as much as we can and return whether the socket is still alive
    if (sendBuffer.empty()) {
        return true;
    }

    // Nothing to send, assume we're alive
    int numSent = send(sendBuffer);
    if (numSent > 0) {
        sendBuffer.consumeFront(numSent);
    }

    // Done!
    return (numSent != -1);
}

bool SSSLState::sendAll(const string& buffer) {
    int totalSent = 0;
    while (totalSent < (int)buffer.size()) {
        int numSent = send(&buffer[totalSent], (int)buffer.size() - totalSent);
        if (numSent == -1) {
            return false;
        }
        totalSent += numSent;
    }
    return true;
}

bool SSSLState::recvAppend(SFastBuffer& recvBuffer) {
    char buffer[1024 * 16];
    int numRecv = 0;
    while ((numRecv = recv(buffer, sizeof(buffer))) > 0) {
        // Got some more data
        recvBuffer.append(buffer, numRecv);
    }

    // Return whether or not the socket is still alive
    return (numRecv != -1);
}

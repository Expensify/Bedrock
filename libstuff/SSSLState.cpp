#include "SSSLState.h"
#include "mbedtls/ssl.h"

#include <mbedtls/error.h>
#include <mbedtls/net_sockets.h>

#include <libstuff/libstuff.h>
#include <libstuff/SFastBuffer.h>


SSSLState::SSSLState(const string& hostname) : SSSLState(hostname, -1) {}

SSSLState::SSSLState(const string& hostname, int socket) {
    mbedtls_entropy_init(&ec);
    mbedtls_ctr_drbg_init(&ctr_drbg);
    mbedtls_ssl_config_init(&conf);
    mbedtls_ssl_init(&ssl);
    mbedtls_net_init(&net_ctx);

    // Hostname here is expected to contain the port. I.e.: expensify.com:443
    // We need to split it into its componenets.
    string domain;
    uint16_t port;
    SParseHost(hostname, domain, port);

    // Do a bunch of TLS initialization.
    int lastResult = 0;
    char errorBuffer[500] = {0};

    // Disable tls 1.3
    // TODO: Fix? This (maybe?) causes a use-after-free bug to appear when we try and run backups.
    mbedtls_ssl_conf_max_version(&conf, MBEDTLS_SSL_MAJOR_VERSION_3, MBEDTLS_SSL_MINOR_VERSION_3);

    lastResult = mbedtls_ctr_drbg_seed(&ctr_drbg, mbedtls_entropy_func, &ec, nullptr, 0);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ctr_drbg_seed failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }

    // If no socket was supplied, create our own.
    if (socket == -1) {
        lastResult = mbedtls_net_connect(&net_ctx, domain.c_str(),to_string(port).c_str(), MBEDTLS_NET_PROTO_TCP);
        if (lastResult) {
            mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
            STHROW("mbedtls_net_connect failed with error " + to_string(lastResult) + ": " + errorBuffer);
        }
        lastResult = mbedtls_net_set_nonblock(&net_ctx);
        if (lastResult) {
            mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
            STHROW("mbedtls_net_set_nonblock failed with error " + to_string(lastResult) + ": " + errorBuffer);
        }
    } else {
        // Otherwise, just borrow the existing socket.
        net_ctx.fd = socket;
    }

    lastResult = mbedtls_ssl_config_defaults(&conf, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_config_defaults failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }

    // These two calls do not return error codes.
    mbedtls_ssl_conf_authmode(&conf, MBEDTLS_SSL_VERIFY_OPTIONAL);
    mbedtls_ssl_conf_rng(&conf, mbedtls_ctr_drbg_random, &ctr_drbg);

    lastResult = mbedtls_ssl_set_hostname(&ssl, domain.c_str());
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_set_hostname failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }

    lastResult = mbedtls_ssl_setup(&ssl, &conf);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_setup failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }

    mbedtls_ssl_set_bio(&ssl, &net_ctx, mbedtls_net_send, mbedtls_net_recv, nullptr);
}

SSSLState::~SSSLState() {
    // I *beleive* this closes the socket. Let's check.
    mbedtls_net_free(&net_ctx);
    mbedtls_ssl_free(&ssl);
    mbedtls_ssl_config_free(&conf);
    mbedtls_ctr_drbg_free(&ctr_drbg);
    mbedtls_entropy_free(&ec);
}


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

bool SSSLState::recvAppend(SFastBuffer& recvBuffer) {
    // Keep trying to receive as long as we can
    char buffer[1024 * 16];
    int numRecv = 0;
    while ((numRecv = recv(buffer, sizeof(buffer))) > 0) {
        // Got some more data
        recvBuffer.append(buffer, numRecv);
    }

    // Return whether or not the socket is still alive
    return (numRecv != -1);
}

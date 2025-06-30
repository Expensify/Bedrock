#include "SSSLState.h"
#include "mbedtls/ssl.h"

#include <mbedtls/error.h>
#include <mbedtls/net_sockets.h>

#include <libstuff/libstuff.h>
#include <libstuff/SFastBuffer.h>
#include <string>

SSSLState::SSSLState(const string& hostname) : SSSLState(hostname, -1) {}
SSSLState::SSSLState(const string& hostname, int socket) {
    mbedtls_entropy_init(&ec);
    mbedtls_ctr_drbg_init(&ctr_drbg);
    mbedtls_ssl_config_init(&conf);
    mbedtls_ssl_init(&ssl);
    mbedtls_net_init(&net_ctx);
    mbedtls_x509_crt_init(&cacert);

    // Hostname here is expected to contain the port. I.e.: expensify.com:443
    // We need to split it into its componenets.
    string domain;
    uint16_t port;
    if (!SParseHost(hostname, domain, port)) {
        STHROW("Invalid host: " + hostname);
    }

    // Do a bunch of TLS initialization.
    int lastResult = 0;
    char errorBuffer[500] = {0};
    lastResult = mbedtls_ctr_drbg_seed(&ctr_drbg, mbedtls_entropy_func, &ec, nullptr, 0);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ctr_drbg_seed failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }
    SINFO("mbedtls_ctr_drbg_seed succeeded");

    // Load OS default CA certificates for peer verification
    // Try loading from the certificate directory first (Ubuntu's preferred method)
    lastResult = mbedtls_x509_crt_parse_path(&cacert, "/etc/ssl/certs/");
    if (lastResult < 0) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        SWARN("Failed to load CA certificates from /etc/ssl/certs/ directory. Error: " + to_string(lastResult) + ": " + errorBuffer);

        // If directory loading failed, try the bundle file as fallback
        lastResult = mbedtls_x509_crt_parse_file(&cacert, "/etc/ssl/certs/ca-certificates.crt");
        if (lastResult < 0) {
            mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
            SWARN("Bundle file fallback also failed. SSL certificate verification may fail. Error: " + to_string(lastResult) + ": " + errorBuffer);
        } else {
            SINFO("Successfully loaded CA certificates from /etc/ssl/certs/ca-certificates.crt (fallback)");
        }
    } else if (lastResult > 0) {
        SWARN("Loaded CA certificates from /etc/ssl/certs/ directory, but " + to_string(lastResult) + " certificates failed to parse");
    } else {
        SINFO("Successfully loaded all CA certificates from /etc/ssl/certs/ directory (0 failures)");
    }

    lastResult = mbedtls_ssl_set_hostname(&ssl, domain.c_str());
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_set_hostname failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }
    SINFO("mbedtls_ssl_set_hostname succeeded for domain: " + domain);

    // If no socket was supplied, create our own.
    if (socket == -1) {
        lastResult = mbedtls_net_connect(&net_ctx, domain.c_str(),to_string(port).c_str(), MBEDTLS_NET_PROTO_TCP);
        if (lastResult) {
            mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
            STHROW("mbedtls_net_connect failed with error (" + domain + ":" + to_string(port) + "): " + to_string(lastResult) + ": " + errorBuffer);
        }
        SINFO("mbedtls_net_connect succeeded to " + domain + ":" + to_string(port));
        lastResult = mbedtls_net_set_nonblock(&net_ctx);
        if (lastResult) {
            mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
            STHROW("mbedtls_net_set_nonblock failed with error " + to_string(lastResult) + ": " + errorBuffer);
        }
        SINFO("mbedtls_net_set_nonblock succeeded");
    } else {
        // Otherwise, just borrow the existing socket.
        net_ctx.fd = socket;
    }

    lastResult = mbedtls_ssl_config_defaults(&conf, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_config_defaults failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }
    SINFO("mbedtls_ssl_config_defaults succeeded");

    // These calls do not return error codes.
    mbedtls_ssl_conf_authmode(&conf, MBEDTLS_SSL_VERIFY_REQUIRED);
    SINFO("mbedtls_ssl_conf_authmode set to MBEDTLS_SSL_VERIFY_REQUIRED");
    mbedtls_ssl_conf_rng(&conf, mbedtls_ctr_drbg_random, &ctr_drbg);
    SINFO("mbedtls_ssl_conf_rng succeeded");
    mbedtls_ssl_conf_ca_chain(&conf, &cacert, nullptr);
    SINFO("mbedtls_ssl_conf_ca_chain succeeded");

    lastResult = mbedtls_ssl_setup(&ssl, &conf);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_setup failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }
    SINFO("mbedtls_ssl_setup succeeded");

    mbedtls_ssl_set_bio(&ssl, &net_ctx, mbedtls_net_send, mbedtls_net_recv, nullptr);
    SINFO("mbedtls_ssl_set_bio succeeded");

    SINFO("SSSLState constructor completed successfully for " + hostname);
}

SSSLState::~SSSLState() {
    // Note that this closes the socket if one is set, so there is no need (and in fact it is a bug) to close it otherwise.
    mbedtls_net_free(&net_ctx);
    mbedtls_ssl_free(&ssl);
    mbedtls_ssl_config_free(&conf);
    mbedtls_ctr_drbg_free(&ctr_drbg);
    mbedtls_entropy_free(&ec);
    mbedtls_x509_crt_free(&cacert);
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
    char buffer[1024 * 16];
    int numRecv = 0;
    while ((numRecv = recv(buffer, sizeof(buffer))) > 0) {
        // Got some more data
        recvBuffer.append(buffer, numRecv);
    }

    // Return whether or not the socket is still alive
    return (numRecv != -1);
}

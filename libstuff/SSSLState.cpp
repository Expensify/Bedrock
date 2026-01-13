#include "SSSLState.h"
#include "mbedtls/ssl.h"
#include <mbedtls/error.h>
#include <mbedtls/net_sockets.h>
#include <libstuff/libstuff.h>
#include <libstuff/SFastBuffer.h>

mbedtls_entropy_context SSSLState::_ec;
mbedtls_ctr_drbg_context SSSLState::_ctr_drbg;
mbedtls_ssl_config SSSLState::_conf;
mbedtls_x509_crt SSSLState::_cacert;

void SSSLState::initConfig()
{
    mbedtls_entropy_init(&_ec);
    mbedtls_x509_crt_init(&_cacert);
    mbedtls_ctr_drbg_init(&_ctr_drbg);
    mbedtls_ssl_config_init(&_conf);

    char errorBuffer[500] = {0};
    int lastResult = mbedtls_ctr_drbg_seed(&_ctr_drbg, mbedtls_entropy_func, &_ec, nullptr, 0);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ctr_drbg_seed failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }

    // Load environment or OS default (based on Ubuntu) CA certificates for peer verification.
    // NOTE: Environment variable should have a trailing slash.
    const char* envCertPath = getenv("CERT_PATH");
    const string certPath = envCertPath ? envCertPath : "/etc/ssl/certs/";
    lastResult = mbedtls_x509_crt_parse_path(&_cacert, certPath.c_str());
    bool successfullyLoadedCACerts = false;
    if (lastResult < 0) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        SWARN("Failed to load CA certificates from " + certPath + " directory. Error: " + to_string(lastResult) + ": " + errorBuffer);

        // If directory loading failed, try the bundle file as fallback
        lastResult = mbedtls_x509_crt_parse_file(&_cacert, (certPath + "ca-certificates.crt").c_str());
        if (lastResult < 0) {
            mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
            SWARN("Bundle file fallback also failed. SSL certificate verification may fail. Error: " + to_string(lastResult) + ": " + errorBuffer);
        }
    } else if (lastResult > 0) {
        SWARN("Loaded CA certificates from " + certPath + " directory, but " + to_string(lastResult) + " certificates failed to parse");
        successfullyLoadedCACerts = true;
    } else {
        successfullyLoadedCACerts = true;
    }

    // These calls do not return error codes.
    mbedtls_ssl_conf_authmode(&_conf, successfullyLoadedCACerts ? MBEDTLS_SSL_VERIFY_REQUIRED : MBEDTLS_SSL_VERIFY_OPTIONAL);
    mbedtls_ssl_conf_rng(&_conf, mbedtls_ctr_drbg_random, &_ctr_drbg);
    mbedtls_ssl_conf_ca_chain(&_conf, &_cacert, nullptr);

    lastResult = mbedtls_ssl_config_defaults(&_conf, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_config_defaults failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }
}

void SSSLState::freeConfig()
{
    mbedtls_ssl_config_free(&_conf);
    mbedtls_ctr_drbg_free(&_ctr_drbg);
    mbedtls_entropy_free(&_ec);
    mbedtls_x509_crt_free(&_cacert);
}

SSSLState::SSSLState(const string& hostname, int socket)
{
    mbedtls_ssl_init(&ssl);
    mbedtls_net_init(&net_ctx);

    // Hostname here is expected to contain the port. I.e.: expensify.com:443
    // We need to split it into its componenets.
    string domain;
    uint16_t port;
    if (!SParseHost(hostname, domain, port)) {
        STHROW("Invalid host: " + hostname);
    }

    int lastResult = 0;
    char errorBuffer[500] = {0};
    lastResult = mbedtls_ssl_set_hostname(&ssl, domain.c_str());
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_set_hostname failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }

    // If no socket was supplied, create our own.
    // Note that this provides no option to pass configuration to the socket. Particularly, we can't set it as
    // non-blocking, meaning that if connections are slow, we block until they complete (or fail), which can
    // hold database transactions open that should close.
    if (socket == -1) {
        lastResult = mbedtls_net_connect(&net_ctx, domain.c_str(), to_string(port).c_str(), MBEDTLS_NET_PROTO_TCP);
        if (lastResult) {
            mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
            STHROW("mbedtls_net_connect failed with error (" + domain + ":" + to_string(port) + "): " + to_string(lastResult) + ": " + errorBuffer);
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

    lastResult = mbedtls_ssl_setup(&ssl, &_conf);
    if (lastResult) {
        mbedtls_strerror(lastResult, errorBuffer, sizeof(errorBuffer));
        STHROW("mbedtls_ssl_setup failed with error " + to_string(lastResult) + ": " + errorBuffer);
    }

    mbedtls_ssl_set_bio(&ssl, &net_ctx, mbedtls_net_send, mbedtls_net_recv, nullptr);
}

SSSLState::~SSSLState()
{
    // Note that this closes the socket if one is set, so there is no need (and in fact it is a bug) to close it otherwise.
    mbedtls_net_free(&net_ctx);
    mbedtls_ssl_free(&ssl);
}

int SSSLState::send(const char* buffer, int length)
{
    // Send as much as possible and report what happened
    SASSERT(buffer);
    const int numSent = mbedtls_ssl_write(&ssl, (unsigned char*) buffer, length);
    if (numSent > 0) {
        return numSent;
    }

    // Handle the result
    switch (numSent) {
        case MBEDTLS_ERR_SSL_WANT_READ:
        case MBEDTLS_ERR_SSL_WANT_WRITE:
        case MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY:
            return 0; // retry

        case MBEDTLS_ERR_SSL_CONN_EOF:
            // Connection closed by peer. This can occur when trying to send after server has
            // closed (e.g., after sending error response). Normal with proxies.
            SDEBUG("SSL connection closed during send (EOF)");
            return -1;

        default:
            // Error
            char errStr[100];
            mbedtls_strerror(numSent, errStr, 100);
            SINFO("SSL reports send error #" << numSent << " (" << errStr << ")");
            return -1;
    }
}

int SSSLState::recv(char* buffer, int length)
{
    // Receive as much as we can and report what happened
    SASSERT(buffer);
    const int numRecv = mbedtls_ssl_read(&ssl, (unsigned char*) buffer, length);
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

        case MBEDTLS_ERR_SSL_CONN_EOF:
            // Connection closed by peer (TCP FIN received). This is normal when server closes
            // after sending response, especially through proxies where the proxy may tear down
            // the CONNECT tunnel when target server closes. Don't log as error.
            SDEBUG("SSL connection closed (EOF)");
            return -1;

        default:
            // Error
            char errStr[100];
            mbedtls_strerror(numRecv, errStr, 100);
            SINFO("SSL reports recv error #" << numRecv << " (" << errStr << ")");
            return -1;
    }
}

int SSSLState::send(const SFastBuffer& buffer)
{
    // Unwind the buffer
    return send(buffer.c_str(), (int) buffer.size());
}

bool SSSLState::sendConsume(SFastBuffer& sendBuffer)
{
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
    return numSent != -1;
}

bool SSSLState::recvAppend(SFastBuffer& recvBuffer)
{
    char buffer[1024 * 16];
    int numRecv = 0;
    while ((numRecv = recv(buffer, sizeof(buffer))) > 0) {
        // Got some more data
        recvBuffer.append(buffer, numRecv);
    }

    // Return whether or not the socket is still alive
    return numRecv != -1;
}

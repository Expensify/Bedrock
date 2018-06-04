#include "libstuff.h"
#include <mbedtls/error.h>
#include <mbedtls/net.h>

#include "mbedtls/certs.h"
#include "mbedtls/debug.h"



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

// handle the old case of client instantiation
SSSLState* SSSLOpen(int s, SX509* x509) {
    return SSSLOpen(s, x509, false);
}

// --------------------------------------------------------------------------
SSSLState* SSSLOpen(int s, SX509* x509, bool server) {
    // Initialize the SSL state
    SASSERT(s >= 0);
    SSSLState* state = new SSSLState;
    state->s = s;
    // compatibility with mbedtls
    //state->ctx.fd = s;



    mbedtls_ssl_init( &state->ssl );
    mbedtls_ssl_config_init( &state->conf );

        mbedtls_ssl_conf_dbg(&state->conf, MBEDTLS_DEBUG, NULL);
    mbedtls_debug_set_threshold(4);


    mbedtls_entropy_init( &state->ec );
    mbedtls_ctr_drbg_init( &state->ctr_drbg );

    SDEBUG("MB ctr_drbg_seed");
    mbedtls_ctr_drbg_seed(&state->ctr_drbg, mbedtls_entropy_func, &state->ec, 0, 0);

    
    // if we are receiving an SSL connection (say, from another node), then we need to act as server for the handshake.
    if(server) {

        SDEBUG("MB ssl_config_defaults");
        mbedtls_ssl_config_defaults(&state->conf, MBEDTLS_SSL_IS_SERVER, MBEDTLS_SSL_TRANSPORT_STREAM, 0);

        SDEBUG("MB SSL OPENED IN SERVER MODE");
    } else {
        //mbedtls_ssl_set_hostname( &state->ssl, "mbed TLS Client 1" );
        SDEBUG("MB SSL OPENED IN CLIENT MODE");
        mbedtls_ssl_config_defaults(&state->conf, MBEDTLS_SSL_IS_CLIENT, MBEDTLS_SSL_TRANSPORT_STREAM, 0);
    }
    SDEBUG("MB ssl_setup");
    mbedtls_ssl_setup(&state->ssl, &state->conf);
    SDEBUG("MB ssl_conf_authmode");
    mbedtls_ssl_conf_authmode(&state->conf, MBEDTLS_SSL_VERIFY_NONE);
    SDEBUG("MB ssl_conf_rng");
    mbedtls_ssl_conf_rng(&state->conf, mbedtls_ctr_drbg_random, &state->ctr_drbg);
    SDEBUG("MB ssl_set_bio");
    mbedtls_ssl_set_bio(&state->ssl, &state->s, mbedtls_net_send, mbedtls_net_recv, 0);



    if (x509) {
        // Add the certificate
        mbedtls_ssl_conf_ca_chain(&state->conf, x509->cert.next, 0);
        SASSERT(mbedtls_ssl_conf_own_cert(&state->conf, &x509->cert, &x509->pk) == 0);
    }
    return state;
}

// --------------------------------------------------------------------------
int SSSLSend(SSSLState* sslState, const char* buffer, int length) {
    // Send as much as possible and report what happened
    SDEBUG("SSSLSend Main Func state " << sslState << " buffer " << buffer);
    //SASSERT(sslState && buffer);
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
string SSSLGetState(SSSLState* ssl) {
    // Just return direct
    SASSERT(ssl);
#define SSLSTATE(_STATE_)                                                                                              \
    case _STATE_:                                                                                                      \
        return #_STATE_
    switch (ssl->ssl.state) {
        SSLSTATE(MBEDTLS_SSL_HELLO_REQUEST);
        SSLSTATE(MBEDTLS_SSL_CLIENT_HELLO);
        SSLSTATE(MBEDTLS_SSL_SERVER_HELLO);
        SSLSTATE(MBEDTLS_SSL_SERVER_CERTIFICATE);
        SSLSTATE(MBEDTLS_SSL_SERVER_KEY_EXCHANGE);
        SSLSTATE(MBEDTLS_SSL_CERTIFICATE_REQUEST);
        SSLSTATE(MBEDTLS_SSL_SERVER_HELLO_DONE);
        SSLSTATE(MBEDTLS_SSL_CLIENT_CERTIFICATE);
        SSLSTATE(MBEDTLS_SSL_CLIENT_KEY_EXCHANGE);
        SSLSTATE(MBEDTLS_SSL_CERTIFICATE_VERIFY);
        SSLSTATE(MBEDTLS_SSL_CLIENT_CHANGE_CIPHER_SPEC);
        SSLSTATE(MBEDTLS_SSL_CLIENT_FINISHED);
        SSLSTATE(MBEDTLS_SSL_SERVER_CHANGE_CIPHER_SPEC);
        SSLSTATE(MBEDTLS_SSL_SERVER_FINISHED);
        SSLSTATE(MBEDTLS_SSL_FLUSH_BUFFERS);
        SSLSTATE(MBEDTLS_SSL_HANDSHAKE_OVER);
    default:
        return "(unknown)";
    }
#undef SSLSTATE
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
    mbedtls_ssl_free(&ssl->ssl);
    delete ssl;
}

// --------------------------------------------------------------------------
int SSSLSend(SSSLState* ssl, const string& buffer) {
    // Unwind the buffer
    // make sure we're sending the right thing.
    SDEBUG("MB Call to SSSLSend with " << typeid(buffer).name() << " cstr " << buffer.c_str());
    return SSSLSend(ssl, buffer.c_str(), (int)buffer.size());
}

// --------------------------------------------------------------------------
bool SSSLSendConsume(SSSLState* ssl, string& sendBuffer) {
    // Send as much as we can and return whether the socket is still alive
    if (sendBuffer.empty()) {
        return true;
    }

    // Nothing to send, assume we're alive
    int numSent = SSSLSend(ssl, sendBuffer);
    if (numSent > 0) {
        SConsumeFront(sendBuffer, numSent);
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
bool SSSLRecvAppend(SSSLState* ssl, string& recvBuffer) {
    // Keep trying to receive as long as we can
    SASSERT(ssl);
    char buffer[1024 * 16];
    int totalRecv = 0;
    int numRecv = 0;
    while ((numRecv = SSSLRecv(ssl, buffer, sizeof(buffer))) > 0) {
        // Got some more data
        recvBuffer.append(buffer, numRecv);
        totalRecv += numRecv;
    }

    // Return whether or not the socket is still alive
    return (numRecv != -1);
}

void MBEDTLS_DEBUG( void *ctx, int level,
                      const char *file, int line,
                      const char *str )
{
    ((void) level);
    switch (level) {
        case 4:
            SDEBUG("MBEDTLS DEBUG level " << level << " file " << file << " line " << line << " remark " << str);
            break;
        case 3: 
            SDEBUG("MBEDTLS DEBUG level " << level << " file " << file << " line " << line << " remark " << str);
            break;
        case 2:
            SDEBUG("MBEDTLS DEBUG level " << level << " file " << file << " line " << line << " remark " << str);
            break;
        case 1:
            SWARN("MBEDTLS DEBUG level " << level << " file " << file << " line " << line << " remark " << str);
            break;
    }
    
    fflush(  (FILE *) ctx  );
}

// --------------------------------------------------------------------------
string SSSLError(int val)
{
    char error_buf[100];
    mbedtls_strerror( val, error_buf, 100 );
    SDEBUG("SSSLError Parsed as " << val << ": " << error_buf);
    return error_buf;
}

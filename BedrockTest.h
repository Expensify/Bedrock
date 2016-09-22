/// bedrock/BedrockTest.h
#ifndef _BEDROCKTEST_H
#define _BEDROCKTEST_H
#include "BedrockServer.h"

// Creates and controls a test environment
struct BedrockTester : public SDataClient
{
    // Public attributes
    BedrockServer* server;

    // Public methods
    void startServer( SData args = SData() );
    void stopServer( );
    void waitForResponses( );
    void sendQuery( const string& query, int numToSend = 1, int numActiveConnections = 10 );
    void testRequest( const SData& request, const SData& correctResponse );

    // Test harness interface
    BedrockTester( );
    void loop( uint64_t& nextActivity );

    // SDataClient interface
    virtual void onResponse( const string& host, const SData& request, const SData& response );
};

/// bedrock/BedrockTest.h
#endif

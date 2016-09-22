#include "libstuff.h"

// --------------------------------------------------------------------------
void SDataClient::sendRequest( const string& host, const SData& request )
{
    // First see if we already have an idle connection open to that host
    Connection* connection = 0;
    SFOREACH( list<Connection*>, idleConnectionList, connectionIt )
    {
        // See if it's to that host
        Connection* c = *connectionIt;
        if( SIEquals( c->host, host ) )
        {
            // Reuse this connection
            SDEBUG( "Reusing idle connection to '" << host << "'" );
            connection = c;
            idleConnectionList.erase( connectionIt );
            break;
        }
    }

    // If no connection, create a new one
    if( !connection )
    {
        // Open a socket to there
        SDEBUG( "Creating new connection to '" << host << "'" );
        connection = new Connection();
        connection->host = host;
        connection->s    = openSocket( host );
    }

    // Record and send the request
    SINFO( "Sending '" << request.methodLine << "' to '" << host << "'" );
    connection->s->sendBuffer += request.serialize();
    connection->request = request;
    activeConnectionList.push_back( connection );
}

// --------------------------------------------------------------------------
SDataClient::~SDataClient( )
{
    // Just clean up all the connections
    SFOREACH( list<Connection*>, activeConnectionList, connectionIt )
    {
        // Clean it up
        Connection* connection = *connectionIt;
        SWARN( "Prematurely closing connection to '" << connection->host << "' while waiting for response to '" << connection->request.methodLine << "'" );
        closeSocket( connection->s );
        SDELETE( connection );
    }
    activeConnectionList.clear( );
    SFOREACH( list<Connection*>, idleConnectionList, connectionIt )
    {
        // Clean it up
        Connection* connection = *connectionIt;
        SINFO( "Closing connection to '" << connection->host << "'" );
        closeSocket( connection->s );
        SDELETE( connection );
    }
    idleConnectionList.clear( );
}

// --------------------------------------------------------------------------
void SDataClient::postSelect( fd_map& fdm )
{
    // Let the class do its thing
    STCPManager::postSelect( fdm );

    // Processes all active connections
    for( list<Connection*>::iterator connectionIt=activeConnectionList.begin();
         connectionIt!=activeConnectionList.end(); )
    {
        // See if we got a response on this active connection
        Connection* connection = *connectionIt;
        list<Connection*>::iterator lastConnectionIt = connectionIt++;
        int responseSize = 0;
        SData response;
        if( (responseSize = response.deserialize( connection->s->recvBuffer )) )
        {
            // Got a response; clear the request so we can reuse this
            // connection.
            SINFO( "Request '" << connection->request.methodLine << "' received response '" << response.methodLine << "' from '" << connection->host << "', recycling connection" );
            SConsumeFront( connection->s->recvBuffer, responseSize );
            onResponse( connection->host, connection->request, response );
            connection->request.clear();
            activeConnectionList.erase( lastConnectionIt );
            idleConnectionList.push_back( connection );
        }
        else if( connection->s->state == STCP_CLOSED )
        {
            // Socket closed, clean it up.  But first, were we waiting on
            // response?
            if( !connection->request.empty() )
            {
                // Request didn't get a response, fabricate a failure
                SWARN( "Socket died while waiting on '" << connection->request.methodLine << "' from '" << connection->host << "'" );
                SData response( "500 Premature connection close" );
                onResponse( connection->host, connection->request, response );
            }
            else SINFO( "Socket to '" << connection->host << "' closed." );

            // Clean it up
            closeSocket( connection->s );
            SDELETE( connection );
            activeConnectionList.erase( lastConnectionIt );
        }
    }

    // Clean up any dead idle connections
    for( list<Connection*>::iterator connectionIt=idleConnectionList.begin();
         connectionIt!=idleConnectionList.end(); )
    {
        // Verify it's still open
        Connection* connection = *connectionIt;
        list<Connection*>::iterator lastConnectionIt = connectionIt++;
        if( connection->s->state == STCP_CLOSED )
        {
            // Clean this up
            SDEBUG( "Idle connection to '" << connection->host << "' died, closing." );
            closeSocket( connection->s );
            SDELETE( connection );
            idleConnectionList.erase( lastConnectionIt );
        }
    }
}

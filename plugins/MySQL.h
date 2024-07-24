#pragma once
#include <libstuff/libstuff.h>
#include "BedrockServer.h"
#include "BedrockPlugin.h"

#define MYSQL_NUM_VARIABLES 292
extern const char* g_MySQLVariables[MYSQL_NUM_VARIABLES][2];

/**
  * Simple convenience structure to construct MySQL packets
  */
struct MySQLPacket {
    // Attributes
    // MySQL sequenceID which is used by clients and servers to
    // order packets and resets back to 0 when a new "command" starts.
    uint8_t sequenceID;

    // The packet payload
    string payload;

    /**
     * Constructor
     */
    MySQLPacket();

    /**
     * Compose a MySQL packet ready for sending
     *
     * @return Binary packet in MySQL format
     */
    string serialize();

    /**
     * Parse a MySQL packet from the wire
     *
     * @param packet Binary data received from the MySQL client
     * @param size   length of packet
     * @return       Number of bytes deserialized, or 0 on failure
     */
    int deserialize(const char* packet, const size_t size);

    /**
     * Creates a MySQL length-encoded integer
     * See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html
     *
     * @param val Integer value to be length-encoded
     * @return    Lenght-encoded integer value
     */
    static string lenEncInt(uint64_t val);

    /**
     * Creates a MySQL length-encoded string
     * See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_strings.htm
     *
     * @param str The string to be length-encoded
     * @return    length-encoded string
     */
    static string lenEncStr(const string& str);

    /**
     * Creates the packet sent from the server to new connections
     * See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
     */
    static string serializeHandshake();

    /**
     * Creates the packet used to respond to a COM_QUERY request
     * See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html
     *
     * @param sequenceID The sequenceID of the request we are responding to
     * @param result     The results of the query we were asked to execte
     * @return           A series of MySQL packets ready to be sent to the client
     */
    static string serializeQueryResponse(int sequenceID, const SQResult& result);

    /**
     * Creatse a standard OK packet
     * See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
     *
     * @param sequenceID The sequenceID of the request we are responding to
     * @return           The OK packet to be sent to the client
     */
    static string serializeOK(int sequenceID);

    /**
     * Sends ERR
     * See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
     *
     * @param sequenceID The sequenceID of the request we are responding to
     * @param code       The error code to show the user
     * @param message    The error message to show the user
     * @return           The ERR packet to be sent to the client
     */
    static string serializeERR(int sequenceID, uint16_t code, const string& message);
};

/**
 * This plugin allows MySQL clients to connect to a bedrock instance. It requires the DB plugin
 * or some other plugin that can process "Query" commandsin order to actually work, in other
 * words this is essentially a network protocol wrapper aound the DB plugin.
 */
class BedrockPlugin_MySQL : public BedrockPlugin {
  public:
    BedrockPlugin_MySQL(BedrockServer& s);
    virtual const string& getName() const override;

    // This is an empty implementation but I'm including it here to make it clear
    // that this plugin does not support any commands on purpose.
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand) override;

    // This plugin listens on MySQL's port by default, but can be changed via CLI flag.
    virtual string getPort() override;

    // This function is called when bedrock accepts a new connection on a port owned
    // by a given plugin. We use it to send the MySQL handshake.
    virtual void onPortAccept(STCPManager::Socket* s) override;

    // This function is called when bedrock receives data on a port owned by a given plugin.
    // We do basically all query and processing in here.
    virtual void onPortRecv(STCPManager::Socket* s, SData& request) override;

    // This function is called when a requests completes, we use it to send OK and ERR Packets
    // as appropriate based on the results of onPortRecv().
    virtual void onPortRequestComplete(const BedrockCommand& command, STCPManager::Socket* s) override;

    // Our fake mysql version. We don't necessarily
    // conform to the same functionality or standards as this version, however
    // some clients do version checking to see if they support connecting. We've
    // set this to a major recent version so that modern clients can connect. In theory
    // you can safely increment this to any valid future version unless there's breaking
    // changes in the protocol.
    static constexpr auto mysqlVersion = "8.0.35";

  private:
    // Attributes
    static const string name;
    string commandName;
};

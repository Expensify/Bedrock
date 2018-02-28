#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

#define MYSQL_NUM_VARIABLES 292
extern const char* g_MySQLVariables[MYSQL_NUM_VARIABLES][2];

/**
  * Simple convenience structure to construct MySQL packets
  */
struct MySQLPacket {
    // Attributes
    uint8_t sequenceID;
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
     * @return       Number of bytes deserialized, or 0 on failure
     */
    int deserialize(const string& packet);

    /**
     * Creates a MySQL length-encoded integer
     * See: https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
     *
     * @param val Integer value to be length-encoded
     * @return    Lenght-encoded integer value
     */
    static string lenEncInt(uint64_t val);

    /**
     * Creates a MySQL length-encoded string
     * See: https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::LengthEncodedString
     *
     * @param str The string to be length-encoded
     * @return    The length-encoded string
     */
    static string lenEncStr(const string& str);

    /**
     * Creates the packet sent from the server to new connections
     * See: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
     */
    static string serializeHandshake();

    /**
     * Creates the packet used to respond to a COM_QUERY request
     * See: https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition320
     *
     * @param sequenceID The sequenceID of the request we are responding to
     * @param result     The results of the query we were asked to execte
     * @return           A series of MySQL packets ready to be sent to the client
     */
    static string serializeQueryResponse(int sequenceID, const SQResult& result);

    /**
     * Creatse a standard OK packet
     * See: https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
     *
     * @param sequenceID The sequenceID of the request we are responding to
     * @return           The OK packet to be sent to the client
     */
    static string serializeOK(int sequenceID);

    /**
     * Sends ERR
     * See: https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html#cs-packet-err-error-code
     *
     * @param sequenceID The sequenceID of the request we are responding to
     * @param code       The error code to show the user
     * @param message    The error message to show the user
     * @return           The ERR packet to be sent to the client
     */
    static string serializeERR(int sequenceID, uint16_t code, const string& message);
};

/**
 * Declare the class we're going to implement below
 */
class BedrockPlugin_MySQL : public BedrockPlugin {
  public:
    // Indicate which functions we are implementing
    virtual string getName() { return "MySQL"; }
    virtual void initialize(const SData& args, BedrockServer& server) { _args = args; }
    virtual string getPort() {
        return _args.isSet("-mysql.host") ? _args["-mysql.host"] : "localhost:3306";
    }
    virtual void onPortAccept(STCPManager::Socket* s);
    virtual void onPortRecv(STCPManager::Socket* s, SData& request);
    virtual void onPortRequestComplete(const BedrockCommand& command, STCPManager::Socket* s);

  private:
    // Attributes
    SData _args;
};

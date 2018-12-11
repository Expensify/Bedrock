#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_Jobs : public BedrockPlugin {
  public:
    // We were using MAX_SIZE_SMALL in GetJob to check the job name, but now GetJobs accepts more than one job name,
    // because of that, we need to increase the size of the param to be able to accept around 50 job names.
    static constexpr int64_t MAX_SIZE_NAME = 255 * 50;

    // Implement base class interface
    virtual void initialize(const SData& args, BedrockServer& server);
    virtual string getName() { return "Jobs"; }
    virtual void upgradeDatabase(SQLite& db);
    virtual bool peekCommand(SQLite& db, BedrockCommand& command);
    virtual bool processCommand(SQLite& db, BedrockCommand& command);
    virtual void handleFailedReply(const BedrockCommand& command);

  private:
    atomic<uint64_t> lastJobID;
    static int64_t getNextID(SQLite& db);

    // Helper functions
    string _constructNextRunDATETIME(const string& lastScheduled, const string& lastRun, const string& repeat);
    bool _validateRepeat(const string& repeat) { return !_constructNextRunDATETIME("", "", repeat).empty(); }
    bool _hasPendingChildJobs(SQLite& db, int64_t jobID);
    bool _isValidSQLiteDateModifier(const string& modifier);

    // Keep a reference to the server.
    BedrockServer* _server = nullptr;
};

#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_Jobs : public BedrockPlugin {
  public:
    BedrockPlugin_Jobs(BedrockServer& s);

    // We were using MAX_SIZE_SMALL in GetJob to check the job name, but now GetJobs accepts more than one job name,
    // because of that, we need to increase the size of the param to be able to accept around 50 job names.
    static constexpr int64_t MAX_SIZE_NAME = 255 * 50;

    // Set of supported verbs for jobs with case-insensitive matching.
    static const set<string,STableComp>supportedRequestVerbs;

    // Implement base class interface
    virtual string getName() { return "Jobs"; }
    virtual void upgradeDatabase(SQLite& db);
    virtual bool peekCommand(SQLite& db, BedrockCommand& command);
    virtual bool processCommand(SQLite& db, BedrockCommand& command);
    virtual void handleFailedReply(const BedrockCommand& command);

  private:
    static int64_t getNextID(SQLite& db);

    // Helper functions
    string _constructNextRunDATETIME(const string& lastScheduled, const string& lastRun, const string& repeat);
    bool _validateRepeat(const string& repeat) { return !_constructNextRunDATETIME("", "", repeat).empty(); }
    void _validatePriority(const int64_t priority);
    bool _hasPendingChildJobs(SQLite& db, int64_t jobID);
    bool _isValidSQLiteDateModifier(const string& modifier);
};

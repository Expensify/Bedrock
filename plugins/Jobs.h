#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_Jobs : public BedrockPlugin {
  public:
    // We were using MAX_SIZE_SMALL in GetJob to check the job name, but now GetJobs accepts more than one job name,
    // because of that, we need to increase the size of the param to be able to accept around 50 job names.
    static constexpr int64_t MAX_SIZE_NAME = 255 * 50;

    // Set a default priority.
    static constexpr int64_t JOBS_DEFAULT_PRIORITY = 500;

    // Implement base class interface
    virtual void initialize(const SData& args, BedrockServer& server);
    virtual string getName() { return "Jobs"; }
    virtual void upgradeDatabase(SQLite& db);
    virtual bool peekCommand(SQLite& db, BedrockCommand& command);
    virtual bool processCommand(SQLite& db, BedrockCommand& command);
    virtual void handleFailedReply(const BedrockCommand& command);

  private:
    // Generate a job ID.
    static int64_t getNextID(SQLite& db);

    static bool peekCancelJob(SQLite& db, BedrockCommand& command);
    static bool peekCreateJob(SQLite& db, BedrockCommand& command);
    static bool peekCreateJobs(SQLite& db, BedrockCommand& command);
    static void peekCreateCommon(SQLite& db, BedrockCommand& command, list<STable>& jsonJobs);
    static bool peekGetJob(SQLite& db, BedrockCommand& command);
    static bool peekGetJobs(SQLite& db, BedrockCommand& command);
    static void peekGetCommon(SQLite& db, BedrockCommand& command);
    static bool peekQueryJob(SQLite& db, BedrockCommand& command);

    // Handle the process portion of each command.
    static bool processCancelJob(SQLite& db, BedrockCommand& command);
    static bool processCreateJob(SQLite& db, BedrockCommand& command);

    // Returns the list of job IDs created.
    static list<string> processCreateCommon(SQLite& db, BedrockCommand& command, list<STable>& jsonJobs);
    static bool processCreateJobs(SQLite& db, BedrockCommand& command);
    static bool processDeleteJob(SQLite& db, BedrockCommand& command);
    static bool processFailJob(SQLite& db, BedrockCommand& command);
    static bool processFinishJob(SQLite& db, BedrockCommand& command);
    static bool processGetJob(SQLite& db, BedrockCommand& command);
    static bool processGetJobs(SQLite& db, BedrockCommand& command);

    // Returns the list of jobs retrieved. Each string is a serialized JSON object.
    static list<string> processGetCommon(SQLite& db, BedrockCommand& command);
    static bool processQueryJob(SQLite& db, BedrockCommand& command);
    static bool processRequeueJobs(SQLite& db, BedrockCommand& command);
    static bool processRetryJob(SQLite& db, BedrockCommand& command);
    static bool processUpdateJob(SQLite& db, BedrockCommand& command);

    // Helper functions
    static string _constructNextRunDATETIME(const string& lastScheduled, const string& lastRun, const string& repeat);
    static bool _validateRepeat(const string& repeat) { return !_constructNextRunDATETIME("", "", repeat).empty(); }
    static bool _hasPendingChildJobs(SQLite& db, int64_t jobID);
    static bool _isValidSQLiteDateModifier(const string& modifier);

    // Keep a reference to the server so we can send commands back to it if required.
    static BedrockServer* _server;

    // Keep a global instance of our plugin.
    static atomic<BedrockPlugin_Jobs*> _instance;

    // We create these inside `processCommand` to diable noop update mode for all Jobs commands.
    class scopedDisableNoopMode {
      public:
        scopedDisableNoopMode(SQLite& db) : _db(db) {
            _wasNoop = db.getUpdateNoopMode();
            if (_wasNoop) {
                _db.setUpdateNoopMode(false);
            }
        }
        ~scopedDisableNoopMode() {
            if (_wasNoop) {
                _db.setUpdateNoopMode(true);
            }
        }
      private:
        SQLite& _db;
        bool _wasNoop;
    };
};

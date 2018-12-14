#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Optimizing for unique jobs:
// In the trivial case, we walk the tables until we find the matching job.
// This means, on average, we hit half the tables in the DB.
// This has poor conflict performance.
//
// AWESOME HACK:
// Rollback and restart a transaction every time we finish a table.
// How expensive is this? I don't know.
//
// It makes the previous transaction irrelevant for conflicts though.
//
// It still takes a long time to walk the whole DB. Let's test for performance, it's faster to walk a small table than
// a big one, but maybe not a bunch of small tables instead of one big one.

// Declare the class we're going to implement below
class BedrockPlugin_Jobs : public BedrockPlugin {
  public:
    // We were using MAX_SIZE_SMALL in GetJob to check the job name, but now GetJobs accepts more than one job name,
    // because of that, we need to increase the size of the param to be able to accept around 50 job names.
    static constexpr int64_t MAX_SIZE_NAME = 255 * 50;

    // Ever changing this will break existing data if not done carefully.
    static constexpr int64_t TABLE_COUNT = 100;

    // Set a default priority.
    static constexpr int64_t JOBS_DEFAULT_PRIORITY = 500;

    // Implement base class interface
    virtual void initialize(const SData& args, BedrockServer& server);
    virtual string getName() { return "Jobs"; }
    virtual void upgradeDatabase(SQLite& db);
    virtual bool peekCommand(SQLite& db, BedrockCommand& command);
    virtual bool processCommand(SQLite& db, BedrockCommand& command);
    virtual void handleFailedReply(const BedrockCommand& command);

    // Turns numbers into table names.
    static string getTableName(int64_t number);

    // This lets us map names to tables. We don't want to do this in the general case, because we have lots of jobs
    // with identical names, and we want those to be spread out around the DB. However, we do this for unique jobs,
    // since we need to look them up by ID, and being that the names are inherently *not* the same, these will end up
    // spread around the DB.
    // NOTE: This makes it an error to create a job without passing `unique`, and then pass `unique` at a later time,
    // you will most likely end up with two jobs, as the first one will not have chosen a table based on the job name.
    static int64_t getTableNumberForJobName(const string& name);

  private:

    // Structure to return data used by finish/retry commands.
    struct jobInfo {
        int64_t jobID;
        int64_t parentJobID;
        string state;
        string nextRun;
        string lastRun;
        string repeat;
    };

    static atomic<int64_t> currentStartTableNumber;

    // Generate a job ID.
    static int64_t getNextID(SQLite& db, int64_t shouldMatch = -1);

    static bool peekCancelJob(SQLite& db, BedrockCommand& command);
    static bool peekCreateJob(SQLite& db, BedrockCommand& command);
    static bool peekCreateJobs(SQLite& db, BedrockCommand& command);
    static void peekCreateCommon(SQLite& db, BedrockCommand& command, list<STable>& jsonJobs);
    static bool peekGetJob(SQLite& db, BedrockCommand& command);
    static bool peekGetJobs(SQLite& db, BedrockCommand& command);
    static void peekGetCommon(SQLite& db, BedrockCommand& command);
    static bool peekQueryJob(SQLite& db, BedrockCommand& command);

    // Both migration commands can be removed once migrated to sharded DB.
    static bool peekMigrateJobs(SQLite& db, BedrockCommand& command);

    // Handle the process portion of each command.
    static bool processCancelJob(SQLite& db, BedrockCommand& command);
    static bool processCreateJob(SQLite& db, BedrockCommand& command);

    // Returns the list of job IDs created.
    static list<int64_t> processCreateCommon(SQLite& db, BedrockCommand& command, list<STable>& jsonJobs);
    static bool processCreateJobs(SQLite& db, BedrockCommand& command);
    static bool processDeleteJob(SQLite& db, BedrockCommand& command);
    static bool processFailJob(SQLite& db, BedrockCommand& command);
    static bool processFinishJob(SQLite& db, BedrockCommand& command);
    static bool processGetJob(SQLite& db, BedrockCommand& command);
    static bool processGetJobs(SQLite& db, BedrockCommand& command);

    // Both migration commands can be removed once migrated to sharded DB.
    static bool processMigrateJobs(SQLite& db, BedrockCommand& command);

    // Returns the list of jobs retrieved. Each string is a serialized JSON object.
    static list<string> processGetCommon(SQLite& db, BedrockCommand& command);
    static bool processRequeueJobs(SQLite& db, BedrockCommand& command);
    static bool processRetryJob(SQLite& db, BedrockCommand& command);

    // Retry and Finish are variations on the same command.
    static jobInfo processRetryFinishCommon(SQLite& db, BedrockCommand& command);

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

    // We create these inside `processCommand` to disable noop update mode for all Jobs commands.
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

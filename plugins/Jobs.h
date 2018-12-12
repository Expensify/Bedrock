#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"



// THINGS WE NEED.
// 1. We need a callback for conflicts to put jobs back in the list.
// 2. We need to load data from the table when we go mastering. If we do this in a bunch of threads, it's probably
// fast.
// 3. We need to migrate legacy jobs from the old table. This is likely slow, but we only need to do it once. If we let
// a single self-repeating command do this, it will slowly move work into the other tables, and then there will be at
// least some work for other threads to do while the migration finishes. This will have to keep gettableJobs up-to-date
// as it moves things.

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

  private:
    // These vectors have size 3, because there's one map for each priority level.
    // Each map is a set of job names mapped to job IDs for that name. These are all jobs that a QUEUED or RUNQUEUED.
    // The custom comparator function allows us to support the GLOB syntax for finding jabs based on partial names.
    // Note that this map contains the *expected* state of the database. If custom manual queries are run against the
    // jobs tables from outside this plugin, there may be entries in here for unusable commands. In that case,
    // `GetJob(s)` may return a `404 Job Not Found` when it tries to get these jobs.
    // Alternatively, if jobs are added externally, the caller will need to re-scan tables to add them, or send a
    // `ReloadQueueState` command for the specific job IDs affected.
    // TODO: Implement `ReloadQueueState` and `RescanTables`.
    static vector<map<string, list<int64_t>>> gettableJobs;
    static vector<mutex> gettableJobsMutexes;

    // Turns numbers into table names.
    static string getTableName(int64_t number);

    // Structure to return data used by finish/retry commands.
    struct jobInfo {
        int64_t jobID;
        int64_t parentJobID;
        string state;
        string nextRun;
        string lastRun;
        string repeat;
    };

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

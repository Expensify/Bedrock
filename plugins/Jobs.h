#pragma once
#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_Jobs : public BedrockPlugin {
  friend class BedrockJobsCommand;
  public:
    BedrockPlugin_Jobs(BedrockServer& s);
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);
    virtual const string& getName() const;
    virtual void upgradeDatabase(SQLite& db);

    // We were using MAX_SIZE_SMALL in GetJob to check the job name, but now GetJobs accepts more than one job name,
    // because of that, we need to increase the size of the param to be able to accept around 50 job names.
    static constexpr int64_t MAX_SIZE_NAME = 255 * 50;

    // Set of supported verbs for jobs with case-insensitive matching.
    static const set<string,STableComp>supportedRequestVerbs;

    const bool isLive;

  private:
    static const string name;
    static const int64_t JOBS_DEFAULT_PRIORITY;
};

class BedrockJobsCommand : public BedrockCommand {
  public:
    BedrockJobsCommand(SQLiteCommand&& baseCommand, BedrockPlugin* plugin);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);
    virtual void handleFailedReply();

  private:
    // Helper functions
    string _constructNextRunDATETIME(SQLite& db, const string& lastScheduled, const string& lastRun, const string& repeat);
    bool _validateRepeat(SQLite& db, const string& repeat) { return !_constructNextRunDATETIME(db, "", "", repeat).empty(); }
    bool _hasPendingChildJobs(SQLite& db, int64_t jobID);
    void _validatePriority(const int64_t priority);

    // Do not throw an exception when something goes wrong with the query to update a job's retryAfter.
    // Update the job to the failed state and log a Bugbot instead.
    // This is to avoid causing GetJob(s) to error which will render BWM unable to fetch any jobs that need to be run.
    void _handleFailedRetryAfterQuery(SQLite& db, const string& jobID);

    bool mockRequest;

    // Returns true if this command can skip straight to leader for process.
    bool canEscalateImmediately(SQLiteCommand& baseCommand);
};

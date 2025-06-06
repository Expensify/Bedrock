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

/*
Let's discuss parent/child jobs.
When a parent job is started, there's nothing in particular that makes it a parent, it's just a job.
When the actual job script runs, it has the ability to create children of this parents,
which it can do by creating new jobs and passing the existing job's ID as `parentJobID`.
This is only allowed when the parent is RUNNING, RUNQUEUED or PAUSED.

Runqueued is a special case of RUNNING for retryable jobs. In the case they get stuck, they are not perpetually left RUNNING,
they can be retried as if they were queued.

CancelJob can be called only on a child job, but only if it doesn't in turn have children of its own.
RUNNING, FINISHED and PAUSED jobs cannot be cancelled, this functionality exists to remove jobs that have been queued
but not started yet.

Grandchildren are not allowed, which makes the code from above in `CancelJob` unclear. It's likely just overly cautious gaurding.

Jobs are generally created in the QUEUED state, meaning they're ready to start, however, child jobs follow different rules.
If the parent job is currently RUNNING or RUNQUEUED, then the child is created PAUSED. If the parent is in any other state,
then the normal rule of creating the job in QUEUED applies.

Parent jobs should always be paused when children are running.

Calling FinishJob on a parent job sets its state to PAUSED and its childrens states to QUEUED.
Calling FinishJob on a child job will resume the parent if there are no other outstanding children
(i.e., only the last child should resume the parent).

Any child job (or standalone job) should be deleted on completion.

Calling CancelJob may also cause the parent to resume for the last child.

*/

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

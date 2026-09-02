#include "Jobs.h"

#include <BedrockServer.h>
#include <libstuff/SQResult.h>
#include <sqlitecluster/SQLitePeer.h>
#include <sqlitecluster/SQLiteUtils.h>

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

// Only SCHEDULED repeats shorter than this re-anchor to now after a missed window; longer intervals
// keep their scheduled time since they may need to run on a specific calendar date.
static constexpr int64_t REPEAT_REANCHOR_THRESHOLD_SECONDS = 4 * 60 * 60;

// These top-level keys are Bedrock metadata, not caller-owned job data.
static const set<string> IGNORED_BEDROCK_JOB_DATA_KEYS = {
    "_bedrockRerunIfDataChanged",
    "retryAfterCount",
    "originalNextRun",
    "_commitCounts",
};

// Apply worker changes only when a newer enqueue did not change the same top-level field. Nested values use the same
// semantic comparison as expectedData, so object member order and insignificant whitespace do not create conflicts.
static string mergeRetryJobData(SQLite& db, const string& currentData, const string& expectedData, const string& workerData)
{
    const auto valueMatches = [&](const string& left, const string& right) {
        const string comparison =
            "NOT EXISTS("
            "SELECT fullkey, type, atom FROM JSON_TREE(CASE WHEN " + left +
            "Type IN ('array', 'object') THEN " + left + "Value ELSE 'null' END) "
            "EXCEPT "
            "SELECT fullkey, type, atom FROM JSON_TREE(CASE WHEN " + right +
            "Type IN ('array', 'object') THEN " + right + "Value ELSE 'null' END)"
            ") AND NOT EXISTS("
            "SELECT fullkey, type, atom FROM JSON_TREE(CASE WHEN " + right +
            "Type IN ('array', 'object') THEN " + right + "Value ELSE 'null' END) "
            "EXCEPT "
            "SELECT fullkey, type, atom FROM JSON_TREE(CASE WHEN " + left +
            "Type IN ('array', 'object') THEN " + left + "Value ELSE 'null' END)"
            ")";
        return "((" + left + "Type IS NULL AND " + right + "Type IS NULL) OR (" + left + "Type = " + right +
               "Type AND ((" + left + "Type NOT IN ('array', 'object') AND " + left + "Atom IS " + right +
               "Atom) OR (" + left + "Type IN ('array', 'object') AND " + comparison + "))))";
    };

    const string currentMatchesExpected = valueMatches("current", "expected");
    const string workerMatchesExpected = valueMatches("worker", "expected");
    SQResult result;
    if (!db.read(
            "WITH currentEntries AS ("
            "SELECT key, value, type, atom FROM JSON_EACH(" + SQ(currentData) + ")"
            "), expectedEntries AS ("
            "SELECT key, value, type, atom FROM JSON_EACH(" + SQ(expectedData) + ")"
            "), workerEntries AS ("
            "SELECT key, value, type, atom FROM JSON_EACH(" + SQ(workerData) + ")"
            "), allKeys AS ("
            "SELECT key FROM currentEntries UNION SELECT key FROM expectedEntries UNION SELECT key FROM workerEntries"
            "), entryState AS ("
            "SELECT allKeys.key, "
            "currentEntries.value AS currentValue, currentEntries.type AS currentType, currentEntries.atom AS currentAtom, "
            "expectedEntries.value AS expectedValue, expectedEntries.type AS expectedType, expectedEntries.atom AS expectedAtom, "
            "workerEntries.value AS workerValue, workerEntries.type AS workerType, workerEntries.atom AS workerAtom "
            "FROM allKeys "
            "LEFT JOIN currentEntries USING (key) "
            "LEFT JOIN expectedEntries USING (key) "
            "LEFT JOIN workerEntries USING (key)"
            "), comparedEntries AS ("
            "SELECT *, " + currentMatchesExpected + " AS currentMatchesExpected, " +
            workerMatchesExpected + " AS workerMatchesExpected FROM entryState"
            "), selectedEntries AS ("
            "SELECT key, "
            "CASE WHEN currentMatchesExpected AND NOT workerMatchesExpected THEN workerValue ELSE currentValue END AS value, "
            "CASE WHEN currentMatchesExpected AND NOT workerMatchesExpected THEN workerType ELSE currentType END AS type "
            "FROM comparedEntries"
            ") "
            "SELECT COALESCE(JSON_GROUP_OBJECT(key, CASE type "
            "WHEN 'true' THEN JSON('true') "
            "WHEN 'false' THEN JSON('false') "
            "WHEN 'null' THEN JSON('null') "
            "WHEN 'array' THEN JSON(value) "
            "WHEN 'object' THEN JSON(value) "
            "ELSE value END), '{}') "
            "FROM selectedEntries WHERE type IS NOT NULL;",
            result) || result.empty()) {
        STHROW("502 Failed to merge retry job data");
    }
    return result[0][0];
}

static string preserveRerunIfDataChangedSQL(const string& newDataExpression)
{
    return "IIF(JSON_TYPE(data, '$._bedrockRerunIfDataChanged') = 'true', "
           "JSON_SET(" + newDataExpression + ", '$._bedrockRerunIfDataChanged', JSON('true')), " +
           newDataExpression + ")";
}

static string stripRerunIfDataChanged(const string& data)
{
    STable publicData = SParseJSONObject(data);
    if (!SContains(publicData, "_bedrockRerunIfDataChanged")) {
        return data;
    }
    publicData.erase("_bedrockRerunIfDataChanged");
    return SComposeJSONObject(publicData);
}

const int64_t BedrockPlugin_Jobs::JOBS_DEFAULT_PRIORITY = 500;
const string BedrockPlugin_Jobs::name("Jobs");
const string& BedrockPlugin_Jobs::getName() const
{
    return name;
}

set<string> BedrockPlugin_Jobs::getCrashedBedrockJobPatterns()
{
    shared_lock<decltype(_crashedBedrockJobPatternMutex)> lock(_crashedBedrockJobPatternMutex);
    return _crashedBedrockJobPatterns;
}

STable BedrockPlugin_Jobs::getInfo()
{
    STable info;
    shared_lock<decltype(_crashedBedrockJobPatternMutex)> lock(_crashedBedrockJobPatternMutex);
    info["crashedBedrockJobs"] = _crashedBedrockJobPatterns.size();
    info["crashedBedrockJobList"] = SComposeJSONArray(vector<string>(_crashedBedrockJobPatterns.begin(), _crashedBedrockJobPatterns.end()));
    return info;
}

void BedrockPlugin_Jobs::onNodeLogin(SQLitePeer* peer)
{
    list<string> jobPatterns;
    {
        shared_lock<decltype(_crashedBedrockJobPatternMutex)> lock(_crashedBedrockJobPatternMutex);
        jobPatterns.assign(_crashedBedrockJobPatterns.begin(), _crashedBedrockJobPatterns.end());
    }
    if (jobPatterns.empty()) {
        return;
    }

    // Send the current blacklist to the joining peer so it enforces the same patterns if it becomes leader. Tag it as
    // propagated so the peer stores it without re-broadcasting, and set a timeout so this send can't block for long.
    SINFO("Sending " << jobPatterns.size() << " blacklisted bedrock job patterns to node " << peer->name << " on login");
    SData crashJobCommand("CrashBedrockJob");
    crashJobCommand["names"] = SComposeList(jobPatterns);
    crashJobCommand["isPropagated"] = "true";
    crashJobCommand["timeout"] = "5000";
    server.broadcastCommand(crashJobCommand, peer->name);
}

const set<string, STableComp> BedrockPlugin_Jobs::supportedRequestVerbs = {
    "GetJob",
    "GetJobs",
    "QueryJob",
    "CreateJob",
    "CreateJobs",
    "CancelJob",
    "UpdateJob",
    "RetryJob",
    "FinishJob",
    "FailJob",
    "DeleteJob",
    "RequeueJobs",
    "CrashBedrockJob",
    "ClearCrashedBedrockJobs",
};

bool BedrockJobsCommand::canEscalateImmediately(SQLiteCommand& baseCommand)
{
    // This is a set of commands that we will escalate to leader without waiting. It's not intended to be complete but
    // to solve the biggest issues we have with slow escalation times (i.e., this is usually a problem for `FinishJob`).
    static const set<string> commands = {"CreateJob", "CreateJobs", "FinishJob"};
    return commands.count(baseCommand.request.methodLine);
}

// Disable noop mode for the lifetime of this object.
class scopedDisableNoopMode {
public:
    scopedDisableNoopMode(SQLite& db) : _db(db)
    {
        _wasNoop = db.getUpdateNoopMode();
        if (_wasNoop) {
            _db.setUpdateNoopMode(false);
        }
    }

    ~scopedDisableNoopMode()
    {
        if (_wasNoop) {
            _db.setUpdateNoopMode(true);
        }
    }

private:
    SQLite& _db;
    bool _wasNoop;
};

BedrockJobsCommand::BedrockJobsCommand(SQLiteCommand&& baseCommand, BedrockPlugin* plugin) :
    BedrockCommand(move(baseCommand), plugin, canEscalateImmediately(baseCommand))
{
}

BedrockPlugin_Jobs::BedrockPlugin_Jobs(BedrockServer& s) :
    BedrockPlugin(s),
    isLive(server.args.isSet("-live"))
{
}

unique_ptr<BedrockCommand> BedrockPlugin_Jobs::getCommand(SQLiteCommand&& baseCommand)
{
    if (supportedRequestVerbs.count(baseCommand.request.getVerb())) {
        return make_unique<BedrockJobsCommand>(move(baseCommand), this);
    }
    return nullptr;
}

// ==========================================================================
void BedrockPlugin_Jobs::upgradeDatabase(SQLite& db)
{
    // Create or verify the jobs table
    bool ignore;
    SASSERT(db.verifyTable("jobs",
                           "CREATE TABLE jobs ( "
                               "created     TIMESTAMP NOT NULL, "
                               "jobID       INTEGER NOT NULL PRIMARY KEY, "
                               "state       TEXT NOT NULL, "
                               "name        TEXT NOT NULL, "
                               "nextRun     TIMESTAMP NOT NULL, "
                               "lastRun     TIMESTAMP, "
                               "repeat      TEXT NOT NULL, "
                               "data        TEXT NOT NULL, "
                               "priority    INTEGER NOT NULL DEFAULT " + SToStr(JOBS_DEFAULT_PRIORITY) + ", "
                               "parentJobID INTEGER NOT NULL DEFAULT 0, "
                               "retryAfter  TEXT NOT NULL DEFAULT \"\")",
                           ignore));
    // verify and conditionally create indexes
    SASSERT(db.verifyIndex("jobsName", "jobs", "( name )", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsParentJobIDState", "jobs", "( parentJobID, state ) WHERE parentJobID != 0", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsStatePriorityNextRunName", "jobs", "( state, priority, nextRun, name )", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsPriorityNextRunManualSmartScanMerchantAndCategory", "jobs", "(priority, nextRun) WHERE state IN ('QUEUED', 'RUNQUEUED') AND name GLOB 'manual/SmartScanMerchantAndCategory*'", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsPriorityNextRunManualSmartScanAmountAndCurrency", "jobs", "(priority, nextRun) WHERE state IN ('QUEUED', 'RUNQUEUED') AND name GLOB 'manual/SmartScanAmountAndCurrency*'", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsPriorityNextRunManualSmartScanCreated", "jobs", "(priority, nextRun) WHERE state IN ('QUEUED', 'RUNQUEUED') AND name GLOB 'manual/SmartScanCreated*'", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsPriorityNextRunManualSmartScanIsCash", "jobs", "(priority, nextRun) WHERE state IN ('QUEUED', 'RUNQUEUED') AND name GLOB 'manual/SmartScanIsCash*'", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsPriorityNextRunManualSmartScan", "jobs", "(priority, nextRun) WHERE state IN ('QUEUED', 'RUNQUEUED') AND name GLOB 'manual/SmartScan*'", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsManualSmartscanReceiptID", "jobs", "( JSON_EXTRACT(data, '$.receiptID') ) WHERE JSON_VALID(data) AND name GLOB 'manual/SmartScan*'", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsPriorityNextRunWWWProd", "jobs", "(priority, nextRun) WHERE state IN ('QUEUED', 'RUNQUEUED') AND name GLOB 'www-prod/*'", false, !BedrockPlugin_Jobs::isLive));
    SASSERT(db.verifyIndex("jobsPriorityNextRunWWWStag", "jobs", "(priority, nextRun) WHERE state IN ('QUEUED', 'RUNQUEUED') AND name GLOB 'www-stag/*'", false, !BedrockPlugin_Jobs::isLive));
}

// ==========================================================================
bool BedrockJobsCommand::peek(SQLite& db)
{
    const string& requestVerb = request.getVerb();

    // Jobs commands can only crash if their name and data are identical
    if (request.isSet("name") && request.isSet("data")) {
        crashIdentifyingValues.insert("name");
        crashIdentifyingValues.insert("data");
    }

    // We can potentially change this, so we set it here.
    mockRequest = request.isSet("mockRequest");

    if (SIEquals(requestVerb, "CrashBedrockJob") || SIEquals(requestVerb, "ClearCrashedBedrockJobs")) {
        // These change this node's in-memory job blacklist and don't touch the DB, so we complete them here in peek.
        // They must originate locally (an operator on this node) or from a peer over the private command port -- both
        // have an empty _source -- so reject anything from the public command port.
        if (!request["_source"].empty()) {
            STHROW("401 Unauthorized");
        }

        BedrockPlugin_Jobs* jobsPlugin = static_cast<BedrockPlugin_Jobs*>(_plugin);
        if (SIEquals(requestVerb, "CrashBedrockJob")) {
            const list<string> names = SParseList(request["names"]);
            if (names.empty()) {
                SINFO("Got CrashBedrockJob with no 'names'. Nothing to blacklist.");
            } else {
                unique_lock<decltype(jobsPlugin->_crashedBedrockJobPatternMutex)> lock(jobsPlugin->_crashedBedrockJobPatternMutex);
                for (const auto& name : names) {
                    jobsPlugin->_crashedBedrockJobPatterns.insert(name);
                }
                SINFO("Blacklisting bedrock jobs (now have " << jobsPlugin->_crashedBedrockJobPatterns.size() << " patterns): " << SComposeList(names));
            }
        } else {
            unique_lock<decltype(jobsPlugin->_crashedBedrockJobPatternMutex)> lock(jobsPlugin->_crashedBedrockJobPatternMutex);
            jobsPlugin->_crashedBedrockJobPatterns.clear();
        }

        // Propagate a locally-originated change to the rest of the cluster so a new leader still enforces it after a
        // failover. Peer-delivered commands arrive with "isPropagated" set so recipients update locally without
        // re-broadcasting -- every intra-cluster command has an empty _source, so _source can't distinguish a local
        // command from a propagated one, which would otherwise loop forever.
        if (!request.isSet("isPropagated")) {
            SData propagated = request;
            propagated["isPropagated"] = "true";
            _plugin->server.broadcastCommand(propagated);
        }
        return true;
    }

    if (SIEquals(requestVerb, "GetJob") || SIEquals(requestVerb, "GetJobs")) {
        // - GetJob( name )
        // - GetJobs( name, numResults )
        //
        //     Atomically dequeues one or more jobs, if available.
        //
        //     Parameters:
        //     - name - list of name patterns of jobs to match. If only one name is passed, you can use '*' to match any job.
        //     - numResults - (optional) Optional for GetJob, required for GetJobs. Maximum number of jobs to dequeue.
        //     - connection - (optional) If "wait" will pause up to "timeout" for a match
        //     - jobPriority - (optional) Only check for jobs with this priority
        //     - timeout - (optional) maximum time (in ms) to wait, default forever
        //
        //     Returns:
        //     - 200 - OK
        //         . GetJob
        //           o jobID - unique ID of the job
        //           o name  - name of the actual job matched
        //           o data  - JSON data associated with this job
        //         . GetJobs
        //           o jobs - Array of JSON objects, each matching the result of GetJob
        //     - 303 - Timeout
        //     - 404 - No jobs found
        //
        BedrockPlugin::verifyAttributeSize(request, "name", 1, BedrockPlugin_Jobs::MAX_SIZE_NAME);
        if (SIEquals(requestVerb, "GetJobs") != request.isSet("numResults")) {
            if (SIEquals(requestVerb, "GetJobs")) {
                STHROW("402 Missing numResults");
            } else {
                STHROW("402 Cannot use numResults with GetJob; try GetJobs");
            }
        }

        if (request.isSet("jobPriority")) {
            int64_t priority = request.calc64("jobPriority");
            _validatePriority(priority);
        }

        return false;
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(requestVerb, "QueryJob")) {
        // - QueryJob( jobID )
        //
        //     Returns all known information about a given job.
        //
        //     Parameters:
        //     - jobID - Identifier of the job to query
        //
        //     Returns:
        //     - 200 - OK
        //         . created - creation time of this job
        //         . jobID - unique ID of the job
        //         . state - One of QUEUED, RUNNING, FINISHED
        //         . name  - name of the actual job matched
        //         . nextRun - timestamp of next scheduled run
        //         . lastRun - timestamp it was last run
        //         . repeat - recurring description
        //         . data - JSON data associated with this job
        //     - 404 - No jobs found
        //
        BedrockPlugin::verifyAttributeInt64(request, "jobID", 1);

        // Verify there is a job like this
        SQResult result;
        if (!db.read("SELECT created, jobID, state, name, nextRun, lastRun, repeat, "
                     "JSON_REMOVE(data, '$._bedrockRerunIfDataChanged'), retryAfter, priority "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(request.calc64("jobID")) + ";",
                     result)) {
            STHROW("502 Select failed");
        }
        if (result.empty()) {
            STHROW("404 No job with this jobID");
        }
        jsonContent["created"] = result[0][0];
        jsonContent["jobID"] = result[0][1];
        jsonContent["state"] = result[0][2];
        jsonContent["name"] = result[0][3];
        jsonContent["nextRun"] = result[0][4];
        jsonContent["lastRun"] = result[0][5];
        jsonContent["repeat"] = result[0][6];
        jsonContent["data"] = result[0][7];
        jsonContent["retryAfter"] = result[0][8];
        jsonContent["priority"] = result[0][9];
        return true; // Successfully processed
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(requestVerb, "CreateJob") || SIEquals(requestVerb, "CreateJobs")) {
        try {
            list<STable> jsonJobs;
            if (SIEquals(requestVerb, "CreateJob")) {
                BedrockPlugin::verifyAttributeSize(request, "name", 1, BedrockPlugin_Jobs::MAX_SIZE_SMALL);
                jsonJobs.push_back(request.nameValueMap);
            } else {
                list<string> multipleJobs;
                multipleJobs = SParseJSONArray(request["jobs"]);
                if (multipleJobs.empty()) {
                    STHROW("401 Invalid JSON");
                }

                for (auto& job : multipleJobs) {
                    STable jobObject = SParseJSONObject(job);
                    if (jobObject.empty()) {
                        STHROW("401 Invalid JSON");
                    }

                    // Verify that name is present for every job
                    if (!SContains(job, "name")) {
                        STHROW("402 Missing name");
                    }

                    jsonJobs.push_back(jobObject);
                }
            }

            for (auto& job : jsonJobs) {
                // If no priority set, set it
                int64_t priority = SContains(job, "jobPriority") ? SToInt(job["jobPriority"]) : BedrockPlugin_Jobs::JOBS_DEFAULT_PRIORITY;

                // We'd initially intended for any value to be allowable here, but for
                // performance reasons, we currently will only allow specific values to
                // try and keep queries fast. If you pass an invalid value, we'll throw
                // here so that the caller can know that he did something wrong rather
                // than having his job sit unprocessed in the queue forever. Hopefully
                // we can remove this restriction in the future.
                _validatePriority(priority);

                // Throw if data is not a valid JSON object, otherwise UPDATE query will fail.
                if (SContains(job, "data") && SParseJSONObject(job["data"]).empty() && job["data"] != "{}") {
                    STHROW("402 Data is not a valid JSON Object");
                }
                if (!job["data"].empty()) {
                    job["data"] = stripRerunIfDataChanged(job["data"]);
                }

                // Validate retryAfter
                if (SContains(job, "retryAfter") && job["retryAfter"] != "" && !SIsValidSQLiteDateModifier(job["retryAfter"])) {
                    STHROW("402 Malformed retryAfter");
                }

                // rerunIfDataChanged runs the job again if its data changes while it is running.
                // It requires unique=true and overwrite=true.
                if (SContains(job, "rerunIfDataChanged") && !job["rerunIfDataChanged"].empty() &&
                    !SIEquals(job["rerunIfDataChanged"], "true") && !SIEquals(job["rerunIfDataChanged"], "false")) {
                    STHROW("402 Malformed rerunIfDataChanged");
                }
                if (SContains(job, "rerunIfDataChanged") && SIEquals(job["rerunIfDataChanged"], "true") &&
                    (!SContains(job, "unique") || job["unique"] != "true" ||
                     (SContains(job, "overwrite") && job["overwrite"] != "true" && job["overwrite"] != ""))) {
                    STHROW("402 rerunIfDataChanged requires unique=true and overwrite enabled");
                }

                // Validate that the parentJobID exists and is in the right state if one was passed.
                // Also verify that the parent job doesn't have a retryAfter set.
                int64_t parentJobID = SContains(job, "parentJobID") ? SToInt64(job["parentJobID"]) : 0;
                if (parentJobID) {
                    SINFO("parentJobID passed, checking existing job with ID " << parentJobID);
                    SQResult result;
                    if (!db.read("SELECT state, data, JSON_TYPE(data, '$._bedrockRerunIfDataChanged') = 'true' "
                                 "FROM jobs WHERE jobID = " + SQ(parentJobID) + ";", result)) {
                        STHROW("502 Select failed");
                    }
                    if (result.empty()) {
                        STHROW("404 parentJobID does not exist");
                    }
                    if (!SIEquals(result[0][0], "RUNNING") && !SIEquals(result[0][0], "RUNQUEUED") && !SIEquals(result[0][0], "PAUSED")) {
                        SWARN("Trying to create child job with parent jobID#" << parentJobID << ", but parent isn't RUNNING or PAUSED (" << result[0][0] << ")");
                        STHROW("405 Can only create child job when parent is RUNNING, RUNQUEUED or PAUSED");
                    }
                    if (result[0][2] == "1") {
                        STHROW("405 rerunIfDataChanged jobs cannot own child jobs");
                    }

                    // Verify that the parent and child job have the same `mockRequest` setting, update them to match if
                    // not. Note that this is the first place we'll look at `mockRequest` while handling this command so
                    // any change made here will happen early enough for all of our existing checks to work correctly, and
                    // everything should be good when we get to `processCommand`.
                    STable parentData = SParseJSONObject(result[0][1]);
                    bool parentIsMocked = parentData.find("mockRequest") != parentData.end();
                    bool childIsMocked = request.isSet("mockRequest");

                    if (parentIsMocked && !childIsMocked) {
                        mockRequest = true;
                        SDEBUG("Setting child job to mocked to match parent.");
                    } else if (!parentIsMocked && childIsMocked) {
                        mockRequest = false;
                        SDEBUG("Setting child job to non-mocked to match parent.");
                    }
                }

                // Verify unique, but only do so when creating a single job using CreateJob
                if (SContains(job, "unique") && job["unique"] == "true") {
                    SQResult result;
                    SINFO("Unique flag was passed, checking existing job with name " << job["name"] << ", mocked? "
                          << (mockRequest ? "true" : "false"));
                    string operation = mockRequest ? "IS NOT" : "IS";
                    if (!db.read("SELECT jobID, JSON_REMOVE(data, '$._bedrockRerunIfDataChanged'), parentJobID, "
                                 "JSON_TYPE(data, '$._bedrockRerunIfDataChanged') = 'true' "
                                 "FROM jobs "
                                 "WHERE name = " + SQ(job["name"]) +
                                 "  AND JSON_EXTRACT(data, '$.mockRequest') " + operation + " NULL;",
                                 result)) {
                        STHROW("502 Select failed");
                    }

                    // If there's no job or the existing job doesn't match the data we've been passed, escalate to leader.
                    if (!result.empty()) {
                        // If the parent passed does not match the parent the job already had, then it must mean we did something
                        // wrong or made a bad CQ, so we throw so we can investigate. Updating the parent here would be
                        // confusing, as it could leave the original parent in a bad state (like for example paused forever)
                        if (result[0][2] != "0" && result[0][2] != job["parentJobID"]) {
                            STHROW("404 Trying to create a child that already exists, but it is tied to a different parent");
                        }
                        const bool alreadyRerunIfDataChanged = result[0][3] == "1";
                        const bool optsIntoRerunIfDataChanged =
                            SContains(job, "rerunIfDataChanged") && SIEquals(job["rerunIfDataChanged"], "true");
                        const string incomingData = job["data"].empty() ? "{}" : job["data"];
                        const bool matchingData =
                            SJSONEquals(result[0][1], incomingData, IGNORED_BEDROCK_JOB_DATA_KEYS);
                        if (SIEquals(requestVerb, "CreateJob") && matchingData &&
                            (alreadyRerunIfDataChanged || !optsIntoRerunIfDataChanged)) {
                            // Return early, no need to pass to leader, there are no more jobs to create.
                            SINFO("Job already existed and unique flag was passed, reusing existing job " << result[0][0] << ", mocked? " << (mockRequest ? "true" : "false"));
                            jsonContent["jobID"] = result[0][0];
                            return true;
                        }
                    }
                }
            }
        } catch (const exception& e) {
            SALERT("Failed creating job", {{"what", e.what()}});
            throw;
        }
        return false;
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "CancelJob")) {
        // - CancelJob(jobID)
        //
        //     Cancel a QUEUED, RUNQUEUED, FAILED child job.
        //
        //     Parameters:
        //     - jobID  - ID of the job to cancel
        //
        //     Returns:
        //     - 200 - OK
        //     - 402 - Cannot cancel jobs that are running
        //
        BedrockPlugin::verifyAttributeInt64(request, "jobID", 1);
        int64_t jobID = request.calc64("jobID");

        SQResult result;
        if (!db.read("SELECT j.jobID, j.state, j.parentJobID, (SELECT COUNT(1) FROM jobs WHERE parentJobID != 0 AND parentJobID=" + SQ(jobID) + ") children "
                     "FROM jobs j "
                     "WHERE j.jobID=" + SQ(jobID) + ";",
                     result)) {
            STHROW("502 Select failed");
        }

        // Verify the job exists
        if (result.empty() || result[0][0].empty()) {
            STHROW("404 No job with this jobID");
        }

        // If the job has any children, we are using the command in the wrong way
        if (SToInt64(result[0][3]) != 0) {
            STHROW("404 Invalid jobID - Cannot cancel a job with children");
        }

        // The command should only be called from a child job, throw if the job doesn't have a parent
        if (SToInt64(result[0][2]) == 0) {
            STHROW("404 Invalid jobID - Cannot cancel a job without a parent");
        }

        // Don't process the command if the job has finished or it's already running.
        if (result[0][1] == "FINISHED" || result[0][1] == "RUNNING") {
            SINFO("CancelJob called on a " << result[0][0] << " state, skipping");
            return true; // Done
        }

        // Verify that we are not trying to cancel a PAUSED job.
        if (result[0][1] == "PAUSED") {
            SALERT("Trying to cancel a job " << request["jobID"] << " that is PAUSED");
            return true; // Done
        }

        return false; // Need to process command
    }

    // Didn't recognize this command
    return false;
}

// ==========================================================================
void BedrockJobsCommand::process(SQLite& db)
{
    // Disable noop update mode for jobs.
    scopedDisableNoopMode disable(db);

    // Pull out some helpful variables
    const string& requestVerb = request.getVerb();

    if (SIEquals(requestVerb, "CreateJob") || SIEquals(requestVerb, "CreateJobs")) {
        // - CreateJob( name, [data], [firstRun], [repeat], [jobPriority], [unique], [overwrite], [rerunIfDataChanged], [parentJobID], [retryAfter] )
        //
        //     Creates a "job" for future processing by a worker.
        //
        //     Parameters:
        //     - name  - An arbitrary string identifier (case insensitive)
        //     - data  - A JSON object describing work to be done (optional)
        //     - firstRun - A "YYYY-MM-DD HH:MM:SS" datetime of when this job should next execute (optional)
        //     - repeat - A description of how to repeat (optional)
        //     - jobPriority - High priorities go first (optional, default 500)
        //     - unique - if true, it will check that no other job with this name already exists, if it does it will
        //                return that jobID
        //     - overwrite - Only applicable when unique is true. When set to true it will overwrite the existing job
        //                   with the new job data
        //     - rerunIfDataChanged - With unique and overwrite enabled, data changed by an enqueue while the job runs
        //                       schedules a subsequent run with the latest payload. This is separate from retryAfter
        //                       failure recovery. An opted-in job cannot own child jobs.
        //     - parentJobID - The ID of the parent job (optional)
        //     - retryAfter - Amount of auto-retries before marking job as failed (optional)
        //
        //     Returns:
        //     - jobID - Unique identifier of this job
        //
        // - CreateJobs (jobs)
        //
        //     Creates a list of jobs.
        //
        //     Parameters:
        //     - jobs (json array):
        //          - name  - An arbitrary string identifier (case insensitive)
        //          - data  - A JSON object describing work to be done (optional)
        //          - firstRun - A "YYYY-MM-DD HH:MM:SS" datetime of when this job should next execute (optional)
        //          - repeat - A description of how to repeat (optional)
        //          - jobPriority - High priorities go first (optional, default 500)
        //          - unique - if true, it will check that no other job with this name already exists, if it does it will
        //                     return that jobID
        //          - overwrite - Only applicable when unique is true. When set to true it will overwrite the existing job
        //                        with the new job data
        //          - rerunIfDataChanged - Enables the same subsequent-run behavior as CreateJob
        //          - parentJobID - The ID of the parent job (optional)
        //          - retryAfter - Amount of auto-retries before marking job as failed (optional)
        //
        //     Returns:
        //     - jobIDs - array with the unique identifier of the jobs
        //

        list<STable> jsonJobs;
        if (SIEquals(requestVerb, "CreateJob")) {
            jsonJobs.push_back(request.nameValueMap);
        } else {
            list<string> multipleJobs;
            multipleJobs = SParseJSONArray(request["jobs"]);
            if (multipleJobs.empty()) {
                STHROW("401 Invalid JSON");
            }

            for (auto& job : multipleJobs) {
                STable jobObject = SParseJSONObject(job);
                if (jobObject.empty()) {
                    STHROW("401 Invalid JSON");
                }

                jsonJobs.push_back(jobObject);
            }
        }

        list<string> jobIDs;
        for (auto& job : jsonJobs) {
            // If unique flag was passed and the job exist in the DB, then we can finish the command without escalating to
            // leader.

            // This marker is owned exclusively by Bedrock. Callers cannot create or overwrite it.
            if (!job["data"].empty()) {
                job["data"] = stripRerunIfDataChanged(job["data"]);
            }

            // If this is a mock request, we insert that into the data.
            string originalData = job["data"];
            if (mockRequest) {
                // Mocked jobs should never repeat.
                job.erase("repeat");

                if (job["data"].empty()) {
                    job["data"] = "{\"mockRequest\":true}";
                } else {
                    STable data = SParseJSONObject(job["data"]);
                    data["mockRequest"] = "true";
                    job["data"] = SComposeJSONObject(data);
                }
            }

            int64_t updateJobID = 0;
            bool existingRerunIfDataChanged = false;
            bool enablesRerunIfDataChangedWithoutDataChange = false;
            if (SContains(job, "unique") && job["unique"] == "true") {
                SQResult result;
                SDEBUG("Unique flag was passed, checking existing job with name " << job["name"] << ", mocked? "
                       << (mockRequest ? "true" : "false"));
                string operation = mockRequest ? "IS NOT" : "IS";
                if (!db.read("SELECT jobID, JSON_REMOVE(data, '$._bedrockRerunIfDataChanged'), "
                             "JSON_TYPE(data, '$._bedrockRerunIfDataChanged') = 'true' "
                             "FROM jobs "
                             "WHERE name = " + SQ(job["name"]) +
                             "  AND JSON_EXTRACT(data, '$.mockRequest') " + operation + " NULL;",
                             result)) {
                    STHROW("502 Select failed");
                }

                if (!result.empty()) {
                    existingRerunIfDataChanged = result[0][2] == "1";
                    const bool optsIntoRerunIfDataChanged =
                        SContains(job, "rerunIfDataChanged") && SIEquals(job["rerunIfDataChanged"], "true");
                    const string incomingData = job["data"].empty() ? "{}" : job["data"];
                    const bool matchingData =
                        SJSONEquals(result[0][1], incomingData, IGNORED_BEDROCK_JOB_DATA_KEYS);
                    enablesRerunIfDataChangedWithoutDataChange =
                        optsIntoRerunIfDataChanged && !existingRerunIfDataChanged && matchingData;

                    // If the data matches, update the job only when enabling rerunIfDataChanged for the first time.
                    if (matchingData && (existingRerunIfDataChanged || !optsIntoRerunIfDataChanged)) {
                        SINFO("Job already existed and unique flag was passed, reusing existing job "
                              << result[0][0] << ", mocked? " << (mockRequest ? "true" : "false"));

                        // If we are calling CreateJob, return early, there are no more jobs to create.
                        if (SIEquals(requestVerb, "CreateJob")) {
                            jsonContent["jobID"] = result[0][0];
                            return;
                        }

                        // Append new jobID to list of created jobs.
                        jobIDs.push_back(result[0][0]);
                        continue;
                    }

                    updateJobID = SToInt64(result[0][0]);
                }
            }

            const string& currentTime = SCURRENT_TIMESTAMP();

            // If no "firstRun" was provided, use right now
            const string& safeFirstRun = !SContains(job, "firstRun") || job["firstRun"].empty() ? SQ(SCURRENT_TIMESTAMP_MS()) : SQ(job["firstRun"]);

            // If no data was provided, use an empty object
            const string& safeData = !SContains(job, "data") || job["data"].empty() ? SQ("{}") : SQ(job["data"]);
            const string& safeOriginalData = originalData.empty() ? SQ("{}") : SQ(originalData);

            // If a repeat is provided, validate it
            if (SContains(job, "repeat")) {
                if (job["repeat"].empty()) {
                    SWARN("Repeat is set in CreateJob, but is set to the empty string. Job Name: "
                          << job["name"] << ", removing attribute.");
                    job.erase("repeat");
                } else if (!_validateRepeat(db, job["repeat"])) {
                    STHROW("402 Malformed repeat");
                }
            }

            // If no priority set, set it
            int64_t priority = SContains(job, "jobPriority") ? SToInt(job["jobPriority"]) : (SContains(job, "priority") ? SToInt(job["priority"]) : BedrockPlugin_Jobs::JOBS_DEFAULT_PRIORITY);

            // Validate the priority passed in
            _validatePriority(priority);

            // Validate that the parentJobID exists and is in the right state if one was passed.
            int64_t parentJobID = SContains(job, "parentJobID") ? SToInt64(job["parentJobID"]) : 0;
            if (parentJobID) {
                SQResult result;
                if (!db.read("SELECT state, parentJobID, data "
                             "FROM jobs WHERE jobID = " + SQ(parentJobID) + ";", result)) {
                    STHROW("502 Select failed");
                }
                if (result.empty()) {
                    STHROW("404 parentJobID does not exist");
                }
                if (!SIEquals(result[0][0], "RUNNING") && !SIEquals(result[0][0], "RUNQUEUED") && !SIEquals(result[0][0], "PAUSED")) {
                    SWARN("Trying to create child job with parent jobID#" << parentJobID << ", but parent isn't RUNNING, RUNQUEUED or PAUSED (" << result[0][0] << ")");
                    STHROW("405 Can only create child job when parent is RUNNING, RUNQUEUED or PAUSED");
                }
                // Verify that the parent and child job have the same `mockRequest` setting.
                STable parentData = SParseJSONObject(result[0][2]);
                if (mockRequest != (parentData.find("mockRequest") != parentData.end())) {
                    STHROW("405 Parent and child jobs must have matching mockRequest setting");
                }

                // Prevent jobs from creating grandchildren
                if (!SIEquals(result[0][1], "0")) {
                    SWARN("Trying to create grandchild job with parent jobID#" << parentJobID);
                    STHROW("405 Cannot create grandchildren");
                }
            }

            // Are we creating a new job, or updating an existing job?
            if (updateJobID) {
                const bool rerunIfDataChanged = existingRerunIfDataChanged ||
                    (SContains(job, "rerunIfDataChanged") && SIEquals(job["rerunIfDataChanged"], "true"));
                if (rerunIfDataChanged && !existingRerunIfDataChanged) {
                    SQResult children;
                    if (!db.read("SELECT jobID FROM jobs WHERE parentJobID != 0 AND parentJobID=" +
                                 SQ(updateJobID) + " LIMIT 1;", children)) {
                        STHROW("502 Failed to select child jobs");
                    }
                    if (!children.empty()) {
                        STHROW("405 rerunIfDataChanged jobs cannot own child jobs");
                    }
                }
                if (enablesRerunIfDataChangedWithoutDataChange) {
                    // Only enable rerunIfDataChanged. Keep the existing repeat and priority if they were not provided.
                    list<string> updateList = {
                        "data = JSON_SET(data, '$._bedrockRerunIfDataChanged', JSON('true'))"};
                    if (SContains(job, "repeat")) {
                        updateList.push_back("repeat = " + SQ(SToUpper(job["repeat"])));
                    }
                    if (SContains(job, "jobPriority") || SContains(job, "priority")) {
                        updateList.push_back("priority = " + SQ(priority));
                    }
                    if (!db.writeIdempotent("UPDATE jobs SET " + SComposeList(updateList) +
                                            " WHERE jobID = " + SQ(updateJobID) + ";")) {
                        STHROW("502 update query failed");
                    }
                } else if (!SContains(job, "overwrite") || job["overwrite"] == "true" || job["overwrite"] == "") {
                    // Update the job data and keep rerunIfDataChanged enabled.
                    const string updatedData = rerunIfDataChanged ?
                        "JSON_SET(JSON_PATCH(data, " + safeData +
                        "), '$._bedrockRerunIfDataChanged', JSON('true'))" :
                        "JSON_PATCH(data, " + safeData + ")";

                    // Update the existing job.
                    if (!db.writeIdempotent("UPDATE jobs SET "
                                             "repeat   = " + SQ(SToUpper(job["repeat"])) + ", " +
                                             "data     = " + updatedData + ", " +
                                             "priority = " + SQ(priority) + " " +
                                           "WHERE jobID = " + SQ(updateJobID) + ";")) {
                        STHROW("502 update query failed");
                    }
                }

                // If we are calling CreateJob, return early, there are no more jobs to create.
                if (SIEquals(requestVerb, "CreateJob")) {
                    jsonContent["jobID"] = SToStr(updateJobID);
                    return;
                }

                // Append new jobID to list of created jobs.
                jobIDs.push_back(SToStr(updateJobID));
            } else {
                // Normal jobs start out in the QUEUED state, meaning they are ready to run immediately.
                // Child jobs normally start out in the PAUSED state, and are switched to QUEUED when the parent
                // finishes itself (and itself becomes PAUSED).  However, if the parent is already PAUSED when
                // the child is created (indicating a child is creating a sibling) then the new child starts
                // in the QUEUED state.
                auto initialState = "QUEUED";
                if (parentJobID) {
                    auto parentState = db.read("SELECT state FROM jobs WHERE jobID=" + SQ(parentJobID) + ";");
                    if (SIEquals(parentState, "RUNNING") || SIEquals(parentState, "RUNQUEUED")) {
                        initialState = "PAUSED";
                    }
                }

                // If no data was provided, use an empty object
                const string& safeRetryAfter = SContains(job, "retryAfter") && !job["retryAfter"].empty() ? SQ(job["retryAfter"]) : SQ("");
                const bool rerunIfDataChanged = SContains(job, "rerunIfDataChanged") && SIEquals(job["rerunIfDataChanged"], "true");
                const string dataToInsert = rerunIfDataChanged ?
                    "JSON_SET(" + safeData + ", '$._bedrockRerunIfDataChanged', JSON('true'))" :
                    safeData;

                // Create this new job with a new generated ID
                const int64_t jobIDToUse = SQLiteUtils::getRandomID(db, "jobs", "jobID");
                SINFO("Next jobID: " << jobIDToUse);
                if (!db.writeIdempotent("INSERT INTO jobs ( jobID, created, state, name, nextRun, repeat, data, priority, parentJobID, retryAfter ) "
                         "VALUES( " +
                            SQ(jobIDToUse) + ", " +
                            currentTime + ", " +
                            SQ(initialState) + ", " +
                            SQ(job["name"]) + ", " +
                            safeFirstRun + ", " +
                            SQ(SToUpper(job["repeat"])) + ", " +
                            dataToInsert + ", " +
                            SQ(priority) + ", " +
                            SQ(parentJobID) + ", " +
                            safeRetryAfter + " " +
                         " );")) {
                    STHROW("502 insert query failed");
                }

                if (SIEquals(requestVerb, "CreateJob")) {
                    jsonContent["jobID"] = SToStr(jobIDToUse);
                    return;
                }

                // Append new jobID to list of created jobs.
                jobIDs.push_back(SToStr(jobIDToUse));
            }
        }

        jsonContent["jobIDs"] = SComposeJSONArray(jobIDs);

        return; // Successfully processed
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(requestVerb, "GetJob") || SIEquals(requestVerb, "GetJobs")) {
        // If we're here it's because peekCommand found some data; re-execute
        // the query for real now.  However, this time we will order by
        // priority.  We do this as six separate queries so we only have one
        // unbounded column in each query.  Additionally, we wrap each inner
        // query in a "SELECT *" such that we can have an "ORDER BY" and
        // "LIMIT" *before* we UNION ALL them together.  Looks gnarly, but it
        // works!
        SQResult result;
        const list<string> nameList = SParseList(request["name"]);
        string safeNumResults = SQ(max(request.calc("numResults"), 1));
        mockRequest = mockRequest || request.isSet("getMockedJobs");

        const set<string> crashedJobPatterns = static_cast<BedrockPlugin_Jobs*>(_plugin)->getCrashedBedrockJobPatterns();

        string selectQuery;
        if (request.isSet("jobPriority")) {
            selectQuery =
                "SELECT jobID, name, data, parentJobID, retryAfter, created, repeat, lastRun, nextRun, priority "
                "FROM jobs "
                "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                "AND priority=" + SQ(request.calc("jobPriority")) + " "
                "AND " + SQ(SCURRENT_TIMESTAMP_MS()) + ">=nextRun "
                "AND +name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                "ORDER BY nextRun ASC LIMIT " + safeNumResults + ";";
        } else {
            selectQuery =
                "SELECT jobID, name, data, parentJobID, retryAfter, created, repeat, lastRun, nextRun, priority FROM ( "
                "SELECT * FROM ("
                "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                "FROM jobs "
                "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                "AND priority=1000 "
                "AND " + SQ(SCURRENT_TIMESTAMP_MS()) + ">=nextRun "
                "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
                "UNION ALL "
                "SELECT * FROM ("
                "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                "FROM jobs "
                "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                "AND priority=850 "
                "AND " + SQ(SCURRENT_TIMESTAMP_MS()) + ">=nextRun "
                "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
                "UNION ALL "
                "SELECT * FROM ("
                "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                "FROM jobs "
                "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                "AND priority=750 "
                "AND " + SQ(SCURRENT_TIMESTAMP_MS()) + ">=nextRun "
                "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
                "UNION ALL "
                "SELECT * FROM ("
                "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                "FROM jobs "
                "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                "AND priority=500 "
                "AND " + SQ(SCURRENT_TIMESTAMP_MS()) + ">=nextRun "
                "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
                "UNION ALL "
                "SELECT * FROM ("
                "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                "FROM jobs "
                "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                "AND priority=250 "
                "AND " + SQ(SCURRENT_TIMESTAMP_MS()) + ">=nextRun "
                "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
                "UNION ALL "
                "SELECT * FROM ("
                "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                "FROM jobs "
                "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                "AND priority=0 "
                "AND " + SQ(SCURRENT_TIMESTAMP_MS()) + ">=nextRun "
                "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
                ") "
                "ORDER BY priority DESC "
                "LIMIT " + safeNumResults + ";";
        }
        if (!db.read(selectQuery, result)) {
            STHROW("502 Query failed");
        }

        // Are there any results?
        if (result.empty()) {
            // Ah, there were before, but aren't now -- nothing found
            // **FIXME: If "Connection: wait" should re-apply the hold.  However, this is super edge
            //          as we could only get here if the job somehow got consumed between the peek
            //          and process -- which could happen during heavy load.  But it'd just return
            //          no results (which is correct) faster than it would otherwise time out.  Either
            //          way the worker will likely just loop, so it doesn't really matter.
            STHROW("404 No job found");
        }

        // Fail (rather than run) any candidate whose name matches a GLOB pattern blacklisted via CrashBedrockJob. This
        // lets us surgically disable a misbehaving job without stopping BWM for everyone. We only touch the candidates
        // this query already selected (all due to run), so it's a small write by jobID rather than a scan of the whole
        // table, and future-scheduled jobs are never candidates so they're left alone until they're due.
        set<string> crashedJobIDs;
        if (!crashedJobPatterns.empty()) {
            list<string> candidateJobIDs;
            for (size_t c = 0; c < result.size(); ++c) {
                candidateJobIDs.push_back(result[c][0]);
            }
            list<string> globClauses;
            for (const auto& pattern : crashedJobPatterns) {
                globClauses.push_back("name GLOB " + SQ(pattern));
            }
            SQResult crashed;
            if (!db.read("SELECT jobID FROM jobs WHERE jobID IN (" + SQList(candidateJobIDs) + ") AND (" + SComposeList(globClauses, " OR ") + ");", crashed)) {
                STHROW("502 Failed to select blacklisted jobs");
            }
            for (const auto& row : crashed) {
                crashedJobIDs.insert(row[0]);
            }
            if (!crashedJobIDs.empty()) {
                const list<string> failIDs(crashedJobIDs.begin(), crashedJobIDs.end());
                SINFO("Failing blacklisted bedrock jobs: " << SComposeList(failIDs));
                if (!db.writeIdempotent("UPDATE jobs SET state='FAILED' WHERE jobID IN (" + SQList(failIDs) + ");")) {
                    STHROW("502 Failed to fail blacklisted jobs");
                }
            }
        }

        // There should only be at most one result if GetJob
        SASSERT(!SIEquals(requestVerb, "GetJob") || result.size() <= 1);

        // Prepare to update the rows, while also creating all the child objects
        list<string> nonRetriableJobs;
        list<STable> retriableJobs;
        list<string> jobList;
        for (size_t c = 0; c < result.size(); ++c) {
            SASSERT(result[c].size() == 10); // jobID, name, data, parentJobID, retryAfter, created, repeat, lastRun, nextRun, priority

            // Skip any candidate we just failed for matching the blacklist so it's never returned to the worker.
            if (crashedJobIDs.count(result[c][0])) {
                continue;
            }

            // Add this object to our output
            STable job;
            job["jobID"] = result[c][0];
            job["name"] = result[c][1];
            job["data"] = db.read("SELECT JSON_REMOVE(" + SQ(result[c][2]) +
                                  ", '$._bedrockRerunIfDataChanged');");
            job["retryAfter"] = result[c][4];
            job["created"] = result[c][5];
            job["repeat"] = result[c][6];
            job["lastRun"] = result[c][7];
            job["nextRun"] = result[c][8];
            job["priority"] = result[c][9];
            int64_t parentJobID = SToInt64(result[c][3]);

            if (parentJobID) {
                // Has a parent job, add the parent data
                job["parentJobID"] = SToStr(parentJobID);;
                job["parentData"] = db.read("SELECT JSON_REMOVE(data, '$._bedrockRerunIfDataChanged') "
                                            "FROM jobs WHERE jobID = " + SQ(parentJobID) + ";");
            }

            // Add jobID to the respective list depending on if retryAfter is set
            if (result[c][4] != "") {
                retriableJobs.push_back(job);
            } else {
                nonRetriableJobs.push_back(result[c][0]);
            }

            // See if this job has any FINISHED/CANCELLED child jobs, indicating it is being resumed
            SQResult childJobs;
            if (!db.read("SELECT jobID, JSON_REMOVE(data, '$._bedrockRerunIfDataChanged'), state "
                         "FROM jobs "
                         "WHERE parentJobID != 0 "
                         "  AND parentJobID = " + SQ(result[c][0]) + " "
                         "  AND state IN ('FINISHED', 'CANCELLED');",
                         childJobs)) {
                STHROW("502 Failed to select finished child jobs");
            }

            if (!childJobs.empty()) {
                // Add arrays of children jobs to our response, 2 arrays to clearly distinguish between finished and cancelled children.
                list<string> finishedChildJobArray;
                list<string> cancelledChildJobArray;
                for (auto row : childJobs) {
                    STable childJob;
                    childJob["jobID"] = row[0];
                    childJob["data"] = row[1];

                    if (row[2] == "FINISHED") {
                        finishedChildJobArray.push_back(SComposeJSONObject(childJob));
                    } else {
                        cancelledChildJobArray.push_back(SComposeJSONObject(childJob));
                    }
                }
                job["finishedChildJobs"] = SComposeJSONArray(finishedChildJobArray);
                job["cancelledChildJobs"] = SComposeJSONArray(cancelledChildJobArray);
            }

            STable jobData = SParseJSONObject(job["data"]);
            if (SToInt(jobData["retryAfterCount"]) >= 10) {
                // We will fail this job, don't return it.
                continue;
            }
            jobList.push_back(SComposeJSONObject(job));
        }

        if (!nonRetriableJobs.empty()) {
            SINFO("Updating jobs without retryAfter " << SComposeList(nonRetriableJobs));
            string updateQuery = "UPDATE jobs "
                "SET state='RUNNING', "
                "lastRun=" + SCURRENT_TIMESTAMP() + " "
                "WHERE jobID IN (" + SQList(nonRetriableJobs) + ");";
            if (!db.writeIdempotent(updateQuery)) {
                STHROW("502 Update failed");
            }
        }

        if (!retriableJobs.empty()) {
            for (auto job : retriableJobs) {
                SDEBUG("Updating job with retryAfter " << job["jobID"]);
                STable jobData = SParseJSONObject(job["data"]);
                if (SToInt(jobData["retryAfterCount"]) >= 10) {
                    SINFO("Job " << job["jobID"] << " has retried 10 times, marking it as FAILED.");
                    string failQuery = "UPDATE jobs "
                        "SET state='FAILED' "
                        "WHERE jobID = " + SQ(job["jobID"]) + ";";
                    if (!db.writeIdempotent(failQuery)) {
                        STHROW("502 Update failed");
                    }
                    continue;
                }
                string currentTime = SUNQUOTED_CURRENT_TIMESTAMP();
                string retryAfterDateTime = "DATETIME(" + SQ(currentTime) + ", " + SQ(job["retryAfter"]) + ")";
                string repeatDateTime = _constructNextRunDATETIME(db, job["nextRun"], currentTime, job["repeat"]);
                string nextRunDateTime = repeatDateTime != "" ? "MIN(" + retryAfterDateTime + ", " + repeatDateTime + ")" : retryAfterDateTime;
                bool isRepeatBasedOnScheduledTime = SToUpper(job["repeat"]).find("SCHEDULED") != string::npos;
                string dataUpdateQuery = " ";
                if (!SStartsWith(job["name"], "manual")) {
                    // Set this so we don't retry infinitely for non manual jobs (see above)
                    // We also set originalNextRun so we don't lose track of the original nextRun (which we are overriding here)
                    dataUpdateQuery = ", data = JSON_SET(data, '$.retryAfterCount', COALESCE(JSON_EXTRACT(data, '$.retryAfterCount'), 0) + 1" + (isRepeatBasedOnScheduledTime ? ", '$.originalNextRun', " + SQ(job["nextRun"]) + ") ": ") ");
                }
                string updateQuery = "UPDATE jobs "
                    "SET state = 'RUNQUEUED', "
                    "lastRun = " + SQ(currentTime) + ", " +
                    "nextRun = " + nextRunDateTime +
                    dataUpdateQuery +
                    "WHERE jobID = " + SQ(job["jobID"]) + ";";

                try {
                    if (!db.writeIdempotent(updateQuery)) {
                        _handleFailedRetryAfterQuery(db, job["jobID"]);
                        continue;
                    }
                } catch (const SQLite::constraint_error& e) {
                    _handleFailedRetryAfterQuery(db, job["jobID"]);
                    continue;
                }
            }
        }

        // Format the results as is appropriate for what was requested
        if (SIEquals(requestVerb, "GetJob")) {
            // Single response
            if (jobList.size() != 1) {
                if (jobList.size() > 1) {
                    STHROW("500 More than 1 job to return, how is this possible?");
                }
                response.content = "{}";
                return;
            }
            response.content = jobList.front();
        } else {
            // Multiple responses
            SASSERT(SIEquals(requestVerb, "GetJobs"));
            jsonContent["jobs"] = SComposeJSONArray(jobList);
        }
        return; // Successfully processed
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(requestVerb, "UpdateJob")) {
        // - UpdateJob( jobID, data, [repeat] )
        //
        //     Atomically updates the data associated with a job.
        //
        //     Parameters:
        //     - jobID - ID of the job to delete
        //     - data  - A JSON object describing work to be done
        //     - repeat - A description of how to repeat (optional)
        //     - jobPriority - The priority of the job (optional)
        //     - nextRun - The time to run the job (optional)
        //
        BedrockPlugin::verifyAttributeInt64(request, "jobID", 1);
        BedrockPlugin::verifyAttributeSize(request, "data", 1, BedrockPlugin_Jobs::MAX_SIZE_BLOB);

        // If a repeat is provided, validate it
        if (request.isSet("repeat")) {
            if (request["repeat"].empty()) {
                SWARN("Repeat is set in UpdateJob, but is set to the empty string. jobID: "
                      << request["jobID"] << ".");
            } else if (!_validateRepeat(db, request["repeat"])) {
                STHROW("402 Malformed repeat");
            }
        }

        // If a priority is provided, validate it
        if (request.isSet("jobPriority")) {
            int64_t priority = request.calc64("jobPriority");
            _validatePriority(priority);
        }

        // Verify there is a job like this
        SQResult result;
        if (!db.read("SELECT jobID, nextRun, lastRun, JSON_EXTRACT(data, '$.mockRequest') "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(request.calc64("jobID")) + ";",
                     result)) {
            STHROW("502 Select failed");
        }
        if (result.empty() || !SToInt64(result[0][0])) {
            STHROW("404 No job with this jobID");
        }

        const string& nextRun = result[0][1];
        const string& lastRun = result[0][2];
        mockRequest = result[0][3] == "1";

        // Remove the internal marker from the request data. Restore it only if it is set on the stored job.
        // Keep the stored mockRequest value too.
        const string sanitizedRequestData = stripRerunIfDataChanged(request["data"]);
        const string newData = mockRequest
            ? db.read("SELECT IIF(JSON_VALID(" + SQ(request["data"]) + "), JSON_SET(" + SQ(sanitizedRequestData) +
                      ", '$.mockRequest', JSON('true')), '{}');")
            : db.read("SELECT IIF(JSON_VALID(" + SQ(request["data"]) + "), JSON_REMOVE(" + SQ(sanitizedRequestData) +
                      ", '$.mockRequest'), '{}');");

        // Passed next run takes priority over the one computed via the repeat feature
        string newNextRun;
        if (request["nextRun"].empty()) {
            newNextRun = request["repeat"].size() ? _constructNextRunDATETIME(db, nextRun, lastRun, request["repeat"]) : "";
        } else {
            newNextRun = SQ(request["nextRun"]);
        }

        // Update the data
        if (!db.writeIdempotent("UPDATE jobs "
                                "SET data = " + preserveRerunIfDataChangedSQL(SQ(newData)) +
                                (SToInt(request["shouldClearRepeat"]) ? ", repeat=''" :
                                 request["repeat"].size() ? ", repeat=" + SQ(SToUpper(request["repeat"])) : "") +
                                (!newNextRun.empty() ? ", nextRun=" + newNextRun : "") +
                                (request.isSet("jobPriority") ? ", priority=" + SQ(request.calc64("jobPriority")) + " " : "") +
                                " WHERE jobID = " +
                                SQ(request.calc64("jobID")) + ";")) {
            STHROW("502 Update failed");
        }
        return; // Successfully processed
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(requestVerb, "RetryJob") || SIEquals(requestVerb, "FinishJob")) {
        // - RetryJob( jobID, [delay], [nextRun], [name], [data], [ignoreRepeat], [expectedData] )
        //
        //     Re-queues a RUNNING job.
        //     The nextRun logic for the job is decided in the following way
        //      - If the job is configured to "repeat", and we are not passed
        //     an ignoreRepeat param, it will schedule the job for the next repeat time.
        //     - Else, if "nextRun" is set, it will schedule the job to run at that time
        //     - Else, if "delay" is set, it will schedule the job to run in "delay" seconds
        //
        //     Optionally, the job name can be updated, meaning that you can move the job to
        //     a different queue.  I.e, you can change the name from "foo" to "bar"
        //
        //     Use this when a job was only partially completed but
        //     interrupted in a non-fatal way.
        //
        //     Parameters:
        //     - jobID        - ID of the job to requeue
        //     - delay        - Number of seconds to wait before retrying
        //     - nextRun      - datetime of next scheduled run
        //     - name         - An arbitrary string identifier (case insensitive)
        //     - data         - Data to associate with this job
        //     - jobPriority  - The new priority to set for this job
        //     - ignoreRepeat - Ignore the job's repeat param when figuring out when to retry the job
        //     - expectedData - Immutable data returned by GetJob/GetJobs. For an opted-in job, Bedrock preserves
        //                      newer data and requeues the job when this snapshot no longer matches.
        //
        // - FinishJob( jobID, [data], [expectedData] )
        //
        //     Finishes a job.  If it's set to recur, reschedules it. If
        //     not recurring, deletes it.
        //
        //     Parameters:
        //     - jobID  - ID of the job to finish
        //     - data   - Data to associate with this finished job
        //     - expectedData - Immutable data returned by GetJob/GetJobs. For an opted-in job, Bedrock preserves
        //                      newer data and immediately requeues the job when this snapshot no longer matches.
        //
        BedrockPlugin::verifyAttributeInt64(request, "jobID", 1);
        int64_t jobID = request.calc64("jobID");

        // Verify there is a job like this and it's running
        SQResult result;
        if (!db.read("SELECT state, nextRun, lastRun, repeat, parentJobID, json_extract(data, '$.mockRequest'), "
                     "retryAfter, json_extract(data, '$.originalNextRun'), data, "
                     "JSON_TYPE(data, '$._bedrockRerunIfDataChanged') = 'true' "
                     "FROM jobs "
                     "WHERE jobID = " + SQ(jobID) + ";",
                     result)) {
            STHROW("502 Select failed");
        }
        if (result.empty()) {
            STHROW("404 No job with this jobID");
        }

        const string& state = result[0][0];
        const string& nextRun = result[0][1];
        const string& lastRun = result[0][2];
        string repeat = result[0][3];
        int64_t parentJobID = SToInt64(result[0][4]);
        mockRequest = result[0][5] == "1";
        const string retryAfter = result[0][6];
        const string originalDataNextRun = result[0][7];
        const string& currentData = result[0][8];
        const bool rerunIfDataChanged = result[0][9] == "1";

        // Make sure we're finishing a job that's actually running.
        if (state != "RUNNING" && state != "RUNQUEUED" && !mockRequest) {
            SINFO("Trying to finish job#" << jobID << ", but isn't RUNNING or RUNQUEUED (" << state << ")");
            STHROW("405 Can only retry/finish RUNNING and RUNQUEUED jobs");
        }

        const auto calculateNextRun = [&]() {
            const bool ignoreRepeat = request.test("ignoreRepeat");
            if (!repeat.empty() && !ignoreRepeat) {
                string lastScheduled = nextRun;
                if (!retryAfter.empty() && SToUpper(repeat).find("SCHEDULED") != string::npos) {
                    lastScheduled = originalDataNextRun;
                }
                return _constructNextRunDATETIME(db, lastScheduled, lastRun, repeat);
            }
            if (!SIEquals(requestVerb, "RetryJob")) {
                return string();
            }

            const string& requestedNextRun = request["nextRun"];
            if (!requestedNextRun.empty()) {
                return SQ(requestedNextRun);
            }

            SINFO("nextRun isn't set, using delay");
            const int64_t delay = request.calc64("delay");
            if (delay < 0) {
                STHROW("402 Must specify a non-negative delay when retrying");
            }
            const string retryRepeat = "FINISHED, +" + SToStr(delay) + " SECONDS";
            const string safeNextRun = _constructNextRunDATETIME(db, nextRun, lastRun, retryRepeat);
            if (safeNextRun.empty()) {
                STHROW("402 Malformed delay");
            }
            return safeNextRun;
        };

        if (rerunIfDataChanged && request.isSet("expectedData")) {
            BedrockPlugin::verifyAttributeSize(request, "expectedData", 1, BedrockPlugin_Jobs::MAX_SIZE_BLOB);
            if (SParseJSONObject(request["expectedData"]).empty() && request["expectedData"] != "{}") {
                STHROW("402 expectedData is not a valid JSON Object");
            }
            if (!SJSONEquals(currentData, request["expectedData"], IGNORED_BEDROCK_JOB_DATA_KEYS)) {
                // The caller-owned payload changed after this worker dequeued the job. Preserve the current payload,
                // name, priority, and terminal state. RetryJob can still apply non-conflicting worker data changes.
                SINFO("Requeueing rerun-if-data-changed job#" << jobID << " after " << requestVerb
                      << " because its data changed while the worker was running");
                if (SIEquals(requestVerb, "FinishJob")) {
                    if (!db.writeIdempotent("UPDATE jobs SET state = 'QUEUED', nextRun = " + SQ(SCURRENT_TIMESTAMP_MS()) +
                                            ", data = JSON_REMOVE(data, '$.retryAfterCount')" +
                                            " WHERE jobID = " + SQ(jobID) + ";")) {
                        STHROW("502 Failed to requeue rerun-if-data-changed job");
                    }
                    return;
                }

                string dataExpression = "JSON_REMOVE(data, '$.retryAfterCount')";
                if (!request["data"].empty()) {
                    const string workerData = stripRerunIfDataChanged(request["data"]);
                    const STable parsedWorkerData = SParseJSONObject(workerData);
                    const bool workerIsMocked = SContains(parsedWorkerData, "mockRequest");
                    if (mockRequest != workerIsMocked) {
                        SWARN("Not updating mockRequest field of job data.");
                        STHROW("500 Mock Mismatch");
                    }
                    const string mergedData = mergeRetryJobData(db, currentData, request["expectedData"], workerData);
                    dataExpression = "JSON_REMOVE(" + SQ(mergedData) + ", '$.retryAfterCount')";
                }

                const string safeNewNextRun = calculateNextRun();
                if (!db.writeIdempotent("UPDATE jobs SET state = 'QUEUED', nextRun = " + safeNewNextRun +
                                        ", data = " + dataExpression +
                                        " WHERE jobID = " + SQ(jobID) + ";")) {
                    STHROW("502 Failed to retry rerun-if-data-changed job");
                }
                return;
            }
        }

        // If we have a parent, make sure it is PAUSED.  This is to just
        // double-check that child jobs aren't somehow running in parallel to
        // the parent.
        if (parentJobID) {
            auto parentState = db.read("SELECT state FROM jobs WHERE jobID=" + SQ(parentJobID) + ";");
            if (!SIEquals(parentState, "PAUSED")) {
                SINFO("Trying to finish/retry job#" << jobID << ", but parent isn't PAUSED (" << parentState << ")");
                STHROW("405 Can only retry/finish child job when parent is PAUSED");
            }
        }

        // Delete any FINISHED/CANCELLED child jobs, but leave any PAUSED children alone (as those will signal that
        // we just want to re-PAUSE this job so those new children can run)
        if (!db.writeIdempotent("DELETE FROM jobs WHERE parentJobID != 0 AND parentJobID=" + SQ(jobID) + " AND state IN ('FINISHED', 'CANCELLED');")) {
            STHROW("502 Failed deleting finished/cancelled child jobs");
        }

        // If we've been asked to update the data, let's do that
        auto data = request["data"];
        if (!data.empty()) {
            // Remove the internal marker from the worker data. Restore it only if it is set on the stored job.
            data = stripRerunIfDataChanged(data);
            // See if the new data says it's mocked.
            STable newData = SParseJSONObject(data);
            bool newMocked = newData.find("mockRequest") != newData.end();

            // If both sets of data don't match each other, this is an error, we don't know who to trust.
            // We don't worry about the state of the request header for mockRequest here, as we expect that the Bedrock
            // client won't always set it when finishing or retrying a job. We'll just use what's in the data.
            if (mockRequest != newMocked) {
                SWARN("Not updating mockRequest field of job data.");
                STHROW("500 Mock Mismatch");
            }

            // If the Job data indicates that this job should be deleted, clear the repeat value so that we delete this job further down.
            if (SContains(newData, "delete") && newData["delete"] == "true") {
                SINFO("Job was marked for deletion in the data object, clearing repeat value.");
                repeat = "";
            }

            // Update the data to the new value.
            if (!db.writeIdempotent("UPDATE jobs SET data = " + preserveRerunIfDataChangedSQL(SQ(data)) +
                                    " WHERE jobID = " + SQ(jobID) + ";")) {
                STHROW("502 Failed to update job data");
            }
        }

        // Reset the retryAfterCount (set by GetJob(s)).
        if (!db.writeIdempotent("UPDATE jobs SET data = JSON_REMOVE(data, '$.retryAfterCount') WHERE jobID=" + SQ(jobID) + ";")) {
            STHROW("502 Failed to update job retryAfterCount");
        }

        // If we are finishing a job that has child jobs, set its state to paused.
        if (SIEquals(requestVerb, "FinishJob") && _hasPendingChildJobs(db, jobID)) {
            // Update the parent job to PAUSED. Also update its nextRun: in case it has a retryAfter, GetJobs set the nextRun too far in the future (to account for retryAfter), so set it to what it should
            // be now that it is waiting on its children to complete.
            SINFO("Job has child jobs, PAUSING parent, QUEUING children");
            if (!db.writeIdempotent("UPDATE jobs SET state='PAUSED', nextRun=" + SQ(lastRun) + " WHERE jobID=" + SQ(jobID) + ";")) {
                STHROW("502 Parent update failed");
            }

            // Also un-pause any child jobs such that they can run
            if (!db.writeIdempotent("UPDATE jobs SET state='QUEUED' "
                          "WHERE state='PAUSED' "
                            "AND parentJobID != 0 AND parentJobID=" + SQ(jobID) + ";")) {
                STHROW("502 Child update failed");
            }

            // All done processing this command
            return;
        }

        // If this is RetryJob and we want to update the name and/or priority, let's do that
        const string& name = request["name"];
        if (SIEquals(requestVerb, "RetryJob")) {
            list<string> updates;
            if (!name.empty()) {
                updates.push_back("name=" + SQ(name) + " ");
            }
            if (request.isSet("jobPriority")) {
                _validatePriority(request.calc64("jobPriority"));
                updates.push_back("priority=" + SQ(request["jobPriority"]) + " ");
            }
            if (!updates.empty()) {
                bool success = db.writeIdempotent("UPDATE jobs SET " + SComposeList(updates, ", ") + " WHERE jobID=" + SQ(jobID) + ";");
                if (!success) {
                    STHROW("502 Failed to update job name/priority");
                }
            }
        }

        const string safeNewNextRun = calculateNextRun();

        // The job is set to be rescheduled.
        if (!safeNewNextRun.empty()) {
            // The "nextRun" at this point is still
            // storing the last time this job was *scheduled* to be run;
            // lastRun contains when it was *actually* run.
            SINFO("Rescheduling job#" << jobID << ": " << safeNewNextRun);

            // Update this job
            if (!db.writeIdempotent("UPDATE jobs SET nextRun=" + safeNewNextRun + ", state='QUEUED' WHERE jobID=" + SQ(jobID) + ";")) {
                STHROW("502 Update failed");
            }
        } else {
            // We are done with this job.  What do we do with it?
            SASSERT(!SIEquals(requestVerb, "RetryJob"));
            if (parentJobID) {
                // This is a child job.  Mark it as finished.
                if (!db.writeIdempotent("UPDATE jobs SET state='FINISHED' WHERE jobID=" + SQ(jobID) + ";")) {
                    STHROW("502 Failed to mark job as FINISHED");
                }

                // Resume the parent if this is the last pending child
                if (!_hasPendingChildJobs(db, parentJobID)) {
                    SINFO("Job has parentJobID: " + SToStr(parentJobID) +
                          " and no other pending children, resuming parent job");
                    if (!db.writeIdempotent("UPDATE jobs SET state='QUEUED' where jobID=" + SQ(parentJobID) + ";")) {
                        STHROW("502 Update failed");
                    }
                }
            } else {
                // This is a standalone (not a child) job; delete it.
                if (!db.writeIdempotent("DELETE FROM jobs WHERE jobID=" + SQ(jobID) + ";")) {
                    STHROW("502 Delete failed");
                }

                // At this point, all child jobs should already be deleted, but
                // let's double check.
                if (!db.read("SELECT 1 FROM jobs WHERE parentJobID != 0 AND parentJobID=" + SQ(jobID) + " LIMIT 1;").empty()) {
                    STHROW("405 Failed to delete a job with outstanding children");
                }
            }
        }

        // Successfully processed
        return;
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "CancelJob")) {
        // - CancelJob (jobID)
        //
        //     Cancel a QUEUED, RUNQUEUED, FAILED child job.
        //
        //     Parameters:
        //     - jobID  - ID of the job to cancel
        //
        int64_t jobID = request.calc64("jobID");

        // Cancel the job
        if (!db.writeIdempotent("UPDATE jobs SET state='CANCELLED' WHERE jobID=" + SQ(jobID) + ";")) {
            STHROW("502 Failed to update job data");
        }

        // If this was the last queued child, resume the parent
        SQResult result;
        if (!db.read("SELECT parentJobID "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(jobID) + ";",
                     result)) {
            STHROW("502 Select failed");
        }
        const string& safeParentJobID = SQ(result[0][0]);
        if (!db.read("SELECT count(1) "
                     "FROM jobs "
                     "WHERE parentJobID != 0 AND parentJobID=" + safeParentJobID + " AND "
                       "state IN ('QUEUED', 'RUNQUEUED', 'RUNNING');",
                     result)) {
            STHROW("502 Select failed");
        }
        if (SToInt64(result[0][0]) == 0) {
            SINFO("Cancelled last QUEUED child, resuming the parent: " << safeParentJobID);
            if (!db.writeIdempotent("UPDATE jobs SET state='QUEUED' WHERE jobID=" + safeParentJobID + ";")) {
                STHROW("502 Failed to update job data");
            }
        }

        // All done processing this command
        return;
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(requestVerb, "FailJob")) {
        // - FailJob( jobID, [data], [expectedData] )
        //
        //     Fails a job.
        //
        //     Parameters:
        //     - jobID - ID of the job to fail
        //     - data  - Data to associate with this failed job
        //     - expectedData - Immutable data returned by GetJob/GetJobs. For an opted-in job, Bedrock preserves
        //                      newer data and immediately requeues the job when this snapshot no longer matches.
        //
        BedrockPlugin::verifyAttributeInt64(request, "jobID", 1);
        const int64_t jobID = request.calc64("jobID");

        // Verify there is a job like this and it's running
        SQResult result;
        if (!db.read("SELECT state, data, "
                     "JSON_TYPE(data, '$._bedrockRerunIfDataChanged') = 'true' "
                     "FROM jobs "
                     "WHERE jobID = " + SQ(jobID) + ";",
                     result)) {
            STHROW("502 Select failed");
        }
        if (result.empty()) {
            STHROW("404 No job with this jobID");
        }
        const string& state = result[0][0];
        const string& currentData = result[0][1];
        const bool rerunIfDataChanged = result[0][2] == "1";

        // Make sure we're failing a job that's actually running or running with a retryAfter
        if (state != "RUNNING" && state != "RUNQUEUED") {
            SINFO("Trying to fail job#" << request["jobID"] << ", but isn't RUNNING or RUNQUEUED (" << state << ")");
            STHROW("405 Can only fail RUNNING or RUNQUEUED jobs");
        }

        if (rerunIfDataChanged && request.isSet("expectedData")) {
            BedrockPlugin::verifyAttributeSize(request, "expectedData", 1, BedrockPlugin_Jobs::MAX_SIZE_BLOB);
            if (SParseJSONObject(request["expectedData"]).empty() && request["expectedData"] != "{}") {
                STHROW("402 expectedData is not a valid JSON Object");
            }
            if (!SJSONEquals(currentData, request["expectedData"], IGNORED_BEDROCK_JOB_DATA_KEYS)) {
                // The caller-owned payload changed after this worker dequeued the job. Preserve it and run it rather
                // than allowing the older worker's fatal outcome or data to strand the job in FAILED.
                SINFO("Requeueing rerun-if-data-changed job#" << jobID
                      << " after FailJob because its data changed while the worker was running");
                if (!db.writeIdempotent("UPDATE jobs SET state = 'QUEUED', nextRun = " + SQ(SCURRENT_TIMESTAMP_MS()) +
                                        ", data = JSON_REMOVE(data, '$.retryAfterCount')" +
                                        " WHERE jobID = " + SQ(jobID) + ";")) {
                    STHROW("502 Failed to requeue rerun-if-data-changed job");
                }
                return;
            }
        }

        // Are we updating the data too?
        list<string> updateList;
        if (request.isSet("data")) {
            // Update public data while preserving Bedrock's private opt-in marker.
            const string sanitizedRequestData = stripRerunIfDataChanged(request["data"]);
            const string newData = db.read("SELECT IIF(JSON_VALID(" + SQ(request["data"]) + "), "
                                           + SQ(sanitizedRequestData) + ", '{}');");
            updateList.push_back("data = " + preserveRerunIfDataChangedSQL(SQ(newData)));
        }

        // Not repeating; just finish
        updateList.push_back("state='FAILED'");

        // Update this job
        if (!db.writeIdempotent("UPDATE jobs SET " + SComposeList(updateList) + " WHERE jobID = " + SQ(jobID) + ";")) {
            STHROW("502 Fail failed");
        }

        // Successfully processed
        return;
    }
    // ----------------------------------------------------------------------
    else if (SIEquals(requestVerb, "DeleteJob")) {
        // - DeleteJob( jobID )
        //
        //     Deletes a given job.
        //
        //     Parameters:
        //     - jobID - ID of the job to delete
        //
        BedrockPlugin::verifyAttributeInt64(request, "jobID", 1);

        // Verify there is a job like this and it's not running
        SQResult result;
        if (!db.read("SELECT state "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(request.calc64("jobID")) + ";",
                     result)) {
            STHROW("502 Select failed");
        }
        if (result.empty()) {
            STHROW("404 No job with this jobID");
        }
        if (result[0][0] == "RUNNING") {
            STHROW("405 Can't delete a RUNNING job");
        }
        if (result[0][0] == "PAUSED") {
            STHROW("405 Can't delete a parent jobs with children running");
        }

        // Delete the job
        if (!db.writeIdempotent("DELETE FROM jobs "
                      "WHERE jobID=" +
                      SQ(request.calc64("jobID")) + ";")) {
            STHROW("502 Delete failed");
        }

        // Successfully processed
        return;
    }
    // Requeue a job for which a getJob(s) command could not complete.
    else if (SIEquals(requestVerb, "RequeueJobs")) {
        SINFO("Requeueing jobs with IDs: " << request["jobIDs"]);
        list<int64_t> jobIDs = SParseIntegerList(request["jobIDs"]);

        if (jobIDs.size()) {
            const string& name = request["name"];
            string nameQuery = name.empty() ? "" : ", name = " + SQ(name) + "";
            string decrementFailuresQuery;
            if (request.test("decrementFailures")) {
                decrementFailuresQuery = ", data = JSON_SET(data, '$.retryAfterCount', COALESCE(JSON_EXTRACT(data, '$.retryAfterCount'), 1) - 1)";
            }
            string updateQuery = "UPDATE jobs SET state = 'QUEUED', nextRun = created" + nameQuery + decrementFailuresQuery + " WHERE jobID IN(" + SQList(jobIDs) + ");";
            if (!db.writeIdempotent(updateQuery)) {
                STHROW("502 RequeueJobs update failed");
            }
        }

        return;
    }
}

string BedrockJobsCommand::_constructNextRunDATETIME(SQLite& db, const string& lastScheduled, const string& lastRun, const string& repeat)
{
    if (repeat.empty()) {
        return "";
    }

    // Some "canned" times for convenience
    if (SIEquals(repeat, "HOURLY")) {
        return "STRFTIME( '%Y-%m-%d %H:00:00', DATETIME( " + SCURRENT_TIMESTAMP() + ", '+1 HOUR' ) )";
    }
    if (SIEquals(repeat, "DAILY")) {
        return "DATETIME( " + SCURRENT_TIMESTAMP() + ", '+1 DAY', 'START OF DAY'  )";
    }
    if (SIEquals(repeat, "WEEKLY")) {
        return "DATETIME( " + SCURRENT_TIMESTAMP() + ", '+1 DAY', 'WEEKDAY 0', 'START OF DAY' )";
    }

    // Not canned, split the advanced repeat into its parts
    list<string> parts = SParseList(SToUpper(repeat));
    if (parts.size() < 2) {
        SWARN("Syntax error, failed parsing repeat '" << repeat << "': too short.");
        return "";
    }

    // Make sure the first part indicates the base (eg, what we are modifying)
    const string base = parts.front();
    parts.pop_front();
    string baseExpr;
    if (base == "SCHEDULED") {
        baseExpr = SQ(lastScheduled);
    } else if (base == "STARTED") {
        baseExpr = SQ(lastRun);
    } else if (base == "FINISHED") {
        baseExpr = SCURRENT_TIMESTAMP();
    } else {
        SWARN("Syntax error, failed parsing repeat '" << repeat << "': missing base (" << base << ")");
        return "";
    }

    // Apply the repeat's date modifiers to a base timestamp, returning the result as a quoted SQL
    // literal (or "" on a syntax error).
    auto applyModifiers = [&](const string& startExpr) -> string {
        string current = startExpr;
        for (const string& part : parts) {
            // This isn't supported natively by SQLite, so do it manually here instead.
            if (SToUpper(part) == "START OF HOUR") {
                SQResult result;
                if (!db.read("SELECT STRFTIME('%Y-%m-%d %H:00:00', " + current + ");", result) || result.empty()) {
                    SWARN("Syntax error, failed parsing repeat " + part);
                    return "";
                }

                current = SQ(result[0][0]);
            } else if (!SIsValidSQLiteDateModifier(part)) {
                // Validate the sqlite date modifiers
                SWARN("Syntax error, failed parsing repeat " + part);
                return "";
            } else {
                SQResult result;
                if (!db.read("SELECT DATETIME(" + current + ", " + SQ(part) + ");", result) || result.empty()) {
                    SWARN("Syntax error, failed parsing repeat " + part);
                    return "";
                }

                current = SQ(result[0][0]);
            }
        }
        return current;
    };

    string nextRun = applyModifiers(baseExpr);
    if (nextRun.empty()) {
        return "";
    }

    // After an outage, every missed SCHEDULED job becomes runnable at once (thundering herd). Skip the missed
    // cycles to each job's next phase slot to preserve stagger; leave long intervals for calendar-bound jobs.
    if (base == "SCHEDULED") {
        const string nowExpr = SCURRENT_TIMESTAMP();
        SQResult result;
        if (!db.read("SELECT " + nextRun + " <= " + nowExpr + ", "
                     "STRFTIME('%s', " + nextRun + ") - STRFTIME('%s', " + SQ(lastScheduled) + "), "
                     "STRFTIME('%s', " + nowExpr + ") - STRFTIME('%s', " + SQ(lastScheduled) + ");", result) || result.empty()) {
            SWARN("Failed evaluating SCHEDULED repeat staleness for '" << repeat << "'");
            return "";
        }
        const bool missedWindow = result[0][0] == "1";
        const int64_t intervalSeconds = SToInt64(result[0][1]);
        const int64_t secondsBehind = SToInt64(result[0][2]);

        // Only worth re-anchoring if the window was missed and the interval is short enough.
        if (missedWindow && intervalSeconds > 0 && intervalSeconds < REPEAT_REANCHOR_THRESHOLD_SECONDS) {
            // Only fixed-offset modifiers (eg "+30 SECONDS") have a constant period we can skip by arithmetic.
            // Snapping modifiers like "START OF HOUR" round the result, so their realized delta isn't the period;
            // leave those to their own (coarse, low-volume) catch-up rather than re-anchor them off-schedule.
            bool fixedInterval = true;
            for (const string& part : parts) {
                if (part.empty() || (part.front() != '+' && part.front() != '-')) {
                    fixedInterval = false;
                    break;
                }
            }

            if (fixedInterval) {
                // Jump whole intervals past lastScheduled to the first slot after now, keeping each job's phase.
                const int64_t skipSeconds = (secondsBehind / intervalSeconds + 1) * intervalSeconds;
                if (!db.read("SELECT DATETIME(" + SQ(lastScheduled) + ", '+" + SToStr(skipSeconds) + " SECONDS');", result) || result.empty()) {
                    SWARN("Failed re-anchoring SCHEDULED repeat for '" << repeat << "'");
                    return "";
                }
                nextRun = SQ(result[0][0]);
            }
        }
    }

    return "DATETIME(" + nextRun + ")";
}

// ==========================================================================

bool BedrockJobsCommand::_hasPendingChildJobs(SQLite& db, int64_t jobID)
{
    // Returns true if there are any children of this jobID in a "pending" (eg,
    // running or yet to run) state
    SQResult result;
    if (!db.read("SELECT 1 "
                 "FROM jobs "
                 "WHERE parentJobID != 0 AND parentJobID = " + SQ(jobID) + " " +
                 " AND state IN ('QUEUED', 'RUNQUEUED', 'RUNNING', 'PAUSED') "
                 "LIMIT 1;",
                 result)) {
        STHROW("502 Select failed");
    }
    return !result.empty();
}

void BedrockJobsCommand::_validatePriority(const int64_t priority)
{
    // We'd initially intended for any value to be allowable here, but for
    // performance reasons, we currently will only allow specific values to
    // try and keep queries fast. If you pass an invalid value, we'll throw
    // here so that the caller can know that he did something wrong rather
    // than having his job sit unprocessed in the queue forever. Hopefully
    // we can remove this restriction in the future.
    list<int64_t> validPriorities = {0, 250, 500, 750, 850, 1000};
    if (!SContains(validPriorities, priority)) {
        STHROW("402 Invalid priority value");
    }
}

void BedrockJobsCommand::_handleFailedRetryAfterQuery(SQLite& db, const string& jobID)
{
    SALERT("ENSURE_BUGBOT Query error when updating job with retryAfter. JobID: " << jobID);
    if (!db.writeIdempotent("UPDATE jobs "
                            "SET state = 'FAILED' "
                            "WHERE jobID = " + SQ(jobID) + ";")) {
        STHROW("502 Update failed");
    }
}

void BedrockJobsCommand::handleFailedReply()
{
    if (SIEquals(request.methodLine, "GetJob") || SIEquals(request.methodLine, "GetJobs")) {
        list<string> jobIDs;
        if (SIEquals(request.methodLine, "GetJob")) {
            STable jobJSON = SParseJSONObject(response.content);
            if (jobJSON.find("jobID") != jobJSON.end()) {
                jobIDs.push_back(jobJSON["jobID"]);
            }
        } else {
            STable jobsJSON = SParseJSONObject(response.content);
            list<string> jobs = SParseJSONArray(jobsJSON["jobs"]);
            for (auto& job : jobs) {
                STable jobJSON = SParseJSONObject(job);
                if (jobJSON.find("jobID") != jobJSON.end()) {
                    jobIDs.push_back(jobJSON["jobID"]);
                }
            }
        }
        SINFO("Failed sending response to '" << request.methodLine << "', re-queueing jobs: " << SComposeList(jobIDs));
        SData requeue("RequeueJobs");
        requeue["jobIDs"] = SComposeList(jobIDs);
        requeue["decrementFailures"] = "true";

        // Keep the request ID so we'll be able to associate these in the logs.
        requeue["requestID"] = request["requestID"];
        auto cmd = make_unique<BedrockJobsCommand>(SQLiteCommand(move(requeue)), _plugin);
        cmd->initiatingClientID = -1;
        _plugin->server.runCommand(move(cmd));
    }
}

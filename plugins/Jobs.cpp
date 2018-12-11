#include "Jobs.h"
#include "../BedrockServer.h"

atomic<BedrockPlugin_Jobs*> BedrockPlugin_Jobs::_instance(nullptr);
BedrockServer* BedrockPlugin_Jobs::_server = nullptr;

#undef SLOGPREFIX
#define SLOGPREFIX "{" << _instance.load()->getName() << "} "

int64_t BedrockPlugin_Jobs::getNextID(SQLite& db)
{
    int64_t newID = 0;
    while (!newID) {
        // Make sure this fits even in a signed int64_t, and is positive.
        newID = SRandom::rand64();
        if (newID < 0) {
            newID = -newID;
        }
        newID %= INT64_MAX;
        string result = db.read( "SELECT jobID FROM jobs WHERE jobID = " + to_string(newID) + ";");
        if (!result.empty()) {
            // This one exists! Pick a new one.
            newID = 0;
        }
    }
    return newID;
}

void BedrockPlugin_Jobs::upgradeDatabase(SQLite& db) {
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
                               "priority    INTEGER NOT NULL DEFAULT " + to_string(JOBS_DEFAULT_PRIORITY) + ", "
                               "parentJobID INTEGER NOT NULL DEFAULT 0, "
                               "retryAfter  TEXT NOT NULL DEFAULT \"\")",
                           ignore));

    // These indexes are not used by the Bedrock::Jobs plugin, but provided for easy analysis
    // using the Bedrock::DB plugin.
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsName     ON jobs ( name     );"));
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsParentJobIDState ON jobs ( parentJobID, state ) WHERE parentJobID != 0;"));
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsStatePriorityNextRunName ON jobs ( state, priority, nextRun, name );"));
}

bool BedrockPlugin_Jobs::peekCommand(SQLite& db, BedrockCommand& command) {
    const string requestVerb = command.request.getVerb();

    // Each command is unique, so if the command causes a crash, we'll identify it on a unique random number.
    command.request["crashID"] = to_string(SRandom::rand64());
    command.crashIdentifyingValues.insert("crashID");

    // Reset the content object. It could have been written by a previous call to this function that conflicted.
    command.jsonContent.clear();
    command.response.clear();

    if (SIEquals(requestVerb, "CancelJob")) return peekCancelJob(db, command);
    if (SIEquals(requestVerb, "CreateJob")) return peekCreateJob(db, command);
    if (SIEquals(requestVerb, "CreateJobs")) return peekCreateJobs(db, command);
    if (SIEquals(requestVerb, "GetJob")) return peekGetJob(db, command);
    if (SIEquals(requestVerb, "GetJobs")) return peekGetJobs(db, command);
    if (SIEquals(requestVerb, "QueryJob")) return peekQueryJob(db, command);

    // Didn't recognize this command
    return false;
}

bool BedrockPlugin_Jobs::peekCancelJob(SQLite& db, BedrockCommand& command) {
    verifyAttributeInt64(command.request, "jobID", 1);
    int64_t jobID = command.request.calc64("jobID");

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

        // Done.
        return true;
    }

    // Verify that we are not trying to cancel a PAUSED job.
    if (result[0][1] == "PAUSED") {
        STHROW("402 Can't cancel PAUSED job"); 

        // Done.
        return true;
    }

    // Need to process command
    return false;
}

void BedrockPlugin_Jobs::peekCreateCommon(SQLite& db, BedrockCommand& command, list<STable>& jsonJobs) {
    for (auto& job : jsonJobs) {
        // If no priority set, set it
        int64_t priority = SContains(job, "jobPriority") ? SToInt(job["jobPriority"]) : JOBS_DEFAULT_PRIORITY;

        // We'd initially intended for any value to be allowable here, but for
        // performance reasons, we currently will only allow specific values to
        // try and keep queries fast. If you pass an invalid value, we'll throw
        // here so that the caller can know that he did something wrong rather
        // than having his job sit unprocessed in the queue forever. Hopefully
        // we can remove this restriction in the future.
        if (priority != 0 && priority != 500 && priority != 1000) {
            STHROW("402 Invalid priority value");
        }

        // Throw if data is not a valid JSON object, otherwise UPDATE query will fail.
        if (SContains(job, "data") && SParseJSONObject(job["data"]).empty() && job["data"] != "{}") {
            STHROW("402 Data is not a valid JSON Object");
        }

        // Validate retryAfter
        if (SContains(job, "retryAfter") && job["retryAfter"] != "" && !_isValidSQLiteDateModifier(job["retryAfter"])){
            STHROW("402 Malformed retryAfter");
        }

        // Validate that the parentJobID exists and is in the right state if one was passed.
        // Also verify that the parent job doesn't have a retryAfter set.
        int64_t parentJobID = SContains(job, "parentJobID") ? SToInt64(job["parentJobID"]) : 0;
        if (parentJobID) {
            SINFO("parentJobID passed, checking existing job with ID " << parentJobID);
            SQResult result;
            if (!db.read("SELECT state, retryAfter, data FROM jobs WHERE jobID=" + SQ(parentJobID) + ";", result)) {
                STHROW("502 Select failed");
            }
            if (result.empty()) {
                STHROW("404 parentJobID does not exist");
            }
            if (result[0][1] != "") {
                STHROW("402 Auto-retrying parents cannot have children");
            }
            if (!SIEquals(result[0][0], "RUNNING") && !SIEquals(result[0][0], "PAUSED")) {
                SWARN("Trying to create child job with parent jobID#" << parentJobID << ", but parent isn't RUNNING or PAUSED (" << result[0][0] << ")");
                STHROW("405 Can only create child job when parent is RUNNING or PAUSED");
            }

            // Verify that the parent and child job have the same `mockRequest` setting, update them to match if
            // not. Note that this is the first place we'll look at `mockRequest` while handling this command so
            // any change made here will happen early enough for all of our existing checks to work correctly, and
            // everything should be good when we get to `processCommand`.
            STable parentData = SParseJSONObject(result[0][2]);
            bool parentIsMocked = parentData.find("mockRequest") != parentData.end();
            bool childIsMocked = command.request.isSet("mockRequest");

            if (parentIsMocked && !childIsMocked) {
                command.request["mockRequest"] = "true";
                SINFO("Setting child job to mocked to match parent.");
            } else if (!parentIsMocked && childIsMocked) {
                command.request.erase("mockRequest");
                SINFO("Setting child job to non-mocked to match parent.");
            }
        }
    }
}

bool BedrockPlugin_Jobs::peekCreateJob(SQLite& db, BedrockCommand& command) {
    verifyAttributeSize(command.request, "name", 1, MAX_SIZE_SMALL);
    list<STable> jsonJobs;
    jsonJobs.push_back(command.request.nameValueMap);
    peekCreateCommon(db, command, jsonJobs);
    auto& job = jsonJobs.front();

    // If this is a unique job that already exists, we won't need to escalate it.
    if (SContains(job, "unique") && job["unique"] == "true") {
        SINFO("Unique flag was passed, checking existing job with name " << job["name"] << ", mocked? " << (command.request.isSet("mockRequest") ? "true" : "false"));
        SQResult result;
        string operation = command.request.isSet("mockRequest") ? "IS NOT" : "IS";
        if (!db.read("SELECT jobID, data "
                     "FROM jobs "
                     "WHERE name=" + SQ(job["name"]) +
                     "  AND JSON_EXTRACT(data, '$.mockRequest') " + operation + " NULL;",
                     result)) {
            STHROW("502 Select failed");
        }

        // If we found a job, and our it matches the passed-in data (or there was no passed-in data), we're done.
        if (!result.empty() && ((job["data"].empty() && result[0][1] == "{}") || (!job["data"].empty() && result[0][1] == job["data"]))) {
            SINFO("Job already existed and unique flag was passed, reusing existing job " << result[0][0] << ", mocked? " << (command.request.isSet("mockRequest") ? "true" : "false"));
            command.jsonContent["jobID"] = result[0][0];
            return true;
        }
    }

    // Escalate to write.
    return false;
}

bool BedrockPlugin_Jobs::peekCreateJobs(SQLite& db, BedrockCommand& command) {
    list<STable> jsonJobs;
    list<string> multipleJobs;
    multipleJobs = SParseJSONArray(command.request["jobs"]);
    if (multipleJobs.empty()) {
        STHROW("401 Invalid JSON");
    }
    for (auto& job : multipleJobs) {
        STable jobObject = SParseJSONObject(job);
        if (jobObject.empty()) {
            STHROW("401 Invalid JSON");
        }
        if (!SContains(job, "name")) {
            STHROW("402 Missing name");
        }
        jsonJobs.push_back(jobObject);
    }
    peekCreateCommon(db, command, jsonJobs);

    // Escalate to write.
    return false;
}

void BedrockPlugin_Jobs::peekGetCommon(SQLite& db, BedrockCommand& command) {
    verifyAttributeSize(command.request, "name", 1, MAX_SIZE_NAME);

    // Get the list
    SQResult result;
    const list<string> nameList = SParseList(command.request["name"]);
    bool mockRequest = command.request.isSet("mockRequest") || command.request.isSet("getMockedJobs");
    if (!db.read("SELECT 1 "
                 "FROM jobs "
                 "WHERE state in ('QUEUED', 'RUNQUEUED') "
                    "AND priority IN (0, 500, 1000) "
                    "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                    "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(command.request["name"])) + " " + 
                    string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                 "LIMIT 1;",
                 result)) {
        STHROW("502 Query failed");
    }

    if (result.empty()) {
        STHROW("404 No job found");
    }
}

bool BedrockPlugin_Jobs::peekGetJob(SQLite& db, BedrockCommand& command) {
    if (command.request.isSet("numResults")) {
        STHROW("402 Cannot use numResults with GetJob; try GetJobs");
    }
    peekGetCommon(db, command);
    return false;
}

bool BedrockPlugin_Jobs::peekGetJobs(SQLite& db, BedrockCommand& command) {
    if (!command.request.isSet("numResults")) {
        STHROW("402 Missing numResults");
    }
    peekGetCommon(db, command);
    return false;
}

bool BedrockPlugin_Jobs::peekQueryJob(SQLite& db, BedrockCommand& command) {
    verifyAttributeInt64(command.request, "jobID", 1);
    SQResult result;
    if (!db.read("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, retryAfter, priority "
                 "FROM jobs "
                 "WHERE jobID=" + SQ(command.request.calc64("jobID")) + ";",
                 result)) {
        STHROW("502 Select failed");
    }
    if (result.empty()) {
        STHROW("404 No job with this jobID");
    }
    command.jsonContent["created"] = result[0][0];
    command.jsonContent["jobID"] = result[0][1];
    command.jsonContent["state"] = result[0][2];
    command.jsonContent["name"] = result[0][3];
    command.jsonContent["nextRun"] = result[0][4];
    command.jsonContent["lastRun"] = result[0][5];
    command.jsonContent["repeat"] = result[0][6];
    command.jsonContent["data"] = result[0][7];
    command.jsonContent["retryAfter"] = result[0][8];
    command.jsonContent["priority"] = result[0][9];
    
    // Done.
    return true;
}

bool BedrockPlugin_Jobs::processCommand(SQLite& db, BedrockCommand& command) {
    // Disable noop update mode for jobs.
    scopedDisableNoopMode disable(db);

    // Pull out some helpful variables
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;
    const string& requestVerb = request.getVerb();

    // Reset the content object. It could have been written by a previous call to this function that conflicted in
    // multi-write.
    command.jsonContent.clear();
    command.response.clear();

    if (SIEquals(requestVerb, "CreateJob")) return processCreateJob(db, command);
    if (SIEquals(requestVerb, "CreateJobs")) return processCreateJobs(db, command);

    // ----------------------------------------------------------------------
    if (SIEquals(requestVerb, "GetJob") || SIEquals(requestVerb, "GetJobs")) {
        // If we're here it's because peekCommand found some data; re-execute
        // the query for real now.  However, this time we will order by
        // priority.  We do this as three separate queries so we only have one
        // unbounded column in each query.  Additionally, we wrap each inner
        // query in a "SELECT *" such that we can have an "ORDER BY" and
        // "LIMIT" *before* we UNION ALL them together.  Looks gnarly, but it
        // works!
        SQResult result;
        const list<string> nameList = SParseList(request["name"]);
        string safeNumResults = SQ(max(request.calc("numResults"),1));
        bool mockRequest = command.request.isSet("mockRequest") || command.request.isSet("getMockedJobs");
        string selectQuery =
            "SELECT jobID, name, data, parentJobID, retryAfter, created, repeat, lastRun, nextRun FROM ( "
                "SELECT * FROM ("
                    "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                    "FROM jobs "
                    "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                        "AND priority=1000 "
                        "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
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
                        "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
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
                        "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                        "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(request["name"])) + " " +
                        string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                    "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
            ") "
            "ORDER BY priority DESC "
            "LIMIT " + safeNumResults + ";";
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

        // There should only be at most one result if GetJob
        SASSERT(!SIEquals(requestVerb, "GetJob") || result.size()<=1);

        // Prepare to update the rows, while also creating all the child objects
        list<string> nonRetriableJobs;
        list<STable> retriableJobs;
        list<string> jobList;
        for (size_t c=0; c<result.size(); ++c) {
            SASSERT(result[c].size() == 9); // jobID, name, data, parentJobID, retryAfter, created, repeat, lastRun, nextRun

            // Add this object to our output
            STable job;
            SINFO("Returning jobID " << result[c][0] << " from " << requestVerb);
            job["jobID"] = result[c][0];
            job["name"] = result[c][1];
            job["data"] = result[c][2];
            job["created"] = result[c][5];
            int64_t parentJobID = SToInt64(result[c][3]);

            if (parentJobID) {
                // Has a parent job, add the parent data
                job["parentJobID"] = SToStr(parentJobID);
                job["parentData"] = db.read("SELECT data FROM jobs WHERE jobID=" + SQ(parentJobID) + ";");
            }

            // Add jobID to the respective list depending on if retryAfter is set
            if (result[c][4] != "") {
                job["retryAfter"] = result[c][4];
                job["repeat"] = result[c][6];
                job["lastRun"] = result[c][7];
                job["nextRun"] = result[c][8];
                retriableJobs.push_back(job);
            } else {
                nonRetriableJobs.push_back(result[c][0]);

                // Only non-retryable jobs can have children so see if this job has any
                // FINISHED/CANCELLED child jobs, indicating it is being resumed
                SQResult childJobs;
                if (!db.read("SELECT jobID, data, state FROM jobs WHERE parentJobID != 0 AND parentJobID=" + result[c][0] + " AND state IN ('FINISHED', 'CANCELLED');", childJobs)) {
                    STHROW("502 Failed to select finished child jobs");
                }

                if (!childJobs.empty()) {
                    // Add associative arrays of all children depending on their states
                    list<string> finishedChildJobArray;
                    list<string> cancelledChildJobArray;
                    for (auto row : childJobs.rows) {
                        STable childJob;
                        childJob["jobID"] = row[0];
                        childJob["data"] = row[1];

                        if (row[2] ==  "FINISHED") {
                            finishedChildJobArray.push_back(SComposeJSONObject(childJob));
                        } else {
                            cancelledChildJobArray.push_back(SComposeJSONObject(childJob));
                        }
                    }
                    job["finishedChildJobs"] = SComposeJSONArray(finishedChildJobArray);
                    job["cancelledChildJobs"] = SComposeJSONArray(cancelledChildJobArray);
                }
            }

            jobList.push_back(SComposeJSONObject(job));
        }

        // Update jobs without retryAfter
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

        // Update jobs with retryAfter
        if (!retriableJobs.empty()) {
            SINFO("Updating jobs with retryAfter");
            for (auto job : retriableJobs) {
                string currentTime = SCURRENT_TIMESTAMP();
                string retryAfterDateTime = "DATETIME(" + currentTime + ", " + SQ(job["retryAfter"]) + ")";
                string repeatDateTime = _constructNextRunDATETIME(job["nextRun"], job["lastRun"] != "" ? job["lastRun"] : job["nextRun"], job["repeat"]);
                string nextRunDateTime = repeatDateTime != "" ? "MIN(" + retryAfterDateTime + ", " + repeatDateTime + ")" : retryAfterDateTime;
                string updateQuery = "UPDATE jobs "
                                     "SET state='RUNQUEUED', "
                                         "lastRun=" + currentTime + ", "
                                         "nextRun=" + nextRunDateTime + " "
                                     "WHERE jobID = " + SQ(job["jobID"]) + ";";
                if (!db.writeIdempotent(updateQuery)) {
                    STHROW("502 Update failed");
                }
            }
        }

        // Format the results as is appropriate for what was requested
        if (SIEquals(requestVerb, "GetJob")) {
            // Single response
            SASSERT(jobList.size() == 1);
            response.content = jobList.front();
        } else {
            // Multiple responses
            SASSERT(SIEquals(requestVerb, "GetJobs"));
            content["jobs"] = SComposeJSONArray(jobList);
        }
        return true; // Successfully processed
    }

    else if (SIEquals(requestVerb, "UpdateJob")) {
        // - UpdateJob( jobID, data, [repeat] )
        //
        //     Atomically updates the data associated with a job.
        //
        //     Parameters:
        //     - jobID - ID of the job to delete
        //     - data  - A JSON object describing work to be done
        //     - repeat - A description of how to repeat (optional)
        //
        verifyAttributeInt64(request, "jobID", 1);
        verifyAttributeSize(request, "data", 1, MAX_SIZE_BLOB);

        // If a repeat is provided, validate it
        if (request.isSet("repeat")) {
            if (request["repeat"].empty()) {
                SWARN("Repeat is set in UpdateJob, but is set to the empty string. jobID: "
                      << request["jobID"] << ", removing attribute.");
                request.erase("repeat");
            } else if (!_validateRepeat(request["repeat"])) {
                STHROW("402 Malformed repeat");
            }
        }

        // Verify there is a job like this
        SQResult result;
        if (!db.read("SELECT jobID, nextRun, lastRun "
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

        // Are we rescheduling?
        const string& newNextRun = request.isSet("repeat") ? _constructNextRunDATETIME(nextRun, lastRun, request["repeat"]) : "";

        // Update the data
        if (!db.writeIdempotent("UPDATE jobs "
                                "SET data=" +
                                SQ(request["data"]) + " " +
                                (request.isSet("repeat") ? ", repeat=" + SQ(SToUpper(request["repeat"])) : "") +
                                (!newNextRun.empty() ? ", nextRun=" + newNextRun : "") +
                                "WHERE jobID=" +
                                SQ(request.calc64("jobID")) + ";")) {
            STHROW("502 Update failed");
        }
        return true; // Successfully processed
    }

    else if (SIEquals(requestVerb, "RetryJob") || SIEquals(requestVerb, "FinishJob")) {
        // - RetryJob( jobID, [delay], [nextRun], [name], [data] )
        //
        //     Re-queues a RUNNING job.
        //     The nextRun logic for the job is decided in the following way
        //      - If the job is configured to "repeat" it will schedule
        //     the job for the next repeat time.
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
        //     - jobID   - ID of the job to requeue
        //     - delay   - Number of seconds to wait before retrying
        //     - nextRun - datetime of next scheduled run
        //     - name    - An arbitrary string identifier (case insensitive)
        //     - data    - Data to associate with this job
        //
        // - FinishJob( jobID, [data] )
        //
        //     Finishes a job.  If it's set to recur, reschedules it. If
        //     not recurring, deletes it.
        //
        //     Parameters:
        //     - jobID  - ID of the job to finish
        //     - data   - Data to associate with this finsihed job
        //
        verifyAttributeInt64(request, "jobID", 1);
        int64_t jobID = request.calc64("jobID");

        // Verify there is a job like this and it's running
        SQResult result;
        if (!db.read("SELECT state, nextRun, lastRun, repeat, parentJobID, json_extract(data, '$.mockRequest') "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(jobID) + ";",
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
        bool mockRequest = result[0][5] == "1";

        // Make sure we're finishing a job that's actually running
        if (state != "RUNNING" && state != "RUNQUEUED" && !mockRequest) {
            SINFO("Trying to finish job#" << jobID << ", but isn't RUNNING or RUNQUEUED (" << state << ")");
            STHROW("405 Can only retry/finish RUNNING and RUNQUEUED jobs");
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

            // Update the data to the new value.
            if (!db.writeIdempotent("UPDATE jobs SET data=" + SQ(data) + " WHERE jobID=" + SQ(jobID) + ";")) {
                STHROW("502 Failed to update job data");
            }
        }

        // If we are finishing a job that has child jobs, set its state to paused.
        if (SIEquals(requestVerb, "FinishJob") && _hasPendingChildJobs(db, jobID)) {
            // Update the parent job to PAUSED
            SINFO("Job has child jobs, PAUSING parent, QUEUING children");
            if (!db.writeIdempotent("UPDATE jobs SET state='PAUSED' WHERE jobID=" + SQ(jobID) + ";")) {
                STHROW("502 Parent update failed");
            }

            // Also un-pause any child jobs such that they can run
            if (!db.writeIdempotent("UPDATE jobs SET state='QUEUED' "
                          "WHERE state='PAUSED' "
                            "AND parentJobID != 0 AND parentJobID=" + SQ(jobID) + ";")) {
                STHROW("502 Child update failed");
            }

            // All done processing this command
            return true;
        }

        // If this is RetryJob and we want to update the name, let's do that
        const string& name = request["name"];
        if (!name.empty() && SIEquals(requestVerb, "RetryJob")) {
            if (!db.writeIdempotent("UPDATE jobs SET name=" + SQ(name) + " WHERE jobID=" + SQ(jobID) + ";")) {
                STHROW("502 Failed to update job name");
            }
        }

        string safeNewNextRun = "";
        // If this is set to repeat, get the nextRun value
        if (!repeat.empty()) {
            safeNewNextRun = _constructNextRunDATETIME(nextRun, lastRun, repeat);
        } else if (SIEquals(requestVerb, "RetryJob")) {
            const string& newNextRun = request["nextRun"];

            if (newNextRun.empty()) {
                SINFO("nextRun isn't set, using delay");
                int64_t delay = request.calc64("delay");
                if (delay < 0) {
                    STHROW("402 Must specify a non-negative delay when retrying");
                }
                repeat = "FINISHED, +" + SToStr(delay) + " SECONDS";
                safeNewNextRun = _constructNextRunDATETIME(nextRun, lastRun, repeat);
                if (safeNewNextRun.empty()) {
                    STHROW("402 Malformed delay");
                }
            } else {
                safeNewNextRun = SQ(newNextRun);
            }
        }

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
                    SWARN("Child jobs still exist when deleting parent job, ignoring.");
                }
            }
        }

        // Successfully processed
        return true;
    } else if (SIEquals(request.methodLine, "CancelJob")) {
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
        return true;
    } else if (SIEquals(requestVerb, "FailJob")) {
        // - FailJob( jobID, [data] )
        //
        //     Fails a job.
        //
        //     Parameters:
        //     - jobID - ID of the job to fail
        //     - data  - Data to associate with this failed job
        //
        verifyAttributeInt64(request, "jobID", 1);

        // Verify there is a job like this and it's running
        SQResult result;
        if (!db.read("SELECT state, nextRun, lastRun, repeat "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(request.calc64("jobID")) + ";",
                     result)) {
            STHROW("502 Select failed");
        }
        if (result.empty()) {
            STHROW("404 No job with this jobID");
        }
        const string& state = result[0][0];

        // Make sure we're failing a job that's actually running
        if (state != "RUNNING") {
            SINFO("Trying to fail job#" << request["jobID"] << ", but isn't RUNNING (" << state << ")");
            STHROW("405 Can only fail RUNNING jobs");
        }

        // Are we updating the data too?
        list<string> updateList;
        if (request.isSet("data")) {
            // Update the data too
            updateList.push_back("data=" + SQ(request["data"]));
        }

        // Not repeating; just finish
        updateList.push_back("state='FAILED'");

        // Update this job
        if (!db.writeIdempotent("UPDATE jobs SET " + SComposeList(updateList) + "WHERE jobID=" + SQ(request.calc64("jobID")) + ";")) {
            STHROW("502 Fail failed");
        }

        // Successfully processed
        return true;
    }

    else if (SIEquals(requestVerb, "DeleteJob")) {
        // - DeleteJob( jobID )
        //
        //     Deletes a given job.
        //
        //     Parameters:
        //     - jobID - ID of the job to delete
        //
        verifyAttributeInt64(request, "jobID", 1);

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
        return true;
    }

    // Requeue a job for which a getJob(s) command could not complete.
    else if (SIEquals(requestVerb, "RequeueJobs")) {
        SINFO("Requeueing jobs with IDs: " << command.request["jobIDs"]);
        list<int64_t> jobIDs = SParseIntegerList(command.request["jobIDs"]);
        if (jobIDs.size()) {
            string updateQuery = "UPDATE jobs SET state = 'QUEUED', nextRun = DATETIME("+ SCURRENT_TIMESTAMP() + ") WHERE jobID IN(" + SQList(jobIDs)+ ");";
            if (!db.writeIdempotent(updateQuery)) {
                STHROW("502 RequeueJobs update failed");
            }
        }

        return true;
    }

    // Didn't recognize this command
    return false;
}


bool BedrockPlugin_Jobs::processCancelJob(SQLite& db, BedrockCommand& command) {
    return true;
}

list<string> BedrockPlugin_Jobs::processCreateCommon(SQLite& db, BedrockCommand& command, list<STable>& jsonJobs) {
    list<string> jobIDs;
    for (auto& job : jsonJobs) {
        // If unique flag was passed and the job exist in the DB, then we can finish the command without escalating to
        // master.

        // If this is a mock request, we insert that into the data.
        string originalData = job["data"];
        if (command.request.isSet("mockRequest")) {
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
        if (SContains(job, "unique") && job["unique"] == "true") {
            SQResult result;
            SINFO("Unique flag was passed, checking existing job with name " << job["name"] << ", mocked? "
                  << (command.request.isSet("mockRequest") ? "true" : "false"));
            string operation = command.request.isSet("mockRequest") ? "IS NOT" : "IS";
            if (!db.read("SELECT jobID, data "
                         "FROM jobs "
                         "WHERE name=" + SQ(job["name"]) +
                         "  AND JSON_EXTRACT(data, '$.mockRequest') " + operation + " NULL;",
                         result)) {
                STHROW("502 Select failed");
            }

            // If we got a result, and it's data is the same as passed, we won't change anything.
            if (!result.empty() && ((job["data"].empty() && result[0][1] == "{}") || (!job["data"].empty() && result[0][1] == job["data"]))) {
                SINFO("Job already existed with matching data, and unique flag was passed, reusing existing job "
                      << result[0][0] << ", mocked? " << (command.request.isSet("mockRequest") ? "true" : "false"));

                // Append new jobID to list of created jobs.
                jobIDs.push_back(result[0][0]);
                continue;
            }

            // If we found a job, but the data was different, we'll need to update it.
            if (!result.empty()) {
                updateJobID = SToInt64(result[0][0]);
            }
        }

        // Record whether or not this job is scheduling itself in the future. If so, it's not suitable for
        // immediate scheduling and won't benefit from disk-access-free assignment to waiting workers.
        if ((!SContains(job, "repeat") || job["repeat"].empty()) && (!SContains(job, "firstRun") && job["firstRun"].empty())) {
            SINFO("Job has no run time, can be scheduled immediately.");
        } else {
            SINFO("Job specified run time or repeat, not suitable for immediate scheduling.");
        }

        // If no "firstRun" was provided, use right now
        const string& safeFirstRun = !SContains(job, "firstRun") || job["firstRun"].empty() ? SCURRENT_TIMESTAMP() : SQ(job["firstRun"]);

        // If no data was provided, use an empty object
        const string& safeData = !SContains(job, "data") || job["data"].empty() ? SQ("{}") : SQ(job["data"]);
        const string& safeOriginalData = originalData.empty() ? SQ("{}") : SQ(originalData);

        // If a repeat is provided, validate it
        if (SContains(job, "repeat")) {
            if (job["repeat"].empty()) {
                SWARN("Repeat is set in CreateJob, but is set to the empty string. Job Name: "
                      << job["name"] << ", removing attribute.");
                job.erase("repeat");
            } else if (!_validateRepeat(job["repeat"])) {
                STHROW("402 Malformed repeat");
            }
        }

        // If no priority set, set it
        int64_t priority = SContains(job, "jobPriority") ? SToInt(job["jobPriority"]) : (SContains(job, "priority") ? SToInt(job["priority"]) : JOBS_DEFAULT_PRIORITY);

        // We'd initially intended for any value to be allowable here, but for
        // performance reasons, we currently will only allow specific values to
        // try and keep queries fast. If you pass an invalid value, we'll throw
        // here so that the caller can know that he did something wrong rather
        // than having his job sit unprocessed in the queue forever. Hopefully
        // we can remove this restriction in the future.
        if (priority != 0 && priority != 500 && priority != 1000) {
            STHROW("402 Invalid priority value");
        }

        // Validate that the parentJobID exists and is in the right state if one was passed.
        int64_t parentJobID = SContains(job, "parentJobID") ? SToInt64(job["parentJobID"]) : 0;
        if (parentJobID) {
            SQResult result;
            if (!db.read("SELECT state, parentJobID, data FROM jobs WHERE jobID=" + SQ(parentJobID) + ";", result)) {
                STHROW("502 Select failed");
            }
            if (result.empty()) {
                STHROW("404 parentJobID does not exist");
            }
            if (!SIEquals(result[0][0], "RUNNING") && !SIEquals(result[0][0], "PAUSED")) {
                SWARN("Trying to create child job with parent jobID#" << parentJobID << ", but parent isn't RUNNING or PAUSED (" << result[0][0] << ")");
                STHROW("405 Can only create child job when parent is RUNNING or PAUSED");
            }

            // Verify that the parent and child job have the same `mockRequest` setting.
            STable parentData = SParseJSONObject(result[0][2]);
            if (command.request.isSet("mockRequest") != (parentData.find("mockRequest") != parentData.end())) {
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
            // Update the existing job.
            if(!db.writeIdempotent("UPDATE jobs SET "
                                     "repeat   = " + SQ(SToUpper(job["repeat"])) + ", " +
                                     "data     = JSON_PATCH(data, " + safeData + "), " +
                                     "priority = " + SQ(priority) + " " +
                                   "WHERE jobID = " + SQ(updateJobID) + ";"))
            {
                STHROW("502 update query failed");
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
                if (SIEquals(parentState, "RUNNING")) {
                    initialState = "PAUSED";
                }
            }

            // If no data was provided, use an empty object
            const string& safeRetryAfter = SContains(job, "retryAfter") && !job["retryAfter"].empty() ? SQ(job["retryAfter"]) : SQ("");

            // Create this new job with a new generated ID
            const int64_t jobIDToUse = getNextID(db);
            SINFO("Next jobID to be used " << jobIDToUse);
            if (!db.writeIdempotent("INSERT INTO jobs ( jobID, created, state, name, nextRun, repeat, data, priority, parentJobID, retryAfter ) "
                     "VALUES( " +
                        SQ(jobIDToUse) + ", " +
                        SCURRENT_TIMESTAMP() + ", " +
                        SQ(initialState) + ", " +
                        SQ(job["name"]) + ", " +
                        safeFirstRun + ", " +
                        SQ(SToUpper(job["repeat"])) + ", " +
                        safeData + ", " +
                        SQ(priority) + ", " +
                        SQ(parentJobID) + ", " +
                        safeRetryAfter + " " +
                     " );"))
            {
                STHROW("502 insert query failed");
            }

            // Append new jobID to list of created jobs.
            jobIDs.push_back(SToStr(jobIDToUse));
        }
    }
    return jobIDs;
}

bool BedrockPlugin_Jobs::processCreateJob(SQLite& db, BedrockCommand& command) {
    list<STable> jsonJobs;
    jsonJobs.push_back(command.request.nameValueMap);
    auto jobIDs = processCreateCommon(db, command, jsonJobs);
    command.jsonContent["jobID"] = jobIDs.front();
    return true;
}

bool BedrockPlugin_Jobs::processCreateJobs(SQLite& db, BedrockCommand& command) {
    list<STable> jsonJobs;
    list<string> multipleJobs;
    multipleJobs = SParseJSONArray(command.request["jobs"]);
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
    auto jobIDs = processCreateCommon(db, command, jsonJobs);
    command.jsonContent["jobIDs"] = SComposeJSONArray(jobIDs);
    return true;
}

bool BedrockPlugin_Jobs::processDeleteJob(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processFailJob(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processFinishJob(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processGetJob(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processGetJobs(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processQueryJob(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processRequeueJobs(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processRetryJob(SQLite& db, BedrockCommand& command) {
    return true;
}

bool BedrockPlugin_Jobs::processUpdateJob(SQLite& db, BedrockCommand& command) {
    return true;
}


string BedrockPlugin_Jobs::_constructNextRunDATETIME(const string& lastScheduled,
                                                     const string& lastRun,
                                                     const string& repeat) {
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
    list<string> safeParts;
    string base = parts.front();
    parts.pop_front();
    if (base == "SCHEDULED") {
        safeParts.push_back(SQ(lastScheduled));
    } else if (base == "STARTED") {
        safeParts.push_back(SQ(lastRun));
    } else if (base == "FINISHED") {
        safeParts.push_back(SCURRENT_TIMESTAMP());
    } else {
        SWARN("Syntax error, failed parsing repeat '" << repeat << "': missing base (" << base << ")");
        return "";
    }

    for (const string& part : parts) {
        // Validate the sqlite date modifiers
        if (!_isValidSQLiteDateModifier(part)){
            SWARN("Syntax error, failed parsing repeat "+part);
            return "";
        }

        safeParts.push_back(SQ(part));
    }

    // Combine the parts together and return the full DATETIME statement
    return "DATETIME( " + SComposeList(safeParts) + " )";
}

bool BedrockPlugin_Jobs::_hasPendingChildJobs(SQLite& db, int64_t jobID) {
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

bool BedrockPlugin_Jobs::_isValidSQLiteDateModifier(const string& modifier) {
    // See: https://www.sqlite.org/lang_datefunc.html
    list<string> parts = SParseList(SToUpper(modifier));
    for (const string& part : parts) {
        // Simple regexp validation
        if (SREMatch("^(\\+|-)\\d{1,3} (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)S?$", part)) {
            continue;
        }
        if (SREMatch("^START OF (DAY|MONTH|YEAR)$", part)) {
            continue;
        }
        if (SREMatch("^WEEKDAY [0-6]$", part)) {
            continue;
        }

        // Couldn't match this part to any valid syntax
        SINFO("Syntax error, failed parsing date modifier '" << modifier << "' on part '" << part << "'");
        return false;
    }

    // Matched all parts, valid syntax
    return true;
}

void BedrockPlugin_Jobs::handleFailedReply(const BedrockCommand& command) {
    if (SIEquals(command.request.methodLine, "GetJob") || SIEquals(command.request.methodLine, "GetJobs")) {

        list<string> jobIDs;
        if (SIEquals(command.request.methodLine, "GetJob")) {
            STable jobJSON = SParseJSONObject(command.response.content);
            if (jobJSON.find("jobID") != jobJSON.end()) {
                jobIDs.push_back(jobJSON["jobID"]);
            }
        } else {
            STable jobsJSON = SParseJSONObject(command.response.content);
            list<string> jobs = SParseJSONArray(jobsJSON["jobs"]);
            for (auto& job : jobs) {
                STable jobJSON = SParseJSONObject(job);
                if (jobJSON.find("jobID") != jobJSON.end()) {
                    jobIDs.push_back(jobJSON["jobID"]);
                }
            }
        }
        SINFO("Failed sending response to '" << command.request.methodLine << "', re-queueing jobs: "<< SComposeList(jobIDs));
        if(_server) {
            SData requeue("RequeueJobs");
            requeue["jobIDs"] = SComposeList(jobIDs);

            // Keep the request ID so we'll be able to associate these in the logs.
            requeue["requestID"] = command.request["requestID"];
            SQLiteCommand cmd(move(requeue));
            cmd.initiatingClientID = -1;
            _server->acceptCommand(move(cmd));
        } else {
            SWARN("No server, can't re-queue jobs: " << SComposeList(jobIDs));
        }
    }
}

void BedrockPlugin_Jobs::initialize(const SData& args, BedrockServer& server) {
    _server = &server;
    _instance = this;
}

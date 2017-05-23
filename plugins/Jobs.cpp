#include "Jobs.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

#define JOBS_DEFAULT_PRIORITY 500

// ==========================================================================
void BedrockPlugin_Jobs::upgradeDatabase(SQLite& db) {
    // Create or verify the jobs table
    bool ignore;
    bool oldSchema = db.verifyTable("jobs", "CREATE TABLE jobs ( "
                                            "created  TIMESTAMP NOT NULL, "
                                            "jobID    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                                            "state    TEXT NOT NULL, "
                                            "name     TEXT NOT NULL, "
                                            "nextRun  TIMESTAMP NOT NULL, "
                                            "lastRun  TIMESTAMP, "
                                            "repeat   TEXT NOT NULL, "
                                            "data     TEXT NOT NULL, "
                                            "priority INTEGER NOT NULL DEFAULT " + SToStr(JOBS_DEFAULT_PRIORITY) + ", "
                                            "parentJobID INTEGER NOT NULL DEFAULT 0 )",
                     ignore);

    bool newSchema = db.verifyTable("jobs", "CREATE TABLE jobs ( "
                                            "created  TIMESTAMP NOT NULL, "
                                            "jobID    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                                            "state    TEXT NOT NULL, "
                                            "name     TEXT NOT NULL, "
                                            "nextRun  TIMESTAMP NOT NULL, "
                                            "lastRun  TIMESTAMP, "
                                            "repeat   TEXT NOT NULL, "
                                            "data     TEXT NOT NULL, "
                                            "priority INTEGER NOT NULL DEFAULT " + SToStr(JOBS_DEFAULT_PRIORITY) + ", "
                                            "parentJobID INTEGER NOT NULL DEFAULT 0, "
                                            "retryAfter TEXT NOT NULL DEFAULT \"\" )",
                     ignore);

    // @todo remove when we migrate to the new schema
    SASSERT(oldSchema || newSchema);

    // These indexes are not used by the Bedrock::Jobs plugin, but provided for easy analysis
    // using the Bedrock::DB plugin.
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsState    ON jobs ( state    );"));
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsName     ON jobs ( name     );"));
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsNextRun  ON jobs ( nextRun  );"));
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsLastRun  ON jobs ( lastRun  );"));
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsPriority ON jobs ( priority );"));
    SASSERT(db.write("CREATE INDEX IF NOT EXISTS jobsParentJobIDState ON jobs ( parentJobID, state );"));

    // This index is used to optimize the Bedrock::Jobs::GetJob call.
    SASSERT(db.write(
        "CREATE INDEX IF NOT EXISTS jobsStatePriorityNextRunName ON jobs ( state, priority, nextRun, name );"));
}

// ==========================================================================
bool BedrockPlugin_Jobs::peekCommand(SQLite& db, BedrockCommand& command) {
    // Pull out some helpful variables
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;

    // ----------------------------------------------------------------------
    if (SIEquals(request.methodLine, "UpdateJob")) {
        // - UpdateJob( jobID, data, [repeat] )
        //
        //     Atomically updates the data associated with a job.
        //
        //     Parameters:
        //     - jobID - ID of the job to delete
        //     - data  - A JSON object describing work to be done
        //     - repeat - A description of how to repeat (optional)
        //     Returns:
        //     - 200 - OK
        //     - 402 - Auto-retrying jobs cannot be updated once running
        //
        verifyAttributeInt64(request, "jobID", 1);
        verifyAttributeSize(request, "data", 1, MAX_SIZE_BLOB);

        // Verify there is a job like this
        SQResult result;
        string state = db.read("SELECT state FROM jobs WHERE jobID=" + SQ(request.calc64("jobID")) + ";");
        if (state == "RUNQUEUED") {
            throw "402 Auto-retrying jobs cannot be updated once running";
        }
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "GetJob") || SIEquals(request.methodLine, "GetJobs")) {
        // - GetJob( name )
        // - GetJobs( name, numResults )
        //
        //     Atomically dequeues one or more jobs, if available.
        //
        //     Parameters:
        //     - name - name pattern of jobs to match
        //     - numResults - maximum number of jobs to dequeue
        //     - connection - (optional) If "wait" will pause up to "timeout" for a match
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
        verifyAttributeSize(request, "name", 1, MAX_SIZE_SMALL);
        if (SIEquals(request.methodLine, "GetJobs") != request.isSet("numResults")) {
            if (SIEquals(request.methodLine, "GetJobs")) {
                throw "402 Missing numResults";
            } else {
                throw "402 Cannot use numResults with GetJob; try GetJobs";
            }
        }

        // Get the list
        SQResult result;
        const string& name = request["name"];
        if (!db.read("SELECT 1 "
                     "FROM jobs "
                     "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                     "  AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                     "  AND name GLOB " + SQ(name) + " "
                     "LIMIT 1;",
                     result)) {
            throw "502 Query failed";
        }

        // If we didn't get any results, just return an empty list
        if (result.empty() || SToInt(result[0][0]) == 0) {
            // Did the caller set "Connection: wait"?  If so, put a "hold"
            // on this request -- we'll clear the hold when we get a new
            // job.
            if (SIEquals(request["Connection"], "wait")) {
                // Place a hold on this request waiting for new jobs in this
                // state.
                SINFO("No results found and 'Connection: wait'; placing request on hold until we get a new job "
                      "matching name '"
                      << request["name"] << "'");
                request["HeldBy"] = "Jobs:" + name;
                response.clear(); // Clear default response so we don't accidentally think we're done
                return false;     // Not processed
            } else {
                // Don't hold, just respond with no results
                throw "404 No job found";
            }
        }

        // Looks like there might be results -- queue this for processing
        SINFO("Found results, but waiting for processCommand to update");
        return false;
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "QueryJob")) {
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
        verifyAttributeInt64(request, "jobID", 1);

        // Verify there is a job like this
        SQResult result;
        if (!db.read("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(request.calc64("jobID")) + ";",
                     result)) {
            throw "502 Select failed";
        }
        if (result.empty()) {
            throw "404 No job with this jobID";
        }
        content["created"] = result[0][0];
        content["jobID"] = result[0][1];
        content["state"] = result[0][2];
        content["name"] = result[0][3];
        content["nextRun"] = result[0][4];
        content["lastRun"] = result[0][5];
        content["repeat"] = result[0][6];
        content["data"] = result[0][7];
        return true; // Successfully processed
    } 
    
    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "CreateJob")) {

        // Recurring auto-retrying jobs open the doors to a whole new world of potential bugs
        // so we're intentionally not adding support for them them yet
        if (request.isSet("repeat") && request.isSet("retryAfter")) {
            throw "402 Recurring auto-retrying jobs are not supported";
        }

        // If parentJobID is passed, verify that the parent job doesn't have a retryAfter set
        int64_t parentJobID = request.calc64("parentJobID");
        if (parentJobID) {
            SINFO("parentJobID passed, checking existing job with ID " << parentJobID);
            SQResult result;
            if (!db.read("SELECT retryAfter "
                        "FROM jobs "
                        "WHERE jobID=" + SQ(parentJobID) + ";",
                        result)) {
                throw "502 Select failed";
            }
            if (!result.empty() && result[0][0] != "") {
                throw "402 Auto-retrying parents cannot have children";
            }
        }

        // Validate retryAfter
        if (request.isSet("retryAfter") && request["retryAfter"] != "" && !_isValidSQLiteDateModifier(request["retryAfter"])){
            throw "402 Malformed retryAfter";
        }

        // If unique flag was passed and the job exist in the DB, then we can
        // finish the command without escalating to master.
        if (!request.test("unique")) {
            // Not unique; need to process
            return false;
        }

        // Throw if retryAfter was passed for unique job
        if (request.isSet("retryAfter")) {
            throw "405 Unique jobs can't be retried";
        }

        // Verify unique
        SQResult result;
        SINFO("Unique flag was passed, checking existing job with name " << request["name"]);
        if (!db.read("SELECT jobID, data "
                     "FROM jobs "
                     "WHERE name=" + SQ(request["name"]) + ";",
                     result)) {
            throw "502 Select failed";
        }
        // If there's no job, escalate to master.
        if (result.empty()){
            return false;
        }

        // If the existing job doesn't match the data we've been passed, escalate to master.
        if (result[0][1] != request["data"]) {
            return false;
        }

        // Supposed to be unique but not; notify the caller and return that we
        // are processed
        SINFO("Job already existed and unique flag was passed, reusing existing job " << result[0][0]);
        content["jobID"] = result[0][0];
        return true;
    }

    // Didn't recognize this command
    return false;
}

// ==========================================================================
bool BedrockPlugin_Jobs::processCommand(SQLite& db, BedrockCommand& command) {
    // Pull out some helpful variables
    SData& request = command.request;
    SData& response = command.response;
    STable& content = command.jsonContent;

    // ----------------------------------------------------------------------
    if (SIEquals(request.methodLine, "CreateJob")) {
        // - CreateJob( name, [data], [firstRun], [repeat], [priority], [unique], [parentJobID], [retryAfter] )
        //
        //     Creates a "job" for future processing by a worker.
        //
        //     Parameters:
        //     - name  - An arbitrary string identifier (case insensitive)
        //     - data  - A JSON object describing work to be done (optional)
        //     - firstRun - A "YYYY-MM-DD HH:MM:SS" datetime of when
        //                  this job should next execute (optional)
        //     - repeat - A description of how to repeat (optional)
        //     - priority - High priorities go first (optional, default 500)
        //     - unique - if true, it will check that no other job with this name already exists, if it does it will
        //     return that jobID
        //     - parentJobID - The ID of the parent job (optional)
        //     - retryAfter - The ID of the parent job (optional)
        //
        //     Returns:
        //     - jobID - Unique identifier of this job
        //
        verifyAttributeSize(request, "name", 1, MAX_SIZE_SMALL);

        // If unique flag was passed and the job exist in the DB, then we can finish the command without escalating to
        // master.
        uint64_t updateJobID = 0;
        if (request.test("unique")) {
            SQResult result;
            SINFO("Unique flag was passed, checking existing job with name " << request["name"]);
            if (!db.read("SELECT jobID, data "
                         "FROM jobs "
                         "WHERE name=" + SQ(request["name"]) + ";",
                         result)) {
                throw "502 Select failed";
            }

            // If we got a result, and it's data is the same as passed, we won't change anything.
            if (!result.empty() && result[0][1] == request["data"]) {
                SINFO("Job already existed with matching data, and unique flag was passed, reusing existing job "
                      << result[0][0]);
                content["jobID"] = result[0][0];
                return true;
            }

            // If we found a job, but the data was different, we'll need to update it.
            if (!result.empty()) {
                updateJobID = SToInt64(result[0][0]);
            }

            // Alert if job is unique and retryAfter is set
            // This shouldn't happen since we validate this peekCommand
            if (request.isSet("retryAfter") && request["retryAfter"] != "") {
                SALERT("Unique jobs shouldn't be retried");
            }
        }

        // If no "firstRun" was provided, use right now
        const string& safeFirstRun = request["firstRun"].empty() ? SCURRENT_TIMESTAMP() : SQ(request["firstRun"]);

        // If no data was provided, use an empty object
        const string& safeData = request["data"].empty() ? SQ("{}") : SQ(request["data"]);

        // If a repeat is provided, validate it
        if (request.isSet("repeat")) {
            if (request["repeat"].empty()) {
                SWARN("Repeat is set in CreateJob, but is set to the empty string. Job Name: "
                      << request["name"] << ", removing attribute.");
                request.erase("repeat");
            } else if (!_validateRepeat(request["repeat"])) {
                throw "402 Malformed repeat";
            }
        }

        // If no priority set, set it
        int64_t priority = request.isSet("priority") ? request.calc("priority") : JOBS_DEFAULT_PRIORITY;

        // Validate that the parentJobID exists and is in the right state if one was passed.
        int64_t parentJobID = request.calc64("parentJobID");
        if (parentJobID) {
            SQResult result;
            if (!db.read("SELECT state, retryAfter FROM jobs WHERE jobID=" + SQ(parentJobID) + ";", result)) {
                throw "502 Select failed";
            }
            if (result.empty()) {
                throw "404 parentJobID does not exist";
            } else if (!SIEquals(result[0][0], "RUNNING") && !SIEquals(result[0][0], "PAUSED")) {
                SWARN("Trying to create child job with parent jobID#" << parentJobID << ", but parent isn't RUNNING or PAUSED (" << result[0][0] << ")");
                throw "405 Can only create child job when parent is RUNNING or PAUSED";
            }
            else if (result[0][1] != "") {
                SALERT("Auto-retrying parents shouldn't have children, parentJobID:" << parentJobID );
            }
        }

        // We'd initially intended for any value to be allowable here, but for
        // performance reasons, we currently will only allow specific values to
        // try and keep queries fast. If you pass an invalid value, we'll throw
        // here so that the caller can know that he did something wrong rather
        // than having his job sit unprocessed in the queue forever. Hopefully
        // we can remove this restriction in the future.
        if (priority != 0 && priority != 500 && priority != 1000) {
            throw "402 Invalid priority value";
        }

        // Are we creating a new job, or updating an existing job?
        if (updateJobID) {
            // Update the existing job.
            if(!db.write("UPDATE JOBS SET "
                           "repeat   = " + SQ(SToUpper(request["repeat"])) + ", " +
                           "data     = JSON_PATCH(data, " + safeData + "), " +
                           "priority = " + SQ(priority) + " " +
                         "WHERE jobID = " + SQ(updateJobID) + ";"))
            {
                throw "502 update query failed";
            }
            content["jobID"] = SToStr(updateJobID);
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
            const string& safeRetryAfter = request["retryAfter"].empty() ? "\"\"" : SQ(request["retryAfter"]);

            // Create this new job
            if (!db.write("INSERT INTO jobs ( created, state, name, nextRun, repeat, data, priority, parentJobID, retryAfter ) "
                     "VALUES( " +
                        SCURRENT_TIMESTAMP() + ", " +
                        SQ(initialState) + ", " +
                        SQ(request["name"]) + ", " +
                        safeFirstRun + ", " +
                        SQ(SToUpper(request["repeat"])) + ", " +
                        safeData + ", " +
                        SQ(priority) + ", " +
                        SQ(parentJobID) + ", " +
                        safeRetryAfter +
                     " );"))
            {
                throw "502 insert query failed";
            }
            // Return the new jobID
            const int64_t lastInsertRowID = db.getLastInsertRowID();
            const int64_t maxJobID = SToInt64(db.read("SELECT MAX(jobID) FROM jobs;"));
            if (lastInsertRowID != maxJobID) {
                SALERT("We might be returning the wrong jobID maxJobID=" << maxJobID
                                                                         << " lastInsertRowID=" << lastInsertRowID);
            }
            content["jobID"] = SToStr(lastInsertRowID);
         }

        // Release workers waiting on this state
        // TODO: No "HeldBy" anymore. If a plugin wants to hold a command, it should own it until it's done.
        // node->clearCommandHolds("Jobs:" + request["name"]);

        return true; // Successfully processed
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "GetJob") || SIEquals(request.methodLine, "GetJobs")) {
        // If we're here it's because peekCommand found some data; re-execute
        // the query for real now.  However, this time we will order by
        // priority.  We do this as three separate queries so we only have one
        // unbounded column in each query.  Additionally, we wrap each inner
        // query in a "SELECT *" such that we can have an "ORDER BY" and
        // "LIMIT" *before* we UNION ALL them together.  Looks gnarly, but it
        // works!
        SQResult result;
        const string& name = request["name"];
        string safeNumResults = SQ(max(request.calc("numResults"),1)); 
        string selectQuery =
            "SELECT jobID, name, data, parentJobID, retryAfter FROM ( "
                "SELECT * FROM ("
                    "SELECT jobID, name, data, priority, parentJobID, retryAfter "
                    "FROM jobs "
                    "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                    "  AND priority=1000"
                    "  AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                    "  AND name GLOB " + SQ(name) + " "
                    "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
            "UNION ALL "
                "SELECT * FROM ("
                    "SELECT jobID, name, data, priority, parentJobID, retryAfter "
                    "FROM jobs "
                    "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                    "  AND priority=500"
                    "  AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                    "  AND name GLOB " + SQ(name) + " "
                    "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
            "UNION ALL "
                "SELECT * FROM ("
                    "SELECT jobID, name, data, priority, parentJobID, retryAfter "
                    "FROM jobs "
                    "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                    "  AND priority=0"
                    "  AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                    "  AND name GLOB " + SQ(name) + " "
                    "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
            ") "
            "ORDER BY priority DESC "
            "LIMIT " + safeNumResults + ";";
        if (!db.read(selectQuery, result)) {
            throw "502 Query failed";
        }

        // Are there any results?
        if (result.empty()) {
            // Ah, there were before, but aren't now -- nothing found
            // **FIXME: If "Connection: wait" should re-apply the hold.  However, this is super edge
            //          as we could only get here if the job somehow got consumed between the peek
            //          and process -- which could happen during heavy load.  But it'd just return
            //          no results (which is correct) faster than it would otherwise time out.  Either
            //          way the worker will likely just loop, so it doesn't really matter.
            throw "404 No job found";
        }

        // There should only be at most one result if GetJob
        SASSERT(!SIEquals(request.methodLine, "GetJob") || result.size()<=1);

        // Prepare to update the rows, while also creating all the child objects
        list<string> nonRetriableJobs;
        list<STable> retriableJobs;
        list<string> jobList;
        for (size_t c=0; c<result.size(); ++c) {
            SASSERT(result[c].size() == 5);

            // Add jobID to the respective list
            if (result[c][4] == "") {
                nonRetriableJobs.push_back(result[c][0]);
            } else {
                STable job;
                job["jobID"] = result[c][0];
                job["retryAfter"] = result[c][4];
                retriableJobs.push_back(job);
            }

            // See if this job has any FINISHED child jobs, indicating it is being resumed
            SQResult finishedChildJobs;
            if (!db.read("SELECT jobID, data FROM jobs WHERE parentJobID=" + result[c][0] + " AND state='FINISHED';", finishedChildJobs)) {
                throw "502 Failed to select finished child jobs";
            }

            // Add this object to our output
            STable job;
            job["jobID"] = result[c][0];
            job["name"] = result[c][1];
            job["data"] = result[c][2];
            int64_t parentJobID = SToInt64(result[c][3]);
            if (parentJobID) {
                // Has a parent job, add the parent data
                job["parentJobID"] = SToStr(parentJobID);;
                job["parentData"] = db.read("SELECT data FROM jobs WHERE jobID=" + SQ(parentJobID) + ";");
            }
            if (!finishedChildJobs.empty()) {
                // Add an associative array of all children
                list<string> finishedChildJobArray;
                for (auto row : finishedChildJobs.rows) {
                    STable finishedChildJob;
                    finishedChildJob["jobID"] = row[0];
                    finishedChildJob["data"] = row[1];
                    finishedChildJobArray.push_back(SComposeJSONObject(finishedChildJob));
                }
                job["finishedChildJobs"] = SComposeJSONArray(finishedChildJobArray);
            }
            jobList.push_back(SComposeJSONObject(job));
        }

        // Update jobs without retryAfter
        if (!nonRetriableJobs.empty()) {
            string jobIDs = SComposeList(nonRetriableJobs);
            SINFO("Updating jobs without retryAfter " << jobIDs);
            string updateQuery = "UPDATE jobs "
                                 "SET state='RUNNING', "
                                 "lastRun=" + SCURRENT_TIMESTAMP() + " "
                                 "WHERE jobID IN (" + jobIDs + ");";
            if (!db.write(updateQuery)) {
                throw "502 Update failed";
            }
        }

        // Update jobs with retryAfter
        if (!retriableJobs.empty()) {
            SINFO("Updating jobs with retryAfter");
            for (auto job : retriableJobs) {
                string jobID = job["jobID"];
                string retryAfter = job["retryAfter"];
                string updateQuery = "UPDATE jobs "
                                     "SET state='RUNQUEUED', "
                                     "  lastRun=" + SCURRENT_TIMESTAMP() + ", "
                                     "  nextRun=DATETIME(" + SCURRENT_TIMESTAMP() + ", " + SQ(retryAfter) + ") "
                                     "WHERE jobID = " + jobID + ";";
                if (!db.write(updateQuery)) {
                    throw "502 Update failed";
                }
            }
        }

        // Format the results as is appropriate for what was requested
        if (SIEquals(request.methodLine, "GetJob")) {
            // Single response
            SASSERT(jobList.size() == 1);
            response.content = jobList.front();
        } else {
            // Multiple responses
            SASSERT(SIEquals(request.methodLine, "GetJobs"));
            content["jobs"] = SComposeJSONArray(jobList);
        }
        return true; // Successfully processed
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "UpdateJob")) {
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
                throw "402 Malformed repeat";
            }
        }

        // Verify there is a job like this
        SQResult result;
        if (!db.read("SELECT jobID, nextRun, lastRun "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(request.calc64("jobID")) + ";",
                     result)) {
            throw "502 Select failed";
        }
        if (result.empty() || !SToInt(result[0][0])) {
            throw "404 No job with this jobID";
        }

        const string& nextRun = result[0][1];
        const string& lastRun = result[0][2];

        // Are we rescheduling?
        const string& newNextRun = request.isSet("repeat") ? _constructNextRunDATETIME(nextRun, lastRun, request["repeat"]) : "";

        // Update the data
        if (!db.write("UPDATE jobs "
                      "SET data=" +
                      SQ(request["data"]) + " " +
                      (request.isSet("repeat") ? ", repeat=" + SQ(SToUpper(request["repeat"])) : "") +
                      (!newNextRun.empty() ? ", nextRun=" + newNextRun : "") +
                          "WHERE jobID=" +
                      SQ(request.calc64("jobID")) + ";")) {
            throw "502 Update failed";
        }
        return true; // Successfully processed
    }

    // ----------------------------------------------------------------------
    else if (SIEquals(request.methodLine, "RetryJob") || SIEquals(request.methodLine, "FinishJob")) {
        // - RetryJob( jobID, delay, [data] )
        //
        //     Re-queues a RUNNING job for "delay" seconds in the future,
        //     unless the job is configured to "repeat" in which case it will
        //     just schedule for the next repeat time.
        //     Use this when a job was only partially completed but
        //     interrupted in a non-fatal way.
        //
        //     Parameters:
        //     - jobID  - ID of the job to retry
        //     - delay  - Number of seconds to wait before retrying
        //     - data   - Data to associate with this finsihed job
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
        SQResult result;
        if (!db.read("SELECT state, nextRun, lastRun, repeat, parentJobID "
                     "FROM jobs "
                     "WHERE jobID=" + SQ(jobID) + ";",
                     result)) {
            throw "502 Select failed";
        }

        // Verify there is a job like this and it's running
        if (result.empty()) {
            throw "404 No job with this jobID";
        }
        const string& state = result[0][0];
        const string& nextRun = result[0][1];
        const string& lastRun = result[0][2];
        string repeat = result[0][3];
        int64_t parentJobID = SToInt(result[0][4]);

        // Make sure we're finishing a job that's actually running
        if (state != "RUNNING" && state != "RUNQUEUED") {
            SWARN("Trying to finish job#" << jobID << ", but isn't RUNNING or RUNQUEUED (" << state << ")");
            throw "405 Can only retry/finish RUNNING and RUNQUEUED jobs";
        }

        // If we have a parent, make sure it is PAUSED.  This is to just
        // double-check that child jobs aren't somehow running in parallel to
        // the parent.
        if (parentJobID) {
            auto parentState = db.read("SELECT state FROM jobs WHERE jobID=" + SQ(parentJobID) + ";");
            if (!SIEquals(parentState, "PAUSED")) {
                SWARN("Trying to finish job#" << jobID << ", but parent isn't PAUSED (" << parentState << ")");
                throw "405 Can only retry/finish child job when parent is PAUSED";
            }
        }

        // Delete any FINISHED child jobs, but leave any PAUSED children alone (as those will signal that
        // we just want to re-PAUSE this job so those new children can run)
        if (!db.write("DELETE FROM jobs WHERE parentJobID=" + SQ(jobID) + " AND state='FINISHED';")) {
            throw "502 Failed deleting finished child jobs";
        }

        // If we've been asked to update the data, let's do that
        auto data = request["data"];
        if (!data.empty()) {
            if (!db.write("UPDATE jobs SET data=" + SQ(data) + " WHERE jobID=" + SQ(jobID) + ";")) {
                throw "502 Failed to update job data";
            }
        }

        // If we are finishing a job that has child jobs, set its state to paused.
        if (SIEquals(request.methodLine, "FinishJob") && _hasPendingChildJobs(db, jobID)) {
            // Update the parent job to PAUSED
            SINFO("Job has child jobs, PAUSING parent, QUEUING children");
            if (!db.write("UPDATE jobs SET state='PAUSED' WHERE jobID=" + SQ(jobID) + ";")) {
                throw "502 Parent update failed";
            }

            // Also un-pause any child jobs such that they can run
            if (!db.write("UPDATE jobs SET state='QUEUED' "
                          "WHERE state='PAUSED' "
                            "AND parentJobID=" + SQ(jobID) + ";")) {
                throw "502 Child update failed";
            }

            // All done processing this command
            return true;
        }

        // If we're doing RetryJob and there isn't a repeat, construct one with the delay
        if (repeat.empty() && SIEquals(request.methodLine, "RetryJob")) {
            // Make sure there is a delay
            int64_t delay = request.calc64("delay");
            if (delay < 0) {
                throw "402 Must specify a non-negative delay when retrying";
            }
            repeat = "FINISHED, +" + SToStr(delay) + " SECONDS";
        }

        // Are we rescheduling?
        if (!repeat.empty()) {
            // Configured to repeat.  The "nextRun" at this point is still
            // storing the last time this job was *scheduled* to be run;
            // lastRun contains when it was *actually* run.
            // NOTE: we're not including retryAfter when computing lastScheduled
            //       because we currently don't support retryable and recurring jobs
            const string& lastScheduled = nextRun;
            const string& newNextRun = _constructNextRunDATETIME(lastScheduled, lastRun, repeat);
            if (newNextRun.empty()) {
                throw "402 Malformed repeat";
            }
            SINFO("Rescheduling job#" << jobID << ": " << newNextRun);

            // Update this job
            if (!db.write("UPDATE jobs SET nextRun=" + newNextRun + ", state='QUEUED' WHERE jobID=" + SQ(jobID) + ";")) {
                throw "502 Update failed";
            }
        } else {
            // We are done with this job.  What do we do with it?
            SASSERT(!SIEquals(request.methodLine, "RetryJob"));
            if (parentJobID) {
                // This is a child job.  Mark it as finished.
                if (!db.write("UPDATE jobs SET state='FINISHED' WHERE jobID=" + SQ(jobID) + ";")) {
                    throw "502 Failed to mark job as FINISHED";
                }

                // Resume the parent if this is the last pending child
                if (!_hasPendingChildJobs(db, parentJobID)) {
                    SINFO("Job has parentJobID: " + SToStr(parentJobID) +
                          " and no other pending children, resuming parent job");
                    if (!db.write("UPDATE jobs SET state = 'QUEUED' where jobID=" + SQ(parentJobID) + ";")) {
                        throw "502 Update failed";
                    }
                }
            } else {
                // This is a standalone (not a child) job; delete it.  
                if (!db.write("DELETE FROM jobs WHERE jobID=" + SQ(jobID) + ";")) {
                    throw "502 Delete failed";
                }

                // At this point, all child jobs should already be deleted, but
                // let's double check.
                if (!db.read("SELECT 1 FROM jobs WHERE parentJobID=" + SQ(jobID) + " LIMIT 1;").empty()) {
                    SWARN("Child jobs still exist when deleting parent job, ignoring.");
                }
            }
        }

        // Successfully processed
        return true;
    }

    // ----------------------------------------------------------------------

    else if (SIEquals(request.methodLine, "FailJob")) {
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
            throw "502 Select failed";
        }
        if (result.empty()) {
            throw "404 No job with this jobID";
        }
        const string& state = result[0][0];

        // Make sure we're failing a job that's actually running
        if (state != "RUNNING") {
            SWARN("Trying to fail job#" << request["jobID"] << ", but isn't RUNNING (" << state << ")");
            throw "405 Can only fail RUNNING jobs";
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
        if (!db.write("UPDATE jobs SET " + SComposeList(updateList) + "WHERE jobID=" + SQ(request.calc64("jobID")) + ";")) {
            throw "502 Fail failed";
        }

        // Successfully processed
        return true;
    }

    // ----------------------------------------------------------------------

    else if (SIEquals(request.methodLine, "DeleteJob")) {
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
            throw "502 Select failed";
        }
        if (result.empty()) {
            throw "404 No job with this jobID";
        }
        if (result[0][0] == "RUNNING") {
            throw "405 Can't delete a RUNNING job";
        }

        // Delete the job
        if (!db.write("DELETE FROM jobs "
                      "WHERE jobID=" +
                      SQ(request.calc64("jobID")) + ";")) {
            throw "502 Delete failed";
        }

        // Successfully processed
        return true;
    }

    // Didn't recognize this command
    return false;
}

// Under this line we don't know the "node", so remove from the log prefix
#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

// ==========================================================================
string BedrockPlugin_Jobs::_constructNextRunDATETIME(const string& lastScheduled, const string& lastRun,
                                                     const string& repeat) {
    // Some "canned" times for convenience
    if (SIEquals(repeat, "HOURLY"))
        return "STRFTIME( '%Y-%m-%d %H:00:00', DATETIME( " + SCURRENT_TIMESTAMP() + ", '+1 HOUR' ) )";
    if (SIEquals(repeat, "DAILY"))
        return "DATETIME( " + SCURRENT_TIMESTAMP() + ", '+1 DAY', 'START OF DAY'  )";
    if (SIEquals(repeat, "WEEKLY"))
        return "DATETIME( " + SCURRENT_TIMESTAMP() + ", '+1 DAY', 'WEEKDAY 0', 'START OF DAY' )";

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

    // Validate the sqlite date modifiers
    if (!_isValidSQLiteDateModifier(SComposeList(parts))){
        SWARN("Syntax error, failed parsing repeat");
        return "";
    }

    for (const string& part : parts) {
        if (SREMatch("^(\\+|-)\\d{1,3} (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)S?$", part)) {
            safeParts.push_back(SQ(part));
        } else if (SREMatch("^START OF (DAY|MONTH|YEAR)$", part)) {
            safeParts.push_back(SQ(part));
        } else if (SREMatch("^WEEKDAY [0-6]$", part)) {
            safeParts.push_back(SQ(part));
        }
    }

    // Combine the parts together and return the full DATETIME statement
    return "DATETIME( " + SComposeList(safeParts) + " )";
}

// ==========================================================================
bool BedrockPlugin_Jobs::_hasPendingChildJobs(SQLite& db, int64_t jobID) {
    // Returns true if there are any children of this jobID in a "pending" (eg,
    // running or yet to run) state
    SQResult result;
    if (!db.read("SELECT 1 "
                 "FROM jobs "
                 "WHERE parentJobID = " + SQ(jobID) + " " + 
                 "  AND state IN ('QUEUED', 'RUNQUEUED', 'RUNNING', 'PAUSED') "
                 "LIMIT 1;",
                 result)) {
        throw "502 Select failed";
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

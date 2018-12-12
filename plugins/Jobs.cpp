#include "Jobs.h"
#include "../BedrockServer.h"

atomic<BedrockPlugin_Jobs*> BedrockPlugin_Jobs::_instance(nullptr);
BedrockServer* BedrockPlugin_Jobs::_server = nullptr;
atomic<int64_t> BedrockPlugin_Jobs::currentStartTableNumber(0);

#undef SLOGPREFIX
#define SLOGPREFIX "{" << _instance.load()->getName() << "} "

int64_t BedrockPlugin_Jobs::getNextID(SQLite& db, int64_t shouldMatch)
{
    int64_t newID = 0;
    while (!newID) {
        // Select a random number. 
        newID = SRandom::rand64();

        // We've taken an unsigned number, and stuffed it into a signed value, so it could be negative.
        // If it's *the most negative value*, just making it positive doesn't work, cause integer math, and
        // two's-compliment causing it to overflow back to the number you started with. So we disallow that value.
        if (newID == INT64_MIN) {
            newID++;
        }

        // Ok, now we can take the absolute value, and know we have a positive value that fits in our int64_t.
        newID = labs(newID);

        // `shouldMatch` allows us to pick an ID that will end up in the same table as an existing ID.
        if (shouldMatch >= 0) {
            int64_t currentModulo = newID % TABLE_COUNT;
            int64_t desiredModulo = shouldMatch % TABLE_COUNT;

            // It's possible to overflow at either end of our integer space, so we always subtract to the correct
            // modulus, unless we're already close to 0.
            if (newID <= TABLE_COUNT) {
                newID = newID + desiredModulo - currentModulo;
            } else {
                newID = newID - currentModulo + desiredModulo;
            }
        }

        // Now we're done with confusing integer math, let's double-check that this doesn't already exist.
        string result = db.read( "SELECT jobID FROM " + getTableName(newID) + " WHERE jobID = " + to_string(newID) + ";");
        if (!result.empty()) {
            // This one exists! Pick a new one.
            newID = 0;
        }
    }

    // Great, this works!
    return newID;
}

int64_t BedrockPlugin_Jobs::getTableNumberForJobName(const string& name) {
    int64_t number = 0;
    for (size_t i = 0; i < name.size(); i++) {
        number += name[i];
    }
    return number % TABLE_COUNT;
}


string BedrockPlugin_Jobs::getTableName(int64_t number) {
    if (number < 0) {
        return "jobs";
    }
    return "jobs"s + (((number % TABLE_COUNT) < 10) ? "0" : "") + to_string(number % TABLE_COUNT);
}

void BedrockPlugin_Jobs::upgradeDatabase(SQLite& db) {
    // Create or verify the jobs table
    bool ignore;
    for (int64_t tableNum = -1; tableNum < TABLE_COUNT; tableNum++) {
        string tableName = getTableName(tableNum);
        SASSERT(db.verifyTable(tableName,
                               "CREATE TABLE " + tableName + " ( "
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
        SASSERT(db.write("CREATE INDEX IF NOT EXISTS " + tableName + "Name     ON " + tableName + " ( name     );"));
        SASSERT(db.write("CREATE INDEX IF NOT EXISTS " + tableName + "ParentJobIDState ON " + tableName + " ( parentJobID, state ) WHERE parentJobID != 0;"));
        SASSERT(db.write("CREATE INDEX IF NOT EXISTS " + tableName + "StatePriorityNextRunName ON " + tableName + " ( state, priority, nextRun, name );"));
    }
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
    string tableName = getTableName(jobID);

    SQResult result;
    if (!db.read("SELECT j.jobID, j.state, j.parentJobID, (SELECT COUNT(1) FROM " + tableName + " WHERE parentJobID != 0 AND parentJobID=" + SQ(jobID) + ") children "
                 "FROM " + tableName + " j "
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
            string tableName = getTableName(parentJobID);
            SINFO("parentJobID passed, checking existing job with ID " << parentJobID);
            SQResult result;
            if (!db.read("SELECT state, retryAfter, data FROM " + tableName + " WHERE jobID=" + SQ(parentJobID) + ";", result)) {
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
        string tableName = getTableName(getTableNumberForJobName(job["name"]));
        SINFO("Unique flag was passed, checking existing job with name " << job["name"] << ", mocked? " << (command.request.isSet("mockRequest") ? "true" : "false"));
        SQResult result;
        string operation = command.request.isSet("mockRequest") ? "IS NOT" : "IS";
        if (!db.read("SELECT jobID, data "
                     "FROM " + tableName +" "
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

    // This only runs on slaves,so where we start doesn't really matter, as long as we find *something*.
    int64_t startAt = SRandom::rand64() % TABLE_COUNT;
    int64_t current = startAt;
    int64_t checkCount = 0;
    while (true) {
        checkCount++;
        string tableName = getTableName(current);
        if (!db.read("SELECT 1 "
                     "FROM " + tableName + " "
                     "WHERE state in ('QUEUED', 'RUNQUEUED') "
                        "AND priority IN (0, 500, 1000) "
                        "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                        "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(command.request["name"])) + " " +
                        string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                     "LIMIT 1;",
                     result)) {
            STHROW("502 Query failed");
        }

        // If we found any results, we're good! We'll escalate to master.
        if (!result.empty()) {
            SINFO("Checked " << checkCount << " tables for jobs in peek.");
            return;
        }

        // If not, look at the next table.
        current++;
        current %= TABLE_COUNT;
        if (current == startAt) {
            // Did all of them, I guess we're done.
            break;
        }
    }

    SINFO("Checked " << checkCount << " tables for jobs in peek.");

    // If we found *no* results in *any* tables, here we are.
    STHROW("404 No job found");
}

bool BedrockPlugin_Jobs::peekGetJob(SQLite& db, BedrockCommand& command) {
    if (command.request.isSet("numResults")) {
        STHROW("402 Cannot use numResults with GetJob; try GetJobs");
    }
    if (_server && _server->getState() == SQLiteNode::MASTERING) {
        // Don't even bother on master, we want to look at as few tables as possible, we'll just skip to process.
        return false;
    }
    peekGetCommon(db, command);
    return false;
}

bool BedrockPlugin_Jobs::peekGetJobs(SQLite& db, BedrockCommand& command) {
    if (!command.request.isSet("numResults")) {
        STHROW("402 Missing numResults");
    }
    if (_server && _server->getState() == SQLiteNode::MASTERING) {
        // Don't even bother on master, we want to look at as few tables as possible, we'll just skip to process.
        return false;
    }
    peekGetCommon(db, command);
    return false;
}

bool BedrockPlugin_Jobs::peekQueryJob(SQLite& db, BedrockCommand& command) {
    verifyAttributeInt64(command.request, "jobID", 1);
    int64_t jobID = command.request.calc64("jobID");
    string tableName = getTableName(jobID);
    SQResult result;
    if (!db.read("SELECT created, jobID, state, name, nextRun, lastRun, repeat, data, retryAfter, priority "
                 "FROM " + tableName + " "
                 "WHERE jobID=" + SQ(jobID) + ";",
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
    const string& requestVerb = request.getVerb();

    // Reset the content object. It could have been written by a previous call to this function that conflicted in
    // multi-write.
    command.jsonContent.clear();
    command.response.clear();

    if (SIEquals(requestVerb, "CancelJob")) return processCancelJob(db, command);
    if (SIEquals(requestVerb, "CreateJob")) return processCreateJob(db, command);
    if (SIEquals(requestVerb, "CreateJobs")) return processCreateJobs(db, command);
    if (SIEquals(requestVerb, "DeleteJob")) return processDeleteJob(db, command);
    if (SIEquals(requestVerb, "FailJob")) return processFailJob(db, command);
    if (SIEquals(requestVerb, "FinishJob")) return processFinishJob(db, command);
    if (SIEquals(requestVerb, "GetJob")) return processGetJob(db, command);
    if (SIEquals(requestVerb, "GetJobs")) return processGetJobs(db, command);
    if (SIEquals(requestVerb, "RequeueJobs")) return processRequeueJobs(db, command);
    if (SIEquals(requestVerb, "RetryJob")) return processRetryJob(db, command);
    if (SIEquals(requestVerb, "UpdateJob")) return processUpdateJob(db, command);

    // Didn't recognize this command
    return false;
}


bool BedrockPlugin_Jobs::processCancelJob(SQLite& db, BedrockCommand& command) {
    int64_t jobID = command.request.calc64("jobID");
    string tableName = getTableName(jobID);

    // Cancel the job
    if (!db.writeIdempotent("UPDATE " + tableName + " SET state='CANCELLED' WHERE jobID=" + SQ(jobID) + ";")) {
        STHROW("502 Failed to update job data");
    }

    // If this was the last queued child, resume the parent
    SQResult result;
    if (!db.read("SELECT parentJobID "
                 "FROM " + tableName + " "
                 "WHERE jobID=" + SQ(jobID) + ";",
                 result)) {
        STHROW("502 Select failed");
    }
    const string& safeParentJobID = SQ(result[0][0]);
    if (!db.read("SELECT count(1) "
                 "FROM " + tableName + " "
                 "WHERE parentJobID != 0 AND parentJobID=" + safeParentJobID + " AND "
                   "state IN ('QUEUED', 'RUNQUEUED', 'RUNNING');",
                 result)) {
        STHROW("502 Select failed");
    }
    if (SToInt64(result[0][0]) == 0) {
        SINFO("Cancelled last QUEUED child, resuming the parent: " << safeParentJobID);
        if (!db.writeIdempotent("UPDATE " + tableName + " SET state='QUEUED' WHERE jobID=" + safeParentJobID + ";")) {
            STHROW("502 Failed to update job data");
        }
    }

    return true;
}

list<int64_t> BedrockPlugin_Jobs::processCreateCommon(SQLite& db, BedrockCommand& command, list<STable>& jsonJobs) {

    // Store the data we need to update the in-memory map of available jobs.
    map<int64_t, pair<string, int64_t>> jobIDToNameAndPriorityMap;

    list<int64_t> jobIDs;
    for (auto& job : jsonJobs) {
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
            string tableName = getTableName(getTableNumberForJobName(job["name"]));

            SQResult result;
            SINFO("Unique flag was passed, checking existing job with name " << job["name"] << ", mocked? "
                  << (command.request.isSet("mockRequest") ? "true" : "false"));
            string operation = command.request.isSet("mockRequest") ? "IS NOT" : "IS";
            if (!db.read("SELECT jobID, data "
                         "FROM " + tableName + " "
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
                jobIDs.push_back(SToInt64(result[0][0]));
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
            string tableName = getTableName(parentJobID);
            SQResult result;
            if (!db.read("SELECT state, parentJobID, data FROM " + tableName + " WHERE jobID=" + SQ(parentJobID) + ";", result)) {
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
            string tableName = getTableName(updateJobID);
            // Update the existing job.
            if(!db.writeIdempotent("UPDATE " + tableName + " SET "
                                     "repeat   = " + SQ(SToUpper(job["repeat"])) + ", " +
                                     "data     = JSON_PATCH(data, " + safeData + "), " +
                                     "priority = " + SQ(priority) + " " +
                                   "WHERE jobID = " + SQ(updateJobID) + ";"))
            {
                STHROW("502 update query failed");
            }

            // Append new jobID to list of created jobs.
            jobIDs.push_back(updateJobID);
        } else {
            // Normal jobs start out in the QUEUED state, meaning they are ready to run immediately.
            // Child jobs normally start out in the PAUSED state, and are switched to QUEUED when the parent
            // finishes itself (and itself becomes PAUSED).  However, if the parent is already PAUSED when
            // the child is created (indicating a child is creating a sibling) then the new child starts
            // in the QUEUED state.
            auto initialState = "QUEUED";
            if (parentJobID) {
                string tableName = getTableName(parentJobID);
                auto parentState = db.read("SELECT state FROM " + tableName + " WHERE jobID=" + SQ(parentJobID) + ";");
                if (SIEquals(parentState, "RUNNING")) {
                    initialState = "PAUSED";
                }
            }

            // If no data was provided, use an empty object
            const string& safeRetryAfter = SContains(job, "retryAfter") && !job["retryAfter"].empty() ? SQ(job["retryAfter"]) : SQ("");

            // Figure out a jobID for this job. This is special, because we have a couple ways we need to do this.
            // We need to find unique jobs by name alone, so we have a special hash function for those.
            // We also always insert parent/child jobs into the same table, so that we don't need to do a bunch of
            // UNIONS and JOINs across tables when we check that relationship.
            // Otherwise, the table chosen is entirely random.
            int64_t jobIDToUse = 0;
            if (SContains(job, "unique") && job["unique"] == "true") {
                jobIDToUse = getNextID(db, getTableNumberForJobName(job["name"]));
            } else if (parentJobID) {
                jobIDToUse = getNextID(db, parentJobID);
            } else {
                jobIDToUse = getNextID(db);
            }
            string tableName = getTableName(jobIDToUse);

            SINFO("Next jobID to be used " << jobIDToUse);
            if (!db.writeIdempotent("INSERT INTO " + tableName + " ( jobID, created, state, name, nextRun, repeat, data, priority, parentJobID, retryAfter ) "
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
            jobIDs.push_back(jobIDToUse);
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
    verifyAttributeInt64(command.request, "jobID", 1);
    int64_t jobID = command.request.calc64("jobID");
    string tableName = getTableName(jobID);

    // Verify there is a job like this and it's not running
    SQResult result;
    if (!db.read("SELECT state "
                 "FROM " + tableName + " "
                 "WHERE jobID=" + SQ(jobID) + ";",
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
        STHROW("405 Can't delete a parent job with children running");
    }

    // Delete the job
    if (!db.writeIdempotent("DELETE FROM " + tableName + " "
                  "WHERE jobID=" +
                  SQ(jobID) + ";")) {
        STHROW("502 Delete failed");
    }

    return true;
}

bool BedrockPlugin_Jobs::processFailJob(SQLite& db, BedrockCommand& command) {
    verifyAttributeInt64(command.request, "jobID", 1);
    int64_t jobID = command.request.calc64("jobID");
    string tableName = getTableName(jobID);

    // Verify there is a job like this and it's running
    SQResult result;
    if (!db.read("SELECT state "
                 "FROM " + tableName + " "
                 "WHERE jobID=" + SQ(jobID) + ";",
                 result)) {
        STHROW("502 Select failed");
    }
    if (result.empty()) {
        STHROW("404 No job with this jobID");
    }
    const string& state = result[0][0];

    // Make sure we're failing a job that's actually running
    if (state != "RUNNING") {
        SINFO("Trying to fail job#" << jobID << ", but isn't RUNNING (" << state << ")");
        STHROW("405 Can only fail RUNNING jobs");
    }

    // Set state to failed.
    string updateString = "state='FAILED'";

    // Update the data too.
    if (command.request.isSet("data")) {
        updateString += ", data=" + SQ(command.request["data"]);
    }

    // Make the change.
    if (!db.writeIdempotent("UPDATE " + tableName + " SET " + updateString + "WHERE jobID=" + SQ(jobID) + ";")) {
        STHROW("502 Fail failed");
    }

    return true;
}

bool BedrockPlugin_Jobs::processGetJob(SQLite& db, BedrockCommand& command) {
    auto jobList = processGetCommon(db, command);
    SASSERT(jobList.size() == 1);
    command.response.content = jobList.front();
    return true;
}

bool BedrockPlugin_Jobs::processGetJobs(SQLite& db, BedrockCommand& command) {
    auto jobList = processGetCommon(db, command);
    command.jsonContent["jobs"] = SComposeJSONArray(jobList);
    return true;
}

list<string> BedrockPlugin_Jobs::processGetCommon(SQLite& db, BedrockCommand& command) {
    // If we're here it's because peekCommand found some data; re-execute
    // the query for real now.  However, this time we will order by
    // priority.  We do this as three separate queries so we only have one
    // unbounded column in each query.  Additionally, we wrap each inner
    // query in a "SELECT *" such that we can have an "ORDER BY" and
    // "LIMIT" *before* we UNION ALL them together.  Looks gnarly, but it
    // works!
    SQResult result;
    const list<string> nameList = SParseList(command.request["name"]);
    string safeNumResults = SQ(max(command.request.calc("numResults"),1));
    bool mockRequest = command.request.isSet("mockRequest") || command.request.isSet("getMockedJobs");

    // The following line is *important* for random table selection. We want to make sure we eventually hit every
    // table, so we've chosen *11* as our offset value, (specifically to work with a TABLE_COUNT of 100). This will
    // increment by 11 each time, and the modulus will hit every one of our 100 tables before starting over. To do this
    // the increment value must not be a factor of the TABLE_COUNT, or you will skip some tables (if we'd chosen 10,
    // then we'd only hit tables 0, 10, 20, 30, etc). You might ask, "Why not just increment by 1? Then it doesn't
    // matter how this works with the table count." That's true, but if each `GetJob` command starts on sequential
    // table numbers, they'll be more likely to conflict, if they don't find a suitable job in the *first* table they
    // look at.
    // This way, we've guaranteed that all 100 tables will get looked at over 100 commands, and done our best to
    // minimize conflicts.
    // TODO: Add hinting based on existing contents of DB.
    int64_t startAt = currentStartTableNumber.fetch_add(11) % TABLE_COUNT;
    int64_t current = startAt;
    int64_t checkCount = 0;
    uint64_t start = STimeNow();
    while (true) {
        checkCount++;
        string tableName = getTableName(current);

        string selectQuery =
            "SELECT jobID, name, data, parentJobID, retryAfter, created, repeat, lastRun, nextRun FROM ( "
                "SELECT * FROM ("
                    "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                    "FROM " + tableName + " "
                    "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                        "AND priority=1000 "
                        "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                        "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(command.request["name"])) + " " +
                        string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                    "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
            "UNION ALL "
                "SELECT * FROM ("
                    "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                    "FROM " + tableName + " "
                    "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                        "AND priority=500 "
                        "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                        "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(command.request["name"])) + " " +
                        string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                    "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
            "UNION ALL "
                "SELECT * FROM ("
                    "SELECT jobID, name, data, priority, parentJobID, retryAfter, created, repeat, lastRun, nextRun "
                    "FROM " + tableName + " "
                    "WHERE state IN ('QUEUED', 'RUNQUEUED') "
                        "AND priority=0 "
                        "AND " + SCURRENT_TIMESTAMP() + ">=nextRun "
                        "AND name " + (nameList.size() > 1 ? "IN (" + SQList(nameList) + ")" : "GLOB " + SQ(command.request["name"])) + " " +
                        string(!mockRequest ? " AND JSON_EXTRACT(data, '$.mockRequest') IS NULL " : "") +
                    "ORDER BY nextRun ASC LIMIT " + safeNumResults +
                ") "
            ") "
            "ORDER BY priority DESC "
            "LIMIT " + safeNumResults + ";";
        if (!db.read(selectQuery, result)) {
            STHROW("502 Query failed");
        }

        if (!result.empty()) {
            // We found at least one!
            break;
        }

        // If not, look at the next table.
        current++;
        current %= TABLE_COUNT;
        if (current == startAt) {
            // Did all of them, I guess we're done.
            break;
        }
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

    uint64_t elapsed = STimeNow() - start;
    string elapsedMS = to_string(elapsed / 1000) + "." + to_string((elapsed % 1000) / 100);
    SINFO("Checked " << checkCount << " tables for jobs in process in " << elapsedMS << "ms.");

    // Prepare to update the rows, while also creating all the child objects
    list<string> nonRetriableJobs;
    list<STable> retriableJobs;
    list<string> jobList;
    for (size_t c=0; c<result.size(); ++c) {
        SASSERT(result[c].size() == 9); // jobID, name, data, parentJobID, retryAfter, created, repeat, lastRun, nextRun

        // Add this object to our output
        STable job;
        SINFO("Returning jobID " << result[c][0] << " from " << command.request.methodLine);
        job["jobID"] = result[c][0];
        job["name"] = result[c][1];
        job["data"] = result[c][2];
        job["created"] = result[c][5];
        int64_t parentJobID = SToInt64(result[c][3]);

        if (parentJobID) {
            string tableName = getTableName(parentJobID);
            // Has a parent job, add the parent data
            job["parentJobID"] = to_string(parentJobID);
            job["parentData"] = db.read("SELECT data FROM " + tableName + " WHERE jobID=" + SQ(parentJobID) + ";");
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
            string tableName = getTableName(SToInt64(result[c][0]));
            if (!db.read("SELECT jobID, data, state FROM " + tableName + " WHERE parentJobID != 0 AND parentJobID=" + result[c][0] + " AND state IN ('FINISHED', 'CANCELLED');", childJobs)) {
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
        for (auto& job : nonRetriableJobs) {
            int64_t jobID = SToInt64(job);
            string tableName = getTableName(jobID);
            string updateQuery = "UPDATE " + tableName + " "
                                 "SET state='RUNNING', "
                                     "lastRun=" + SCURRENT_TIMESTAMP() + " "
                                 "WHERE jobID=" + SQ(jobID) + ";";
            if (!db.writeIdempotent(updateQuery)) {
                STHROW("502 Update failed");
            }
        }
    }

    // Update jobs with retryAfter
    if (!retriableJobs.empty()) {
        SINFO("Updating jobs with retryAfter");
        for (auto job : retriableJobs) {
            int64_t jobID = SToInt64(job["jobID"]);
            string tableName = getTableName(jobID);
            string currentTime = SCURRENT_TIMESTAMP();
            string retryAfterDateTime = "DATETIME(" + currentTime + ", " + SQ(job["retryAfter"]) + ")";
            string repeatDateTime = _constructNextRunDATETIME(job["nextRun"], job["lastRun"] != "" ? job["lastRun"] : job["nextRun"], job["repeat"]);
            string nextRunDateTime = repeatDateTime != "" ? "MIN(" + retryAfterDateTime + ", " + repeatDateTime + ")" : retryAfterDateTime;
            string updateQuery = "UPDATE " + tableName + " "
                                 "SET state='RUNQUEUED', "
                                     "lastRun=" + currentTime + ", "
                                     "nextRun=" + nextRunDateTime + " "
                                 "WHERE jobID = " + SQ(jobID) + ";";
            if (!db.writeIdempotent(updateQuery)) {
                STHROW("502 Update failed");
            }
        }
    }

    return jobList;
}

bool BedrockPlugin_Jobs::processRequeueJobs(SQLite& db, BedrockCommand& command) {
    SINFO("Requeueing jobs with IDs: " << command.request["jobIDs"]);
    list<int64_t> jobIDs = SParseIntegerList(command.request["jobIDs"]);
    for (auto& jobID: jobIDs) {
        string tableName = getTableName(jobID);
        string updateQuery = "UPDATE " + tableName + " SET state = 'QUEUED', nextRun = DATETIME("+ SCURRENT_TIMESTAMP() + ") WHERE jobID=" + SQ(jobID) + ";";
        if (!db.writeIdempotent(updateQuery)) {
            STHROW("502 RequeueJobs update failed");
        }
    }

    return true;
}

bool BedrockPlugin_Jobs::processFinishJob(SQLite& db, BedrockCommand& command) {
    jobInfo info = processRetryFinishCommon(db, command);
    string tableName = getTableName(info.jobID);

    // If we are finishing a job that has child jobs, set its state to paused.
    if (_hasPendingChildJobs(db, info.jobID)) {
        // Update the parent job to PAUSED
        SINFO("Job has child jobs, PAUSING parent, QUEUING children");
        if (!db.writeIdempotent("UPDATE " + tableName + " SET state='PAUSED' WHERE jobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Parent update failed");
        }

        // Also un-pause any child jobs such that they can run
        if (!db.writeIdempotent("UPDATE " + tableName + " SET state='QUEUED' "
                      "WHERE state='PAUSED' "
                        "AND parentJobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Child update failed");
        }

        // All done processing this command
        return true;
    }

    // If the job is repeating, update the nextRun value, and we're done.
    if (!info.repeat.empty()) {
        string safeNewNextRun = _constructNextRunDATETIME(info.nextRun, info.lastRun, info.repeat);
        // The "nextRun" at this point is still
        // storing the last time this job was *scheduled* to be run;
        // lastRun contains when it was *actually* run.
        SINFO("Rescheduling job#" << info.jobID << ": " << safeNewNextRun);

        // Update this job
        if (!db.writeIdempotent("UPDATE " + tableName + " SET nextRun=" + safeNewNextRun + ", state='QUEUED' WHERE jobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Update failed");
        }
        return true;
    }

    // For non-repeating jobs, we handle them differently depending on whether they're children or not.
    if (info.parentJobID) {
        // This is a child job.  Mark it as finished.
        if (!db.writeIdempotent("UPDATE " + tableName + " SET state='FINISHED' WHERE jobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Failed to mark job as FINISHED");
        }

        // Resume the parent if this is the last pending child
        if (!_hasPendingChildJobs(db, info.parentJobID)) {
            SINFO("Job has parentJobID: " + to_string(info.parentJobID) + " with no other pending children, resuming parent job");
            if (!db.writeIdempotent("UPDATE " + tableName + " SET state='QUEUED' where jobID=" + SQ(info.parentJobID) + ";")) {
                STHROW("502 Update failed");
            }
        }
    } else {
        // This is a standalone (not a child) job; delete it.
        if (!db.writeIdempotent("DELETE FROM " + tableName + " WHERE jobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Delete failed");
        }
    }

    return true;
}

bool BedrockPlugin_Jobs::processRetryJob(SQLite& db, BedrockCommand& command) {
    jobInfo info = processRetryFinishCommon(db, command);
    string tableName = getTableName(info.jobID);

    // We can update the name of jobs that we're going to retry.
    const string& name = command.request["name"];
    if (!name.empty()) {
        if (!db.writeIdempotent("UPDATE " + tableName + " SET name=" + SQ(name) + " WHERE jobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Failed to update job name");
        }
    }
    string safeNewNextRun = "";
    // If this is set to repeat, get the nextRun value
    if (!info.repeat.empty()) {
        safeNewNextRun = _constructNextRunDATETIME(info.nextRun, info.lastRun, info.repeat);
    } else {
        // Otherwise, we'll build one from the `nextRun` or `delay` value.
        const string& newNextRun = command.request["nextRun"];
        if (newNextRun.empty()) {
            SINFO("nextRun isn't set, using delay");
            int64_t delay = command.request.calc64("delay");
            if (delay < 0) {
                STHROW("402 Must specify a non-negative delay when retrying");
            }
            string newRepeat = "FINISHED, +" + to_string(delay) + " SECONDS";
            safeNewNextRun = _constructNextRunDATETIME(info.nextRun, info.lastRun, newRepeat);
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
        SINFO("Rescheduling job#" << info.jobID << ": " << safeNewNextRun);

        // Update this job
        if (!db.writeIdempotent("UPDATE " + tableName + " SET nextRun=" + safeNewNextRun + ", state='QUEUED' WHERE jobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Update failed");
        }
    }

    return true;
}

BedrockPlugin_Jobs::jobInfo BedrockPlugin_Jobs::processRetryFinishCommon(SQLite& db, BedrockCommand& command) {
    verifyAttributeInt64(command.request, "jobID", 1);
    jobInfo info = {0, 0, "", "", "", ""};
    info.jobID = command.request.calc64("jobID");
    string tableName = getTableName(info.jobID);

    // Verify there is a job like this and it's running
    SQResult result;
    if (!db.read("SELECT state, nextRun, lastRun, repeat, parentJobID, json_extract(data, '$.mockRequest') "
                 "FROM " + tableName + " "
                 "WHERE jobID=" + SQ(info.jobID) + ";",
                 result)) {
        STHROW("502 Select failed");
    }
    if (result.empty()) {
        STHROW("404 No job with this jobID");
    }

    info.state = result[0][0];
    info.nextRun = result[0][1];
    info.lastRun = result[0][2];
    info.repeat = result[0][3];
    info.parentJobID = SToInt64(result[0][4]);
    bool mockRequest = result[0][5] == "1";

    // Make sure we're finishing a job that's actually running
    if (info.state != "RUNNING" && info.state != "RUNQUEUED" && !mockRequest) {
        SINFO("Trying to finish job#" << info.jobID << ", but isn't RUNNING or RUNQUEUED (" << info.state << ")");
        STHROW("405 Can only retry/finish RUNNING and RUNQUEUED jobs");
    }

    // If we have a parent, make sure it is PAUSED.  This is to just
    // double-check that child jobs aren't somehow running in parallel to
    // the parent.
    if (info.parentJobID) {
        auto parentState = db.read("SELECT state FROM " + tableName + " WHERE jobID=" + SQ(info.parentJobID) + ";");
        if (!SIEquals(parentState, "PAUSED")) {
            SINFO("Trying to finish/retry job#" << info.jobID << ", but parent isn't PAUSED (" << parentState << ")");
            STHROW("405 Can only retry/finish child job when parent is PAUSED");
        }
    }

    // Delete any FINISHED/CANCELLED child jobs, but leave any PAUSED children alone (as those will signal that
    // we just want to re-PAUSE this job so those new children can run)
    if (!db.writeIdempotent("DELETE FROM " + tableName + " WHERE parentJobID != 0 AND parentJobID=" + SQ(info.jobID) + " AND state IN ('FINISHED', 'CANCELLED');")) {
        STHROW("502 Failed deleting finished/cancelled child jobs");
    }

    // If we've been asked to update the data, let's do that
    auto data = command.request["data"];
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
        if (!db.writeIdempotent("UPDATE " + tableName + " SET data=" + SQ(data) + " WHERE jobID=" + SQ(info.jobID) + ";")) {
            STHROW("502 Failed to update job data");
        }
    }

    return info;
}

bool BedrockPlugin_Jobs::processUpdateJob(SQLite& db, BedrockCommand& command) {
    verifyAttributeInt64(command.request, "jobID", 1);
    verifyAttributeSize(command.request, "data", 1, MAX_SIZE_BLOB);
    int64_t jobID = command.request.calc64("jobID");
    string tableName = getTableName(jobID);

    // If a repeat is provided, validate it
    if (command.request.isSet("repeat")) {
        if (command.request["repeat"].empty()) {
            SWARN("Repeat is set in UpdateJob, but is set to the empty string. jobID: "
                  << command.request["jobID"] << ", removing attribute.");
            command.request.erase("repeat");
        } else if (!_validateRepeat(command.request["repeat"])) {
            STHROW("402 Malformed repeat");
        }
    }

    // Verify there is a job like this
    SQResult result;
    if (!db.read("SELECT jobID, nextRun, lastRun "
                 "FROM " + tableName + " "
                 "WHERE jobID=" + SQ(jobID) + ";",
                 result)) {
        STHROW("502 Select failed");
    }
    if (result.empty()) {
        STHROW("404 No job with this jobID");
    }

    const string& nextRun = result[0][1];
    const string& lastRun = result[0][2];

    // Are we rescheduling?
    const string& newNextRun = command.request.isSet("repeat") ? _constructNextRunDATETIME(nextRun, lastRun, command.request["repeat"]) : "";

    // Update the data
    if (!db.writeIdempotent("UPDATE " + tableName + " "
                            "SET data=" +
                            SQ(command.request["data"]) + " " +
                            (command.request.isSet("repeat") ? ", repeat=" + SQ(SToUpper(command.request["repeat"])) : "") +
                            (!newNextRun.empty() ? ", nextRun=" + newNextRun : "") +
                            "WHERE jobID=" +
                            SQ(jobID) + ";")) {
        STHROW("502 Update failed");
    }

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
    string tableName = getTableName(jobID);
    // Returns true if there are any children of this jobID in a "pending" (eg,
    // running or yet to run) state
    SQResult result;
    if (!db.read("SELECT 1 "
                 "FROM " + tableName + " "
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

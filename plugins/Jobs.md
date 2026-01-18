# Bedrock::Jobs -- Rock solid job queuing
Bedrock::Jobs is a plugin to the [Bedrock data foundation](../README.md) that manages a scheduled job queue.  Commands include:

 * **CreateJob( name, [data], [firstRun], [repeat], [sequentialKey] )** - Schedules a new job, optionally in the future, optionally to repeat.
   * *name* - Any arbitrary string name for this job.
   * *data* - (optional) An arbitrary data blob to associate with this job, typically JSON encoded.
   * *firstRun* - (optional) The time/date on which to run this job the first time, in "YYYY-MM-DD [HH:MM:SS]" format
   * *repeat* - (optional) Description of how this job should repeat (see ["Repeat Syntax"](#repeat-syntax) below)
   * *sequentialKey* - (optional) Jobs with the same sequentialKey run one at a time, in creation order (see ["Sequential Jobs"](#sequential-jobs) below)

 * **GetJob( name, [connection: wait, [timeout] ] )** - Waits for a match (if requested) and atomically dequeues exactly one job.
   * *name* - A pattern to match in GLOB syntax (eg, "Foo*" will get the first job whose name starts with "Foo")
   * *connection* - (optional) If set to "wait", will wait up to "timeout" ms for the match
   * *timeout* - (optional) Number of ms to wait for a match

 * **UpdateJob( jobID, data )** - Updates the data associated with a job.
   * *jobID* - Identifier of the job to update
   * *data* - New data object to associate with the job
   * *repeat* - A description of how often to repeat this job (optional)
   * *jobPriority* - New priority of the job (optional)

 * **QueryJob( jobID )** - Retrieves the current state and data associated with a job.
   * *jobID* - Identifier of the job to query

 * **FinishJob( jobID, [data] )** - Marks a job as finished, which causes it to repeat if requested.
   * *jobID* - Identifier of the job to finish
   * *data* - (optional) New data object to associate with the job (especially useful if repeating, to pass state to the next worker).

 * **DeleteJob( jobID )** - Removes all trace of a job.
   * *jobID* - Identifier of the job to delete

 * **RetryJob( jobID )** - Removes all trace of a job.
   * *jobID* - Identifier of the job to retry
   * *nextRun* - (optional) The time/date on which the job should be set to run again, in "YYYY-MM-DD [HH:MM:SS]" format. This is ignored if the job is set to repeat.
   * *delay* - (optional) Number of seconds to wait before retrying. This is ignored if the job is set to repeat or if "nextRun" is set.
   * *name* - (optional) Any arbitrary string name for this job.
   * *data* - (optional) Data to associate with this job
   * *ignoreRepeat* - (optional) Ignore a job's repeat parameter when calculating when to retry the job

## Sample Session
This provides comprehensive functionality for scheduled, recurring, atomically-processed jobs by blocking workers.  For example, first create a job and assign it some data to be used by the worker:

    $ nc localhost 8888
    CreateJob
    name: foo
    data: {"value":1}
    repeat: finished, +1 minute
    
    200 OK
    Content-Length: 11
    
    {"jobID":1}

Next, a worker queries for a job:  (Protip: Set "Connection: wait" and "Timeout: 60000" to wait up to 60s for a response and thus get instant worker activation, without high-frequency worker polling.)

    GetJob
    name: foo
    
    200 OK
    Content-Length: 43
    
    {"data":{"value":1},"jobID":1,"name":"foo"}

This atomically dequeues exactly one job, returning the data associated with that job.  As the worker operates on the job, it can report incremental progress back to Bedrock:

    UpdateJob
    jobID: 1
    data: {"value":2}
    
    200 OK

This allows some other party (such as the service that queued the job) to optionally track progress on the job (eg, to show a progress bar): (*coming soon*)

    QueryJob
    jobID: 1
    
    200 OK
    Content-Length: 43
    
    {"data":{"value":2},"jobID":1,"name":"foo","state":"RUNNING"}

When the worker finishes, it marks it as complete.  Additionally, it can provide final data on the job, which will be provided to the next worker in the event this job is a recurring one.

    FinishJob
    jobID: 1
    data: {"value":3}
    
    200 OK

In this case, the job was configured to repeat in one minute.  This means a request for the job immediately after fails:

    GetJob
    name: foo
    
    404 No job found

But as we can see, the job is there, queued for the future:

    Query
    query: select * from jobs;
    
    200 OK
    Content-Length: 110
    
    [["2014-12-29 07:38:51",1,"QUEUED","foo","2014-12-29 07:39:51","2014-12-29 07:39:04","finished, +1 minute",{"value":2}]]

Once 1 minute elapses, the job is available to be worked on again -- and is seeded with the data provided when it was finished last time.  This is a very simple, reliable mechanism to allow one job to finish where the last job left off (eg, when processing a feed where it's bad to double-process the same entry):

    GetJob
    name: foo
    
    200 OK
    Content-Length: 43
    
    {"data":{"value":3},"jobID":1,"name":"foo"}

## Repeat Syntax
It's surprisingly tricky to come up with a succint but powerful language to describe all the myriad possible recurring patterns.  With this in mind, we lean heavily upon the extensive capabilities already built into sqlite.  Specifically, a recurring pattern is defined as a "base" and one or more "modifiers":

### Repeat Base
The "base" defines from which moment in time to calculate the next time the job should run.  Three separate moments are supported:

* **SCHEDULED** - Reschedule from the moment the job was *scheduled* to run, regardless of when it actually did.
* **STARTED** - Reschedule from the moment the job *started* to run.
* **FINISHED** - Reschedule from the moment the job *finished*.

To contrast these options, consider the example of a job that:

* Was scheduled to start at 1:00pm
* Actually started at 1:15pm
* Took 30 minutes to complete

If we configured a job to repeat after 1 hour, it would run again at the following times depending on the base:

* *SCHEDULED, +1 HOUR* = 2:00pm
* *STARTED, +1 HOUR* = 2:15pm
* *FINISHED, +1 HOUR* = 2:45pm

### Repeat Modifiers
Rather than invent a new syntax for how to calculate offsets, we simply reuse the following [sqlite datetime modifiers](https://www.sqlite.org/lang_datefunc.html):

* **+/- NNN MINUTES/HOURS/DAYS/MONTHS/YEARS** - Advance forward or backwards by the stated number of intervals.
* **START OF DAY/MONTH/YEAR** - Rewinds to the start of the interval
* **WEEKDAY N** - Advances to the next day matching the number (eg, 0=Sunday, 1=Monday, ..., 6=Saturday)

### Repeat Examples
The above takes some getting used to, but is incredibly powerful for expressing a wide range of possible repeat scenarios:

* *FINISHED, +1 HOUR* - Waits 60 minutes before repeating
* *SCHEDULED, +1 HOUR* - Runs once every hour, and also "catches up" (eg, runs multiple times back to back) if any given hour is missed.
* *FINISHED, +1 DAY, START OF DAY, +4 HOURS* - Runs every day at 4am UTC
* *FINISHED, +1 DAY, WEEKDAY 1, START OF DAY, +6 HOURS* - Runs every Monday at 6am UTC

### Canned Repeat Schedules
Confused by all the above?  No problem -- there are a few "canned" patterns built in for simplicity:

* **HOURLY** = FINISHED, + 1 HOUR
* **DAILY** = FINISHED, + 1 DAY
* **WEEKLY** = FINISHED, + 7 DAYS

These are useful if you generally want something to happen *approximately but no greater* than the indicated frequency.

### Sequential Jobs
Jobs with the same `sequentialKey` run one at a time, in creation order. New jobs with a key that's already active are set to `WAITING` state. When the active job completes (FinishJob/FailJob/DeleteJob), the oldest WAITING job is promoted to `QUEUED`.

**Unsupported:** `repeat` and `parentJobID` with `sequentialKey` - these jobs don't get deleted on completion, so WAITING jobs would never be promoted.

---
layout: default
---

# Bedrock::Jobs -- Rock solid job queuing
Bedrock::Jobs is a plugin to the [Bedrock data foundation](../README.md) that manages a scheduled job queue.  Commands include:

 * **CreateJob( name, [data], [firstRun], [repeat] )** - Schedules a new job, optionally in the future, optionally to repeat.
   * *name* - Any arbitrary string name for this job.
   * *data* - (optional) An arbitrary data blob to associate with this job, typically JSON encoded.
   * *firstRun* - (optional) The time/date on which to run this job the first time, in "YYYY-MM-DD [HH:MM:SS]" format
   * *repeat* - (optional) Description of how this job should repeat (see ["Repeat Syntax"](#repeat-syntax) below)

 * **GetJob( name, [connection: wait, [timeout] ] )** - Waits for a match (if requested) and atomically dequeues exactly one job.
   * *name* - A pattern to match in GLOB syntax (eg, "Foo*" will get the first job whose name starts with "Foo")
   * *connection* - (optional) If set to "wait", will wait up to "timeout" ms for the match
   * *timeout* - (optional) Number of ms to wait for a match

 * **GetJobs( name, numResults [connection: wait, [timeout] ] )** - Waits for a match (if requested) and atomically dequeues up to the number of requested jobs.
   * *name* - A pattern to match in GLOB syntax (eg, "Foo*" will get the first job whose name starts with "Foo")
   * *numResults* - Maximum number of jobs to dequeue
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

## Sample Session
This provides comprehensive functionality for scheduled, recurring, atomically-processed jobs by blocking workers.  For example, first create a job and assign it some data to be used by the worker:

    $ nc localhost 8888
    CreateJob
    name: CheckLiveness
    data: {"url":"https://bedrockdb.com"}
    repeat: finished, +1 minute
    
    200 OK
    Content-Length: 11
    
    {"jobID":1}

Next, a worker queries for a job:  (Protip: Set "Connection: wait" and "Timeout: 60000" to wait up to 60s for a response and thus get instant worker activation, without high-frequency worker polling.)

    GetJob
    name: CheckLiveness
    
    200 OK
    Content-Length: 72
    
    {"data":{"url":"https://bedrockdb.com"},"jobID":1,"name":"CheckLiveness"}

This atomically dequeues exactly one job, returning the data associated with that job.  As the worker operates on the job, it can report incremental progress back to Bedrock:

    UpdateJob
    jobID: 1
    data: {"url":"https://bedrockdb.com","status":"CHECKING"}
    
    200 OK

This allows some other party (such as the service that queued the job) to optionally track progress on the job (eg, to show a progress bar):

    QueryJob
    jobID: 1
    
    200 OK
    Content-Length: 237

    {"created":"2016-10-18 18:45:19","data":{"url":"https://bedrockdb.com","status":"CHECKING"},"jobID":1,"lastRun":"2016-10-18 18:45:33","name":"CheckLiveness","nextRun":"2016-10-18 18:45:19","repeat":"FINISHED, +1 MINUTE","state":"RUNNING"}

When the worker finishes, it marks it as complete.  Additionally, it can provide final data on the job, which will be provided to the next worker in the event this job is a recurring one.

    FinishJob
    jobID: 1
    data: {"url":"https://bedrockdb.com","failCount":1}
    
    200 OK

In this case, the job was configured to repeat in one minute.  This means a request for the job immediately after fails:

    GetJob
    name: *
    
    404 No job found

But as we can see, the job is there, queued for the future:

    Query: SELECT * FROM jobs;
    
    200 OK
    Content-Length: 262
    
    created | jobID | state | name | nextRun | lastRun | repeat | data | priority | parentJobID
    2016-10-18 18:45:19 | 1 | QUEUED | CheckLiveness | 2016-10-18 18:54:11 | 2016-10-18 18:52:54 | FINISHED, +1 MINUTE | {"url":"https://bedrockdb.com","failCount":1} | 0 | 0

Once 1 minute elapses, the job is available to be worked on again -- and is seeded with the data provided when it was finished last time.  This is a very simple, reliable mechanism to allow one job to finish where the last job left off (eg, when processing a feed where it's bad to double-process the same entry):

    GetJob
    name: *
    
    200 OK
    Content-Length: 86
    
    {"data":{"url":"https://bedrockdb.com","failCount":1},"jobID":1,"name":"CheckLiveness"}

## Blocking not polling
The point of a job system is to distribute a bunch of worker processes across a bunch of different servers.  To do this means each of those job servers is continuously asking for work.  A straightforward way to do that is to have each worker just "poll" Bedrock::Jobs at some high frequency until work is available.  However, that's very wasteful, and means there is an average delay between queuing and processing a job equal to half the period of the polling frequency.  Accordingly, Bedrock::Jobs eschews polling in favor of a "blocking" design that allows a worker to wait up to some timeout for work to develop.  So if the worker process requests work when none is available:

    GetJob
    name: SendEmail
    connection: wait
    timeout: 60000

The worker will just block until it's avaiable.  Then if we queue a job from some other process (or server):

    CreateJob
    name: SendEmail
           
    200 OK
    Content-Length: 11
    
    {"jobID":2}

That causes Bedrock::Jobs to instantly respond to the other worker that is waiting for work, thereby processing it immediately without delay:

    200 OK
    Content-Length: 40
    
    {"data":{},"jobID":2,"name":"SendEmail"}

This enables you to build a very efficient, very high performance job queuing engine.  However, there's no need for you to build it yourself -- this is already provided as part of the base Bedrock::Jobs engine in the form of `BedrockWorkerManager`, which is available in the [Bedrock-PHP](https://github.com/Expensify/Bedrock-PHP/blob/main/bin/BedrockWorkerManager.php) repo.  `BedrockWorkerManager` (or "BWM" among friends) is a simple PHP command-line application that:

1. Waits for resources to free up
2. Waits for a job
3. Spawns a worker for that job
4. Goto 1

It's usage is simply:

    sudo -u user php ./bin/BedrockWorkerManager.php --jobName=* --workerPath=/your/code/path --maxLoad=5.0

This will pull down jobs of any name, and look in the `/your/code/path` directory for a worker class that shares the name of the job to be queued.  It will keep spawning new workers so long as new jobs are queued, so long as the total CPU load stays under `maxLoad`.  In general, you can run BWM on all your webservers to also make them into job servers that "soak up" excess capacity to do background operations, without impacting live site performance.

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

* **HOURLY** = FINISHED, +1 HOUR
* **DAILY** = FINISHED, +1 DAY
* **WEEKLY** = FINISHED, +7 DAYS

These are useful if you generally want something to happen *approximately but no greater* than the indicated frequency.

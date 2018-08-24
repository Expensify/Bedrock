# Bedrock::Jobs -- Rock solid job queuing
Bedrock::Jobs is a plugin to the [Bedrock data foundation](../README.md) that manages a scheduled job queue.


## Job State and the Lifecycle of a Job.

These are the possible states a job can be in.

* `QUEUED` - This job is waiting to start. This is the most typical case for a newly created job to be in.
* `RUNNING` - This job has been given to a worker. Nominally, work is being done while this is set, but all we really know is that we've handed this job to a client and that client hasn't told us the job is done. This is the primary source of "stuck jobs" - the connection to the client breaks and it either never receives the job, or it never reports that it finished the job.
* `RUNQUEUED` - This is `RUNNING` for jobs with `RetryAfter` set for them. The job is both `RUNNING` (a client dequeued it and hasn't said it finished), and `QUEUED`. If the client never reports back that the job has finished, it will be run again at the `nextRun` time. For these jobs, `nextRun` is set at dequeue time.
* `PAUSED` - This job is waiting for something else to happen, either a parent or child to finish running. This state is different from `QUEUED` only insofar as jobs wont move directly from `PAUSED` to `RUNNING` (i.e., `PAUSED` jobs can't be dequeued).
* `FINISHED` - This job has completed (it will likely be deleted shortly).
* `FAILED` - Functionally like `FINISHED`, but won't be automatically deleted. 
* `CANCELLED` - This job was canceled without being run. This is effectively like `FINISHED` in terms of how jobs are handled, but is reported separately to a parent job when it occurs for a child job. Jobs can only be canceled if they have not yet changed to `RUNNING`. Note: `CANCELLED` has two `L`s, not one.

## The Relationship Between Parent and Child Jobs

A job is allowed to create "children" which can be any number of other jobs. These need to complete before the original "parent" job will be considered finished. When a job is created, if it it has a parent, and that parent is `RUNNING`, the newly created child job will be `PAUSED`. Otherwise, the newly created child job will be `QUEUED`. Child jobs can only be created for parents that are `RUNNING` or `PAUSED`. When we call `FinishJob` on a parent job with pending children, the parent will be set to `PAUSED`, and the children will all be set to `QUEUED`. You notice that we just said that a `RUNNING` parent creates `PAUSED` children, but we're allowed to create children while the parent is `PAUSED` as well. This allows children to create other children (i.e., sibling jobs) on behalf of their parent. When we call `FinishJob` on the *last* pending child of a parentJob, we reset the parent's state to `QUEUED`, so that it will run again (does this mean that parent jobs are inherently recurring? It seems so.) The `PAUSED` state exists primarily to prevent jobs from being dequeued (which make happen if they were `QUEUED`) until all their co-requisite jobs are ready to run. This state may have been more clearly named `WAITING`.

## RetyAfter - Assuming Lost Jobs Never Ran
Original PR is here for dissection: https://github.com/Expensify/Bedrock/issues/111
Followup is here: https://github.com/Expensify/Bedrock/pull/243

The `retryAfter` flag is essentially a timeout on jobs, where if a caller dequeues a job and does not report it
finished before the `retryAfter` time, we will assume it never ran and allow it to be dequeued again.

## IDEA:
Fix stuck jobs with timeouts. This change does everything that RetryAfter does, but better, and in a more intuitive
way. Add two columns to the DB - timeout, which is a duration, and expires, which is a timestamp. When we create a job,
we can set a timeout, like we do for bedrock commands. When we mark a job as running, we set it's `expires` field to
the current timestamp plus the timeout.

This way, we can periodically "reset" all the jobs that have run past their timeout without being finished, so they can
be run again.

FLAW: This fixes the "stuck jobs" issue, because we can't drop jobs on the way out of Bedrock so that the caller never
receives them, but instead it replaces it with an issue where we can assign a job twice, and have it run twice, since
the opposite may happen - the client may finish processing the job, but then fail to report to bedrock that it was
finished, allowing bedrock to hit the timeout and try again.

Whether dropping a job or repeating it is better is largely up to the job. It may be better to let bedrock err on the
side of repeating jobs, and them let each job worker decide if it can idempotently handle repeated requests.

More thought is needed.

# Jobs - Callable Commands

## `CreateJob`/`CreateJobs`

These commands will create zero or more new jobs, depending on their arguments.
The format for `CreateJobs` is a HTTP-like set of header name/value pairs with the parameters for a single job.
The format for a `CreateJobs` command is a single header named `jobs` containing a JSON array of jobs, each as a JSON
object.

### Parameters for Creating Jobs

* `name` (required) - The name of this job. This can be any arbitrary string, but should be chosen with more care than that. See the rules around retrieving jobs by name in `GetJob`/`GetJobs`.
* `jobPriority` (optional) - The priority of the job. Valid values are 0, 500, and 1000. If not supplied, a default of 500 is used. This parameter controls which jobs are dequeued first. Jobs with a numerically higher priority have priority over jobs with a lower priority (i.e., bigger numbers are dequeued first).
* `unique` (optional) - If set, this job will only be created if another job with the same name does not already exist. If a job with the same name exists, we'll update its stored values for `repeat`, `data`, and `priority`. If a job was marked as `unique`, and a job with a matching name exists, the `jobID` of the existing job will be returned in place of a new `jobID`
* `parentJobID` (optional) - If this job is a child job, specify the `jobID` of its parent job.
* `mockRequest` (optional) - TODO explanation.
* `data` (optional) - Any arbitrary JSON blob that contains parameters needed to run this particular job.
* `firstRun` (optional) - The time/date on which to run this job the first time, in "YYYY-MM-DD [HH:MM:SS]" format. Defaults to the current time.
* `repeat` (optional) - Description of how this job should repeat (see ["Repeat Syntax"](#repeat-syntax) below)
* `retryAfter` (optional) - RetryAfter uses the same SQLite datetime modifiers as mentioned in `repeat`. This effectively is a timeout for how long we wait until we assume a job was lost. *Note:* `retryAfter` cannot be combined with `repeat` or `unique`. I think there's also some restriction on how it can be used in parent/child jobs.
   
### Return Value

`CreateJob` returns a header named `jobID` containing the `jobID` for the newly created job, or existing job if this was a `unique` job that already existed. `CreateJobs` returns a list named `jobIDs` as a JSON array, with each entry corresponding to one of the jobs created, with the same caveat above about existing `jobID`s for `unique` jobs.

#########################################################################

 * **GetJob/GetJobs( name, [connection: wait, [timeout] ] )** - Waits for a match (if requested) and atomically dequeues exactly one job.
   * *name* - A pattern to match in GLOB syntax (eg, "Foo*" will get the first job whose name starts with "Foo")
   * *connection* - (optional) If set to "wait", will wait up to "timeout" ms for the match
   * *timeout* - (optional) Number of ms to wait for a match

 * **CancelJob**

 * **UpdateJob( jobID, data )** - Updates the data associated with a job.
   * *jobID* - Identifier of the job to update
   * *data* - New data object to associate with the job

 * **QueryJob( jobID )** - Retrieves the current state and data associated with a job.
   * *jobID* - Identifier of the job to query

 * **FinishJob( jobID, [data] )** - Marks a job as finished, which causes it to repeat if requested.
   * *jobID* - Identifier of the job to finish
   * *data* - (optional) New data object to associate with the job (especially useful if repeating, to pass state to the next worker).

 * **DeleteJob( jobID )** - Removes all trace of a job.
   * *jobID* - Identifier of the job to delete

 * **FailJob**

 * **RetryJob( jobID )** - Removes all trace of a job. <-- That seems like a copy/paste error.
   * *jobID* - Identifier of the job to retry
   * *nextRun* - (optional) The time/date on which the job should be set to run again, in "YYYY-MM-DD [HH:MM:SS]" format. This is ignored if the job is set to repeat.
   * *delay* - (optional) Number of seconds to wait before retrying. This is ignored if the job is set to repeat or if "nextRun" is set.
   * *name* - (optional) Any arbitrary string name for this job.
   * *data* - (optional) Data to associate with this job

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
It's surprisingly tricky to come up with a succinct but powerful language to describe all the myriad possible recurring patterns.  With this in mind, we lean heavily upon the extensive capabilities already built into sqlite. Specifically, a recurring pattern is defined as a "base" and one or more "modifiers":

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

#pragma once
#include <thread>
#include <future>
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>

class SQLiteThreadPool {
public:
    // Constructor. A SQLiteThreadPool is constructed for a particular database.
    // Note: canWrite is not generally applicable! using it carelessly (i.e., outside of handling replicated traffic)
    // will result in journal corruption! Do not set `canWrite` to true unless you know what you're doing!
    SQLiteThreadPool(SQLite& db, const string threadNamePrefix, bool canWrite = false, size_t threadCount = 0);

    // Destructor.
    ~SQLiteThreadPool();

    // This will asynchronously run the given function. For clarity, this signature looks like:
    // void function(sqlite3*, void*)
    // Passing it an sqlite3* corresponding to the SQLite object this pool was initialized with, and the `data`
    // parameter passed into `runAsync`.
    future<void> runAsync(void* data, function<void(sqlite3*, void*)> f);

    // The name of the database file for this reader pool.
    const string dbFilename;

    // The prefix, to be used with the reader index, to create the thread names.
    const string threadNamePrefix;

private:
    // An item to be placed into the work queue to run asynchronously. Contains the function to run and the data to
    // pass to it.
    struct WorkItem
    {
        // Constructor.
        WorkItem(void* d, packaged_task<void(sqlite3*, void*)>&& t);

        // Members.
        void* data;
        packaged_task<void(sqlite3*, void*)> task;
    };

    // The main function that each thread starts, which waits on a condition variable.
    static void init(size_t threadIndex, SQLiteThreadPool* obj);

    // Synchronization primitives to control access to the work queue.
    mutex m;
    condition_variable cv;

    // The set of all worker threads that belong to this pool.
    list<thread> threads;

    // This is the work queue that we'll use.
    list<WorkItem> workQueue;

    // Flag set by the destructor to indicate all threads should exit.
    atomic<bool> stopping;

    // The list of all the DB handles that our threads will use.
    vector<sqlite3*> dbList;
};

#include "SQLiteThreadPool.h"

SQLiteThreadPool::WorkItem::WorkItem(void* d, packaged_task<void(sqlite3*, void*)>&& t)
    : data(d),
    task(move(t))
{
}

SQLiteThreadPool::SQLiteThreadPool(SQLite& db, const string threadNamePrefix_, bool canWrite, size_t threadCount)
    : dbFilename(db.getFilename()),
    threadNamePrefix(threadNamePrefix_),
    stopping(false),
    dbList(threadCount ? threadCount : thread::hardware_concurrency())
{
    // Create all the DB handles.
    for (size_t i = 0; i < dbList.size(); i++) {
        sqlite3* sqlite_db = 0;
        int result = sqlite3_open_v2(dbFilename.c_str(), &sqlite_db, canWrite ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY, 0);
        if (result != SQLITE_OK) {
            STHROW("500 Couldn't open DB");
        }
        dbList[i] = sqlite_db;
    }

    // Start the thread pool.
    for (size_t i = 0; i < dbList.size(); i++) {
        threads.emplace_back(init, i, this);
    }
}

SQLiteThreadPool::~SQLiteThreadPool()
{
    // We're going to stop, set this before we notify threads, so it's guaranteed that' it's set when we do.
    stopping = true;

    {
        // Lock and notify everyone that we're done. Unlock after so they can finish up.
        unique_lock<mutex> lock(m);
        cv.notify_all();
    }

    // Wait for them to be done.
    for (auto& t : threads) {
        t.join();
    }

    // Delete all our DB handles.
    for (size_t i = 0; i < dbList.size(); i++) {
        SASSERT(!sqlite3_close(dbList[i]));
    }
}

void SQLiteThreadPool::init(size_t threadIndex, SQLiteThreadPool* obj)
{
    SInitialize(obj->threadNamePrefix + to_string(threadIndex));

    // Now we can check if there's work.
    while (true) {
        // Lock around accessing the queue. We need to do this before checking the state of `stopping`, or we could end
        // up missing it getting set, and then locking after the pool destructor already called `notify_all`, and then
        // we'd sit forever in `wait`.
        unique_lock<mutex> lock(obj->m);

        // Check if we're supposed to exit.
        if (obj->stopping) {
            return;
        }

        // See if there's work. If so grab the first item and run it.
        if (obj->workQueue.size()) {
            WorkItem item = move(obj->workQueue.front());
            obj->workQueue.pop_front();

            // IMPORTANT! We need to release this lock before running our task, or we don't actually parallelize
            // anything.
            lock.unlock();
            item.task(obj->dbList[threadIndex], item.data);
        } else {
            // Otherwise, wait for someone to notify us that there's work.
            // When they do, we'll jump back to the top of the loop.
            obj->cv.wait(lock);
        }
    }
}

// We pass a base DB in, the pool will manage copies of that itself. This whole thing generates a packaged_task we can
// enqueue, and return the future from.
future<void> SQLiteThreadPool::runAsync(void* data, function<void(sqlite3*, void*)> f)
{
    // Lock to modify the work queue. Note that we can release this before returning, though it makes little
    // difference.
    unique_lock<mutex> lock(m);

    // Enqueue the task for running.
    workQueue.emplace_back(WorkItem(data, packaged_task<void(sqlite3*, void*)>(f)));

    // Get the result future.
    auto result = workQueue.back().task.get_future();

    // And notify someone there's work to do.
    cv.notify_one();

    // And return the future associated with it so that the caller can wait for it to complete.
    return result;
}

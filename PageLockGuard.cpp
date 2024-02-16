#include "PageLockGuard.h"
mutex PageLockGuard::controlMutex;
map<int64_t, mutex> PageLockGuard::mutexes;
list<int64_t> PageLockGuard::mutexOrder;
map<int64_t, list<int64_t>::iterator> PageLockGuard::mutexOrderFastLookup;
map<int64_t, int64_t> PageLockGuard::mutexCounts;

PageLockGuard::PageLockGuard(int64_t pageNumber) : _pageNumber(pageNumber) {
    if (_pageNumber == 0) {
        return;
    }

    // We need access to the mutex outside of the control lock, so that we aren't blocking other PageLockGuard users while we wait for the page.
    mutex* m;
    {
        lock_guard<mutex> lock(controlMutex);
        auto mutexPair = mutexes.find(_pageNumber);
        if (mutexPair == mutexes.end()) {
            // If there's no mutex for this page, create one. The weird `piecewise_construct` syntax here allows us to create a mutex directly in the map,
            // since mutexes are neither movable nor copyable.
            mutexPair = mutexes.emplace(piecewise_construct, forward_as_tuple(_pageNumber), forward_as_tuple()).first;

            // Set the reference count to 1.
            mutexCounts.emplace(make_pair(_pageNumber, 1l));

            // Set this as the most recently acccessed mutex.
            mutexOrder.push_front(_pageNumber);

            // Store a reference to this item in the list, so that we can quickly find it later.
            mutexOrderFastLookup.emplace(make_pair(_pageNumber, mutexOrder.begin()));
        } else {
            // If the mutex already exists, increment the reference count.
            mutexCounts[_pageNumber]++;

            // If the current mutex was already at the front of the order list, no updates are needed.
            if (mutexOrder.front() != _pageNumber) {
                // Erase the old location of this mutex in the order list and move it to the front.
                mutexOrder.erase(mutexOrderFastLookup.at(_pageNumber));
                mutexOrder.push_front(_pageNumber);

                // And save the new fast lookup at the front.
                mutexOrderFastLookup[_pageNumber] = mutexOrder.begin();
            }
        }

        // In order to keep this list growing forever, we attempt to prune it if it exceeds this value.
        // This value was chosen arbitrarily, but is intended to be sufficiently large. At 500, we'd need 500 separate
        // groups of conflicting commands (each group conflicting on it's own page) all executing simultaneously to hit
        // the limit. This requires at least 500 commands executing simultaneously, and for these mutexes to be useful,
        // it requires significantly more than that, since each mutex is supposed to apply to a set of commands acting
        // on the same page.
        // Additionally, mutexes can't be pruned while in use, so the list will grow over 500 if we do manage to have
        // enough mutexes all attempting to be locked simultaneously.
        static const size_t MAX_PAGE_MUTEXES = 500;
        if (mutexes.size() > MAX_PAGE_MUTEXES) {
            size_t deleted = 0;
            uint64_t start = STimeNow();
            size_t iterationsToTry = mutexes.size() - MAX_PAGE_MUTEXES;

            // We start at the back - the least recently used page - to avoid iterating a bunch of times through the front of the list that we're keeping.
            auto pageIt = mutexOrder.end();
            for (size_t i = 0; i < iterationsToTry; i++) {
                // Move to the previous item in the list. Note we started at `end()`, so we are initially past the end of the list.
                pageIt--;
                int64_t pageToDelete = *pageIt;

                // If there are no threads using this mutex, let's delete it.
                if (mutexCounts[pageToDelete] == 0) {
                    // Delete the mutex itself.
                    mutexes.erase(pageToDelete);

                    // Delete the reference count.
                    mutexCounts.erase(pageToDelete);

                    // Delete the index to the mutex in the order list.
                    mutexOrderFastLookup.erase(pageToDelete);

                    // Remove it from the orderList, and set pageIt to a new valid value (the item just behind the one being deleted).
                    // This will get decremented on the next iteration of this loop, and end up pointing at the item just in front of the one deleted.
                    pageIt = mutexOrder.erase(pageIt);

                    deleted++;
                }
            }

            SINFO("Deleted " << deleted << " page lock mutexes in " << (STimeNow() - start) << "us, " << mutexes.size() << " remaining.");
        }

        // save the mutex such that it will still be accessible once this block ends.
        m = &mutexPair->second;
    }

    // Wait for the given page to be unlocked, and lock it ourself.
    m->lock();
}

PageLockGuard::~PageLockGuard() {
    if (_pageNumber == 0) {
        return;
    }

    lock_guard<mutex> lock(controlMutex);
    auto mutexPair = mutexes.find(_pageNumber);
    mutexPair->second.unlock();

    auto mutexCount = mutexCounts.find(_pageNumber);
    mutexCount->second--;
}

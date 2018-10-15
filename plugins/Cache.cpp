#include "Cache.h"

// ==========================================================================
BedrockPlugin_Cache::LRUMap::LRUMap() {
    // Initialize
}

// ==========================================================================
BedrockPlugin_Cache::LRUMap::~LRUMap() {
    // Just delete all the entries
    while (!empty()) {
        // Pop it off
        popLRU();
    }
}

// ==========================================================================
bool BedrockPlugin_Cache::LRUMap::empty() {
    // Both the map and list are the same size, so check either
    SAUTOLOCK(_mutex);
    return _lruList.empty();
}

// ==========================================================================
void BedrockPlugin_Cache::LRUMap::pushMRU(const string& name) {
    // See if if it's already there
    SAUTOLOCK(_mutex);
    map<string, Entry*>::iterator mapIt = _lruMap.find(name);
    if (mapIt == _lruMap.end()) {
        // Not in the map -- add a new entry
        Entry* entry = new Entry;
        entry->name = name;

        // Insert into the map and list, retaining iterators to both
        entry->mapIt = _lruMap.insert(std::pair<string, Entry*>(name, entry)).first;
        entry->listIt = _lruList.insert(_lruList.end(), entry);
    } else {
        // Already in the map, just move to the end of the list
        Entry* entry = mapIt->second;
        _lruList.erase(entry->listIt);
        entry->listIt = _lruList.insert(_lruList.end(), entry);
    }
}

// ==========================================================================
string BedrockPlugin_Cache::LRUMap::popLRU() {
    // Make sure we're not empty
    SAUTOLOCK(_mutex);
    SASSERT(!empty());

    // Take the first item off the list
    Entry* entry = _lruList.front();
    _lruList.erase(entry->listIt);
    _lruMap.erase(entry->mapIt);
    string nameCopy = entry->name;
    delete entry;
    return nameCopy;
}

// ==========================================================================
BedrockPlugin_Cache::BedrockPlugin_Cache()
    : _maxCacheSize(0) // Will be set inside initialize()
{
    // Nothing to initialize
}

// ==========================================================================
BedrockPlugin_Cache::~BedrockPlugin_Cache() {
    // Nothing to clean up
}

// ==========================================================================
void BedrockPlugin_Cache::initialize(const SData& args, BedrockServer& server) {
    // Check the configuration
    const string& maxCache = SToUpper(args["-cache.max"]);
    int64_t maxCacheSize = SToInt64(maxCache);
    if (SEndsWith(maxCache, "KB"))
        maxCacheSize *= 1024;
    if (SEndsWith(maxCache, "MB"))
        maxCacheSize *= 1024 * 1024;
    if (SEndsWith(maxCache, "GB"))
        maxCacheSize *= 1024 * 1024 * 1024;
    if (!maxCacheSize) {
        // Provide a default
        SINFO("No -cache.max specified, defaulting to 16GB");
        maxCacheSize = (int64_t)16 * 1024 * 1024 * 1024;
    }
    SASSERT(maxCacheSize > 0);
    SINFO("Initializing cache with maximum size of " << maxCacheSize << " bytes");

    // Save this in a class constant, to enable us to access it safely in an
    // unsynchronized manner from other threads.
    //*((int64_t*)&_maxCacheSize) = maxCacheSize;
    int64_t* mutableMaxCacheSize = const_cast<int64_t*>(&_maxCacheSize);
    *mutableMaxCacheSize = maxCacheSize;
}

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

// ==========================================================================
void BedrockPlugin_Cache::upgradeDatabase(SQLite& db) {
    // Create or verify the cache table
    bool ignore;
    while (!db.verifyTable("cache", "CREATE TABLE cache ( "
                                    "name  TEXT NOT NULL PRIMARY KEY, "
                                    "value BLOB NOT NULL ) ",
                           ignore)) {
        // Drop and rebuild the table
        SASSERT(db.write("DROP TABLE cache;"));
    }

    // Add a one row, one column table to keep track of the current size of the cache
    SASSERT(db.verifyTable("cacheSize", "CREATE TABLE cacheSize ( size INTEGER )", ignore));
    SQResult result;
    SASSERT(db.read("SELECT * FROM cacheSize;", result));
    if (result.empty()) {
        // Insert the first (and only) row into the cache
        SASSERT(db.write("INSERT INTO cacheSize VALUES ( 0 );"));
    }

    // Add the triggers to track the cache size.  (Enable recursive triggers so
    // INSERT OR REPLACE triggers a delete when replacing.)
    SASSERT(db.write("PRAGMA recursive_triggers = 1;"));
    SASSERT(db.write("CREATE TRIGGER IF NOT EXISTS cacheOnInsert AFTER INSERT ON cache "
                     "BEGIN "
                     "UPDATE cacheSize SET size = size + LENGTH( NEW.value ); "
                     "END;"));
    SASSERT(db.write("CREATE TRIGGER IF NOT EXISTS cacheOnUpdate AFTER UPDATE ON cache "
                     "BEGIN "
                     "UPDATE cacheSize SET size = size - LENGTH( OLD.value ) + LENGTH( NEW.value ); "
                     "END;"));
    SASSERT(db.write("CREATE TRIGGER IF NOT EXISTS cacheOnDelete AFTER DELETE ON cache "
                     "BEGIN "
                     "UPDATE cacheSize SET size = size - LENGTH( OLD.value ); "
                     "END;"));
}

// ==========================================================================
bool BedrockPlugin_Cache::peekCommand(SQLite& db, BedrockCommand& command) {
    // Pull out some helpful variables
    SData& request = command.request;
    SData& response = command.response;

    // ----------------------------------------------------------------------
    if (SIEquals(request.getVerb(), "ReadCache")) {
        // - ReadCache( name )
        //
        //     Looks up the cached value corresponding to a name, if any.
        //
        //     Parameters:
        //     - name - name pattern with which to search the cache (in GLOB syntax)
        //
        //     Returns:
        //     - 200 - OK
        //         . name  - name matched (as a header)
        //         . value - raw value associated with that name (in the body of the response)
        //     - 404 - No cache found
        //
        verifyAttributeSize(request, "name", 1, MAX_SIZE_SMALL);
        const string& name = request["name"];

        // Get the list
        SQResult result;
        if (!db.read("SELECT name, value "
                     "FROM cache "
                     "WHERE name GLOB " +
                         SQ(name) + " "
                                    "LIMIT 1;",
                     result)) {
            STHROW("502 Query failed");
        }

        // If we didn't get any results, respond failure
        if (result.empty()) {
            // No results
            STHROW("404 No match found");
        } else {
            // Return that item
            SASSERT(result[0].size() == 2);
            response["name"] = result[0][0];
            response.content = result[0][1];

            // Update the LRU Map
            _lruMap.pushMRU(response["name"]);
            return true;
        }
    }

    // Didn't recognize this command
    return false;
}

// ==========================================================================
bool BedrockPlugin_Cache::processCommand(SQLite& db, BedrockCommand& command) {
    // Pull out some helpful variables
    SData& request = command.request;

    // ----------------------------------------------------------------------
    if (SIEquals(request.getVerb(), "WriteCache")) {
        // - WriteCache( name, value, [invalidateName] )
        //
        //     Records a named value into the cache, overwriting any other value
        //     with the same name.  Also, optionally invalidates other
        //     cache entries matching a pattern.
        //
        //     Note: For convenience, the value can either be provided as a
        //     header, or in the content body of the request.
        //
        //     Parameters:
        //     - name           - An arbitrary string identifier (case insensitive)
        //     - value          - Raw data to associate with this value, as a request header (1MB max) or content body
        //     (64MB max)
        //     - invalidateName - A name pattern to erase from the cache (optional)
        //
        verifyAttributeSize(request, "name", 1, MAX_SIZE_SMALL);
        const string& valueHeader = request["value"];
        if (!valueHeader.empty()) {
            // Value is provided via the header -- make sure it's not too long
            if (valueHeader.size() > MAX_SIZE_BLOB) {
                STHROW("402 Value too large, 1MB max -- use content body");
            }
        } else if (!request.content.empty()) {
            // Value is provided via the body -- make sure it's not too long
            if (request.content.size() > 64 * 1024 * 1024) {
                STHROW("402 Content too large, 64MB max");
            }
        } else {
            // No value provided
            STHROW("402 Missing value header or content body");
        }

        // Make sure we're not trying to cache something larger than the cache itself
        int64_t contentSize = valueHeader.empty() ? request.content.size() : valueHeader.size();
        if (contentSize > _maxCacheSize) {
            // Just refuse
            STHROW("402 Content larger than the cache itself");
        }

        // Optionally invalidate other entries in the cache at the same time.
        // Note that we will leave these items in the lruMap in memory, but
        // that's non-harmful.
        if (!request["invalidateName"].empty()) {
            if (!db.write("DELETE FROM cache WHERE name GLOB " + SQ(request["invalidateName"]) + ";"))
                STHROW("502 Query failed (invalidating)");
        }

        // Clear out room for the new object
        while (SToInt64(db.read("SELECT size FROM cacheSize;")) + contentSize > _maxCacheSize) {
            // Find the least recently used (LRU) item if there is one.  (If the server was recently restarted,
            // its LRU might not be fully populated.)
            const string& name = (_lruMap.empty() ? db.read("SELECT name FROM cache LIMIT 1") : _lruMap.popLRU());
            SASSERT(!name.empty());

            // Delete it
            if (!db.write("DELETE FROM cache WHERE name=" + SQ(name) + ";"))
                STHROW("502 Query failed (deleting)");
        }

        // Insert the new entry
        const string& name = request["name"];
        const string& safeValue = SQ(valueHeader.empty() ? request.content : valueHeader);
        if (!db.write("INSERT OR REPLACE INTO cache ( name, value ) "
                      "VALUES( " +
                      SQ(name) + ", " + safeValue + " );"))
            STHROW("502 Query failed (inserting)");

        // Writing is a form of "use", so this is the new MRU.  Note that we're
        // adding it to the MRU, even before we commit.  So if this transaction
        // gets rolled back for any reason, the MRU will have a record for a
        // name that isn't in the database.  But that is fine.
        _lruMap.pushMRU(name);
        return true; // Successfully processed
    }

    // Didn't recognize this command
    return false;
}


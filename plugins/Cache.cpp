#include "Cache.h"

#include <BedrockServer.h>
#include <libstuff/SQResult.h>

const string BedrockPlugin_Cache::name("Cache");
const string& BedrockPlugin_Cache::getName() const {
    return name;
}

BedrockCacheCommand::BedrockCacheCommand(SQLiteCommand&& baseCommand, BedrockPlugin_Cache* plugin) :
  BedrockCommand(move(baseCommand), plugin)
{
}

const set<string, STableComp> BedrockPlugin_Cache::supportedRequestVerbs = {
    "ReadCache",
    "WriteCache",
};

unique_ptr<BedrockCommand> BedrockPlugin_Cache::getCommand(SQLiteCommand&& baseCommand) {
    if (supportedRequestVerbs.count(baseCommand.request.getVerb())) {
        return make_unique<BedrockCacheCommand>(move(baseCommand), this);
    }
    return nullptr;
}

BedrockPlugin_Cache::LRUMap::LRUMap() {
    // Initialize
}

BedrockPlugin_Cache::LRUMap::~LRUMap() {
    // Just delete all the entries
    while (!empty()) {
        // Pop it off
        popLRU();
    }
}

bool BedrockPlugin_Cache::LRUMap::empty() {
    // Both the map and list are the same size, so check either
    lock_guard<decltype(_mutex)> lock(_mutex);
    return _lruList.empty();
}

void BedrockPlugin_Cache::LRUMap::pushMRU(const string& name) {
    // See if if it's already there
    lock_guard<decltype(_mutex)> lock(_mutex);
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
// This returns a pair which is made of up of the LRU item in the cache and
// a bool of whether or not the cache was empty when we tried to pop. If the
// cache is empty, the LRU item will be an empty string and the bool will be false.
pair<string, bool> BedrockPlugin_Cache::LRUMap::popLRU() {
    // Make sure we're not empty
    lock_guard<decltype(_mutex)> lock(_mutex);
    if (empty()) {
        return make_pair("", false);
    }
    // Take the first item off the list
    Entry* entry = _lruList.front();
    _lruList.erase(entry->listIt);
    _lruMap.erase(entry->mapIt);
    string nameCopy = entry->name;
    delete entry;
    return make_pair(nameCopy, true);
}

int64_t BedrockPlugin_Cache::initCacheSize(const string& cacheString) {
    // Check the configuration
    const string& maxCache = SToUpper(cacheString);
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
    return maxCacheSize;
}

BedrockPlugin_Cache::BedrockPlugin_Cache(BedrockServer& s)
    : BedrockPlugin(s), _maxCacheSize(initCacheSize(server.args["-cache.max"]))
{
}

BedrockPlugin_Cache::~BedrockPlugin_Cache() {
    // Nothing to clean up
}

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

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

bool BedrockCacheCommand::peek(SQLite& db) {
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
        BedrockPlugin::verifyAttributeSize(request, "name", 1, BedrockPlugin::MAX_SIZE_SMALL);
        const string& name = request["name"];
        crashIdentifyingValues.insert("name");

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
            SINFO("Pushing " << response["name"] << " to LRU cache");

            // Update the LRU Map
            plugin()._lruMap.pushMRU(response["name"]);
            return true;
        }
    }

    // Didn't recognize this command
    return false;
}

void BedrockCacheCommand::process(SQLite& db) {
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
        //     - value          - Raw data to associate with this name, as a request header (1MB max) or content body
        //     (64MB max)
        //     - invalidateName - A name pattern to erase from the cache (optional)
        //
        BedrockPlugin::verifyAttributeSize(request, "name", 1, BedrockPlugin::MAX_SIZE_SMALL);
        const string& valueHeader = request["value"];
        const string& name = request["name"];
        crashIdentifyingValues.insert("name");
        crashIdentifyingValues.insert("value");

        if (!valueHeader.empty()) {
            // Value is provided via the header -- make sure it's not too long
            if (valueHeader.size() > BedrockPlugin::MAX_SIZE_BLOB) {
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
        if (contentSize > plugin()._maxCacheSize) {
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
        while (SToInt64(db.read("SELECT size FROM cacheSize;")) + contentSize > plugin()._maxCacheSize) {
            // Find the least recently used (LRU) item if there is one.  (If the server was recently restarted,
            // its LRU might not be fully populated.)
            auto popResult = plugin()._lruMap.popLRU();
            const string& name = (popResult.second ? popResult.first : db.read("SELECT name FROM cache LIMIT 1"));
            SASSERT(!name.empty());
            SINFO("Deleting " << response["name"] << " from the cache");

            // Delete it
            if (!db.write("DELETE FROM cache WHERE name=" + SQ(name) + ";")) {
                STHROW("502 Query failed (deleting)");
            }
        }

        // Insert the new entry
        const string& safeValue = SQ(valueHeader.empty() ? request.content : valueHeader);
        if (!db.write("INSERT OR REPLACE INTO cache ( name, value ) "
                      "VALUES( " +
                      SQ(name) + ", " + safeValue + " );")) {
                          STHROW("502 Query failed (inserting)");
                      }

        // Writing is a form of "use", so this is the new MRU.  Note that we're
        // adding it to the MRU, even before we commit.  So if this transaction
        // gets rolled back for any reason, the MRU will have a record for a
        // name that isn't in the database.  But that is fine.
        plugin()._lruMap.pushMRU(name);
        return;
    }
}

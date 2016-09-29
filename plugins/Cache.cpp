////p /src/bedrock/BedrockPlugin_Cache.cpp
#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"
#include "../BedrockTest.h"

// Declare the class we're going to implement below
class BedrockPlugin_Cache : public BedrockNode::Plugin
{
  public:
    // Constructor / Destructor
    BedrockPlugin_Cache();
    ~BedrockPlugin_Cache();

    // Implement base class interface
    virtual string getName() { return "Cache"; }
    virtual void initialize(const SData& args);
    virtual void upgradeDatabase(BedrockNode* node, SQLite& db);
    virtual bool peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
    virtual bool processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command);
    virtual void test(BedrockTester* tester);

  private:
    // Bedrock Cache LRU map
    class LRUMap
    {
      public:
        // Constructor / Destructor
        LRUMap();
        ~LRUMap();

        // Tests if anything is in the map
        bool empty();

        // Mark a name as being the most recently used (MRU)
        void pushMRU(const string& name);

        // Remove the name that is the least recently used (LRU)
        string popLRU();

      private:
        // A single entry being tracked
        struct Entry {
            // Attributes
            string name;
            list<Entry*>::iterator listIt;
            map<string, Entry*>::iterator mapIt;
        };

        // Attributes
        void* _mutex;
        list<Entry*> _lruList;
        map<string, Entry*> _lruMap;
    };

    // Constants
    const int64_t _maxCacheSize;
    LRUMap _lruMap;
};

// Register for auto-discovery at boot
BREGISTER_PLUGIN(BedrockPlugin_Cache);

// ==========================================================================
BedrockPlugin_Cache::LRUMap::LRUMap()
{
    // Initialize
    _mutex = SMutexOpen();
}

// ==========================================================================
BedrockPlugin_Cache::LRUMap::~LRUMap()
{
    // Just delete all the entries
    while (!empty()) {
        // Pop it off
        popLRU();
    }

    // Clean up the mutex
    SMutexClose(_mutex);
}

// ==========================================================================
bool BedrockPlugin_Cache::LRUMap::empty()
{
    // Both the map and list are the same size, so check either
    SAUTOLOCK(_mutex);
    return _lruList.empty();
}

// ==========================================================================
void BedrockPlugin_Cache::LRUMap::pushMRU(const string& name)
{
    // See if if it's already there
    SAUTOLOCK(_mutex);
    map<string, Entry*>::iterator mapIt = _lruMap.find(name);
    if (mapIt == _lruMap.end()) {
        // Not in the map -- add a new entry
        Entry* entry = new Entry;
        entry->name  = name;

        // Insert into the map and list, retaining iterators to both
        entry->mapIt  = _lruMap.insert(std::pair<string, Entry*>(name, entry)).first;
        entry->listIt = _lruList.insert(_lruList.end(), entry);
    } else {
        // Already in the map, just move to the end of the list
        Entry* entry = mapIt->second;
        _lruList.erase(entry->listIt);
        entry->listIt = _lruList.insert(_lruList.end(), entry);
    }
}

// ==========================================================================
string BedrockPlugin_Cache::LRUMap::popLRU()
{
    // Make sure we're not empty
    SAUTOLOCK(_mutex);
    SASSERT(!empty());

    // Take the first item off the list
    Entry* entry = _lruList.front();
    _lruList.erase(entry->listIt);
    _lruMap.erase(entry->mapIt);
    string nameCopy = entry->name;
    SDELETE(entry);
    return nameCopy;
}

// ==========================================================================
BedrockPlugin_Cache::BedrockPlugin_Cache() : _maxCacheSize(0) // Will be set inside initialize()
{
    // Nothing to initialize
}

// ==========================================================================
BedrockPlugin_Cache::~BedrockPlugin_Cache()
{
    // Nothing to clean up
}

// ==========================================================================
void BedrockPlugin_Cache::initialize(const SData& args)
{
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
    *((int64_t*)&_maxCacheSize) = maxCacheSize;
}

#undef SLOGPREFIX
#define SLOGPREFIX "{" << node->name << ":" << getName() << "} "

// ==========================================================================
void BedrockPlugin_Cache::upgradeDatabase(BedrockNode* node, SQLite& db)
{
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
bool BedrockPlugin_Cache::peekCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command)
{
    // Pull out some helpful variables
    SData& request  = command->request;
    SData& response = command->response;
    // STable& content  = command->jsonContent; // Not used

    // ----------------------------------------------------------------------
    if (SIEquals(request.methodLine, "ReadCache")) {
        /// p - ReadCache( name )
        /// p
        /// p     Looks up the cached value corresponding to a name, if any.
        /// p
        /// p     Parameters:
        /// p     - name - name pattern with which to search the cache (in GLOB syntax)
        /// p
        /// p     Returns:
        /// p     - 200 - OK
        /// p         . name  - name matched (as a header)
        /// p         . value - raw value associated with that name (in the body of the response)
        /// p     - 404 - No cache found
        /// p
        BVERIFY_ATTRIBUTE_SIZE("name", 1, BMAX_SIZE_SMALL);
        const string& name = request["name"];

        // Get the list
        SQResult result;
        if (!db.read("SELECT name, value "
                     "FROM cache "
                     "WHERE name GLOB " +
                         SQ(name) + " "
                                    "LIMIT 1;",
                     result)) {
            throw "502 Query failed";
        }

        // If we didn't get any results, respond failure
        if (result.empty()) {
            // No results
            throw "404 No match found";
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
bool BedrockPlugin_Cache::processCommand(BedrockNode* node, SQLite& db, BedrockNode::Command* command)
{
    // Pull out some helpful variables
    SData& request = command->request;
    // SData&  response = command->response; -- Not used
    // STable& content  = command->jsonContent; -- Not used

    // ----------------------------------------------------------------------
    if (SIEquals(request.methodLine, "WriteCache")) {
        /// p - WriteCache( name, value, [invalidateName] )
        /// p
        /// p     Records a named value into the cache, overwriting any other value
        /// p     with the same name.  Also, optionally invalidates other
        /// p     cache entries matching a pattern.
        /// p
        /// p     Note: For convenience, the value can either be provided as a
        /// p     header, or in the content body of the request.
        /// p
        /// p     Parameters:
        /// p     - name           - An arbitrary string identifier (case insensitive)
        /// p     - value          - Raw data to associate with this value, as a request header (1MB max) or content
        /// body (64MB max)
        /// p     - invalidateName - A name pattern to erase from the cache (optional)
        /// p
        BVERIFY_ATTRIBUTE_SIZE("name", 1, BMAX_SIZE_SMALL);
        const string& valueHeader = request["value"];
        if (!valueHeader.empty()) {
            // Value is provided via the header -- make sure it's not too long
            if (valueHeader.size() > BMAX_SIZE_BLOB) {
                throw "402 Value too large, 1MB max -- use content body";
            }
        } else if (!request.content.empty()) {
            // Value is provided via the body -- make sure it's not too long
            if (request.content.size() > 64 * 1024 * 1024) {
                throw "402 Content too large, 64MB max";
            }
        } else {
            // No value provided
            throw "402 Missing value header or content body";
        }

        // Make sure we're not trying to cache something larger than the cache itself
        int64_t contentSize = valueHeader.empty() ? request.content.size() : valueHeader.size();
        if (contentSize > _maxCacheSize) {
            // Just refuse
            throw "402 Content larger than the cache itself";
        }

        // Optionally invalidate other entries in the cache at the same time.
        // Note that we will leave these items in the lruMap in memory, but
        // that's non-harmful.
        if (!request["invalidateName"].empty()) {
            if (!db.write("DELETE FROM cache WHERE name GLOB " + SQ(request["invalidateName"]) + ";"))
                throw "502 Query failed (invalidating)";
        }

        // Clear out room for the new object
        while (SToInt64(db.read("SELECT size FROM cacheSize;")) + contentSize > _maxCacheSize) {
            // Find the least recently used (LRU) item if there is one.  (If the server was recently restarted,
            // its LRU might not be fully populated.)
            const string& name = (_lruMap.empty() ? db.read("SELECT name FROM cache LIMIT 1") : _lruMap.popLRU());
            SASSERT(!name.empty());

            // Delete it
            if (!db.write("DELETE FROM cache WHERE name=" + SQ(name) + ";"))
                throw "502 Query failed (deleting)";
        }

        // Insert the new entry
        const string& name      = request["name"];
        const string& safeValue = SQ(valueHeader.empty() ? request.content : valueHeader);
        if (!db.write("INSERT OR REPLACE INTO cache ( name, value ) "
                      "VALUES( " +
                      SQ(name) + ", " + safeValue + " );"))
            throw "502 Query failed (inserting)";

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

// ==========================================================================
void BedrockPlugin_Cache::test(BedrockTester* tester)
{
    {
        STestTimer test("Testing BedrockPlugin_Cache");
        // Startup a Bedrock::Cache server
        SData args;
        args["-clean"]     = "1";  // First time, let's blow away the db
        args["-cache.max"] = "10"; // 10 bytes
        args["-plugins"] = "cache";
        tester->startServer(args);

        // Write something to the cache
        SData writeCache("WriteCache");
        writeCache["name"]  = "name0";
        writeCache["value"] = "123456"; // 6 bytes long
        SData ok("200 OK");
        tester->testRequest(writeCache, ok);

        // Read it back
        SData readCache("ReadCache");
        readCache["name"] = "name0";
        SData readOK("200 OK");
        readOK.content = writeCache["value"];
        tester->testRequest(readCache, readOK);

        // Write something new to the cache, blowing away the first (due to
        // exceeding the cache limit)
        writeCache["name"] = "name1";
        tester->testRequest(writeCache, ok);

        // Confirm the first was expired, but the second is available
        SData readFail("404 No match found");
        tester->testRequest(readCache, readFail);
        readCache["name"] = "name1";
        tester->testRequest(readCache, readOK);

        // At this point the cache only has 6 bytes in it (one value),
        // but can take up to 10.  Let's add 1 more small value.
        writeCache["name"]  = "name2";
        writeCache["value"] = "1"; // just 1 byte long
        tester->testRequest(writeCache, ok);

        // Confirm the 2nd is still there
        readCache["name"] = "name1";
        tester->testRequest(readCache, readOK);

        // Now do a write on the 4th while invalidating the 2nd
        writeCache["name"]           = "name2";
        writeCache["invalidateName"] = "name1";
        tester->testRequest(writeCache, ok);

        // Finally, confirm the item we invalidated is gone -- even though
        // there's room in the cache for it
        readCache["name"]           = "name1";
        readCache["invalidateName"] = "";
        tester->testRequest(readCache, readFail);

        // Done
        tester->stopServer();
    }
}

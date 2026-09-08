/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    Cache.h
 * Path:    plugins/Cache.h
 * Pair:    Cache.cpp
 *
 * INTENT
 *   Declares a Bedrock plugin implementing a simple named-blob cache backed
 *   by a `cache` SQLite table (with a trigger-maintained running size) and
 *   an in-memory LRU tracker used to decide what to evict when the
 *   configured size limit is reached. Exposed via ReadCache/WriteCache.
 *
 * OBJECTS
 *   BedrockPlugin_Cache                 - the plugin; owns the configured
 *                                          max cache size and the LRU
 *                                          tracker, creates the cache/
 *                                          cacheSize tables and triggers.
 *   BedrockPlugin_Cache::LRUMap         - mutex-protected least-recently-used
 *                                          tracker keyed by name, backed by
 *                                          a doubly-linked list (order) plus
 *                                          a map (O(1) lookup); independent
 *                                          of the DB, so it's a cache hint
 *                                          only, not a source of truth.
 *   BedrockPlugin_Cache::LRUMap::Entry (private struct) - one tracked
 *                                          key, holding iterators into both
 *                                          the list and the map for O(1)
 *                                          removal.
 *   BedrockPlugin_Cache::initCacheSize (static) - parses a size string like
 *                                          "16GB" into a byte count,
 *                                          defaulting to 16GB.
 *   BedrockPlugin_Cache::supportedRequestVerbs (static) - the verb set this
 *                                          plugin answers to ("ReadCache",
 *                                          "WriteCache").
 *   BedrockCacheCommand                 - BedrockCommand implementing
 *                                          peek() (ReadCache) and process()
 *                                          (WriteCache).
 *   BedrockCacheCommand::plugin() (private inline) - casts the base
 *                                          command's plugin pointer back to
 *                                          BedrockPlugin_Cache&.
 *
 * OUT OF PLACE
 *   BedrockPlugin_Cache::LRUMap [CANDIDATE] - a fully generic string-keyed
 *   LRU tracker with no cache-plugin-specific logic at all, nested privately
 *   inside one plugin instead of being a reusable libstuff container other
 *   code could also use.
 *
 * NAME/LOCATION FIT
 *   Fits; it is the cache plugin, filed alongside the other BedrockPlugin_*
 *   implementations in plugins/.
 *
 * NAMING QUALITY
 *   _maxCacheSize and _lruMap use this repo's underscore-prefix convention
 *   for private members, but there is no `private:` after the class's
 *   `public:` label to actually make them private — as written both are
 *   public data members despite the naming implying otherwise.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

// Declare the class we're going to implement below
class BedrockPlugin_Cache : public BedrockPlugin {
public:
    // Constructor / Destructor
    BedrockPlugin_Cache(BedrockServer& s);
    ~BedrockPlugin_Cache();

    // Implement base class interface
    virtual const string& getName() const;
    virtual void upgradeDatabase(SQLite& db);
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);

    static const string name;

    // Bedrock Cache LRU map
    class LRUMap {
public:
        // Constructor / Destructor
        LRUMap();
        ~LRUMap();

        // Tests if anything is in the map
        bool empty();

        // Mark a name as being the most recently used (MRU)
        void pushMRU(const string& name);

        // Remove the name that is the least recently used (LRU)
        pair<string, bool> popLRU();

private:
        // A single entry being tracked
        struct Entry
        {
            // Attributes
            string name;
            list<Entry*>::iterator listIt;
            map<string, Entry*>::iterator mapIt;
        };

        // Attributes
        recursive_mutex _mutex;
        list<Entry*> _lruList;
        map<string, Entry*> _lruMap;
    };

    static int64_t initCacheSize(const string& cacheString);

    // Constants
    const int64_t _maxCacheSize;
    LRUMap _lruMap;
    static const set<string, STableComp> supportedRequestVerbs;
};

class BedrockCacheCommand : public BedrockCommand {
public:
    BedrockCacheCommand(SQLiteCommand&& baseCommand, BedrockPlugin_Cache* plugin);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);

private:
    BedrockPlugin_Cache& plugin()
    {
        return static_cast<BedrockPlugin_Cache&>(*_plugin);
    }
};

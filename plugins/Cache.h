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
        struct Entry {
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
    BedrockPlugin_Cache& plugin() { return static_cast<BedrockPlugin_Cache&>(*_plugin); }
};

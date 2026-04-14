#pragma once
#include <libstuff/libstuff.h>
#include <zstd.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_Zstd : public BedrockPlugin {
public:
    BedrockPlugin_Zstd(BedrockServer& s);
    ~BedrockPlugin_Zstd();

    virtual const string& getName() const;
    virtual void upgradeDatabase(SQLite& db);
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);

    // Returns the compiled compression dictionary for the given ID, or nullptr if not found.
    static ZSTD_CDict* getCompressionDictionary(size_t id);

    // Returns the compiled decompression dictionary for the given ID, or nullptr if not found.
    static ZSTD_DDict* getDecompressionDictionary(size_t id);

    // Loads all dictionaries from the zstdDictionaries table into compiled in-memory maps.
    // Called once at startup from the sync thread, before any queries run.
    static void loadDictionariesFromDB(SQLite& db);

    static const string name;

private:
    struct ZDictionaries
    {
        ZSTD_CDict* compression = nullptr;
        ZSTD_DDict* decompression = nullptr;
    };

    static map<size_t, ZDictionaries> _dictionaries;
};

#pragma once
#include <libstuff/libstuff.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include "../BedrockPlugin.h"

// Forward-declare sqlite3 types to avoid forcing all consumers to include sqlite3 headers.
struct sqlite3;

class BedrockPlugin_Compression : public BedrockPlugin {
public:
    BedrockPlugin_Compression(BedrockServer& s);
    ~BedrockPlugin_Compression();

    virtual const string& getName() const;
    virtual void upgradeDatabase(SQLite& db);
    virtual void initializeFromDB(SQLite& db);
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);

    // Returns the compiled compression dictionary for the given ID, or nullptr if not found.
    static ZSTD_CDict* getCompressionDictionary(size_t id);

    // Returns the compiled decompression dictionary for the given ID, or nullptr if not found.
    static ZSTD_DDict* getDecompressionDictionary(size_t id);

    // Loads all dictionaries from the zstdDictionaries table into compiled in-memory maps.
    // Called once at startup from the sync thread, before any queries run.
    static void loadDictionariesFromDB(SQLite& db);

    // Register the compress(data, dictID) and decompress(data) SQLite UDFs.
    static void registerSQLite(sqlite3* db);

    // Returns decompressed data if input is a zstd frame, otherwise returns input unchanged.
    static string decompress(const string& input);

    static const string name;

    static constexpr int COMPRESSION_LEVEL = 3;

private:
    struct ZDictionaries
    {
        ZSTD_CDict* compression = nullptr;
        ZSTD_DDict* decompression = nullptr;
    };

    static map<size_t, ZDictionaries> _dictionaries;
};

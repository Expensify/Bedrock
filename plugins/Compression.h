/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    Compression.h
 * Path:    plugins/Compression.h
 * Pair:    Compression.cpp
 *
 * INTENT
 *   Declares a Bedrock plugin that registers zstd-dictionary-based
 *   compress()/decompress() SQLite user-defined functions, so other tables
 *   can store compressed column data using dictionaries loaded from a
 *   dedicated DB table. Also exposes static compress/decompress helpers for
 *   callers outside of SQL.
 *
 * OBJECTS
 *   BedrockPlugin_Compression        - the plugin; loads dictionaries at
 *                                       startup (initializeFromDB), creates
 *                                       the zstdDictionaries table
 *                                       (upgradeDatabase), registers the
 *                                       SQLite UDFs (registerSQLite), and
 *                                       exposes compress()/decompress().
 *                                       getCommand() always returns null —
 *                                       this plugin has no BedrockCommand of
 *                                       its own; it is used only via SQL UDF
 *                                       or direct C++ calls from other code.
 *   BedrockPlugin_Compression::ZDictionaries (private struct) - pairs one
 *                                       dictionary ID's compiled ZSTD_CDict*
 *                                       and ZSTD_DDict*.
 *   BedrockPlugin_Compression::_dictionaries (private static) - map of
 *                                       dictionary ID to ZDictionaries,
 *                                       populated once at startup.
 *   COMPRESSION_LEVEL (static constexpr) - zstd level used for all
 *                                       dictionary-based compression.
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; it is the compression plugin, filed alongside the other
 *   BedrockPlugin_* implementations in plugins/.
 *
 * NAMING QUALITY
 *   Consistent with repo convention: BedrockPlugin_<Name> for the class,
 *   underscore prefix on the private static _dictionaries. The header
 *   itself carries clear, load-bearing warnings (dictionary immutability)
 *   right next to the declarations they apply to — worth preserving as-is.
 * ─────────────────────────────────────────────────────────────────────*/
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
    //
    // Beware: dictionaries are immutable. Changing or removing an existing row in zstdDictionaries makes every
    // value compressed against it unreadable, and because compress()/decompress() are registered as
    // SQLITE_DETERMINISTIC, it also silently corrupts every index built on decompress(). Only ever add new
    // dictionary IDs.
    static void loadDictionariesFromDB(SQLite& db);

    // Register the compress(data, dictID) and decompress(data) SQLite UDFs.
    static void registerSQLite(sqlite3* db);

    // Returns input compressed with the given dictionary ID. If dictID is 0 or input is empty,
    // returns input unchanged. Produces a byte-identical zstd frame to the compress() SQL UDF.
    static string compress(const string& input, size_t dictID);

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

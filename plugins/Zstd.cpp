#include "Zstd.h"

#include <BedrockServer.h>
#include <libstuff/SQResult.h>

const string BedrockPlugin_Zstd::name("Zstd");
map<size_t, BedrockPlugin_Zstd::ZDictionaries> BedrockPlugin_Zstd::_dictionaries;

const string& BedrockPlugin_Zstd::getName() const
{
    return name;
}

BedrockPlugin_Zstd::BedrockPlugin_Zstd(BedrockServer& s) :
    BedrockPlugin(s)
{
}

BedrockPlugin_Zstd::~BedrockPlugin_Zstd()
{
    for (auto& [id, dicts] : _dictionaries) {
        if (dicts.compression) {
            ZSTD_freeCDict(dicts.compression);
        }
        if (dicts.decompression) {
            ZSTD_freeDDict(dicts.decompression);
        }
    }
    _dictionaries.clear();
}

unique_ptr<BedrockCommand> BedrockPlugin_Zstd::getCommand(SQLiteCommand&& baseCommand)
{
    return nullptr;
}

void BedrockPlugin_Zstd::upgradeDatabase(SQLite& db)
{
    bool created;
    SASSERT(db.verifyTable("zstdDictionaries",
        "CREATE TABLE zstdDictionaries ("
        "dictionaryID INTEGER PRIMARY KEY, "
        "description TEXT, "
        "dictionary BLOB"
        ")",
        created));
}

ZSTD_CDict* BedrockPlugin_Zstd::getCompressionDictionary(size_t id)
{
    auto it = _dictionaries.find(id);
    if (it != _dictionaries.end()) {
        return it->second.compression;
    }
    return nullptr;
}

ZSTD_DDict* BedrockPlugin_Zstd::getDecompressionDictionary(size_t id)
{
    auto it = _dictionaries.find(id);
    if (it != _dictionaries.end()) {
        return it->second.decompression;
    }
    return nullptr;
}

void BedrockPlugin_Zstd::loadDictionariesFromDB(SQLite& db)
{
    SQResult result;
    if (!db.read("SELECT dictionaryID, dictionary FROM zstdDictionaries;", result)) {
        SWARN("Failed to read zstdDictionaries table.");
        return;
    }

    for (size_t i = 0; i < result.size(); i++) {
        size_t id = SToUInt64(result[i][0]);
        const string dictData = result[i][1];

        if (dictData.empty()) {
            SWARN("Empty dictionary data for dictionaryID " << id << ", skipping.");
            continue;
        }

        int compressionLevel = 3;
        ZSTD_CDict* cdict = ZSTD_createCDict(dictData.data(), dictData.size(), compressionLevel);
        ZSTD_DDict* ddict = ZSTD_createDDict(dictData.data(), dictData.size());

        if (!cdict || !ddict) {
            SWARN("Failed to compile dictionary " << id);
            if (cdict) {
                ZSTD_freeCDict(cdict);
            }
            if (ddict) {
                ZSTD_freeDDict(ddict);
            }
            continue;
        }

        _dictionaries[id] = {cdict, ddict};
        SINFO("Loaded zstd dictionary " << id << " (" << dictData.size() << " bytes)");
    }

    SINFO("Loaded " << _dictionaries.size() << " zstd dictionaries from DB.");
}

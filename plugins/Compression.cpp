#include "Compression.h"

#include <BedrockServer.h>
#include <libstuff/SQResult.h>
#include <libstuff/sqlite3.h>

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

// SQLite UDF implementations

static void sqliteCompress(sqlite3_context* ctx, int argc, sqlite3_value** argv)
{
    if (argc != 2) {
        sqlite3_result_error(ctx, "compress() requires 2 arguments: data and dictionaryID", -1);
        return;
    }

    // If data is NULL, return NULL.
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    // Get dictionary ID. If NULL or 0, return data unchanged.
    if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_value(ctx, argv[0]);
        return;
    }
    size_t dictId = (size_t) sqlite3_value_int64(argv[1]);
    if (dictId == 0) {
        sqlite3_result_value(ctx, argv[0]);
        return;
    }

    // Look up the compiled compression dictionary.
    ZSTD_CDict* cdict = BedrockPlugin_Zstd::getCompressionDictionary(dictId);
    if (!cdict) {
        string err = "compress(): no dictionary found for ID " + to_string(dictId);
        sqlite3_result_error(ctx, err.c_str(), -1);
        return;
    }

    // Get source data.
    const void* src = sqlite3_value_blob(argv[0]);
    int srcLen = sqlite3_value_bytes(argv[0]);
    if (srcLen == 0) {
        sqlite3_result_value(ctx, argv[0]);
        return;
    }

    // Create compression context.
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    if (!cctx) {
        sqlite3_result_error(ctx, "compress(): failed to create compression context", -1);
        return;
    }

    // Enable dictionary ID in compressed output, disable checksums to save space.
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_dictIDFlag, 1);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 0);

    // Allocate output buffer.
    size_t dstCap = ZSTD_compressBound(srcLen);
    void* dst = sqlite3_malloc64(dstCap);
    if (!dst) {
        ZSTD_freeCCtx(cctx);
        sqlite3_result_error_nomem(ctx);
        return;
    }

    // Compress.
    size_t compressedSize = ZSTD_compress_usingCDict(cctx, dst, dstCap, src, srcLen, cdict);
    ZSTD_freeCCtx(cctx);

    if (ZSTD_isError(compressedSize)) {
        sqlite3_free(dst);
        string err = "compress(): " + string(ZSTD_getErrorName(compressedSize));
        sqlite3_result_error(ctx, err.c_str(), -1);
        return;
    }

    sqlite3_result_blob(ctx, dst, (int) compressedSize, sqlite3_free);
}

static void sqliteDecompress(sqlite3_context* ctx, int argc, sqlite3_value** argv)
{
    if (argc != 1) {
        sqlite3_result_error(ctx, "decompress() requires 1 argument", -1);
        return;
    }

    // If data is NULL, return NULL.
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const void* src = sqlite3_value_blob(argv[0]);
    int srcLen = sqlite3_value_bytes(argv[0]);

    // If not a zstd frame, return data unchanged (backward compatible with uncompressed data).
    if (srcLen == 0 || !ZSTD_isFrame(src, srcLen)) {
        sqlite3_result_value(ctx, argv[0]);
        return;
    }

    // Get dictionary ID from the compressed frame.
    unsigned dictId = ZSTD_getDictID_fromFrame(src, srcLen);
    ZSTD_DDict* ddict = nullptr;
    if (dictId != 0) {
        ddict = BedrockPlugin_Zstd::getDecompressionDictionary(dictId);
        if (!ddict) {
            string err = "decompress(): no dictionary found for ID " + to_string(dictId);
            sqlite3_result_error(ctx, err.c_str(), -1);
            return;
        }
    }

    // Get the decompressed size.
    unsigned long long decompressedSize = ZSTD_getFrameContentSize(src, srcLen);
    if (decompressedSize == ZSTD_CONTENTSIZE_UNKNOWN || decompressedSize == ZSTD_CONTENTSIZE_ERROR) {
        sqlite3_result_error(ctx, "decompress(): unable to determine decompressed size", -1);
        return;
    }

    // Allocate output buffer.
    void* dst = sqlite3_malloc64(decompressedSize);
    if (!dst) {
        sqlite3_result_error_nomem(ctx);
        return;
    }

    // Decompress.
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    if (!dctx) {
        sqlite3_free(dst);
        sqlite3_result_error(ctx, "decompress(): failed to create decompression context", -1);
        return;
    }

    size_t actualSize = ZSTD_decompress_usingDDict(dctx, dst, decompressedSize, src, srcLen, ddict);
    ZSTD_freeDCtx(dctx);

    if (ZSTD_isError(actualSize)) {
        sqlite3_free(dst);
        string err = "decompress(): " + string(ZSTD_getErrorName(actualSize));
        sqlite3_result_error(ctx, err.c_str(), -1);
        return;
    }

    sqlite3_result_blob(ctx, dst, (int) actualSize, sqlite3_free);
}

void BedrockPlugin_Zstd::registerSQLite(sqlite3* db)
{
    sqlite3_create_function_v2(db, "compress", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                               nullptr, ::sqliteCompress, nullptr, nullptr, nullptr);
    sqlite3_create_function_v2(db, "decompress", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                               nullptr, ::sqliteDecompress, nullptr, nullptr, nullptr);
}

string BedrockPlugin_Zstd::decompress(const string& input)
{
    if (input.empty()) {
        return input;
    }

    // If not a zstd frame, return unchanged.
    if (!ZSTD_isFrame(input.data(), input.size())) {
        return input;
    }

    // Get dictionary ID from the compressed frame.
    unsigned dictId = ZSTD_getDictID_fromFrame(input.data(), input.size());
    ZSTD_DDict* ddict = nullptr;
    if (dictId != 0) {
        ddict = BedrockPlugin_Zstd::getDecompressionDictionary(dictId);
        if (!ddict) {
            SWARN("decompress(): no dictionary found for ID " << dictId);
            return input;
        }
    }

    // Get the decompressed size.
    unsigned long long decompressedSize = ZSTD_getFrameContentSize(input.data(), input.size());
    if (decompressedSize == ZSTD_CONTENTSIZE_UNKNOWN || decompressedSize == ZSTD_CONTENTSIZE_ERROR) {
        SWARN("decompress(): unable to determine decompressed size");
        return input;
    }

    // Decompress.
    string output(decompressedSize, '\0');
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    if (!dctx) {
        SWARN("decompress(): failed to create decompression context");
        return input;
    }

    size_t actualSize = ZSTD_decompress_usingDDict(dctx, output.data(), decompressedSize,
                                                    input.data(), input.size(), ddict);
    ZSTD_freeDCtx(dctx);

    if (ZSTD_isError(actualSize)) {
        SWARN("decompress(): " << ZSTD_getErrorName(actualSize));
        return input;
    }

    output.resize(actualSize);
    return output;
}

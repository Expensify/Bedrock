#pragma once

#include <string>
#include <cstdint>

// Forward-declare sqlite3 types to avoid forcing all consumers to include sqlite3 headers.
// This keeps the header lightweight; only the implementation needs sqlite3 symbols.
struct sqlite3;
struct sqlite3_context;
struct sqlite3_value;

class SDeburr {
public:
    /**
     * Returns a lowercased, ASCII-only approximation of `input`.
     *
     * - Removes diacritics (e.g., é → e, Å → a, ñ → n)
     * - Applies specific multi-letter folds (e.g., ß → ss, Æ → ae, Œ → oe)
     * - Drops combining marks U+0300–U+036F and non-ASCII code points without mappings
     * - Preserves ASCII punctuation and digits; ASCII letters are lowercased
     *
     * Mirrors lodash's deburr behavior for search normalization and comparisons
     * where diacritics should not affect matching.
     */
    static std::string deburr(const std::string& input);

    /**
     * Register the SQLite UDF `DEBURR(text)` on the provided database handle.
     *
     * - Returns the same value as `deburr(text)`
     * - Marked deterministic to allow SQLite optimizations and query planning
     * - NULL input yields NULL
     */
    static void registerSQLite(sqlite3* db);

private:
    static uint32_t decodeUTF8Codepoint(const unsigned char* bytes, size_t length, size_t& index);
    static const char* deburrMap(uint32_t codepoint);
    static void sqliteDeburr(sqlite3_context* ctx, int argc, sqlite3_value** argv);
};



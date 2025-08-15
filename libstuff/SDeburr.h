#pragma once

#include <array>
#include <string>
#include <cstdint>
#include <vector>

// Forward-declare sqlite3 types to avoid forcing all consumers to include sqlite3 headers.
// This keeps the header lightweight; only the implementation needs sqlite3 symbols.
struct sqlite3;
struct sqlite3_context;
struct sqlite3_value;

using namespace std;

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
    static string deburr(const unsigned char* input);
    static string deburr(const string& input);

    /**
     * Register the SQLite UDF `DEBURR(text)` on the provided database handle.
     *
     * - Returns the same value as `deburr(text)`
     * - Marked deterministic to allow SQLite optimizations and query planning
     * - NULL input yields NULL
     */
    static void registerSQLite(sqlite3* db);

private:
    /**
     * Converts special characters to ASCII equivalents.
     *
     * Examples:
     * - é → "e", Å → "A", ñ → "n" (removes accents)
     * - ß → "ss" (special case b/c the unicode codepoint is much higher than the others, so isn't included in the table)
     * - Unknown characters → returns nullptr (keep as-is)
     */
    static const char* unicodeToAscii(uint32_t codepoint);

    /**
     * Fast lookup table for converting accented characters.
     * The array indices are simply the unicode code points that map to the given character.
     */
    inline static constexpr auto UNICODE_TO_ASCII_MAP = []() constexpr {
        array<const char*, 0x0180> map = {};

        auto mapCodePoints = [&map](const char* value, vector<char32_t> codePoints) {
            for (auto codePoint : codePoints) {
                map[codePoint] = value;
            }
        };

        // Latin-1 Supplement mappings (0xC0..0xFF)
        mapCodePoints("A", {0x00C0, 0x00C1, 0x00C2, 0x00C3, 0x00C4, 0x00C5});
        mapCodePoints("AE", {0x00C6});
        mapCodePoints("C", {0x00C7});
        mapCodePoints("E", {0x00C8, 0x00C9, 0x00CA, 0x00CB});
        mapCodePoints("I", {0x00CC, 0x00CD, 0x00CE, 0x00CF});
        mapCodePoints("D", {0x00D0});
        mapCodePoints("N", {0x00D1});
        mapCodePoints("O", {0x00D2, 0x00D3, 0x00D4, 0x00D5, 0x00D6, 0x00D8});
        mapCodePoints("U", {0x00D9, 0x00DA, 0x00DB, 0x00DC});
        mapCodePoints("Y", {0x00DD});
        mapCodePoints("ss", {0x00DF});
        mapCodePoints("a", {0x00E0, 0x00E1, 0x00E2, 0x00E3, 0x00E4, 0x00E5});
        mapCodePoints("ae", {0x00E6});
        mapCodePoints("c", {0x00E7});
        mapCodePoints("e", {0x00E8, 0x00E9, 0x00EA, 0x00EB});
        mapCodePoints("i", {0x00EC, 0x00ED, 0x00EE, 0x00EF});
        mapCodePoints("d",{0x00F0});
        mapCodePoints("n", {0x00F1});
        mapCodePoints("o", {0x00F2, 0x00F3, 0x00F4, 0x00F5, 0x00F6, 0x00F8});
        mapCodePoints("u", {0x00F9, 0x00FA, 0x00FB, 0x00FC});
        mapCodePoints("y", {0x00FD, 0x00FF});
        mapCodePoints("TH", {0x00DE});
        mapCodePoints("th", {0x00FE});

        // Latin Extended-B mappings (0x0180..0x01FF)
        mapCodePoints("A", {0x0100, 0x0102, 0x0104});
        mapCodePoints("a", {0x0101, 0x0103, 0x0105});
        mapCodePoints("C", {0x0106, 0x0108, 0x010A, 0x010C});
        mapCodePoints("c", {0x0107, 0x0109, 0x010B, 0x010D});
        mapCodePoints("D", {0x010E, 0x0110});
        mapCodePoints("d", {0x0110, 0x0111});
        mapCodePoints("E", {0x0112, 0x0114, 0x0116, 0x0118, 0x011A});
        mapCodePoints("e", {0x0113, 0x0115, 0x0117, 0x0119, 0x011B});
        mapCodePoints("I", {0x0128, 0x012A, 0x012C, 0x012E, 0x0130});
        mapCodePoints("i", {0x0129, 0x012B, 0x012D, 0x012F, 0x0131});
        mapCodePoints("IJ", {0x0132});
        mapCodePoints("ij", {0x0133});
        mapCodePoints("J", {0x0134});
        mapCodePoints("j", {0x0135});
        mapCodePoints("K", {0x0136});
        mapCodePoints("k", {0x0137, 0x0138});
        mapCodePoints("L", {0x0139, 0x013B, 0x013D, 0x013F, 0x0141});
        mapCodePoints("l", {0x013A, 0x013C, 0x013E, 0x0140, 0x0142});
        mapCodePoints("N", {0x0143, 0x0145, 0x0147, 0x014A});
        mapCodePoints("n", {0x0144, 0x0146, 0x0148, 0x0149, 0x014B});
        mapCodePoints("O", {0x014C, 0x014E, 0x0150});
        mapCodePoints("o", {0x014D, 0x014F, 0x0151});
        mapCodePoints("OE", {0x0152});
        mapCodePoints("oe", {0x0153});
        mapCodePoints("R", {0x0154, 0x0156, 0x0158});
        mapCodePoints("r", {0x0155, 0x0157, 0x0159});
        mapCodePoints("S", {0x015A, 0x015C, 0x015E, 0x0160});
        mapCodePoints("s", {0x015B, 0x015D, 0x015F, 0x0161, 0x017F});
        mapCodePoints("T", {0x0162, 0x0164, 0x0166});
        mapCodePoints("t", {0x0163, 0x0165, 0x0167});
        mapCodePoints("U", {0x0168, 0x016A, 0x016C, 0x016E, 0x0170, 0x0172});
        mapCodePoints("u", {0x0169, 0x016B, 0x016D, 0x016F, 0x0171, 0x0173});
        mapCodePoints("W", {0x0174});
        mapCodePoints("w", {0x0175});
        mapCodePoints("Y", {0x0176, 0x0178});
        mapCodePoints("y", {0x0177});
        mapCodePoints("Z", {0x0179, 0x017B, 0x017D});
        mapCodePoints("z", {0x017A, 0x017C, 0x017E});

        return map;
    }();
};



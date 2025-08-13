#pragma once

#include <array>
#include <string>
#include <cstdint>

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
     *
     * It's a flat array that works like a dictionary where the character's number is used as an array index,
     * so we can look up it's ASCII equivalent very quickly.
     */
    inline static constexpr auto UNICODE_TO_ASCII_MAP = []() constexpr {
        array<const char*, 0x0180 - 0x00C0> map = {};

        // Latin-1 Supplement mappings (0xC0..0xFF)
        map[0x00C0 - 0x00C0] = "A"; map[0x00C1 - 0x00C0] = "A"; map[0x00C2 - 0x00C0] = "A"; map[0x00C3 - 0x00C0] = "A"; map[0x00C4 - 0x00C0] = "A"; map[0x00C5 - 0x00C0] = "A";
        map[0x00E0 - 0x00C0] = "a"; map[0x00E1 - 0x00C0] = "a"; map[0x00E2 - 0x00C0] = "a"; map[0x00E3 - 0x00C0] = "a"; map[0x00E4 - 0x00C0] = "a"; map[0x00E5 - 0x00C0] = "a";
        map[0x00C7 - 0x00C0] = "C"; map[0x00E7 - 0x00C0] = "c";
        map[0x00C8 - 0x00C0] = "E"; map[0x00C9 - 0x00C0] = "E"; map[0x00CA - 0x00C0] = "E"; map[0x00CB - 0x00C0] = "E";
        map[0x00E8 - 0x00C0] = "e"; map[0x00E9 - 0x00C0] = "e"; map[0x00EA - 0x00C0] = "e"; map[0x00EB - 0x00C0] = "e";
        map[0x00CC - 0x00C0] = "I"; map[0x00CD - 0x00C0] = "I"; map[0x00CE - 0x00C0] = "I"; map[0x00CF - 0x00C0] = "I";
        map[0x00EC - 0x00C0] = "i"; map[0x00ED - 0x00C0] = "i"; map[0x00EE - 0x00C0] = "i"; map[0x00EF - 0x00C0] = "i";
        map[0x00D1 - 0x00C0] = "N"; map[0x00F1 - 0x00C0] = "n";
        map[0x00D2 - 0x00C0] = "O"; map[0x00D3 - 0x00C0] = "O"; map[0x00D4 - 0x00C0] = "O"; map[0x00D5 - 0x00C0] = "O"; map[0x00D6 - 0x00C0] = "O"; map[0x00D8 - 0x00C0] = "O";
        map[0x00F2 - 0x00C0] = "o"; map[0x00F3 - 0x00C0] = "o"; map[0x00F4 - 0x00C0] = "o"; map[0x00F5 - 0x00C0] = "o"; map[0x00F6 - 0x00C0] = "o"; map[0x00F8 - 0x00C0] = "o";
        map[0x00D9 - 0x00C0] = "U"; map[0x00DA - 0x00C0] = "U"; map[0x00DB - 0x00C0] = "U"; map[0x00DC - 0x00C0] = "U";
        map[0x00F9 - 0x00C0] = "u"; map[0x00FA - 0x00C0] = "u"; map[0x00FB - 0x00C0] = "u"; map[0x00FC - 0x00C0] = "u";
        map[0x00DD - 0x00C0] = "Y"; map[0x00FD - 0x00C0] = "y"; map[0x00FF - 0x00C0] = "y";
        map[0x00DF - 0x00C0] = "ss"; map[0x00C6 - 0x00C0] = "AE"; map[0x00E6 - 0x00C0] = "ae";
        map[0x00DE - 0x00C0] = "TH"; map[0x00FE - 0x00C0] = "th"; map[0x00D0 - 0x00C0] = "D"; map[0x00F0 - 0x00C0] = "d";

        // Latin Extended-A mappings (0x0100..0x017F)
        map[0x0100 - 0x00C0] = "A"; map[0x0102 - 0x00C0] = "A"; map[0x0104 - 0x00C0] = "A";
        map[0x0101 - 0x00C0] = "a"; map[0x0103 - 0x00C0] = "a"; map[0x0105 - 0x00C0] = "a";
        map[0x0106 - 0x00C0] = "C"; map[0x0108 - 0x00C0] = "C"; map[0x010A - 0x00C0] = "C"; map[0x010C - 0x00C0] = "C";
        map[0x0107 - 0x00C0] = "c"; map[0x0109 - 0x00C0] = "c"; map[0x010B - 0x00C0] = "c"; map[0x010D - 0x00C0] = "c";
        map[0x010E - 0x00C0] = "D"; map[0x010F - 0x00C0] = "d";
        map[0x0112 - 0x00C0] = "E"; map[0x0114 - 0x00C0] = "E"; map[0x0116 - 0x00C0] = "E"; map[0x0118 - 0x00C0] = "E"; map[0x011A - 0x00C0] = "E";
        map[0x0113 - 0x00C0] = "e"; map[0x0115 - 0x00C0] = "e"; map[0x0117 - 0x00C0] = "e"; map[0x0119 - 0x00C0] = "e"; map[0x011B - 0x00C0] = "e";
        map[0x0128 - 0x00C0] = "I"; map[0x012A - 0x00C0] = "I"; map[0x012C - 0x00C0] = "I"; map[0x012E - 0x00C0] = "I";
        map[0x0129 - 0x00C0] = "i"; map[0x012B - 0x00C0] = "i"; map[0x012D - 0x00C0] = "i"; map[0x012F - 0x00C0] = "i";
        map[0x0130 - 0x00C0] = "I"; map[0x0131 - 0x00C0] = "i";
        map[0x0143 - 0x00C0] = "N"; map[0x0147 - 0x00C0] = "N"; map[0x0144 - 0x00C0] = "n"; map[0x0148 - 0x00C0] = "n";
        map[0x014C - 0x00C0] = "O"; map[0x014E - 0x00C0] = "O"; map[0x0150 - 0x00C0] = "O";
        map[0x014D - 0x00C0] = "o"; map[0x014F - 0x00C0] = "o"; map[0x0151 - 0x00C0] = "o";
        map[0x0168 - 0x00C0] = "U"; map[0x016A - 0x00C0] = "U"; map[0x016C - 0x00C0] = "U"; map[0x016E - 0x00C0] = "U"; map[0x0170 - 0x00C0] = "U";
        map[0x0169 - 0x00C0] = "u"; map[0x016B - 0x00C0] = "u"; map[0x016D - 0x00C0] = "u"; map[0x016F - 0x00C0] = "u"; map[0x0171 - 0x00C0] = "u";
        map[0x0178 - 0x00C0] = "Y";
        map[0x0141 - 0x00C0] = "L"; map[0x0142 - 0x00C0] = "l";
        map[0x015A - 0x00C0] = "S"; map[0x015B - 0x00C0] = "s";
        map[0x0179 - 0x00C0] = "Z"; map[0x017A - 0x00C0] = "z"; map[0x017B - 0x00C0] = "Z"; map[0x017C - 0x00C0] = "z";
        map[0x0152 - 0x00C0] = "OE"; map[0x0153 - 0x00C0] = "oe";

        return map;
    }();
};



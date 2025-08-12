#include "SDeburr.h"

#include <bit>
#include <cstring>
#include <string>

#include <libstuff/libstuff.h>
#include <libstuff/sqlite3.h>

using namespace std;

/**
 * Converts special characters to ASCII equivalents.
 *
 * Examples:
 * - é → "e", Å → "A", ñ → "n" (removes accents)
 * - ß → "ss", Æ → "AE" (special cases)
 * - Regular letters like "a" or "Z" → returns nullptr (keep as-is)
 * - Accent marks by themselves → returns "" (delete them)
 * - Unknown characters → returns nullptr (keep as-is)
 */
const char* SDeburr::deburrMap(uint32_t codepoint) {
    // Accent marks by themselves (like ´ ` ^) → delete them
    if (codepoint >= 0x0300 && codepoint <= 0x036F) {
        return "";
    }

    // Regular ASCII letters (a-z, A-Z) → keep as-is
    if ((codepoint >= 'A' && codepoint <= 'Z') || (codepoint >= 'a' && codepoint <= 'z')) {
        return nullptr;
    }

    /**
     * Fast lookup tables for converting accented characters.
     *
     * We have two small arrays that work like dictionaries:
     * - latin1[]: Handles characters like é, ñ, Å (common European accents)
     * - extA[]:   Handles characters like ő, ł, ż (less common accents)
     *
     * Instead of checking every character one-by-one, we can jump straight to 
     * the answer by using the character's number as an array index.
     */
    struct Tables {
        const char* latin1[0x100 - 0xC0]; // 0xC0..0xFF (64 entries)
        const char* extA[0x180 - 0x100];  // 0x100..0x17F (128 entries)
        constexpr Tables() : latin1{nullptr}, extA{nullptr} {
            // Latin-1 Supplement mappings
            latin1[0x00C0 - 0x00C0] = "A"; latin1[0x00C1 - 0x00C0] = "A"; latin1[0x00C2 - 0x00C0] = "A"; latin1[0x00C3 - 0x00C0] = "A"; latin1[0x00C4 - 0x00C0] = "A"; latin1[0x00C5 - 0x00C0] = "A";
            latin1[0x00E0 - 0x00C0] = "a"; latin1[0x00E1 - 0x00C0] = "a"; latin1[0x00E2 - 0x00C0] = "a"; latin1[0x00E3 - 0x00C0] = "a"; latin1[0x00E4 - 0x00C0] = "a"; latin1[0x00E5 - 0x00C0] = "a";
            latin1[0x00C7 - 0x00C0] = "C"; latin1[0x00E7 - 0x00C0] = "c";
            latin1[0x00C8 - 0x00C0] = "E"; latin1[0x00C9 - 0x00C0] = "E"; latin1[0x00CA - 0x00C0] = "E"; latin1[0x00CB - 0x00C0] = "E";
            latin1[0x00E8 - 0x00C0] = "e"; latin1[0x00E9 - 0x00C0] = "e"; latin1[0x00EA - 0x00C0] = "e"; latin1[0x00EB - 0x00C0] = "e";
            latin1[0x00CC - 0x00C0] = "I"; latin1[0x00CD - 0x00C0] = "I"; latin1[0x00CE - 0x00C0] = "I"; latin1[0x00CF - 0x00C0] = "I";
            latin1[0x00EC - 0x00C0] = "i"; latin1[0x00ED - 0x00C0] = "i"; latin1[0x00EE - 0x00C0] = "i"; latin1[0x00EF - 0x00C0] = "i";
            latin1[0x00D1 - 0x00C0] = "N"; latin1[0x00F1 - 0x00C0] = "n";
            latin1[0x00D2 - 0x00C0] = "O"; latin1[0x00D3 - 0x00C0] = "O"; latin1[0x00D4 - 0x00C0] = "O"; latin1[0x00D5 - 0x00C0] = "O"; latin1[0x00D6 - 0x00C0] = "O"; latin1[0x00D8 - 0x00C0] = "O";
            latin1[0x00F2 - 0x00C0] = "o"; latin1[0x00F3 - 0x00C0] = "o"; latin1[0x00F4 - 0x00C0] = "o"; latin1[0x00F5 - 0x00C0] = "o"; latin1[0x00F6 - 0x00C0] = "o"; latin1[0x00F8 - 0x00C0] = "o";
            latin1[0x00D9 - 0x00C0] = "U"; latin1[0x00DA - 0x00C0] = "U"; latin1[0x00DB - 0x00C0] = "U"; latin1[0x00DC - 0x00C0] = "U";
            latin1[0x00F9 - 0x00C0] = "u"; latin1[0x00FA - 0x00C0] = "u"; latin1[0x00FB - 0x00C0] = "u"; latin1[0x00FC - 0x00C0] = "u";
            latin1[0x00DD - 0x00C0] = "Y"; latin1[0x00FD - 0x00C0] = "y"; latin1[0x00FF - 0x00C0] = "y";
            latin1[0x00DF - 0x00C0] = "ss"; latin1[0x00C6 - 0x00C0] = "AE"; latin1[0x00E6 - 0x00C0] = "ae";
            latin1[0x00DE - 0x00C0] = "TH"; latin1[0x00FE - 0x00C0] = "th"; latin1[0x00D0 - 0x00C0] = "D"; latin1[0x00F0 - 0x00C0] = "d";

            // Latin Extended-A mappings (0x0100..0x017F)
            extA[0x0100 - 0x0100] = "A"; extA[0x0102 - 0x0100] = "A"; extA[0x0104 - 0x0100] = "A";
            extA[0x0101 - 0x0100] = "a"; extA[0x0103 - 0x0100] = "a"; extA[0x0105 - 0x0100] = "a";
            extA[0x0106 - 0x0100] = "C"; extA[0x0108 - 0x0100] = "C"; extA[0x010A - 0x0100] = "C"; extA[0x010C - 0x0100] = "C";
            extA[0x0107 - 0x0100] = "c"; extA[0x0109 - 0x0100] = "c"; extA[0x010B - 0x0100] = "c"; extA[0x010D - 0x0100] = "c";
            extA[0x010E - 0x0100] = "D"; extA[0x010F - 0x0100] = "d";
            extA[0x0112 - 0x0100] = "E"; extA[0x0114 - 0x0100] = "E"; extA[0x0116 - 0x0100] = "E"; extA[0x0118 - 0x0100] = "E"; extA[0x011A - 0x0100] = "E";
            extA[0x0113 - 0x0100] = "e"; extA[0x0115 - 0x0100] = "e"; extA[0x0117 - 0x0100] = "e"; extA[0x0119 - 0x0100] = "e"; extA[0x011B - 0x0100] = "e";
            extA[0x0128 - 0x0100] = "I"; extA[0x012A - 0x0100] = "I"; extA[0x012C - 0x0100] = "I"; extA[0x012E - 0x0100] = "I";
            extA[0x0129 - 0x0100] = "i"; extA[0x012B - 0x0100] = "i"; extA[0x012D - 0x0100] = "i"; extA[0x012F - 0x0100] = "i";
            extA[0x0130 - 0x0100] = "I"; extA[0x0131 - 0x0100] = "i";
            extA[0x0143 - 0x0100] = "N"; extA[0x0147 - 0x0100] = "N"; extA[0x0144 - 0x0100] = "n"; extA[0x0148 - 0x0100] = "n";
            extA[0x014C - 0x0100] = "O"; extA[0x014E - 0x0100] = "O"; extA[0x0150 - 0x0100] = "O";
            extA[0x014D - 0x0100] = "o"; extA[0x014F - 0x0100] = "o"; extA[0x0151 - 0x0100] = "o";
            extA[0x0168 - 0x0100] = "U"; extA[0x016A - 0x0100] = "U"; extA[0x016C - 0x0100] = "U"; extA[0x016E - 0x0100] = "U"; extA[0x0170 - 0x0100] = "U";
            extA[0x0169 - 0x0100] = "u"; extA[0x016B - 0x0100] = "u"; extA[0x016D - 0x0100] = "u"; extA[0x016F - 0x0100] = "u"; extA[0x0171 - 0x0100] = "u";
            extA[0x0178 - 0x0100] = "Y";
            extA[0x0141 - 0x0100] = "L"; extA[0x0142 - 0x0100] = "l";
            extA[0x015A - 0x0100] = "S"; extA[0x015B - 0x0100] = "s";
            extA[0x0179 - 0x0100] = "Z"; extA[0x017A - 0x0100] = "z"; extA[0x017B - 0x0100] = "Z"; extA[0x017C - 0x0100] = "z";
            extA[0x0152 - 0x0100] = "OE"; extA[0x0153 - 0x0100] = "oe";
        }
    };

    static constexpr Tables tables;

    if (codepoint >= 0x00C0 && codepoint <= 0x00FF) {
        const char* v = tables.latin1[codepoint - 0x00C0];
        return v ? v : nullptr;
    }

    if (codepoint >= 0x0100 && codepoint <= 0x017F) {
        const char* v = tables.extA[codepoint - 0x0100];
        return v ? v : nullptr;
    }

    if (codepoint == 0x1E9E) {
        return "SS"; // ẞ
    }

    return nullptr;
}

/**
 * Remove accents from text to make it easier to search.
 *
 * How it works:
 * 1. Go through each character in the text
 * 2. If it's a regular letter (a-z, A-Z), keep it as-is
 * 3. If it's an accented character (é, ñ, etc.), replace it with the basic version
 * 4. If it's an accent mark by itself, delete it
 * 5. Everything else (numbers, punctuation, emoji) stays the same
 *
 * Examples: "café" → "cafe", "naïve" → "naive", "Zürich" → "Zurich"
 */
string SDeburr::deburr(const string& input) {
    const unsigned char* input_bytes = reinterpret_cast<const unsigned char*>(input.c_str());
    const size_t len = input.size();
    string result;
    result.reserve(len);
    size_t i = 0;
    while (i < len) {
        // Count the leading ones in the current byte
        // 0 leading ones -> ASCII character
        // 1 leading one -> continuation byte
        // 2-4 leading ones -> 2, 3, or 4-byte sequence
        // 5+ leading ones -> invalid UTF-8
        const int numLeadingOnes = countl_one(input_bytes[i]);

        // Speed optimization: copy regular ASCII text in chunks
        if (numLeadingOnes == 0) {
            size_t asciiStart = i++;
            while (i < len && countl_one(input_bytes[i]) == 0) {
                i++;
            }
            result.append(reinterpret_cast<const char*>(input_bytes + asciiStart), i - asciiStart);
            continue;
        }

        // For non-ASCII bytes, check if it's a UTF-8 continuation byte (or invalid UTF-8)
        if (numLeadingOnes == 1 || numLeadingOnes > 4) {
            // This is a continuation byte (10xxxxxx), skip it
            i++;
            continue;
        }

        // This is a valid 2, 3, or 4 byte UTF-8 sequence, decode the full character
        size_t start = i;
        uint32_t codepoint = input_bytes[i];

        // Check if we have enough bytes
        if (i + numLeadingOnes > len) {
            // Not enough bytes, treat as malformed
            i++;
        } else {
            // Extract data bits from leading byte
            codepoint = input_bytes[i] & ((1 << (7 - numLeadingOnes)) - 1);
            i++;

            // Process continuation bytes
            for (int j = 1; j < numLeadingOnes && i < len; j++) {
                unsigned char byte = input_bytes[i];
                if (countl_one(byte) != 1) {
                    // Invalid continuation byte, stop processing this sequence
                    break;
                }
                codepoint = (codepoint << 6) | (byte & 0x3F);
                i++;
            }
        }

        // Map the codepoint through deburrMap
        const char* mapped = deburrMap(codepoint);
        if (mapped == nullptr) {
            // No conversion needed, keep the original character
            result.append(reinterpret_cast<const char*>(input_bytes + start), i - start);
        } else if (*mapped) {
            // Replace with ASCII equivalent (é→e, ß→ss, etc.)
            result.append(mapped);
        } else {
            // Delete this character (accent marks)
        }
    }
    return result;
}

/**
 * SQLite UDF: DEBURR(text) → deburred ASCII string.
 *
 * Behavior:
 * - NULL input → NULL
 * - Non-NULL input → deburred ASCII text
 * - Declared deterministic in registerSQLite to enable SQLite optimizations
 */
void SDeburr::sqliteDeburr(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) {
        sqlite3_result_null(ctx);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }
    const unsigned char* text = sqlite3_value_text(argv[0]);
    if (!text) {
        sqlite3_result_null(ctx);
        return;
    }
    string out = SDeburr::deburr(reinterpret_cast<const char*>(text));
    sqlite3_result_text(ctx, out.c_str(), static_cast<int>(out.size()), SQLITE_TRANSIENT);
}

void SDeburr::registerSQLite(sqlite3* db) {
    // Deterministic to enable optimizations
    sqlite3_create_function_v2(db, "DEBURR", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, sqliteDeburr, nullptr, nullptr, nullptr);
}

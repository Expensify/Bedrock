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
 * - Unknown characters → returns nullptr (keep as-is)
 */
const char* SDeburr::deburrMap(uint32_t codepoint) {
    /**
     * Fast lookup table for converting accented characters.
     *
     * We have a flat array that works like a dictionary:
     * - unified[]: Handles characters from 0xC0 to 0x17F (Latin-1 Supplement + Latin Extended-A)
     *
     * The character's number is used as an array index so we can look up it's ASCII equivalent very quickly.
     */
    struct Tables {
        const char* unified[0x180 - 0xC0]; // 0xC0..0x17F (192 entries)
        constexpr Tables() : unified{} {
            // Latin-1 Supplement mappings (0xC0..0xFF)
            unified[0x00C0 - 0x00C0] = "A"; unified[0x00C1 - 0x00C0] = "A"; unified[0x00C2 - 0x00C0] = "A"; unified[0x00C3 - 0x00C0] = "A"; unified[0x00C4 - 0x00C0] = "A"; unified[0x00C5 - 0x00C0] = "A";
            unified[0x00E0 - 0x00C0] = "a"; unified[0x00E1 - 0x00C0] = "a"; unified[0x00E2 - 0x00C0] = "a"; unified[0x00E3 - 0x00C0] = "a"; unified[0x00E4 - 0x00C0] = "a"; unified[0x00E5 - 0x00C0] = "a";
            unified[0x00C7 - 0x00C0] = "C"; unified[0x00E7 - 0x00C0] = "c";
            unified[0x00C8 - 0x00C0] = "E"; unified[0x00C9 - 0x00C0] = "E"; unified[0x00CA - 0x00C0] = "E"; unified[0x00CB - 0x00C0] = "E";
            unified[0x00E8 - 0x00C0] = "e"; unified[0x00E9 - 0x00C0] = "e"; unified[0x00EA - 0x00C0] = "e"; unified[0x00EB - 0x00C0] = "e";
            unified[0x00CC - 0x00C0] = "I"; unified[0x00CD - 0x00C0] = "I"; unified[0x00CE - 0x00C0] = "I"; unified[0x00CF - 0x00C0] = "I";
            unified[0x00EC - 0x00C0] = "i"; unified[0x00ED - 0x00C0] = "i"; unified[0x00EE - 0x00C0] = "i"; unified[0x00EF - 0x00C0] = "i";
            unified[0x00D1 - 0x00C0] = "N"; unified[0x00F1 - 0x00C0] = "n";
            unified[0x00D2 - 0x00C0] = "O"; unified[0x00D3 - 0x00C0] = "O"; unified[0x00D4 - 0x00C0] = "O"; unified[0x00D5 - 0x00C0] = "O"; unified[0x00D6 - 0x00C0] = "O"; unified[0x00D8 - 0x00C0] = "O";
            unified[0x00F2 - 0x00C0] = "o"; unified[0x00F3 - 0x00C0] = "o"; unified[0x00F4 - 0x00C0] = "o"; unified[0x00F5 - 0x00C0] = "o"; unified[0x00F6 - 0x00C0] = "o"; unified[0x00F8 - 0x00C0] = "o";
            unified[0x00D9 - 0x00C0] = "U"; unified[0x00DA - 0x00C0] = "U"; unified[0x00DB - 0x00C0] = "U"; unified[0x00DC - 0x00C0] = "U";
            unified[0x00F9 - 0x00C0] = "u"; unified[0x00FA - 0x00C0] = "u"; unified[0x00FB - 0x00C0] = "u"; unified[0x00FC - 0x00C0] = "u";
            unified[0x00DD - 0x00C0] = "Y"; unified[0x00FD - 0x00C0] = "y"; unified[0x00FF - 0x00C0] = "y";
            unified[0x00DF - 0x00C0] = "ss"; unified[0x00C6 - 0x00C0] = "AE"; unified[0x00E6 - 0x00C0] = "ae";
            unified[0x00DE - 0x00C0] = "TH"; unified[0x00FE - 0x00C0] = "th"; unified[0x00D0 - 0x00C0] = "D"; unified[0x00F0 - 0x00C0] = "d";

            // Latin Extended-A mappings (0x0100..0x017F)
            unified[0x0100 - 0x00C0] = "A"; unified[0x0102 - 0x00C0] = "A"; unified[0x0104 - 0x00C0] = "A";
            unified[0x0101 - 0x00C0] = "a"; unified[0x0103 - 0x00C0] = "a"; unified[0x0105 - 0x00C0] = "a";
            unified[0x0106 - 0x00C0] = "C"; unified[0x0108 - 0x00C0] = "C"; unified[0x010A - 0x00C0] = "C"; unified[0x010C - 0x00C0] = "C";
            unified[0x0107 - 0x00C0] = "c"; unified[0x0109 - 0x00C0] = "c"; unified[0x010B - 0x00C0] = "c"; unified[0x010D - 0x00C0] = "c";
            unified[0x010E - 0x00C0] = "D"; unified[0x010F - 0x00C0] = "d";
            unified[0x0112 - 0x00C0] = "E"; unified[0x0114 - 0x00C0] = "E"; unified[0x0116 - 0x00C0] = "E"; unified[0x0118 - 0x00C0] = "E"; unified[0x011A - 0x00C0] = "E";
            unified[0x0113 - 0x00C0] = "e"; unified[0x0115 - 0x00C0] = "e"; unified[0x0117 - 0x00C0] = "e"; unified[0x0119 - 0x00C0] = "e"; unified[0x011B - 0x00C0] = "e";
            unified[0x0128 - 0x00C0] = "I"; unified[0x012A - 0x00C0] = "I"; unified[0x012C - 0x00C0] = "I"; unified[0x012E - 0x00C0] = "I";
            unified[0x0129 - 0x00C0] = "i"; unified[0x012B - 0x00C0] = "i"; unified[0x012D - 0x00C0] = "i"; unified[0x012F - 0x00C0] = "i";
            unified[0x0130 - 0x00C0] = "I"; unified[0x0131 - 0x00C0] = "i";
            unified[0x0143 - 0x00C0] = "N"; unified[0x0147 - 0x00C0] = "N"; unified[0x0144 - 0x00C0] = "n"; unified[0x0148 - 0x00C0] = "n";
            unified[0x014C - 0x00C0] = "O"; unified[0x014E - 0x00C0] = "O"; unified[0x0150 - 0x00C0] = "O";
            unified[0x014D - 0x00C0] = "o"; unified[0x014F - 0x00C0] = "o"; unified[0x0151 - 0x00C0] = "o";
            unified[0x0168 - 0x00C0] = "U"; unified[0x016A - 0x00C0] = "U"; unified[0x016C - 0x00C0] = "U"; unified[0x016E - 0x00C0] = "U"; unified[0x0170 - 0x00C0] = "U";
            unified[0x0169 - 0x00C0] = "u"; unified[0x016B - 0x00C0] = "u"; unified[0x016D - 0x00C0] = "u"; unified[0x016F - 0x00C0] = "u"; unified[0x0171 - 0x00C0] = "u";
            unified[0x0178 - 0x00C0] = "Y";
            unified[0x0141 - 0x00C0] = "L"; unified[0x0142 - 0x00C0] = "l";
            unified[0x015A - 0x00C0] = "S"; unified[0x015B - 0x00C0] = "s";
            unified[0x0179 - 0x00C0] = "Z"; unified[0x017A - 0x00C0] = "z"; unified[0x017B - 0x00C0] = "Z"; unified[0x017C - 0x00C0] = "z";
            unified[0x0152 - 0x00C0] = "OE"; unified[0x0153 - 0x00C0] = "oe";
        }
    };

    static constexpr Tables tables;

    if (codepoint >= 0x00C0 && codepoint <= 0x017F) {
        const char* v = tables.unified[codepoint - 0x00C0];
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

        // Check for invalid UTF-8. If invalid, skip the current byte and continue.
        // Cases we check for:
        // - A continuation byte (1 leading one). This should be handled only as part of a multi-byte sequence.
        // - More than 4 leading ones
        // - Not enough bytes to form a valid sequence
        if (numLeadingOnes == 1 || numLeadingOnes > 4 || (i + numLeadingOnes) > len) {
            i++;
            continue;
        }

        /*
         * This is a valid 2, 3, or 4 byte UTF-8 sequence, so next we'll decode the full character.
         *
         * The way this works is best explained by example.
         *
         * The hex codepoint for ñ is U+00F1, which is represented in UTF-8 as a 2-byte sequence: 11000011 10110001
         *
         *   - The first byte has 2 leading ones, which tells us it's a 2-byte sequence.
         *   - The next byte has 1 leading one, which tells us it's a continuation byte (a trailing part of a multi-byte sequence).
         *
         * To get the codepoint (the single number representing the character), we take all the bits from the sequence that aren't "signaling" other information and concat them together.
         *
         *   1. Remove the prefix bits (110) from the first byte. That gives us 00011.
         *   2. Remove the prefix bits (10) from the second byte. That gives us 110001.
         *   3. Concat these together, and the codepoint for ñ is 00011110001 (which, sure enough, is the binary representation of U+00F1).
         */
        size_t start = i;

        // Extract data bits from leading byte
        // The number of prefix bits is 7 - numLeadingOnes, because the leading ones signaling the length of the sequence are always terminated by a 0.
        // For example, two leading ones for a 2-byte sequence results in a prefix of 110 in the first byte of the sequence.
        // Three leading ones for a 3-byte sequence results in a prefix of 1110.
        uint32_t codepoint = input_bytes[i] & ((1 << (7 - numLeadingOnes)) - 1);
        i++;

        // Process continuation bytes. 
        for (int j = 1; j < numLeadingOnes; j++) {
            if (countl_one(input_bytes[i]) != 1) {
                // Invalid continuation byte, stop processing this sequence
                break;
            }

            // Shift the existing codepoint over 6 positions to make room for the new bits
            codepoint <<= 6;

            // Extract lower 6 bits from continuation and concat them to the existing codepoint
            codepoint = codepoint | (input_bytes[i] & 0b00111111);
            i++;
        }

        // Accent marks by themselves (like ´ ` ^) → skip them
        if (codepoint >= 0x0300 && codepoint <= 0x036F) {
            continue;
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

#include "SDeburr.h"

#include <bit>
#include <cstring>
#include <string>

#include <libstuff/libstuff.h>
#include <libstuff/sqlite3.h>

using namespace std;

const char* SDeburr::unicodeToAscii(uint32_t codepoint)
{
    // Check if the codepoint is in the range of unicode characters we can map to ascii.
    // 0x00C0 is the first character we map in the latin1 unicode block: https://en.wikipedia.org/wiki/Latin-1_Supplement
    // 0x017F is the last character we map in the latin-extended-a unicode block: https://en.wikipedia.org/wiki/Latin_Extended-A
    if (codepoint >= 0x00C0 && codepoint <= 0x017F) {
        return SDeburr::UNICODE_TO_ASCII_MAP[codepoint];
    }
    return nullptr;
}

string SDeburr::deburr(const unsigned char* inputBytes)
{
    string result;
    size_t inputLength = strlen(reinterpret_cast<const char*>(inputBytes));
    result.reserve(inputLength);
    size_t i = 0;
    while (i < inputLength) {
        // Count the leading ones in the current byte
        // 0 leading ones -> ASCII character
        // 1 leading one -> continuation byte
        // 2-4 leading ones -> 2, 3, or 4-byte sequence
        // 5+ leading ones -> invalid UTF-8
        const int numLeadingOnes = countl_one(inputBytes[i]);

        // Most common case: regular ASCII text
        if (numLeadingOnes == 0) {
            result.append(1, inputBytes[i]);
            i++;
            continue;
        }

        // Check for invalid UTF-8. If invalid, skip the current byte and continue.
        // Cases we check for:
        // - A continuation byte (1 leading one). This should be handled only as part of a multi-byte sequence.
        // - More than 4 leading ones
        // - Not enough bytes to form a valid sequence
        if (numLeadingOnes == 1 || numLeadingOnes > 4 || (i + numLeadingOnes) > inputLength) {
            SWARN("Encounterd malformed UTF-8");
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
        uint32_t codepoint = inputBytes[i] & ((1 << (7 - numLeadingOnes)) - 1);
        i++;

        // Process continuation bytes.
        for (int j = 1; j < numLeadingOnes; j++) {
            if (countl_one(inputBytes[i]) != 1) {
                // Invalid continuation byte, stop processing this sequence
                break;
            }

            // Shift the existing codepoint over 6 positions to make room for the new bits
            codepoint <<= 6;

            // Extract lower 6 bits from continuation and concat them to the existing codepoint
            codepoint = codepoint | (inputBytes[i] & 0b00111111);
            i++;
        }

        // Accent marks by themselves (like ´ ` ^) → skip them
        if (codepoint >= 0x0300 && codepoint <= 0x036F) {
            continue;
        }

        // Map the unicode codepoint to ascii
        const char* mapped = unicodeToAscii(codepoint);
        if (mapped == nullptr) {
            // No conversion needed, keep the original character
            result.append(reinterpret_cast<const char*>(inputBytes + start), i - start);
        } else if (*mapped) {
            // Replace with ASCII equivalent (é→e, ß→ss, etc.)
            result.append(mapped);
        } else {
            // Delete this character (accent marks)
        }
    }
    return result;
}

string SDeburr::deburr(const string& input)
{
    return deburr(reinterpret_cast<const unsigned char*>(input.c_str()));
}

void SDeburr::registerSQLite(sqlite3* db)
{
    // SQLite UDF: DEBURR(text) → deburred ASCII string.
    // Behavior: NULL input → NULL, Non-NULL input → deburred ASCII text
    // Declared deterministic to enable SQLite optimizations
    auto sqliteDeburr = [](sqlite3_context* ctx, int argc, sqlite3_value** argv) {
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
            string out = SDeburr::deburr(text);
            sqlite3_result_text(ctx, out.c_str(), static_cast<int>(out.size()), SQLITE_TRANSIENT);
        };

    sqlite3_create_function_v2(db, "DEBURR", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, sqliteDeburr, nullptr, nullptr, nullptr);
}

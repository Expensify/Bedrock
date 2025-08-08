#include "SDeburr.h"

#include <cstring>
#include <string>

#include <libstuff/libstuff.h>
#include <libstuff/sqlite3.h>

using std::string;

/**
 * Basic UTF-8 decoder: advances index and returns a decoded Unicode code point.
 *
 * The input is a UTF-8 byte sequence. UTF-8 encodes code points into 1–4 bytes:
 * - 1-byte ASCII: 0xxxxxxx
 * - 2-byte:       110xxxxx 10xxxxxx
 * - 3-byte:       1110xxxx 10xxxxxx 10xxxxxx
 * - 4-byte:       11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
 *
 * We use bitwise masks to classify the leading byte:
 * - (b & 0x80) == 0x00 → ASCII (0xxxxxxx)
 * - (b & 0xE0) == 0xC0 → 2-byte sequence (110xxxxx)
 * - (b & 0xF0) == 0xE0 → 3-byte sequence (1110xxxx)
 * - (b & 0xF8) == 0xF0 → 4-byte sequence (11110xxx)
 *
 * Continuation bytes must have the high bits 10xxxxxx, i.e. (b & 0xC0) == 0x80.
 * If the sequence is malformed (missing or invalid continuation), we fall back to
 * treating the lead byte as a standalone value to avoid throwing in low-level code.
 *
 * Note: This decoder intentionally avoids normalization/validation of overlong or
 * out-of-range sequences because the deburr logic only needs a best-effort fold.
 */
uint32_t SDeburr::decodeUTF8Codepoint(const unsigned char* bytes, size_t length, size_t& index) {
    if (index >= length) {
        return 0;
    }
    uint32_t current = bytes[index++];
    if (current < 0x80) {
        return current;
    }
    // 2-byte: 110xxxxx 10xxxxxx
    if ((current & 0xE0) == 0xC0) {
        if (index >= length) {
            return current;
        }
        uint32_t c2 = bytes[index++];
        if ((c2 & 0xC0) != 0x80) {
            return current;
        }
        return ((current & 0x1F) << 6) | (c2 & 0x3F);
    }
    // 3-byte: 1110xxxx 10xxxxxx 10xxxxxx
    if ((current & 0xF0) == 0xE0) {
        if (index + 1 > length) {
            return current;
        }
        uint32_t c2 = bytes[index++];
        uint32_t c3 = bytes[index++];
        if ((c2 & 0xC0) != 0x80 || (c3 & 0xC0) != 0x80) {
            return current;
        }
        return ((current & 0x0F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
    }
    // 4-byte: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
    if ((current & 0xF8) == 0xF0) {
        if (index + 2 > length) {
            return current;
        }
        uint32_t c2 = bytes[index++];
        uint32_t c3 = bytes[index++];
        uint32_t c4 = bytes[index++];
        if ((c2 & 0xC0) != 0x80 || (c3 & 0xC0) != 0x80 || (c4 & 0xC0) != 0x80) {
            return current;
        }
        return ((current & 0x07) << 18) | ((c2 & 0x3F) << 12) | ((c3 & 0x3F) << 6) | (c4 & 0x3F);
    }
    // invalid or overlong sequence; return the raw lead byte for a graceful degrade
    return current;
}

/**
 * Map a Unicode code point to a deburred ASCII string (or signal pass-through).
 *
 * - For ASCII letters, returns nullptr to signal the caller to copy the original
 *   byte (caller lowercases to avoid unnecessary allocations).
 * - For Latin-1 Supplement and a subset of Latin Extended-A, returns ASCII
 *   approximations (e.g., Å → "a", ß → "ss", Æ → "ae", Œ → "oe").
 * - For combining diacritical marks U+0300–U+036F, returns an empty string to
 *   drop the mark while keeping the previously emitted base character.
 * - For other unmapped non-ASCII code points, returns nullptr so the caller can
 *   drop them (we only keep ASCII + explicitly mapped folds).
 */
const char* SDeburr::deburrMap(uint32_t codepoint) {
    // Combining marks U+0300–U+036F: drop them
    if (codepoint >= 0x0300 && codepoint <= 0x036F) {
        return "";
    }

    // Fast reject ASCII letters (handled by caller's ASCII fast-path anyway)
    if ((codepoint >= 'A' && codepoint <= 'Z') || (codepoint >= 'a' && codepoint <= 'z')) {
        return nullptr;
    }

    /**
     * Compile-time lookup tables for fast O(1) character mapping.
     *
     * Instead of switch/case or hash maps, we use two small fixed arrays:
     * - latin1[]: Maps Latin-1 Supplement range (U+00C0–U+00FF) to ASCII replacements
     * - extA[]:   Maps Latin Extended-A range (U+0100–U+017F) to ASCII replacements
     *
     * Array indices are calculated by subtracting the range base (e.g., codepoint - 0xC0).
     * nullptr entries indicate no mapping exists for that codepoint.
     *
     * The constexpr constructor builds these tables at compile time, so there's
     * zero runtime initialization cost - the lookup data is baked into the binary.
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
 * Convert a UTF-8 string to an ASCII-only approximation by removing diacritics.
 *
 * Algorithm:
 * - Decode the next Unicode code point from UTF-8 (see decoder above).
 * - Look up an ASCII replacement in deburrMap().
 *   - If deburrMap returns nullptr:
 *     - Append the original code point bytes unchanged (preserve case and unmapped chars),
 *       matching lodash's deburr behavior which only alters known Latin letters.
 *   - If deburrMap returns an empty string (""): drop it (combining diacritics).
 *   - Otherwise, append the mapped ASCII sequence (e.g., "ss", "ae").
 *
 * Case is preserved. Mappings should account for proper case where relevant.
 */
std::string SDeburr::deburr(const std::string& input) {
    const unsigned char* in = reinterpret_cast<const unsigned char*>(input.c_str());
    const size_t len = input.size();
    string result;
    result.reserve(len);
    size_t i = 0;
    while (i < len) {
        // Fast path: copy contiguous ASCII bytes in one append
        if (in[i] < 0x80) {
            size_t asciiStart = i++;
            while (i < len && in[i] < 0x80) {
                ++i;
            }
            result.append(reinterpret_cast<const char*>(in + asciiStart), i - asciiStart);
            continue;
        }

        // Non-ASCII: decode a single UTF-8 code point and map
        size_t start = i;
        uint32_t cp = decodeUTF8Codepoint(in, len, i);
        const char* mapped = deburrMap(cp);
        if (mapped == nullptr) {
            // Preserve original code point bytes (non-ASCII) when no mapping exists
            result.append(input, start, i - start);
        } else if (*mapped) {
            result.append(mapped);
        } else {
            // empty mapping => skip (combining mark)
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
 
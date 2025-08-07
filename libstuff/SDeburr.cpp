// Copyright (c) Expensify, Inc.
// SPDX-License-Identifier: MIT

#include "SDeburr.h"

#include <cstring>
#include <string>

#include <libstuff/libstuff.h>
#include <libstuff/sqlite3.h>

using std::string;

namespace {

// Basic UTF-8 decoder: advances index and returns codepoint, or returns byte for invalid sequences
static uint32_t decodeUTF8Codepoint(const unsigned char* bytes, size_t length, size_t& index) {
    if (index >= length) return 0;
    uint32_t current = bytes[index++];
    if (current < 0x80) return current;
    if ((current & 0xE0) == 0xC0) {
        if (index >= length) return current;
        uint32_t c2 = bytes[index++];
        if ((c2 & 0xC0) != 0x80) return current;
        return ((current & 0x1F) << 6) | (c2 & 0x3F);
    }
    if ((current & 0xF0) == 0xE0) {
        if (index + 1 > length) return current;
        uint32_t c2 = bytes[index++];
        uint32_t c3 = bytes[index++];
        if ((c2 & 0xC0) != 0x80 || (c3 & 0xC0) != 0x80) return current;
        return ((current & 0x0F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
    }
    if ((current & 0xF8) == 0xF0) {
        if (index + 2 > length) return current;
        uint32_t c2 = bytes[index++];
        uint32_t c3 = bytes[index++];
        uint32_t c4 = bytes[index++];
        if ((c2 & 0xC0) != 0x80 || (c3 & 0xC0) != 0x80 || (c4 & 0xC0) != 0x80) return current;
        return ((current & 0x07) << 18) | ((c2 & 0x3F) << 12) | ((c3 & 0x3F) << 6) | (c4 & 0x3F);
    }
    return current; // invalid or overlong sequence
}

static const char* deburrMap(uint32_t codepoint) {
    switch (codepoint) {
        // ASCII letters pass-through; caller lowercases
        case 'A': case 'a': case 'B': case 'b': case 'C': case 'c': case 'D': case 'd':
        case 'E': case 'e': case 'F': case 'f': case 'G': case 'g': case 'H': case 'h':
        case 'I': case 'i': case 'J': case 'j': case 'K': case 'k': case 'L': case 'l':
        case 'M': case 'm': case 'N': case 'n': case 'O': case 'o': case 'P': case 'p':
        case 'Q': case 'q': case 'R': case 'r': case 'S': case 's': case 'T': case 't':
        case 'U': case 'u': case 'V': case 'v': case 'W': case 'w': case 'X': case 'x':
        case 'Y': case 'y': case 'Z': case 'z':
            return nullptr;

        // Latin-1 Supplement
        case 0x00C0: case 0x00C1: case 0x00C2: case 0x00C3: case 0x00C4: case 0x00C5: return "a"; // ÀÁÂÃÄÅ
        case 0x00E0: case 0x00E1: case 0x00E2: case 0x00E3: case 0x00E4: case 0x00E5: return "a"; // àáâãäå
        case 0x00C7: case 0x00E7: return "c"; // Çç
        case 0x00C8: case 0x00C9: case 0x00CA: case 0x00CB: return "e"; // ÈÉÊË
        case 0x00E8: case 0x00E9: case 0x00EA: case 0x00EB: return "e"; // èéêë
        case 0x00CC: case 0x00CD: case 0x00CE: case 0x00CF: return "i"; // ÌÍÎÏ
        case 0x00EC: case 0x00ED: case 0x00EE: case 0x00EF: return "i"; // ìíîï
        case 0x00D1: case 0x00F1: return "n"; // Ññ
        case 0x00D2: case 0x00D3: case 0x00D4: case 0x00D5: case 0x00D6: case 0x00D8: return "o"; // ÒÓÔÕÖØ
        case 0x00F2: case 0x00F3: case 0x00F4: case 0x00F5: case 0x00F6: case 0x00F8: return "o"; // òóôõöø
        case 0x00D9: case 0x00DA: case 0x00DB: case 0x00DC: return "u"; // ÙÚÛÜ
        case 0x00F9: case 0x00FA: case 0x00FB: case 0x00FC: return "u"; // ùúûü
        case 0x00DD: case 0x00FD: case 0x00FF: return "y"; // Ýýÿ
        case 0x00DF: return "ss"; // ß
        case 0x00C6: case 0x00E6: return "ae"; // Ææ
        case 0x0152: case 0x0153: return "oe"; // Œœ

        // Latin Extended-A (subset)
        case 0x0100: case 0x0101: case 0x0102: case 0x0103: case 0x0104: case 0x0105: return "a";
        case 0x0106: case 0x0107: case 0x0108: case 0x0109: case 0x010A: case 0x010B: case 0x010C: case 0x010D: return "c";
        case 0x010E: case 0x010F: return "d";
        case 0x0112: case 0x0113: case 0x0114: case 0x0115: case 0x0116: case 0x0117: case 0x0118: case 0x0119: case 0x011A: case 0x011B: return "e";
        case 0x0128: case 0x0129: case 0x012A: case 0x012B: case 0x012C: case 0x012D: case 0x012E: case 0x012F: return "i";
        case 0x0130: case 0x0131: return "i"; // dotless i
        case 0x0143: case 0x0144: case 0x0147: case 0x0148: return "n";
        case 0x014C: case 0x014D: case 0x014E: case 0x014F: case 0x0150: case 0x0151: return "o";
        case 0x0168: case 0x0169: case 0x016A: case 0x016B: case 0x016C: case 0x016D: case 0x016E: case 0x016F: case 0x0170: case 0x0171: return "u";
        case 0x0178: return "y";

        default:
            // Combining marks U+0300–U+036F: skip
            if (codepoint >= 0x0300 && codepoint <= 0x036F) return "";
            return nullptr;
    }
}

static string deburrASCIIImpl(const string& input) {
    const unsigned char* in = reinterpret_cast<const unsigned char*>(input.c_str());
    const size_t len = input.size();
    string result;
    result.reserve(len);
    size_t i = 0;
    while (i < len) {
        size_t start = i;
        uint32_t cp = decodeUTF8Codepoint(in, len, i);
        const char* mapped = deburrMap(cp);
        if (mapped == nullptr) {
            unsigned char byte = in[start];
            if (byte < 0x80) {
                if (byte >= 'A' && byte <= 'Z') byte = static_cast<unsigned char>(byte - 'A' + 'a');
                result.push_back(static_cast<char>(byte));
            }
            // Else drop non-ASCII codepoint with no mapping
        } else if (*mapped) {
            result.append(mapped);
        } else {
            // empty mapping => skip (combining mark)
        }
    }
    return result;
}

static void sqliteDeburr(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 1) { sqlite3_result_null(ctx); return; }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) { sqlite3_result_null(ctx); return; }
    const unsigned char* text = sqlite3_value_text(argv[0]);
    if (!text) { sqlite3_result_null(ctx); return; }
    string out = deburrASCIIImpl(reinterpret_cast<const char*>(text));
    sqlite3_result_text(ctx, out.c_str(), static_cast<int>(out.size()), SQLITE_TRANSIENT);
}

} // namespace

namespace SDeburr {

std::string deburrToASCII(const std::string& input) {
    return deburrASCIIImpl(input);
}

void registerSQLiteDeburr(sqlite3* db) {
    // Deterministic to enable optimizations
    sqlite3_create_function_v2(db, "DEBURR", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, sqliteDeburr, nullptr, nullptr, nullptr);
}

} // namespace SDeburr



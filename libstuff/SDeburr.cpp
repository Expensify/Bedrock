#include "SDeburr.h"

#include <cstring>
#include <string>

#include <libstuff/libstuff.h>
#include <libstuff/sqlite3.h>

using std::string;
#if 0
/**
 * Reads one Unicode character from a UTF-8 encoded string and advances the position.
 *
 * UTF-8 is a variable-length encoding where each Unicode character can take 1-4 bytes:
 * - 1 byte:  U+0000 to U+007F   (ASCII: a, b, c, 1, 2, 3, etc.)
 * - 2 bytes: U+0080 to U+07FF   (Latin, Greek, Cyrillic: é, ñ, α, ß, etc.)
 * - 3 bytes: U+0800 to U+FFFF   (Most other scripts: 中, €, ♥, etc.)
 * - 4 bytes: U+10000 to U+10FFFF (Emoji, rare symbols: 😀, 𝕏, etc.)
 *
 * HOW UTF-8 ENCODING WORKS:
 * Each byte in UTF-8 has a specific bit pattern that tells us what it represents:
 *
 * 1-byte (ASCII):     0xxxxxxx        (values 0x00-0x7F)
 * 2-byte start:       110xxxxx        (values 0xC0-0xDF)
<<<<<<< Updated upstream
 * 3-byte start:       1110xxxx        (values 0xE0-0xEF)  
=======
 * 3-byte start:       1110xxxx        (values 0xE0-0xEF)
>>>>>>> Stashed changes
 * 4-byte start:       11110xxx        (values 0xF0-0xF7)
 * Continuation byte:  10xxxxxx        (values 0x80-0xBF)
 *
 * The 'x' bits contain the actual Unicode codepoint data.
 *
 * EXAMPLE: The character 'é' (U+00E9) is encoded as 2 bytes: 0xC3 0xA9
 * - First byte:  11000011 (0xC3) → tells us "this is a 2-byte sequence"
 * - Second byte: 10101001 (0xA9) → tells us "this is a continuation byte"
 * - Data bits:   00011 101001 → combine to get 11101001 = 0xE9 = 233 = 'é'
 *
 * This function uses bitwise AND operations to check the bit patterns:
 * - (byte & 0x80) == 0x00 → starts with 0, so it's ASCII
<<<<<<< Updated upstream
 * - (byte & 0xE0) == 0xC0 → starts with 110, so it's a 2-byte sequence  
=======
 * - (byte & 0xE0) == 0xC0 → starts with 110, so it's a 2-byte sequence
>>>>>>> Stashed changes
 * - (byte & 0xF0) == 0xE0 → starts with 1110, so it's a 3-byte sequence
 * - (byte & 0xF8) == 0xF0 → starts with 11110, so it's a 4-byte sequence
 *
 * If we encounter malformed UTF-8 (which shouldn't happen in normal text),
 * we just return what we can and continue, rather than crashing.
 */
uint32_t SDeburr::decodeUTF8Codepoint(const unsigned char* bytes, size_t length, size_t& index) {
    // Safety check: make sure we're not reading past the end
    if (index >= length) {
        return 0;
    }

    // Read the first byte and advance our position
    uint32_t current = bytes[index++];

    // Check if it's a simple ASCII character (0xxxxxxx pattern)
    if (current < 0x80) {
        // ASCII characters (a-z, A-Z, 0-9, punctuation, etc.) are stored as-is
        // No further processing needed, just return the byte value
        return current;
    }

    // 2-byte UTF-8 sequence (110xxxxx 10xxxxxx pattern)
    // Covers characters U+0080 to U+07FF (like é, ñ, ß, α, etc.)
    if ((current & 0xE0) == 0xC0) {
        // Check if we have enough bytes left
        if (index >= length) {
            return current;  // Malformed: return what we have
        }

        // Read the second byte
        uint32_t c2 = bytes[index++];

        // Verify it's a valid continuation byte (10xxxxxx pattern)
        if ((c2 & 0xC0) != 0x80) {
            return current;  // Malformed: return the first byte
        }

        // Combine the data bits:
        // - Take lower 5 bits from first byte (current & 0x1F)
        // - Shift them left 6 positions to make room
        // - Take lower 6 bits from second byte (c2 & 0x3F)
        // - OR them together to get the final codepoint
        // Example: é = 0xC3 0xA9 → (0x03 << 6) | 0x29 = 0xE9 = 233
        return ((current & 0x1F) << 6) | (c2 & 0x3F);
    }

    // 3-byte UTF-8 sequence (1110xxxx 10xxxxxx 10xxxxxx pattern)
    // Covers characters U+0800 to U+FFFF (like €, ♥, 中, etc.)
    if ((current & 0xF0) == 0xE0) {
        // Check if we have enough bytes left (need 2 more)
        if (index + 1 > length) {
            return current;  // Malformed: return what we have
        }

        // Read the second and third bytes
        uint32_t c2 = bytes[index++];
        uint32_t c3 = bytes[index++];

        // Verify both are valid continuation bytes (10xxxxxx pattern)
        if ((c2 & 0xC0) != 0x80 || (c3 & 0xC0) != 0x80) {
            return current;  // Malformed: return the first byte
        }

        // Combine the data bits:
        // - Take lower 4 bits from first byte, shift left 12 positions
<<<<<<< Updated upstream
        // - Take lower 6 bits from second byte, shift left 6 positions  
=======
        // - Take lower 6 bits from second byte, shift left 6 positions
>>>>>>> Stashed changes
        // - Take lower 6 bits from third byte
        // - OR them all together
        // Total: 4 + 6 + 6 = 16 bits of data
        return ((current & 0x0F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F);
    }

    // 4-byte UTF-8 sequence (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx pattern)
    // Covers characters U+10000 to U+10FFFF (like emoji 😀, mathematical symbols 𝕏, etc.)
    if ((current & 0xF8) == 0xF0) {
        // Check if we have enough bytes left (need 3 more)
        if (index + 2 > length) {
            return current;  // Malformed: return what we have
        }

        // Read the second, third, and fourth bytes
        uint32_t c2 = bytes[index++];
        uint32_t c3 = bytes[index++];
        uint32_t c4 = bytes[index++];

        // Verify all are valid continuation bytes (10xxxxxx pattern)
        if ((c2 & 0xC0) != 0x80 || (c3 & 0xC0) != 0x80 || (c4 & 0xC0) != 0x80) {
            return current;  // Malformed: return the first byte
        }

        // Combine the data bits:
        // - Take lower 3 bits from first byte, shift left 18 positions
        // - Take lower 6 bits from second byte, shift left 12 positions
        // - Take lower 6 bits from third byte, shift left 6 positions
        // - Take lower 6 bits from fourth byte
        // - OR them all together
        // Total: 3 + 6 + 6 + 6 = 21 bits of data
        return ((current & 0x07) << 18) | ((c2 & 0x3F) << 12) | ((c3 & 0x3F) << 6) | (c4 & 0x3F);
    }

    // If we get here, the first byte doesn't match any valid UTF-8 pattern
    // This shouldn't happen with well-formed UTF-8, but we handle it gracefully
    // by just returning the problematic byte and continuing
    return current;
}

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
std::string SDeburr::deburr(const std::string& input) {
    const unsigned char* in = reinterpret_cast<const unsigned char*>(input.c_str());
    const size_t len = input.size();
    string result;
    result.reserve(len);
    size_t i = 0;
    while (i < len) {
        // Speed optimization: copy regular ASCII text in chunks
        if (in[i] < 0x80) {
            size_t asciiStart = i++;
            while (i < len && in[i] < 0x80) {
                ++i;
            }
            result.append(reinterpret_cast<const char*>(in + asciiStart), i - asciiStart);
            continue;
        }

        // Handle special characters (accented letters, etc.)
        size_t start = i;
        uint32_t cp = decodeUTF8Codepoint(in, len, i);
        const char* mapped = deburrMap(cp);
        if (mapped == nullptr) {
            // No conversion needed, keep the original character
            result.append(reinterpret_cast<const char*>(in + start), i - start);
        } else if (*mapped) {
            // Replace with ASCII equivalent (é→e, ß→ss, etc.)
            result.append(mapped);
        } else {
            // Delete this character (accent marks)
        }
    }
    return result;
}
#endif

// This is only a sample, and needs the rest of:
// Latin-1 Supplement: https://en.wikipedia.org/wiki/Latin-1_Supplement#Character_table
// and Latin Extended-A: https://en.wikipedia.org/wiki/Latin_Extended-A
static const map<string, string> UNICODE_REPLACEMENTS{
    // Latin-1 Supplement
    {"À", "A"},
    {"Á", "A"},
    {"Â", "A"},
    {"Ã", "A"},
    {"Ä", "A"},
    {"Å", "A"},
    {"Ç", "C"},
    {"È", "E"},
    {"É", "E"},
    {"Ê", "E"},
    {"Ë", "E"},
    {"Ì", "I"},
    {"Í", "I"},
    {"Î", "I"},
    {"Ï", "I"},
    {"Ñ", "N"},
    {"Ò", "O"},
    {"Ó", "O"},
    {"Ô", "O"},
    {"Õ", "O"},
    {"Ö", "O"},
    {"Ø", "O"},
    {"Ù", "U"},
    {"Ú", "U"},
    {"Û", "U"},
    {"Ü", "U"},
    {"Ý", "Y"},
    {"à", "a"},
    {"á", "a"},
    {"â", "a"},
    {"ã", "a"},
    {"ä", "a"},
    {"å", "a"},
    {"ç", "c"},
    {"è", "e"},
    {"é", "e"},
    {"ê", "e"},
    {"ë", "e"},
    {"ì", "i"},
    {"í", "i"},
    {"î", "i"},
    {"ï", "i"},
    {"ñ", "n"},
    {"ò", "o"},
    {"ó", "o"},
    {"ô", "o"},
    {"õ", "o"},
    {"ö", "o"},
    {"ø", "o"},
    {"ù", "u"},
    {"ú", "u"},
    {"û", "u"},
    {"ü", "u"},
    {"ý", "y"},
    {"ÿ", "y"},

    // Latin Extended-A
    {"Ā", "A"},
    {"ā", "a"},
    {"Ă", "A"},
    {"ă", "a"},
    {"Ą", "A"},
    {"ą", "a"},
    {"Ć", "C"},
    {"ć", "c"},
    {"Ĉ", "C"},
    {"ĉ", "c"},
    {"Ċ", "C"},
    {"ċ", "c"},
    {"Č", "C"},
    {"č", "c"},
    {"Ď", "D"},
    {"ď", "d"},
    {"Đ", "D"},
    {"đ", "d"},
    {"Ē", "E"},
    {"ē", "e"},
    {"Ĕ", "E"},
    {"ĕ", "e"},
    {"Ė", "E"},
    {"ė", "e"},
    {"Ę", "E"},
    {"ę", "e"},
    {"Ě", "E"},
    {"ě", "e"},
    {"Ĝ", "G"},
    {"ĝ", "g"},
    {"Ğ", "G"},
    {"ğ", "g"},
    {"Ġ", "G"},
    {"ġ", "g"},
    {"Ģ", "G"},
    {"ģ", "g"},
    {"Ĥ", "H"},
    {"ĥ", "h"},
    {"Ħ", "H"},
    {"ħ", "h"},
    {"Ĩ", "I"},
    {"ĩ", "i"},
    {"Ī", "I"},
    {"ī", "i"},
    {"Ĭ", "I"},
    {"ĭ", "i"},
    {"Į", "I"},
    {"į", "i"},
    {"İ", "I"},
    {"Ĵ", "J"},
    {"ĵ", "j"},
    {"Ķ", "K"},
    {"ķ", "k"},
    {"Ĺ", "L"},
    {"ĺ", "l"},
    {"Ļ", "L"},
    {"ļ", "l"},
    {"Ľ", "L"},
    {"ľ", "l"},
    {"Ŀ", "L"},
    {"ŀ", "l"},
    {"Ł", "L"},
    {"ł", "l"},
    {"Ń", "N"},
    {"ń", "n"},
    {"Ņ", "N"},
    {"ņ", "n"},
    {"Ň", "N"},
    {"ň", "n"},
    {"Ō", "O"},
    {"ō", "o"},
    {"Ŏ", "O"},
    {"ŏ", "o"},
    {"Ő", "O"},
    {"ő", "O"},
    {"Ŕ", "R"},
    {"ŕ", "r"},
    {"Ŗ", "R"},
    {"ŗ", "r"},
    {"Ř", "R"},
    {"ř", "r"},
    {"Ś", "S"},
    {"ś", "s"},
    {"Ŝ", "S"},
    {"ŝ", "s"},
    {"Ş", "S"},
    {"ş", "s"},
    {"Š", "S"},
    {"š", "s"},
    {"Ţ", "T"},
    {"ţ", "t"},
    {"Ť", "T"},
    {"ť", "t"},
    {"Ŧ", "T"},
    {"ŧ", "t"},
    {"Ũ", "U"},
    {"ũ", "u"},
    {"Ū", "U"},
    {"ū", "u"},
    {"Ŭ", "U"},
    {"ŭ", "u"},
    {"Ů", "U"},
    {"ů", "u"},
    {"Ű", "U"},
    {"ű", "U"},
    {"Ų", "U"},
    {"ų", "u"},
    {"Ŵ", "W"},
    {"ŵ", "w"},
    {"Ŷ", "Y"},
    {"ŷ", "y"},
    {"Ÿ", "Y"},
    {"Ź", "Z"},
    {"ź", "z"},
    {"Ż", "Z"},
    {"ż", "z"},
    {"Ž", "Z"},
    {"ž", "z"},
};

std::string SDeburr::deburr(const std::string& input) {
    string output;
    const char* current = input.data();
    while (current < input.data() + input.size()) {
        // Determine how many bytes there are in this character. See "Description" here: https://en.wikipedia.org/wiki/UTF-8
        size_t byteLength = 1;
        if ((current[0] & 0b11000000) == 0b11000000) {
            byteLength++;
            if ((current[0] & 0b11100000) == 0b11100000) {
                byteLength++;
                if ((current[0] & 0b11110000) == 0b11110000) {
                    byteLength++;
                }
            }
        }

        // basic ASCII characters can be copied with no additional checking.
        if (byteLength == 1) {
            output += current[0];
            current += byteLength;
            continue;

        }

        // Combining Diacritical Marks allow for adding accent marks to other characters essentailly by following a "base" character
        // with an accent mark. Lodash removes these (https://lodash.com/docs/4.17.15#deburr) so we will as well.
        // See the description here: https://en.wikipedia.org/wiki/Combining_Diacritical_Marks
        if (byteLength == 2) {
            bool isInCombiningDiacriticalMarks =
                (current[0] == 0xCC && current[1] >= 0x80 && current[1] <= 0xBF) ||
                (current[0] == 0xCD && current[1] >= 0x80 && current[1] <= 0xAF);
            if (isInCombiningDiacriticalMarks) {
                // If this *is* in the Combining Diacritical Marks block, we can just drop it and be left with the base character.
                current += byteLength;
                continue;
            }
        }

        // We are now left with any multi-byte unicode character that's not a Combining Diacritical Mark.
        // Create a string to represent it.
        string lookup(current, byteLength);

        // Look up and see if we need to replace this string. If so, do that, otherwise keep the unicode string we already made.
        auto replacement = UNICODE_REPLACEMENTS.find(lookup);
        if (replacement == UNICODE_REPLACEMENTS.end()) {
            output += lookup;
        } else {
            output += replacement->second;
        }

        // We have consumed these bytes and can move on.
        current += byteLength;
    }

    return output;
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

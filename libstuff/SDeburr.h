// Copyright (c) Expensify, Inc.
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

// Forward-declare sqlite3 to avoid forcing all consumers to include sqlite3 headers.
// This keeps the header lightweight; only the implementation needs sqlite3 symbols.
struct sqlite3;

namespace SDeburr {

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
std::string deburrToASCII(const std::string& input);

/**
 * Register the SQLite UDF `DEBURR(text)` on the provided database handle.
 *
 * - Returns the same value as `deburrToASCII(text)`
 * - Marked deterministic to allow SQLite optimizations and query planning
 * - NULL input yields NULL
 */
void registerSQLiteDeburr(sqlite3* db);

} // namespace SDeburr



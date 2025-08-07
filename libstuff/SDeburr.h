// Copyright (c) Expensify, Inc.
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

// Forward-declare sqlite3 to avoid forcing all consumers to include sqlite3 headers
struct sqlite3;

namespace SDeburr {

// Returns a lowercase ASCII-folded version of the input, removing diacritics and
// mapping common Latin characters with accents to ASCII equivalents. Non-ASCII
// codepoints without a specific mapping are dropped. Intended to mirror lodash's
// deburr behavior enough for search normalization.
std::string deburrToASCII(const std::string& input);

// Registers the SQLite UDF `DEBURR(text)` on the provided database handle.
// The function returns the same value as deburrToASCII for the given argument.
void registerSQLiteDeburr(sqlite3* db);

} // namespace SDeburr



/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    Parser.h
 * Path:    libstuff/JSON/Parser.h
 * Pair:    Parser.cpp
 *
 * INTENT
 *   Parse a JSON string into a JSON::Value tree, in a strict mode that
 *   throws on malformed input and a permissive mode that does not.
 *
 * OBJECTS
 *   Parser  - class; two static entry points, read() (throws
 *             JSON::InvalidArgument on parse failure) and readUnsafe()
 *             (swallows parse errors and returns whatever partial value
 *             resulted).
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   "readUnsafe" clearly signals the risk relative to "read"; good.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include <memory>
#include <string>

#include "Value.h"

namespace JSON
{
class Parser
{
public:

    /**
     * Read and parse a json string
     *
     * @param json The json string
     * @return a JSON::Value with the contents
     * @throws JSON::InvalidArgument
     */
    static unique_ptr<Value> read(const string& json);

    /**
     * Read and parse a json string but don't throw an exception if the
     * JSON is invalid. Accept whatever value happens to come out.
     *
     * @param json The json string
     * @return a JSON::Value with the contents
     */
    static unique_ptr<Value> readUnsafe(const string& json);
};
}

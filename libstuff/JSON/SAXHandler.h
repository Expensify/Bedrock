/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SAXHandler.h
 * Path:    libstuff/JSON/SAXHandler.h
 * Pair:    SAXHandler.cpp
 *
 * INTENT
 *   Implements rapidjson's SAX handler interface to build a JSON::Value
 *   tree (with nested objects/arrays) from the stream of callbacks a
 *   rapidjson::Reader emits while parsing.
 *
 * OBJECTS
 *   SAXHandler  - class; implements the full handler surface a
 *                 rapidjson::Reader expects (Null, Bool, Int, Uint,
 *                 Int64, Uint64, Double, String, StartObject, Key,
 *                 EndObject, StartArray, EndArray, RawNumber), tracks a
 *                 stack of in-progress Value contexts, and hands off the
 *                 finished root via getValue().
 *
 * OUT OF PLACE
 *   Nothing.
 *
 * NAME/LOCATION FIT
 *   Fits; name matches the rapidjson SAX handler convention it implements.
 *
 * NAMING QUALITY
 *   Interface method names are fixed by rapidjson's contract. Internal
 *   members (contexts, tempArrayContentLists, root, ownershipGiven) are
 *   clear; root is a manually new/deleted raw pointer that getValue()
 *   later wraps in a unique_ptr on hand-off, an ownership pattern worth
 *   a second look but not a naming problem.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "Value.h"

#include <rapidjson/reader.h>

namespace JSON
{
/**
 * Builds JSON::Value objects from a series of SAX events.
 * @see: https://miloyip.github.io/rapidjson/md_doc_sax.html
 */
class SAXHandler
{
public:

    /**
     * Interface for rapidjson::Reader
     */
    bool Null();
    bool Bool(bool b);
    bool Int(int i);
    bool Uint(unsigned i);
    bool Int64(int64_t i);
    bool Uint64(uint64_t i);
    bool Double(double d);
    bool String(const char* str, size_t length, bool copy);
    bool StartObject();
    bool Key(const char* str, size_t length, bool copy);
    bool EndObject(size_t memberCount);
    bool StartArray();
    bool EndArray(size_t elementCount);
    bool RawNumber(const char* str, rapidjson::SizeType len, bool copy);

    /**
     * End Interface
     */

    /**
     * Retrieve the final parsed value
     *
     * @return the JSON::Value
     */
    unique_ptr<Value> getValue();

    /**
     * Creates a new SAXHandler
     */
    SAXHandler();
    ~SAXHandler();

private:
    void endContext();

    vector<Value*> contexts;

    /**
     * Temporary lists used to store array content during parsing. Lists are chosen for their efficiency in dynamic
     * insertion when the total number of elements is unknown. Once the array is complete, the list content is moved into the
     * final JSON::Value(ARRAY) of the parent context.
     */
    list<list<JSON::Value>> tempArrayContentLists;

    Value* root;
    bool ownershipGiven;
};
}

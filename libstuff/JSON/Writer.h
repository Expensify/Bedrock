/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    Writer.h
 * Path:    libstuff/JSON/Writer.h
 * Pair:    Writer.cpp
 *
 * INTENT
 *   A JSON serializer: build a document incrementally via an
 *   object/array cursor over rapidjson::Writer, or convert a whole
 *   JSON::Value tree to a string in one call (compact or pretty).
 *
 * OBJECTS
 *   Writer  - class; wraps rapidjson::Writer<StringBuffer>. Instance API
 *             (startX/endX, writeMember, writeElement, getString) drives
 *             incremental construction; static serialize()/serializePretty()
 *             instead walk an existing JSON::Value tree directly.
 *
 * OUT OF PLACE
 *   Nothing. The header's direct exposure of rapidjson::Document/Value in
 *   the private writeValue overloads is called out and accepted in
 *   JSON/README.md ("public writer and SAX headers expose RapidJSON
 *   types"), not an accident.
 *
 * NAME/LOCATION FIT
 *   Fits.
 *
 * NAMING QUALITY
 *   startX/endX method pairs are consistent and self-documenting; private
 *   writeValue overloads are appropriately hidden behind the public
 *   writeMember/writeElement templates.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include "Value.h"

#include <cstdint>
#include <list>
#include <string>

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace JSON
{
class Writer
{
public:

    /**
     * Create a new Writer
     * type: The root type of the JSON document, either Writer::object or Writer::array
     */
    Writer(const ValueType type);

    /**
     * Helper function to write a json object member to a rapidjson writer that is writing an object.
     */
    template<typename T>
    void writeMember(const string& memberName, const T& value)
    {
        writeMemberName(memberName);
        writeValue(value);
    }

    /**
     * Helper function to write a json object member to a rapidjson writer that is writing an object.
     */
    template<typename T>
    void writeMember(const string& memberName, const list<T> value)
    {
        startArrayMember(memberName);
        for (typename list<T>::const_iterator valueIt = value.begin(); valueIt != value.end(); ++valueIt) {
            writeValue(*valueIt);
        }
        endArrayMember();
    }

    /**
     * Write a json array element to the current array.
     */
    template<typename T>
    void writeElement(const T& value)
    {
        writeValue(value);
    }

    /**
     * Start writing a json array as a member to the current object.
     * Example:
     * {
     *   "name": [...]
     * }
     *
     * @param name
     */
    void startArrayMember(const string& name);

    /**
     * Start writing a json object as a member to the current object.
     * Example:
     * {
     *   "name": {...}
     * }
     *
     * @param name
     */
    void startObjectMember(const string& name);

    /**
     * Start writing a json array as an element in the current array.
     * Example:
     * [
     *   [...]
     * ]
     */
    void startArrayElement();

    /**
     * Start writing a json object as an element in the current array.
     * Example:
     * [
     *   {...}
     * ]
     */
    void startObjectElement();

    /**
     * Finish writing a json array as a member to the current object.
     */
    void endArrayMember();

    /**
     * Finish writing a json object as a member to the current object.
     */
    void endObjectMember();

    /**
     * Finish writing a json array as an element in the current array.
     */
    void endArrayElement();

    /**
     * Finish writing a json object as an element in the current array.
     */
    void endObjectElement();

    /**
     * Get the json string.
     *
     * @return the json string
     */
    string getString();

    /**
     * Serialize a JSON value into a string
     *
     * @param value The object to be serialized
     * @return the json string
     */
    static string serialize(const JSON::Value& value);

    /**
     * Pretty-serialize a JSON value into a string
     *
     * @param value The object to be serialized
     * @return the human-readable json string
     */
    static string serializePretty(const JSON::Value& value);

private:
    void writeMemberName(const string& name);

    void writeValue(const string& value);

    void writeValue(const rapidjson::Document& value);

    void writeValue(const rapidjson::Value& value);

    void writeValue(const bool value);

    void writeValue(const uint64_t value);

    void writeValue(const int value);

    void writeValue(const unsigned int value);

    void writeValue(const int64_t value);

    ValueType rootType;
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer;

    template<class JSONWriter>
    static void serialize(const JSON::Value& value, JSONWriter& writer);
};
}

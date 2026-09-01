#include "Utils.h"

using namespace std;

const JSON::Value JSON::Utils::EMPTY_OBJECT = JSON::Value(JSON::OBJECT);
const JSON::Value JSON::Utils::EMPTY_ARRAY = JSON::Value(JSON::ARRAY);
const JSON::Value JSON::Utils::NULL_VALUE = JSON::Value(JSON::NIL);
const JSON::Value JSON::Utils::FALSE_VALUE = JSON::Value(false);
const JSON::Value JSON::Utils::TRUE_VALUE = JSON::Value(true);
const JSON::Value JSON::Utils::EMPTY_STRING = JSON::Value("");
const JSON::Value JSON::Utils::ZERO_INT_VALUE = JSON::Value(0);
const JSON::Value JSON::Utils::MAX_INT_VALUE = JSON::Value(INT_MAX);
const JSON::Value JSON::Utils::ZERO_FLOAT_VALUE = JSON::Value(0.0);

void JSON::Utils::recursiveReplaceJSONKeys(JSON::Value& into, const JSON::Value& from, const unordered_set<string>& keysToReplace)
{
    // We only bother merging/replacing in objects. Otherwise the "merged" item is just a copy of "from".
    if (!into.isObject() || !from.isObject()) {
        into = from;
        return;
    }

    // Iterate across everything in "from".
    for (const auto& item : JSON::ConstObjectValue(from)) {
        const string& key = item.first;
        const JSON::Value& value = item.second;

        if (keysToReplace.contains(key)) {
            into[key] = value;
            continue;
        }

        if (into.hasMember(key) && into[key].isObject() && value.isObject()) {
            recursiveReplaceJSONKeys(into[key], value, keysToReplace);
        }
    }
}

void JSON::Utils::stripOutFields(JSON::Value& object, const set<string>& keysToStrip)
{
    if (object.isObject()) {
        auto it = object.objectBegin();
        while (it != object.objectEnd()) {
            if (keysToStrip.count(it->first)) {
                it = object.erase(it);
            } else {
                stripOutFields(it->second, keysToStrip);
                it++;
            }
        }
    } else if (object.isArray()) {
        for (size_t i = 0; i < object.size(); ++i) {
            stripOutFields(object[i], keysToStrip);
        }
    }
}

void JSON::Utils::removeObjectKeysWithNullValues(JSON::Value& node)
{
    if (node.isArray()) {
        for (size_t i = 0; i < node.size(); i++) {
            removeObjectKeysWithNullValues(node[i]);
        }
        return;
    }
    if (!node.isObject()) {
        return;
    }
    const set<string> keys = node.getKeys();
    for (const string& key : keys) {
        if (!node.hasMember(key)) {
            continue;
        }
        JSON::Value& child = node[key];
        if (child.isNull()) {
            node.erase(key);
        } else {
            removeObjectKeysWithNullValues(child);
        }
    }
}

bool JSON::Utils::containAnyKeys(const JSON::Value& json, const set<string>& keys)
{
    if (json.isObject()) {
        for (auto& pair : JSON::ConstObjectValue(json)) {
            if (keys.find(pair.first) != keys.end()) {
                return true;
            }
            if (containAnyKeys(pair.second, keys)) {
                return true;
            }
        }
    } else if (json.isArray()) {
        for (auto& item : JSON::ConstArrayValue(json)) {
            if (containAnyKeys(item, keys)) {
                return true;
            }
        }
    }

    return false;
}

string JSON::Utils::getFirstString(const JSON::Value& json, const string& key)
{
    if (!json.hasMember(key)) {
        return "";
    }
    if (json[key].isString()) {
        return json[key].getString();
    }
    if (json[key].isArray() && json[key].size()) {
        if (json[key][0].isString()) {
            return json[key][0].getString();
        }
    }
    return "";
}

JSON::Value JSON::Utils::convertPathToObject(const list<string>& path, const JSON::Value& value)
{
    return convertPathToObject(path, JSON::Value(value));
}

JSON::Value JSON::Utils::convertPathToObject(const list<string>& path, JSON::Value&& value)
{
    JSON::Value result = move(value);
    for (auto riterator = path.rbegin(); riterator != path.rend(); ++riterator) {
        const string& key = *riterator;
        if (key == "$") {
            // Ignore this
            continue;
        }
        JSON::Value wrappedResult(JSON::OBJECT);
        wrappedResult[key] = move(result);
        result = move(wrappedResult);
    }
    return result;
}

set<string> JSON::Utils::getKeys(const JSON::Value& value)
{
    if (!value.isObject()) {
        throw InvalidArgument("JSON value is not an object");
    }

    set<string> keys;
    for (auto& pair : JSON::ConstObjectValue(value)) {
        keys.insert(pair.first);
    }
    return keys;
}

void JSON::Utils::addDataToInnerObject(JSON::Value& jsonObject, const string& objectKey, const string& key, const JSON::Value& extraData)
{
    if (!jsonObject.hasMember(objectKey)) {
        jsonObject[objectKey] = JSON::Value(JSON::OBJECT);
    }
    jsonObject[objectKey][key] = extraData;
}

JSON::Value JSON::Utils::parseOrDefault(const string& jsonString, const JSON::Value& defaultValue)
{
    if (jsonString.empty()) {
        return JSON::Value(defaultValue);
    }
    try {
        return JSON::Value::parse(jsonString);
    } catch (const JSON::InvalidArgument&) {
        return JSON::Value(defaultValue);
    } catch (const JSON::TypeError&) {
        return JSON::Value(defaultValue);
    }
}

JSON::Value JSON::Utils::applyJSONMergePatch(const string& existingJSON, const JSON::Value& patch)
{
    // RFC 7396 JSON merge patch: if the patch is not an object, the result is the patch (matches SQLite JSON_PATCH).
    if (!patch.isObject()) {
        return JSON::Value(patch);
    }
    if (existingJSON.empty()) {
        return JSON::Value(patch);
    }
    JSON::Value merged = parseOrDefault(existingJSON, JSON::Value(JSON::OBJECT));

    // SQLite JSON_PATCH treats a non-object document as {} when the patch is an object (RFC 7396 merge).
    if (!merged.isObject()) {
        merged = JSON::Value(JSON::OBJECT);
    }
    merged.mergeDeep(JSON::Value(patch), true);
    return merged;
}

string JSON::Utils::sanitizeJSONStringForTransport(const string& input)
{
    string output;
    output.reserve(input.size());

    size_t i = 0;
    while (i < input.size()) {
        const unsigned char c = static_cast<unsigned char>(input[i]);
        if (c <= 0x7F) {
            // Drop literal ASCII control bytes except JSON-safe whitespace (\t, \n, \r).
            if (c == 0x7F || (c <= 0x1F && c != 0x09 && c != 0x0A && c != 0x0D)) {
                i++;
                continue;
            }
            output += input[i];
            i++;
            continue;
        }

        size_t expectedLength = 0;
        unsigned char secondByteMin = 0x80;
        unsigned char secondByteMax = 0xBF;
        if (c >= 0xC2 && c <= 0xDF) {
            expectedLength = 2;
        } else if (c == 0xE0) {
            expectedLength = 3;
            secondByteMin = 0xA0;
        } else if ((c >= 0xE1 && c <= 0xEC) || (c >= 0xEE && c <= 0xEF)) {
            expectedLength = 3;
        } else if (c == 0xED) {
            expectedLength = 3;
            secondByteMax = 0x9F;
        } else if (c == 0xF0) {
            expectedLength = 4;
            secondByteMin = 0x90;
        } else if (c >= 0xF1 && c <= 0xF3) {
            expectedLength = 4;
        } else if (c == 0xF4) {
            expectedLength = 4;
            secondByteMax = 0x8F;
        } else {
            // Skip bytes that cannot begin any valid UTF-8 sequence.
            i++;
            continue;
        }

        if (i + expectedLength > input.size()) {
            // Skip incomplete multi-byte sequences at the end of the payload.
            i++;
            continue;
        }

        const unsigned char secondByte = static_cast<unsigned char>(input[i + 1]);
        if ((secondByte & 0xC0) != 0x80 || secondByte < secondByteMin || secondByte > secondByteMax) {
            // Skip sequences with illegal second-byte ranges.
            i++;
            continue;
        }

        bool hasValidContinuationBytes = true;
        for (size_t j = 2; j < expectedLength; j++) {
            if ((static_cast<unsigned char>(input[i + j]) & 0xC0) != 0x80) {
                hasValidContinuationBytes = false;
                break;
            }
        }

        if (!hasValidContinuationBytes) {
            i++;
            continue;
        }

        output.append(input, i, expectedLength);
        i += expectedLength;
    }

    return output;
}

list<string> JSON::Utils::parseJSONPath(const string& path)
{
    list<string> parts;
    string current;
    bool inQuotes = false;
    for (size_t i = 0; i < path.size(); i++) {
        const char character = path[i];
        if (inQuotes) {
            if (character == '\\' && i + 1 < path.size()) {
                // Keep the escaped character literally (e.g. \" inside a quoted key)
                current += path[++i];
            } else if (character == '"') {
                inQuotes = false;
            } else {
                current += character;
            }
        } else if (character == '"') {
            inQuotes = true;
        } else if (character == '.') {
            parts.push_back(move(current));
            current.clear();
        } else {
            current += character;
        }
    }
    parts.push_back(move(current));
    return parts;
}

#pragma once

#include "Value.h"

#include <list>
#include <set>
#include <string>
#include <unordered_set>

namespace JSON
{
class Utils
{
public:
    static const JSON::Value EMPTY_OBJECT;
    static const JSON::Value EMPTY_ARRAY;
    static const JSON::Value NULL_VALUE;
    static const JSON::Value FALSE_VALUE;
    static const JSON::Value TRUE_VALUE;
    static const JSON::Value EMPTY_STRING;
    static const JSON::Value ZERO_INT_VALUE;
    static const JSON::Value MAX_INT_VALUE;
    static const JSON::Value ZERO_FLOAT_VALUE;

    /**
     * Recursively replace keys listed in `replace` inside `into` with values from `from`.
     *
     * Commonly used for `bankAccounts.additionalData` where certain keys (e.g., apiResult, errorAttemptsCount, and
     * sometimes assetReport) must be fully replaced instead of merged to avoid mixing old and new API responses.
     * If a key in `replace` does not exist in `into`, it is inserted. When both sides have an object at the same key,
     * recursion continues so only the specified keys are replaced while other siblings are preserved.
     *
     * Mutates `into` in place; no-op when either side is non-object.
     * There is coverage in GetWithdrawalAccountsTest illustrating expected behavior.
     */
    static void recursiveReplaceJSONKeys(JSON::Value& into, const JSON::Value& from, const unordered_set<string>& keysToReplace);

    /**
     * Do a deep key search throughout the JSON object to remove any key that matches a given key.
     * Note: This modifies its first argument.
     */
    static void stripOutFields(JSON::Value& object, const set<string>& keysToStrip);

    /**
     * Recursively removes object members whose value is JSON null. Arrays are walked so nested objects are cleaned;
     * array elements that are JSON null are left unchanged. Mutates `node` in place; no-op for non-object, non-array nodes.
     */
    static void removeObjectKeysWithNullValues(JSON::Value& node);

    /**
     * Do a deep key search throughout the JSON object to see if the object contains any of the given keys.
     */
    static bool containAnyKeys(const JSON::Value& json, const set<string>& keys);

    /**
     * Return string from a string or an array (in this case, return the 1st string element)
     */
    static string getFirstString(const JSON::Value& json, const string& key);

    /**
     * Converts an sqlite's JSON path like `$.some.thing` and a value like `1` it converts it
     * into: `{"some":{"thing": 1}}` which is used to send partial onyx updates.
     */
    static JSON::Value convertPathToObject(const list<string>& path, const JSON::Value& value);
    static JSON::Value convertPathToObject(const list<string>& path, JSON::Value&& value);

    static set<string> getKeys(const JSON::Value& value);

    static void addDataToInnerObject(JSON::Value& jsonObject, const string& objectKey, const string& key, const JSON::Value& extraData);

    /**
     * Parses JSON from `jsonString`. If `jsonString` is empty or parsing fails, returns a copy of `defaultValue`.
     */
    static JSON::Value parseOrDefault(const string& jsonString, const JSON::Value& defaultValue);

    /**
     * Applies RFC 7396 JSON merge patch semantics to match SQLite's JSON_PATCH(existing, patch).
     * Uses JSON::Value::mergeDeep with SQLite merge behavior (null deletes keys; arrays replace).
     * If patch is not an object, returns a copy of patch (RFC 7396 root replacement; mergeDeep would no-op).
     * If existingJSON is empty, returns a copy of patch (same as inserting patch as a new NVP value).
     * If existingJSON parses to valid JSON that is not an object (e.g. [] or 1), the target is treated as {}
     * before merging, matching SQLite when applying an object patch to a non-object document.
     */
    static JSON::Value applyJSONMergePatch(const string& existingJSON, const JSON::Value& patch);

    /**
     * Strips ASCII control bytes (0x00-0x1F except \t/\n/\r, and 0x7F) and invalid UTF-8
     * sequences from a serialized JSON string before sending it to external APIs.
     * Example: "hello\x01\xC0\xAFworld\n" becomes "helloworld\n"; valid UTF-8 is preserved.
     */
    static string sanitizeJSONStringForTransport(const string& input);

    /**
     * Splits an SQLite JSON path like `$.potato."email.com"` into its component keys
     * (`$`, `potato`, `email.com`). Double-quoted segments are treated as a single key so
     * dots inside them are preserved, and the surrounding quotes are stripped.
     */
    static list<string> parseJSONPath(const string& path);
};
}

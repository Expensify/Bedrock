#include "Value.h"
#include "Writer.h"
#include "Parser.h"
#include "Utils.h"

#include <algorithm>

#include <libstuff/libstuff.h>

using namespace JSON;

Value::Value() : valueType(NIL)
{
}

Value::Value(const int64_t i) : valueType(INT), intValue(i), usingUnsigned(false)
{
}

Value::Value(const int32_t i) : valueType(INT), intValue(i), usingUnsigned(false)
{
}

Value::Value(const double f) : valueType(FLOAT), floatValue(f)
{
}

Value::Value(const bool b) : valueType(BOOL), boolValue(b)
{
}

Value::Value(const char* s) : valueType(STRING)
{
    if (s) {
        stringValue = s;
    } else {
        STHROW_STACK("Null pointer passed to JSON::Value constructor for C-string");
    }
}

Value::Value(const string& s) : valueType(STRING), stringValue(s)
{
}

Value::Value(string&& s) : valueType(STRING), stringValue(move(s))
{
}

Value::Value(const ValueType type) : valueType(type)
{
    if (type == OBJECT) {
        objectValue = make_shared<map<string, Value>>();
    }

    if (type == ARRAY) {
        arrayValue = make_shared<vector<Value>>();
    }
}

Value::Value(initializer_list<KeyValue> initializerList) : startTime(chrono::high_resolution_clock::now()), valueType(OBJECT)
{
    objectValue = make_shared<map<string, Value>>();
    for (auto& item : initializerList) {
        // The KeyValue has full ownership of its underlying items and it was designed for the specific purpose of being safe to move here
        objectValue->emplace(move(item.key), move(item.value));
    }
    logSlowConstructor();
}

Value::Value(const map<string, Value>& values) : startTime(chrono::high_resolution_clock::now()), valueType(OBJECT), objectValue(make_shared<map<string, Value>>(values))
{
    logSlowConstructor();
}

Value::Value(map<string, Value>&& values) : valueType(OBJECT), objectValue(make_shared<map<string, Value>>(move(values)))
{
}

Value::Value(const uint64_t i) : valueType(INT)
{
    if (i > INT64_MAX) {
        uintValue = i;
        usingUnsigned = true;
    } else {
        intValue = i;
        usingUnsigned = false;
    }
}

Value Value::object(map<string, Value>&& value)
{
    return Value(move(value));
}

Value Value::object(initializer_list<KeyValue> initializerList)
{
    return initializerList;
}

Value Value::singleItemArray(Value&& value)
{
    Value array(JSON::ARRAY);
    array.push_back(move(value));
    return array;
}

Value Value::singleEntryObject(string&& key, Value&& value)
{
    Value object(JSON::OBJECT);
    object.objectValue->emplace(move(key), move(value));
    return object;
}

// An unsigned long long just gets cast to a uint64_t. Why does this happen? In C++11 (for gcc at least) a 'ULL'
// literal type specifier becomes a __int128, and so won't find the intended (uint64_t) constructor, we add one here
// and just call the constructor we actually want.
#ifndef __APPLE__ // OS X thinks this is a duplicate of uint64_t
Value::Value(const unsigned long long i) : Value::Value(static_cast<uint64_t>(i))
{
}

#endif

Value::Value(const Value& v) : startTime(chrono::high_resolution_clock::now())
{
    switch (v.valueType) {
        /** Base types */
        case INT:
            intValue = v.intValue;
            uintValue = v.uintValue;
            valueType = INT;
            usingUnsigned = v.usingUnsigned;
            break;

        case FLOAT:
            floatValue = v.floatValue;
            valueType = FLOAT;
            break;

        case BOOL:
            boolValue = v.boolValue;
            valueType = BOOL;
            break;

        case NIL:
            valueType = NIL;
            break;

        case STRING:
            stringValue = v.stringValue;
            valueType = STRING;
            break;

        /** Compound types */
        case ARRAY:
            arrayValue = make_shared<vector<Value>>(*v.arrayValue);
            valueType = ARRAY;
            logSlowConstructor();
            break;

        case OBJECT:
            objectValue = make_shared<map<string, Value>>(*v.objectValue);
            valueType = OBJECT;
            logSlowConstructor();
            break;
    }
}

Value::Value(Value&& v) noexcept
{
    switch (v.valueType) {
        /** Base types */
        case INT:
            intValue = v.intValue;
            uintValue = v.uintValue;
            valueType = INT;
            usingUnsigned = v.usingUnsigned;
            break;

        case FLOAT:
            floatValue = v.floatValue;
            valueType = FLOAT;
            break;

        case BOOL:
            boolValue = v.boolValue;
            valueType = BOOL;
            break;

        case NIL:
            valueType = NIL;
            break;

        case STRING:
            stringValue = move(v.stringValue);
            valueType = STRING;
            break;

        /** Compound types */
        case ARRAY:
            arrayValue = move(v.arrayValue);
            valueType = ARRAY;
            break;

        case OBJECT:
            objectValue = move(v.objectValue);
            valueType = OBJECT;
            break;
    }
    v.valueType = NIL;
}

Value::~Value() = default;

Value& Value::operator=(const Value& v)
{
    if (this == &v) {
        return *this;
    }

    switch (v.valueType) {
        /** Base types */
        case INT:
            intValue = v.intValue;
            uintValue = v.uintValue;
            valueType = INT;
            usingUnsigned = v.usingUnsigned;
            objectValue.reset();
            arrayValue.reset();
            break;

        case FLOAT:
            floatValue = v.floatValue;
            valueType = FLOAT;
            objectValue.reset();
            arrayValue.reset();
            break;

        case BOOL:
            boolValue = v.boolValue;
            valueType = BOOL;
            objectValue.reset();
            arrayValue.reset();
            break;

        case NIL:
            valueType = NIL;
            objectValue.reset();
            arrayValue.reset();
            break;

        case STRING:
            stringValue = v.stringValue;
            valueType = STRING;
            objectValue.reset();
            arrayValue.reset();
            break;

        /** Compound types */
        case ARRAY:
            // The assignment is performed on a copy, rather than the original value, to avoid a circular reference in the case that we do something like:
            // json = json["key"];
            arrayValue = JSON::Value(v).arrayValue;
            valueType = ARRAY;
            objectValue.reset();
            break;

        case OBJECT:
            // The assignment is performed on a copy, rather than the original value, to avoid a circular reference in the case that we do something like:
            // json = json["key"];
            objectValue = JSON::Value(v).objectValue;
            valueType = OBJECT;
            arrayValue.reset();
            break;
    }

    return *this;
}

Value& Value::operator=(Value&& v)
{
    if (this == &v) {
        return *this;
    }

    switch (v.valueType) {
        /** Base types */
        case INT:
        case FLOAT:
        case BOOL:
        case NIL:
            // Just call the copy assignment operator for primitive types.
            operator=(v);
            break;

        case STRING:
            stringValue = move(v.stringValue);
            valueType = STRING;
            objectValue.reset();
            arrayValue.reset();
            break;

        /** Compound types */
        case ARRAY:
            arrayValue = move(v.arrayValue);
            valueType = ARRAY;
            objectValue.reset();
            v.valueType = NIL;
            v.arrayValue.reset();
            break;

        case OBJECT:
            objectValue = move(v.objectValue);
            valueType = OBJECT;
            arrayValue.reset();
            v.valueType = NIL;
            v.objectValue.reset();
            break;
    }
    return *this;
}

ValueType Value::type() const
{
    return valueType;
}

bool Value::isNull() const
{
    return valueType == JSON::NIL;
}

bool Value::isFloat() const
{
    return valueType == JSON::FLOAT;
}

bool Value::isInt() const
{
    return valueType == JSON::INT;
}

bool Value::isBool() const
{
    return valueType == JSON::BOOL;
}

bool Value::isString() const
{
    return valueType == JSON::STRING;
}

bool Value::isArray() const
{
    return valueType == JSON::ARRAY;
}

bool Value::isObject() const
{
    return valueType == JSON::OBJECT;
}

bool Value::isNegative() const
{
    return valueType == JSON::INT && !usingUnsigned && intValue < 0;
};

bool Value::isHuge() const
{
    return valueType == JSON::INT && usingUnsigned && uintValue > INT64_MAX;
};

bool Value::isNumber() const
{
    return valueType == JSON::INT || valueType == JSON::FLOAT;
};

double Value::getFloat() const
{
    if (valueType == FLOAT) {
        return floatValue;
    }
    if (valueType == INT) {
        return (double) getInt();
    }
    SLogStackTrace(LOG_DEBUG);
    throw JSON::TypeError("JSON Type Error, expected INT or FLOAT actual: " + typeToName(valueType));
}

int64_t Value::getInt() const
{
    ensureType(INT);
    if (isHuge()) {
        SLogStackTrace(LOG_DEBUG);
        throw JSON::TypeError("Value is too large to be returned as an int64_t");
    }
    return usingUnsigned ? uintValue : intValue;
}

uint64_t Value::getUint() const
{
    ensureType(INT);
    if (isNegative()) {
        SLogStackTrace(LOG_DEBUG);
        throw JSON::TypeError("Value is not unsigned");
    }
    return usingUnsigned ? uintValue : intValue;
}

bool Value::getBool() const
{
    ensureType(BOOL);
    return boolValue;
}

bool Value::getBoolFromBinaryIntOrBool() const
{
    if (isBool()) {
        return boolValue;
    }

    if (isInt()) {
        if (usingUnsigned) {
            if (uintValue == 0) {
                return false;
            }
            if (uintValue == 1) {
                return true;
            }
            SDEBUG("JSON error for value: " << this->serialize());
            SLogStackTrace(LOG_DEBUG);
            throw TypeError("JSON Type Error, expected binary integer (0 or 1) but got: " + to_string(uintValue) + " method: 'getBoolFromBinaryIntOrBool'");
        }

        if (intValue == 0) {
            return false;
        }
        if (intValue == 1) {
            return true;
        }
        SDEBUG("JSON error for value: " << this->serialize());
        SLogStackTrace(LOG_DEBUG);
        throw TypeError("JSON Type Error, expected binary integer (0 or 1) but got: " + to_string(intValue) + " method: 'getBoolFromBinaryIntOrBool'");
    }

    SDEBUG("JSON error for value: " << this->serialize());
    SLogStackTrace(LOG_DEBUG);
    throw TypeError("JSON Type Error, expected: 'bool' or 'int' actual: '" + typeToName(valueType) + "' method: 'getBoolFromBinaryIntOrBool'");
}

const string& Value::getString() const
{
    ensureType(STRING);
    return stringValue;
}

Value& Value::operator[](const string& key) &
{
    try {
        ensureType(OBJECT);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " key: '" + key + "' method: 'operator[] &'");
    }

    return (*objectValue)[key];
}

Value Value::operator[](const string& key) &&
{
    try {
        ensureType(OBJECT);
    } catch (JSON::TypeError& e) {
        throw JSON::TypeError(string(e.what()) + " key: '" + key + "' method: 'operator[] &&'");
    }

    return move((*objectValue)[key]);
}

const Value& Value::operator[](const string& key) const&
{
    try {
        ensureType(OBJECT);
    } catch (JSON::TypeError& e) {
        throw JSON::TypeError(string(e.what()) + " key: '" + key + "' method: 'operator[] const&'");
    }

    try {
        return objectValue->at(key);
    } catch (const out_of_range&) {
        throw NotFound("JSON Error, key not found - '" + key + "' method: 'operator[] const&'");
    }
}

Value& Value::operator[](size_t i) &
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " index: '" + to_string(i) + "' method: 'operator[] &'");
    }
    try {
        return arrayValue->at(i);
    } catch (const out_of_range& e) {
        throw NotFound(string(e.what()) + " index: '" + to_string(i) + "' method: 'operator[] &'");
    }
}

Value Value::operator[](size_t i) &&
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " index: '" + to_string(i) + "' method: 'operator[] &&'");
    }
    try {
        return move(arrayValue->at(i));
    } catch (const out_of_range& e) {
        throw NotFound(string(e.what()) + " index: '" + to_string(i) + "' method: 'operator[] &&'");
    }
}

const Value& Value::operator[](size_t i) const&
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " index: '" + to_string(i) + "' method: 'operator[] const&'");
    }
    try {
        return arrayValue->at(i);
    } catch (const out_of_range& e) {
        throw NotFound(string(e.what()) + " index: '" + to_string(i) + "' method: 'operator[] const&'");
    }
}

void Value::push_back(const Value& v)
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'push_back' (copy)");
    }
    arrayValue->push_back(v);
}

void Value::push_back(Value&& v)
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'push_back' (move)");
    }
    arrayValue->push_back(move(v));
}

void Value::push_back(const string& s)
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'push_back' (string copy)");
    }
    arrayValue->emplace_back(s);
}

void Value::push_back(string&& s)
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'push_back' (string move)");
    }
    arrayValue->emplace_back(move(s));
}

void Value::push_back(const char* s)
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'push_back' (c-string)");
    }
    arrayValue->emplace_back(s ? string(s) : string(""));
}

void Value::push_back(double n)
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'push_back' (double)");
    }
    arrayValue->emplace_back(n);
}

void Value::push_back(bool b)
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'push_back' (bool)");
    }
    arrayValue->emplace_back(b);
}

pair<map<string, Value>::iterator, bool> Value::emplace(const string& key, Value&& v)
{
    try {
        ensureType(OBJECT);
    } catch (JSON::TypeError& e) {
        throw JSON::TypeError(string(e.what()) + " method: 'emplace' (move)");
    }
    return objectValue->emplace(key, move(v));
}

Value& Value::back()
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'back'");
    }
    if (arrayValue->empty()) {
        SLogStackTrace(LOG_DEBUG);
        throw NotFound("Cannot call back() on an empty JSON::Value array");
    }
    return arrayValue->back();
}

const Value& Value::back() const
{
    try {
        ensureType(ARRAY);
    } catch (const TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'const back'");
    }
    if (arrayValue->empty()) {
        SLogStackTrace(LOG_DEBUG);
        throw NotFound("Cannot call back() on an empty JSON::Value array");
    }
    return arrayValue->back();
}

string Value::extractStringWithDefault(const string& key, const string& defaultString)
{
    if (!isObject()) {
        return defaultString;
    }

    auto it = objectValue->find(key);
    if (it != objectValue->end() && it->second.isString()) {
        return move(objectValue->extract(it).mapped().stringValue);
    }

    return defaultString;
}

void Value::extractTo(map<string, JSON::Value>::iterator it, JSON::Value& v)
{
    ensureType(OBJECT);
    v.ensureType(OBJECT);
    v.objectValue->insert(objectValue->extract(it));
}

vector<JSON::Value>::iterator Value::erase(vector<JSON::Value>::iterator it)
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'erase' (iterator)");
    }
    return arrayValue->erase(it);
}

void Value::erase(const string& key)
{
    ensureType(OBJECT);
    objectValue->erase(key);
}

map<string, JSON::Value>::iterator Value::erase(map<string, JSON::Value>::iterator it)
{
    ensureType(OBJECT);
    return objectValue->erase(it);
}

pair<map<string, Value>::iterator, bool> Value::insert(const pair<string, Value>& v)
{
    ensureType(OBJECT);
    return objectValue->insert(v);
}

void Value::merge(Value&& v)
{
    ensureType(OBJECT);
    if (v.type() != OBJECT) {
        SINFO("Trying to merge something that is not object, type: " << v.typeToName(v.type()));
        SLogStackTrace(LOG_INFO);
        return;
    }

    if (objectValue == v.objectValue) {
        return;
    }

    // map::merge doesn't replace existing keys, so we have to merge first v.objectValue into an empty map,
    // and then merge this->objectValue to produce the right result
    map<string, JSON::Value> mergeResult;
    mergeResult.merge(*v.objectValue);
    mergeResult.merge(*objectValue);
    *objectValue = move(mergeResult);
}

void Value::merge(const Value& v)
{
    this->merge(Value(v));
}

void Value::mergeDeep(const Value& v, bool useSQLiteMergeBehavior)
{
    mergeDeep(Value(v), useSQLiteMergeBehavior);
}

void Value::mergeDeep(Value&& v, bool useSQLiteMergeBehavior)
{
    ensureType(OBJECT);
    if (v.type() != OBJECT) {
        SINFO("Trying to merge something that is not object, type: " << v.typeToName(v.type()));
        SLogStackTrace(LOG_INFO);
        return;
    }

    if (objectValue == v.objectValue) {
        return;
    }

    const auto mergeStartTime = chrono::high_resolution_clock::now();
    for (auto& it : *v.objectValue) {
        const string& key = it.first;
        Value& patchValue = it.second;

        // RFC 7386: If patch value is null, delete the key
        if (useSQLiteMergeBehavior && patchValue.isNull()) {
            objectValue->erase(key);
            continue;
        }

        // Check if the current key exists in the object
        auto targetIt = objectValue->find(key);
        if (targetIt != objectValue->end()) {
            Value& targetValue = targetIt->second;

            // If both are objects, merge them deeply
            if (targetValue.isObject() && patchValue.isObject()) {
                targetValue.mergeDeep(move(patchValue), useSQLiteMergeBehavior);
            }
            // If both are arrays, concatenate them (or replace if using SQLite behavior)
            else if (targetValue.isArray() && patchValue.isArray()) {
                if (useSQLiteMergeBehavior) {
                    // RFC 7386: Replace arrays instead of concatenating
                    targetValue = move(patchValue);
                } else {
                    shared_ptr<vector<JSON::Value>> targetArray = targetValue.arrayValue;
                    shared_ptr<vector<JSON::Value>> sourceArray = patchValue.arrayValue;
                    move(begin(*sourceArray), end(*sourceArray), back_inserter(*targetArray));
                }
            }
            // Otherwise, overwrite the value
            else {
                targetValue = move(patchValue);
            }
        } else {
            // If the key doesn't exist, just add it
            (*objectValue)[key] = move(patchValue);
        }
    }
    const auto durationMS = chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now() - mergeStartTime).count();
    if (durationMS <= 20) {
        SDEBUG("[timing] Value::mergeDeep(Value&& v) took " << durationMS << " ms.");
    } else {
        SHMMM("[timing] Value::mergeDeep(Value&& v) took " << durationMS << " ms.");
        SLogStackTrace(LOG_INFO);
    }
}

size_t Value::size() const
{
    if (isArray()) {
        return arrayValue->size();
    }
    if (isObject()) {
        return objectValue->size();
    }
    SLogStackTrace(LOG_DEBUG);
    SDEBUG("JSON error for value: " << this->serialize());
    throw TypeError("JSON Type Error, expected: 'object' or 'array' actual: '" + typeToName(valueType) + "' method: 'size'");
}

void Value::shallowCopy(const Value& v)
{
    // We only check for a direct self-reference here. Ideally we would walk the full JSON graph to reject
    // any circular reference, but that is too expensive to the point that it almost invalidates the idea of the shallow copy,
    // so we count on callers to use shallowCopy correctly.
    if (this == &v) {
        STHROW("500 Circular reference detected");
    }

    if (valueType != NIL && v.valueType != valueType) {
        STHROW("500 Change the type of a wrapper in shallow copy is not allowed");
    }

    objectValue.reset();
    arrayValue.reset();
    stringValue.clear();
    floatValue = 0;
    intValue = 0;
    uintValue = 0;
    boolValue = false;
    usingUnsigned = false;
    valueType = v.valueType;

    switch (v.valueType) {
        case INT:
            intValue = v.intValue;
            uintValue = v.uintValue;
            usingUnsigned = v.usingUnsigned;
            break;

        case FLOAT:
            floatValue = v.floatValue;
            break;

        case BOOL:
            boolValue = v.boolValue;
            break;

        case STRING:
            stringValue = v.stringValue;
            break;

        case OBJECT:
            objectValue = v.objectValue;
            break;

        case ARRAY:
            arrayValue = v.arrayValue;
            break;

        case NIL:
            break;
    }
}

void Value::shallowCopy(const string& key, const Value& v)
{
    ensureType(OBJECT);
    (*objectValue)[key].shallowCopy(v);
}

bool Value::hasMember(const string& key) const
{
    try {
        ensureType(OBJECT);
    } catch (TypeError& e) {
        SLogStackTrace(LOG_DEBUG);
        throw TypeError(string(e.what()) + " key: '" + key + "' method: 'hasMember'");
    }
    return objectValue->find(key) != objectValue->end();
}

bool Value::hasIndex(size_t index) const
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        SLogStackTrace(LOG_DEBUG);
        throw TypeError(string(e.what()) + " index: '" + to_string(index) + "' method: 'hasIndex'");
    }
    return index >= 0 && index < arrayValue->size();
}

bool Value::getBoolMemberWithDefault(const string& key, const bool defaultValue) const
{
    if (!isObject()) {
        return defaultValue;
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return defaultValue;
    }
    if (it->second.isBool()) {
        return it->second.getBool();
    }
    return defaultValue;
}

string Value::getStringMemberWithDefault(const string& key, string&& defaultValue) &&
{
    return static_cast<const JSON::Value*>(this)->getStringMemberWithDefault(key, move(defaultValue));
}

string Value::getStringMemberWithDefault(const string& key, string&& defaultValue) &
{
    return static_cast<const JSON::Value*>(this)->getStringMemberWithDefault(key, move(defaultValue));
}

string Value::getStringMemberWithDefault(const string& key) &&
{
    return static_cast<const JSON::Value*>(this)->getStringMemberWithDefault(key, "");
}

string Value::getStringMemberWithDefault(const string& key, string&& defaultValue) const&
{
    if (!isObject()) {
        return move(defaultValue);
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return move(defaultValue);
    }
    if (it->second.isString()) {
        return it->second.getString();
    }
    return move(defaultValue);
}

string Value::getStringMemberWithDefault(const string& key, const string& defaultValue) &&
{
    return static_cast<const JSON::Value*>(this)->getStringMemberWithDefault(key, defaultValue);
}

string Value::getStringMemberWithDefault(const string& key, const string& defaultValue) &
{
    return static_cast<const JSON::Value*>(this)->getStringMemberWithDefault(key, defaultValue);
}

string Value::getStringMemberWithDefault(const string& key) &
{
    return getStringMemberWithDefault(key, JSON::Utils::EMPTY_STRING.getString());
}

const string& Value::getStringMemberWithDefault(const string& key) const &
{
    return getStringMemberWithDefault(key, JSON::Utils::EMPTY_STRING.getString());
}

const string& Value::getStringMemberWithDefault(const string& key, const string& defaultValue) const &
{
    if (!isObject()) {
        return defaultValue;
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return defaultValue;
    }
    if (it->second.isString()) {
        return it->second.getString();
    }
    return defaultValue;
}

const int64_t Value::getIntMemberWithDefault(const string& key, const int64_t defaultValue) const
{
    if (!isObject()) {
        return defaultValue;
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return defaultValue;
    }
    if (it->second.isInt()) {
        return it->second.getInt();
    }

    return defaultValue;
}

const double Value::getFloatMemberWithDefault(const string& key, const double defaultValue) const
{
    if (!isObject()) {
        return defaultValue;
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return defaultValue;
    }
    if (it->second.isFloat()) {
        return it->second.getFloat();
    } else if (it->second.isInt()) {
        if (it->second.isHuge()) {
            return (double) uintValue;
        } else {
            return (double) it->second.getInt();
        }
    }

    return defaultValue;
}

const double Value::getNumericMemberWithDefault(const string& key, const double defaultValue) const
{
    if (!isObject()) {
        return defaultValue;
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return defaultValue;
    }
    if (it->second.isFloat()) {
        return it->second.getFloat();
    }
    if (it->second.isInt()) {
        if (it->second.isHuge()) {
            return static_cast<double>(uintValue);
        }
        return static_cast<double>(it->second.getInt());
    }
    if (it->second.isString()) {
        return SToFloat(it->second.getString());
    }
    return defaultValue;
}

set<string> Value::getValueAsSet(const list<string>& path, const Value& defaultValue) const
{
    const JSON::Value& value = getValueAtPath(path, defaultValue);
    set<string> result;
    if (value.isArray()) {
        result.insert(value.arrayBegin(), value.arrayEnd());
    } else if (value.isString() && value.getString().size()) {
        result.insert(value.getString());
    }

    return result;
}

set<int64_t> Value::getValueAsIntSet(const list<string>& path, const Value& defaultValue) const
{
    set<int64_t> result;
    const JSON::Value& value = getValueAtPath(path, defaultValue);

    if (value.isArray()) {
        for (const auto& item : JSON::ConstArrayValue(value)) {
            if (item.isInt()) {
                result.insert(item.getInt());
            }
        }
    } else if (value.isInt()) {
        result.insert(value.getInt());
    }

    return result;
}

bool Value::hasElement(const JSON::Value& value) const
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        SLogStackTrace(LOG_DEBUG);
        throw TypeError(string(e.what()) + " method: 'hasElement'");
    }
    return find(arrayValue->begin(), arrayValue->end(), value) != arrayValue->end();
}

const JSON::Value& Value::getMemberWithDefault(const string& key) const&
{
    return getMemberWithDefault(key, JSON::Utils::NULL_VALUE);
}

JSON::Value Value::getMemberWithDefault(const string& key) &&
{
    return getMemberWithDefault(key);
}

JSON::Value Value::getMemberWithDefault(const string& key, const JSON::Value& defaultValue) &&
{
    if (!isObject()) {
        return defaultValue;
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return defaultValue;
    }

    // Since *this is a temporary, we can safely move its member out.
    return move(it->second);
}

const JSON::Value& Value::getMemberWithDefault(const string& key, const JSON::Value& defaultValue) const&
{
    if (!isObject()) {
        return defaultValue;
    }
    auto it = objectValue->find(key);
    if (it == objectValue->end()) {
        return defaultValue;
    }
    return it->second;
}

const Value& Value::getValueAtPath(const list<string>& path, const Value& defaultValue) const&
{
    try {
        const Value* current = this;
        for (const string& step : path) {
            if (!current->isObject()) {
                // Trying to use the [] operator will result in a thrown exception, so just return the default here.
                return defaultValue;
            }
            current = &(current->operator[](step));
        }

        // If the value at the end of the path is null and the caller wants a non-null default, return the default.
        if (current->isNull() && !defaultValue.isNull()) {
            return defaultValue;
        } else {
            // If we got all the way to the end with no errors, return the current value.
            return *current;
        }
    } catch (const Error& e) {
    }

    // This will happen in either error case above.
    return defaultValue;
}

Value Value::getValueAtPath(const list<string>& path, const Value& defaultValue) &&
{
    try {
        Value* current = this;
        for (const string& step : path) {
            if (!current->isObject()) {
                // Trying to use the [] operator will result in a thrown exception, so just return the default here.
                return defaultValue;
            }
            current = &(current->operator[](step));
        }

        // If the value at the end of the path is null and the caller wants a non-null default, return the default.
        if (current->isNull() && !defaultValue.isNull()) {
            return defaultValue;
        } else {
            // If we got all the way to the end with no errors, return the current value.
            return move(*current);
        }
    } catch (const Error& e) {
    }

    // This will happen in either error case above.
    return defaultValue;
}

const Value& Value::getValueAtPath(const list<string>& path) const&
{
    return getValueAtPath(path, JSON::Utils::NULL_VALUE);
}

Value Value::getValueAtPath(const list<string>& path) &&
{
    // The `move(*this)` is so this calls the rvalue overload `getValueAtPath() &&` and not `getValueAtPath() const &`
    return move(*this).getValueAtPath(path, JSON::Utils::NULL_VALUE);
}

bool Value::operator==(const JSON::Value& value) const
{
    if (valueType != value.valueType) {
        return false;
    }
    switch (valueType) {
        /** Base types */
        case INT:
            if (isHuge() != value.isHuge()) {
                return false;
            }
            return isHuge() ? getUint() == value.getUint() : getInt() == value.getInt();

        case FLOAT:
            return floatValue == value.floatValue;

        case BOOL:
            return boolValue == value.boolValue;

        case NIL:
            return true;

        case STRING:
            return stringValue == value.stringValue;

        /** Compound types */
        case ARRAY:
            return *arrayValue == *value.arrayValue;

        case OBJECT:
            return *objectValue == *value.objectValue;

        default:
            return false;
    }
}

bool Value::operator!=(const JSON::Value& value) const
{
    return !(*this == value);
}

bool Value::operator<(const JSON::Value& v) const
{
    // Just compare string representations. This is not the fastest but it's easy.
    return this->serialize() < v.serialize();
}

vector<Value>::iterator Value::arrayBegin()
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'arrayBegin'");
    }
    return arrayValue->begin();
}

vector<Value>::iterator Value::arrayEnd()
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'arrayEnd'");
    }
    return arrayValue->end();
}

vector<Value>::const_iterator Value::arrayBegin() const
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'const arrayBegin'");
    }
    ensureType(ARRAY);
    return arrayValue->begin();
}

vector<Value>::const_iterator Value::arrayEnd() const
{
    try {
        ensureType(ARRAY);
    } catch (TypeError& e) {
        throw TypeError(string(e.what()) + " method: 'const arrayEnd'");
    }
    return arrayValue->end();
}

map<string, Value>::iterator Value::objectBegin()
{
    ensureType(OBJECT);
    return objectValue->begin();
}

map<string, Value>::iterator Value::objectEnd()
{
    ensureType(OBJECT);
    return objectValue->end();
}

map<string, Value>::const_iterator Value::objectBegin() const
{
    ensureType(OBJECT);
    return objectValue->begin();
}

map<string, Value>::const_iterator Value::objectEnd() const
{
    ensureType(OBJECT);
    return objectValue->end();
}

set<string> Value::getKeys() const
{
    ensureType(OBJECT);
    set<string> keys;
    for (const auto& item : *objectValue) {
        keys.insert(item.first);
    }
    return keys;
}

string Value::typeToName(const ValueType value) const
{
    auto item = static_cast<underlying_type_t<ValueType>>(value);
    if (item < 0 || item > static_cast<underlying_type_t<ValueType>>(NIL)) {
        return "invalid type";
    }
    static const char* typeNames[] = {"int", "float", "bool", "string", "object", "array", "nil"};
    return typeNames[static_cast<size_t>(item)];
}

string Value::getTypeName() const
{
    return typeToName(valueType);
}

void Value::ensureType(const ValueType desired) const
{
    if (valueType != desired) {
        SDEBUG("JSON error for value: " << this->serialize());
        if (logStackTraceOnEnsureTypeFailure) {
            SLogStackTrace();
        }
        throw JSON::TypeError("JSON Type Error, expected: '" + typeToName(desired) + "' actual: '" + typeToName(valueType) + "'");
    }
}

void Value::logSlowConstructor()
{
    const auto endTime = chrono::high_resolution_clock::now();
    const auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime).count();
    if (duration > 20) {
        SHMMM("[timing] Slow JSON constructor took " << duration << " ms.");
        SLogStackTrace(LOG_INFO);
    }
}

string Value::serialize() const
{
    return JSON::Writer::serialize(*this);
}

string Value::serializePretty() const
{
    return JSON::Writer::serializePretty(*this);
}

Value Value::parse(const string& s)
{
    return move(*JSON::Parser::read(s));
}

Value::operator int64_t() const
{
    return getInt();
}

Value::operator uint64_t() const
{
    return getUint();
}

Value::operator bool() const
{
    return getBool();
}

Value::operator double() const
{
    return getFloat();
}

Value::operator string() const
{
    return getString();
}

void Value::arrayReserve(size_t size)
{
    try {
        ensureType(ARRAY);
    } catch (JSON::TypeError& e) {
        throw JSON::TypeError(string(e.what()) + " method: 'arrayReserve'");
    }
    arrayValue->reserve(size);
}

string Value::getPathString(const list<string>& path)
{
    string pathString = "";
    for (const string& part : path) {
        pathString += "[\"" + part + "\"]";
    }
    return pathString;
}

vector<string> Value::getAsStringArray() const
{
    vector<string> values;
    if (isString()) {
        values.push_back(getString());
    } else if (isNumber()) {
        values.push_back(to_string(getInt()));
    } else if (isArray()) {
        for (const auto& v : JSON::ConstArrayValue(*this)) {
            if (v.isString()) {
                values.push_back(v.getString());
            } else if (v.isNumber()) {
                values.push_back(to_string(v.getInt()));
            }
        }
    }
    return values;
}

void Value::arrayFilter(const function<bool(const JSON::Value&, const size_t)>& validationFunction)
{
    ensureType(ARRAY);
    int validIndex = 0;
    for (size_t i = 0; i < arrayValue->size(); ++i) {
        if (validationFunction((*arrayValue)[i], i)) {
            if (validIndex != i) {
                // Move the valid element to the left-most invalid position
                (*arrayValue)[validIndex] = move((*arrayValue)[i]);
            }
            validIndex++;
        }
    }

    // Shrink the array to the size of valid elements
    arrayValue->resize(validIndex);
}

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <libstuff/libstuff.h>
using namespace std;

namespace JSON
{
inline thread_local atomic<bool> logStackTraceOnEnsureTypeFailure{false};

// Our custom JSON exception class hierarchy looks like this:
// exception
//     JSON::Error
//         JSON::TypeError
//         JSON::NotFound
//         JSON::InvalidArgument

class Error : public SException {
private:
    // Inherit constructors from SException so we can get the stack trace
    using SException::SException;

public:
    // Passing "true" to be able to get a stack trace.
    // Passing "false" to not log where we're throwing the exception from, as it is super noisy in many places. If you're considering
    // to pass "true" instead, please also consider what it will take to not make it super noisy.
    explicit Error(const string& message) : SException("", 0, true, message, {}, "", false)
    {
    }
};

class TypeError : public Error {
public:
    // Inherit constructors from Error
    using Error::Error;

    // Explicit constructor for clarity
    explicit TypeError(const string& message)
        : Error(message)
    {
    }
};

class NotFound : public Error {
public:
    // Inherit constructors from Error
    using Error::Error;

    // Explicit constructor for clarity
    explicit NotFound(const string& message)
        : Error(message)
    {
    }
};

class InvalidArgument : public Error {
public:
    // Inherit constructors from Error
    using Error::Error;

    // Explicit constructor for clarity
    explicit InvalidArgument(const string& message)
        : Error(message)
    {
    }
};

// Possible JSON type of a value.
// If you update this enum you'll need to update Value::typeToName() as well
enum class ValueType : uint8_t
{
    INT, // Note that we separate int and float types, even though JSON doesn't.
    FLOAT,
    BOOL,
    STRING,
    OBJECT,
    ARRAY,
    NIL // `null` in JSON, but that's overloaded.
};
using enum ValueType;

// Forward declare so we can use it in Value
class KeyValue;

/**
 * A JSON value (node). Its type is one of ValueTypes.
 */
class Value
{
    // Allows SAXHandler to build us quickly, from a low level. Coupling++
    friend class SAXHandler;
public:
    ///// Constructors

    /**
     * Default constructor (type = NULL).
     */
    Value();

    /**
     * Copy constructor.
     *
     * @param v Another Value to *COPY*
     */
    Value(const Value& v);

    /**
     * Move constructor.
     *
     * @param v Another Value to *MOVE*
     */
    Value(Value&& v) noexcept;

    /**
     * Out-of-line so the implementation is supplied by libjson.a instead of
     * being emitted into every consumer of this header.
     */
    ~Value();

    /**
     * Constructor from int.
     *
     * @param i The int value
     */
    Value(const uint64_t i);

    /**
     * Constructor from int.
     *
     * @param i The int value
     */
    Value(const int64_t i);

    /*
     * Constructs an object using an initializer_list of key/value pairs. This was added to be able to time the initialization of the `map`,
     * and then find slow cases we can improve.
     */
    Value(initializer_list<KeyValue> initializerList);

    /**
     * Construct an object from a map of strings to values.
     */
    Value(const map<string, Value>& values);
    Value(map<string, Value>&& values);

    // Convenience factory methods for forcing an object. Can be used when otherwise the constructor call would be ambiguous.
    static Value object(map<string, Value>&& value);
    static Value object(initializer_list<KeyValue> initializerList);

    // Similar, but for when you want an array with a single item.
    static Value singleItemArray(Value&& value);

    // Similar, but for when you want an object with a single entry.
    static Value singleEntryObject(string&& key, Value&& value);

    // And a constructor for maps of strings to anything else.
    template<typename T>
    explicit Value(const map<string, T>& values) : startTime(chrono::high_resolution_clock::now()), valueType(OBJECT), objectValue(make_shared<map<string, Value>>())
    {
        for (typename map<string, T>::const_iterator valueIt = values.begin(); valueIt != values.end(); ++valueIt) {
            objectValue->emplace(make_pair(valueIt->first, Value(valueIt->second)));
        }
        logSlowConstructor();
    }

    /**
     *  Construct an object from a map of int64_t to anything else. Note that the keys are converted to strings since JSON values must be keyed by strings.
     */
    template<typename T>
    explicit Value(const map<int64_t, T>& values) : startTime(chrono::high_resolution_clock::now()), valueType(OBJECT), objectValue(make_shared<map<string, Value>>())
    {
        for (typename map<int64_t, T>::const_iterator valueIt = values.begin(); valueIt != values.end(); ++valueIt) {
            objectValue->emplace(make_pair(to_string(valueIt->first), Value(valueIt->second)));
        }
        logSlowConstructor();
    }

    /**
     * Constructor from int.
     *
     * @param i The int value
     *
     * NOTE: OS X thinks this is redundant.
     */
#ifndef __APPLE__
    Value(const unsigned long long i);
#endif

    /**
     * Constructor from int.
     *
     * @param int The int value
     */
    Value(const int32_t i);

    /**
     * Constructor from float.
     *
     * @param f The float value
     */
    Value(const double f);

    /**
     * Constructor from bool.
     *
     * @param b The bool value
     */
    Value(const bool b);

    /**
     * Constructor from pointer to char (C-string).
     *
     * @param s The string value to *COPY*
     */
    Value(const char* s);

    /**
     * Constructor from string
     *
     * @param s The string value to *COPY*
     */
    Value(const string& s);

    /**
     * Constructor from string
     *
     * @param s The string value to *MOVE*
     */
    Value(string&& s);

    /**
     * Array constructor from a list
     *
     * @param a The list of values to *COPY*
     */
    template<class T>
    explicit Value(const list<T>& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        for (typename list<T>::const_iterator valueIt = a.begin(); valueIt != a.end(); ++valueIt) {
            push_back(*valueIt);
        }
    }

    /**
     * Array constructor from a list
     *
     * @param a The list of JSON::Value to *MOVE*
     */
    explicit Value(list<JSON::Value>&& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        move(begin(a), end(a), back_inserter(*arrayValue));
    }

    /**
     * Array constructor from a list
     *
     * @param a The list of values to *MOVE*
     */
    template<class T>
    explicit Value(list<T>&& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        for (typename list<T>::const_iterator valueIt = a.begin(); valueIt != a.end(); ++valueIt) {
            push_back(move(*valueIt));
        }
    }

    /**
     * Array constructor from a set
     *
     * @param a The list of values to *COPY*
     */
    template<class T>
    explicit Value(const set<T>& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        for (typename set<T>::const_iterator valueIt = a.begin(); valueIt != a.end(); ++valueIt) {
            push_back(*valueIt);
        }
    }

    /**
     * Array constructor from a set
     *
     * @param a The set of JSON::Value to *MOVE*
     */
    explicit Value(set<JSON::Value>&& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        move(begin(a), end(a), back_inserter(*arrayValue));
    }

    /**
     * Array constructor from a set
     *
     * @param a The set of values to *MOVE*
     */
    template<class T>
    explicit Value(set<T>&& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        for (typename set<T>::const_iterator valueIt = a.begin(); valueIt != a.end(); ++valueIt) {
            push_back(move(*valueIt));
        }
    }

    /**
     * Array constructor from a vector
     *
     * @param a The list of values to *COPY*
     */
    template<class T>
    explicit Value(const vector<T>& a) : startTime(chrono::high_resolution_clock::now()), valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        for (typename vector<T>::const_iterator valueIt = a.begin(); valueIt != a.end(); ++valueIt) {
            push_back(*valueIt);
        }
        logSlowConstructor();
    }

    /**
     * Array constructor from a vector
     *
     * @param a The list of JSON::Value to *MOVE*
     */
    explicit Value(vector<JSON::Value>&& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        move(begin(a), end(a), back_inserter(*arrayValue));
    }

    /**
     * Array constructor from a vector
     *
     * @param a The list of values to *MOVE*
     */
    template<class T>
    explicit Value(vector<T>&& a) : valueType(ARRAY)
    {
        arrayValue = make_shared<vector<Value>>();
        arrayValue->reserve(a.size());
        for (typename vector<T>::const_iterator valueIt = a.begin(); valueIt != a.end(); ++valueIt) {
            push_back(move(*valueIt));
        }
    }

    // Template cast operator for converting to lists. Can be used for int/string/bool types.
    template<typename T>
    explicit operator list<T>() const
    {
        ensureType(ARRAY);
        list<T> result;
        for (const auto& item : *arrayValue) {
            result.push_back((T) item);
        }

        return result;
    }

    // Template cast operator for converting to sets. Can be used for int/string/bool types.
    template<typename T>
    explicit operator set<T>() const
    {
        ensureType(ARRAY);
        set<T> result;
        for (const auto& item : *arrayValue) {
            result.insert((T) item);
        }

        return result;
    }

    // Template cast operator for converting to maps. Can be used for int/string/bool types.
    template<typename T>
    explicit operator map<string, T>() const
    {
        ensureType(OBJECT);
        map<string, T> result;
        for (const auto& item : *objectValue) {
            result.emplace(make_pair(item.first, (T) item.second));
        }

        return result;
    }

    // Template cast operator for converting to maps with integer as keys. Can be used for int/string/bool types.
    template<typename T>
    explicit operator map<int64_t, T>() const
    {
        ensureType(OBJECT);
        map<int64_t, T> result;
        for (const auto& item : *objectValue) {
            int64_t key;
            try {
                key = stoll(item.first);
            } catch (const exception& e) {
                throw InvalidArgument("Invalid key format for int64_t conversion: " + item.first);
            }
            result.emplace(make_pair(key, (T) item.second));
        }

        return result;
    }

    // Fills in a value with the appropriate type as decided by the type of the value being filled.
    // I.e.,
    // string s;
    // JSON::Value v(/* some object here*/);
    // v["someStringValue"].fill(s);
    //
    // The useful part of this is that you don't need to specify the type anywhere.
    template<typename T>
    void fill(T& toFill)
    {
        toFill = (T) (*this);
    }

    /**
     * Type-specifying constructor
     *
     * @param type the type of this value node.
     */
    Value(const ValueType type);

    ///// Operators

    // Cast to simple types.
    explicit operator int64_t() const;
    explicit operator uint64_t() const;
    explicit operator bool() const;
    explicit operator double() const;
    explicit operator string() const;

    /**
     * Subscript operator, access an element by key.
     *
     * @param key key of the object to access
     */
    Value& operator[](const string& key) &;

    /**
     * Subscript operator, access an element from a r-value by key.
     *
     * @param key key of the object to access
     */
    Value operator[](const string& key) &&;

    /** Subscript operator, access an element by key.
     *
     * @param key key of the object to access
     */
    const Value& operator[](const string& key) const&;

    /** Subscript operator, access an element from a r-value by index.
     *
     * @param i index of the element to access
     */
    Value operator[](size_t i) &&;

    /** Subscript operator, access an element by index.
     *
     * @param i index of the element to access
     */
    Value& operator[](size_t i) &;

    /** Subscript operator, access an element by index.
     *
     * @param i index of the element to access
     */
    const Value& operator[](size_t i) const&;

    /**
     * Implement `emplace` for Object values.
     */
    pair<map<string, Value>::iterator, bool> emplace(const string& key, Value&& v);

    /**
     * Assignment operator.
     *
     * @param v another Value to *COPY*
     */
    Value& operator=(const Value& v);

    /**
     * Move assignment operator.
     *
     * @param v another Value to *MOVE*
     */
    Value& operator=(Value&& v);

    /**
     * Equals operator.
     *
     * @param v A value to compare equality to.
     */
    bool operator==(const JSON::Value& v) const;

    /**
     * Non-equals operator.
     *
     * @param v A value to compare inequality to.
     */
    bool operator!=(const JSON::Value& v) const;

    bool operator<(const JSON::Value& v) const;

    ///// Modifiers

    vector<JSON::Value>::iterator erase(vector<JSON::Value>::iterator it);
    void erase(const string& key);
    map<string, JSON::Value>::iterator erase(map<string, JSON::Value>::iterator it);

    /**
     * Returns a string, or a default if no member is found for the given key, or the member at the given key is not
     * string, and then deletes the original object member.
     */
    string extractStringWithDefault(const string& key, const string& defaultValue = "");

    /**
     * Extracts and moves a node to target object.
     * Note: Extracting a node invalidates its iterator.
     */
    void extractTo(map<string, JSON::Value>::iterator it, JSON::Value& v);

    /**
     * Inserts an element in the array.
     *
     * @param n (a pointer to) the value to add
     */
    void push_back(const Value& n);

    /**
     * Allow push by move.
     */
    void push_back(Value&& n);

    /**
     * Allow pushing string values directly into arrays, by copy, move, and from pointer.
     */
    void push_back(const string& s);
    void push_back(string&& s);
    void push_back(const char* s);
    void push_back(double n);
    void push_back(bool b);

    template<typename I, typename = enable_if_t<is_integral<I>::value>>
    void push_back(I n)
    {
        try {
            ensureType(ARRAY);
        } catch (const TypeError& e) {
            throw TypeError(string(e.what()) + " method: 'push_back' (integral)");
        }

        if constexpr (is_unsigned<I>::value) {
            arrayValue->emplace_back(static_cast<uint64_t>(n));
        } else {
            arrayValue->emplace_back(static_cast<int64_t>(n));
        }
    }

    /**
     * @return the last element from a JSON array
     */
    Value& back();

    /**
     * @return the last element from a JSON array
     */
    const Value& back() const;

    /**
     * Inserts a field in the object.
     *
     * @param v pair <key, value> to insert
     * @return an iterator to the inserted object
     */
    pair<map<string, Value>::iterator, bool> insert(const pair<string, Value>& v);

    /**
     * Performs a shallow merge. only works with objects. Updates this* inline.
     *
     * @param v JSON::Value source object to merge INTO this
     */
    void merge(const Value& v);

    /**
     * Performs a shallow merge. only works with objects. Updates this* inline.
     *
     * @param v JSON::Value source object to *MOVE* and merge INTO this.
     */
    void merge(Value&& v);

    /**
     * Performs a deep merge. only works with objects. Updates this* inline.
     *
     * @param v JSON::Value source object to merge INTO this
     * @param useSQLiteMergeBehavior If true, uses RFC 7386 JSON Merge Patch semantics:
     *                               - null values delete keys
     *                               - arrays are replaced instead of concatenated
     */
    void mergeDeep(const Value& v, bool useSQLiteMergeBehavior = false);

    /**
     * Performs a deep merge. only works with objects. Updates this* inline.
     *
     * @param v JSON::Value source object to *MOVE* and merge INTO this
     * @param useSQLiteMergeBehavior If true, uses RFC 7386 JSON Merge Patch semantics:
     *                               - null values delete keys
     *                               - arrays are replaced instead of concatenated
     */
    void mergeDeep(Value&& v, bool useSQLiteMergeBehavior = false);

    /**
     * If the given Value is an object or array it aliases the same backing storage, otherwise it copies the value.
     */
    void shallowCopy(const Value& v);

    /**
     * If the given Value is an object or array it aliases the same backing storage at `key`, otherwise it copies the value to the given key.
     */
    void shallowCopy(const string& key, const Value& v);

    ///// metadata

    /**
     * Returns what type of value this is.
     *
     * @return a ValueType
     */
    ValueType type() const;

    /**
     * Checks if this value is null
     *
     * @return true/false
     */
    bool isNull() const;

    /**
     * Checks if this value is a float/double
     *
     * @return true/false
     */
    bool isFloat() const;

    /**
     * Checks if this value is a fixed point type (uint64_t or int64_t).
     *
     * @return true/false
     */
    bool isInt() const;

    /**
     * Checks if this value is a negative integer therefore needs to be retrieved with getInt rather than getUint.
     *
     * @return true/false
     */
    bool isNegative() const;

    /**
     * Checks if this value is a positive integer >= INT64_MAX and therefore needs to be retrieved with getUint.
     *
     * @return true/false
     */
    bool isHuge() const;

    /**
     * Checks if this value is an INT or a FLOAT
     *
     * @return true/false
     */
    bool isNumber() const;

    /**
     * Checks if this value is a boolean
     *
     * @return true/false
     */
    bool isBool() const;

    /**
     * Checks if this value is a string
     *
     * @return true/false
     */
    bool isString() const;

    /**
     * Checks if this value is an array
     *
     * @return true/false
     */
    bool isArray() const;

    /**
     * Checks if this value is an object
     *
     * @return true/false
     */
    bool isObject() const;

    /**
     * Returns the number of elements or members of the array or object.
     *
     * @return the size
     */
    size_t size() const;

    /**
     * Checks if the object has a member
     *
     * @return true/false
     */
    bool hasMember(const string& key) const;

    /**
     * Checks if the array has a value at the index
     *
     * @return true/false
     */
    bool hasIndex(size_t index) const;

    /**
     * Returns a bool, or a default if no member is found for the given key, or the member at the given key is not
     * boolean.
     */
    bool getBoolMemberWithDefault(const string& key, const bool defaultValue = false) const;

    /**
     * Returns the string member for a given key, if none is found then a default value is returned.
     *
     * Const-qualified lvalue overloads return a const reference to avoid copies when the
     * caller holds a const JSON::Value (safe because the referenced storage won't be
     * mutated through a const object).
     */
    const string& getStringMemberWithDefault(const string& key, const string& defaultValue) const&;
    const string& getStringMemberWithDefault(const string& key) const&;

    /**
     * This overload has to return by value even if the lvalue is const to avoid returning a reference to a temporary (the moved defaultValue).
     */
    string getStringMemberWithDefault(const string& key, string&& defaultValue) const&;

    /**
     * Non-const (mutable) lvalue overloads return by value to avoid exposing an
     * internal reference that could dangle if the caller later reassigns or moves
     * the JSON::Value.
     */
    string getStringMemberWithDefault(const string& key, const string& defaultValue) &;
    string getStringMemberWithDefault(const string& key, string&& defaultValue) &;
    string getStringMemberWithDefault(const string& key) &;

    /**
     * rvalue overloads return by value to avoid returning reference that would dangle.
     */
    string getStringMemberWithDefault(const string& key, const string& defaultValue) &&;
    string getStringMemberWithDefault(const string& key, string&& defaultValue) &&;
    string getStringMemberWithDefault(const string& key) &&;

    /**
     * Returns the integer member for a given key, if none is found then a default value is returned.
     */
    const int64_t getIntMemberWithDefault(const string& key, const int64_t defaultValue = 0) const;

    /**
     * Returns the float member for a given key, if none is found then a default value is returned.
     */
    const double getFloatMemberWithDefault(const string& key, const double defaultValue = 0.0) const;

    /**
     * Returns the member as a double, coercing from FLOAT, INT, or STRING types.
     * Useful when clients may send numeric values as strings (e.g. OldDot form-encoded requests).
     */
    const double getNumericMemberWithDefault(const string& key, const double defaultValue = 0.0) const;

    /**
     * Returns the Value member for a given key, if none is found then defaultValue is returned.
     */
    const JSON::Value& getMemberWithDefault(const string& key, const JSON::Value& defaultValue) const&;
    JSON::Value getMemberWithDefault(const string& key, const JSON::Value& defaultValue) &&;

    /**
     * Returns the Value member for a given key, if none is found then NULL is returned.
     */
    const JSON::Value& getMemberWithDefault(const string& key) const&;
    JSON::Value getMemberWithDefault(const string& key) &&;

    /**
     * Delete r-value version of getMemberWithDefault to prevent accidental use which would result in a dangling reference.
     */
    const JSON::Value& getMemberWithDefault(const string& key, JSON::Value&& defaultValue) const = delete;

    /**
     * Checks if the array has a member
     *
     * @return true/false
     */
    bool hasElement(const JSON::Value& value) const;

    /**
     * Returns the JSON::Value at a certain path or returns defaultValue if it can't find it
     */
    const Value& getValueAtPath(const list<string>& path, const Value& defaultValue) const&;
    Value getValueAtPath(const list<string>& path, const Value& defaultValue) &&;

    /**
     * Returns the JSON::Value at a certain path or returns NULL if it can't find it
     */
    const Value& getValueAtPath(const list<string>& path) const&;
    Value getValueAtPath(const list<string>& path) &&;

    /**
     * Delete r-value version of getValueAtPath to prevent accidental use which would result in a dangling reference.
     */
    const Value& getValueAtPath(const list<string>& path, Value&& defaultValue) const = delete;

    /**
     * Returns the set<string> at a certain path or returns a defaultValue/null if it can't find it
     */
    set<string> getValueAsSet(const list<string>& path, const Value& defaultValue) const;

    /**
     * Returns the set<int64_t> at a certain path or returns a defaultValue/null if it can't find it
     */
    set<int64_t> getValueAsIntSet(const list<string>& path, const Value& defaultValue) const;

    /**
     * Extracts string representations of values from this JSON value.
     * - If this is a string, returns a single-element vector with the string.
     * - If this is a number, returns a single-element vector with the stringified integer.
     * - If this is an array, returns string representations of each string/number element.
     * - Otherwise returns an empty vector.
     */
    vector<string> getAsStringArray() const;

    ///// Casting

    /**
     * Returns the number as a float as long as the value is a float or an int
     *
     * @return the float
     */
    double getFloat() const;

    /**
     * Returns the value as a int
     *
     * @return the int
     */
    int64_t getInt() const;

    /**
     * Returns the value as an unsigned integer
     *
     * @return the int
     */
    uint64_t getUint() const;

    /**
     * Returns the value as a bool
     *
     * @return the bool
     */
    bool getBool() const;

    /**
     * Returns the value as a bool, accepting both bool type and binary integers (0 or 1).
     *
     * @return the bool value (false for 0, true for 1, or the bool value itself)
     * @throws TypeError if the value is null, a non-binary integer (not 0 or 1), or any other type
     */
    bool getBoolFromBinaryIntOrBool() const;

    /**
     * Returns the value as a string
     *
     * @return the string
     */
    const string& getString() const;

    ///// Iterators

    /**
     * Gets an iterator to the first member in the object
     *
     * @return a map iterator
     */
    map<string, Value>::iterator objectBegin();

    /**
     * Gets an iterator to the end member position in the object (one past the last member)
     *
     * @return a map iterator
     */
    map<string, Value>::iterator objectEnd();

    /**
     * Gets a const iterator to the first member in the object
     *
     * @return a map iterator
     */
    map<string, Value>::const_iterator objectBegin() const;

    /**
     * Gets a const iterator to the end member position in the object (one past the last member)
     *
     * @return a map iterator
     */
    map<string, Value>::const_iterator objectEnd() const;

    /**
     * Returns the keys of the object
     *
     * @return a set of strings
     */
    set<string> getKeys() const;

    /**
     * Gets an iterator to the first element in the array
     *
     * @return a vector iterator
     */
    vector<Value>::iterator arrayBegin();

    /**
     * Gets an iterator to the end element position in the array (one past the last element)
     *
     * @return a vector iterator
     */
    vector<Value>::iterator arrayEnd();

    /**
     * Gets a const iterator to the first element in the array
     *
     * @return a vector iterator
     */
    vector<Value>::const_iterator arrayBegin() const;

    /**
     * Gets a const iterator to the end element position in the array (one past the last element)
     *
     * @return a vector iterator
     */
    vector<Value>::const_iterator arrayEnd() const;

    /**
     *  Set the number of elements the JSON array can hold without requiring reallocation. See: vector::reserve()
     */
    void arrayReserve(size_t size);

    /**
     *  Remove all elements from the array for which validationFunction returns false
     */
    void arrayFilter(const function<bool(const JSON::Value&, const size_t)>& validationFunction);

    template<class InputIt>
    vector<Value>::iterator arrayInsert(vector<Value>::const_iterator pos, InputIt first, InputIt last)
    {
        return arrayValue->insert(pos, first, last);
    }

    /**
     * Return a valid JSON string which is a serialized representation of this value.
     *
     * Note: Serializing an empty string results in """", not "" as you might expect.
     */
    string serialize() const;

    /**
     * Returns a pretty-formatted serialized representation of this object.
     * This is mostly here for debugging purposes. Please don't remove it even if it's unused :)
     */
    string serializePretty() const;

    /**
     * Parse a json object from a string.
     */
    static Value parse(const string& s);

    template<class T>
    static JSON::Value fromDataStructure(const map<string, T>& data)
    {
        JSON::Value json(JSON::OBJECT);
        for (const auto& nvp : data) {
            json[nvp.first] = fromDataStructure(nvp.second);
        }

        return json;
    }

    template<class T, class U, typename = enable_if_t<is_integral<T>::value>>
    static JSON::Value fromDataStructure(const map<T, U>& data)
    {
        JSON::Value json(JSON::OBJECT);
        for (const auto& nvp : data) {
            json[to_string(nvp.first)] = fromDataStructure(nvp.second);
        }

        return json;
    }

    template<class T>
    static JSON::Value fromDataStructure(const T& data)
    {
        return JSON::Value(data);
    }

    /**
     * Returns a string representation of the path.
     * i.e. given `{"report_123", "reportID"}` returns `"[\"report_123\"][\"reportID\"]"`
     */
    static string getPathString(const list<string>& path);

    string getTypeName() const;

protected:

    /*
     * For finding slow constructors. This should stay in the top so it initialized before other instance variables.
     */
    chrono::high_resolution_clock::time_point startTime;

    ValueType valueType;

    double floatValue;
    int64_t intValue;
    uint64_t uintValue;
    bool boolValue;
    string stringValue;

    shared_ptr<map<string, Value>> objectValue;
    shared_ptr<vector<Value>> arrayValue;

    /**
     * Tracks whether we're using uintValue or intValue.
     */
    bool usingUnsigned;

private:
    // Converts a JSON::ValueType into a string version of its type's name
    string typeToName(const ValueType item) const;

    /**
     * Throws if this value isn't the given type.
     *
     * param desired The expected type
     *
     * throws TypeError
     */
    void ensureType(const ValueType desired) const;

    void logSlowConstructor();
};

// Support output stream operations.
inline ostream& operator<<(ostream& output, const Value& val)
{
    output << val.serialize();
    return output;
}

// This is a bit of a hack, because JSON::Value wasn't designed in an object-oriented fashion in the first-place,
// even though it probably should have been. This lets a caller wrap a JSON::Value in an ArrayValue or ObjectValue
// and get begin and end, for use with range based for loops and such. Validation of the types passed in here is an
// exercise for the caller.
template<typename T>
class _ArrayValue {
public:
    _ArrayValue(T& v) : _v(v)
    {
    }

    auto begin()
    {
        return _v.arrayBegin();
    }

    auto end()
    {
        return _v.arrayEnd();
    }

    auto begin() const
    {
        return _v.arrayBegin();
    }

    auto end() const
    {
        return _v.arrayEnd();
    }

    auto empty() const
    {
        return _v.size() == 0;
    }

    T& getValue()
    {
        return _v;
    }

private:
    T& _v;
};

template<typename T>
ostream& operator<<(ostream& output, const _ArrayValue<T>& val)
{
    return output << val.getValue();
}

typedef _ArrayValue<Value> ArrayValue;
typedef _ArrayValue<const Value> ConstArrayValue;

template<typename T>
class _ObjectValue {
public:
    _ObjectValue(T& v) : _v(v)
    {
    }

    auto begin()
    {
        return _v.objectBegin();
    }

    auto end()
    {
        return _v.objectEnd();
    }

    auto begin() const
    {
        return _v.objectBegin();
    }

    auto end() const
    {
        return _v.objectEnd();
    }

    auto empty() const
    {
        return _v.size() == 0;
    }

    T& getValue()
    {
        return _v;
    }

private:
    T& _v;
};

template<typename T>
ostream& operator<<(ostream& output, const _ObjectValue<T>& val)
{
    return output << val.getValue();
}

class KeyValue {
public:
    // This version copies the two values since we cannot move
    KeyValue(const string& a, const Value& b) : key(a), value(b)
    {
    }

    // This version copies the key and move the value
    KeyValue(const string& a, Value&& b) : key(a), value(move(b))
    {
    }

    // This version moves both the key and value
    KeyValue(string&& a, Value&& b) : key(move(a)), value(move(b))
    {
    }

    // Class should be movable only
    KeyValue(KeyValue&&) noexcept = default;
    KeyValue(const KeyValue&) = delete;
    KeyValue& operator=(const KeyValue&) = delete;
    KeyValue& operator=(KeyValue&&) = delete;

    ~KeyValue() = default;

    mutable string key;
    mutable Value value;
};

typedef _ObjectValue<Value> ObjectValue;
typedef _ObjectValue<const Value> ConstObjectValue;
}

#include "JSON/Metrics.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <rapidjson/document.h>

#include <libstuff/libstuff.h>

using namespace std;

namespace
{
enum class StrictValueType : uint8_t
{
    INTEGER,
    FLOAT,
    BOOL,
    STRING,
    OBJECT,
    ARRAY,
    NIL,
};

string normalizeMagnitude(const string_view magnitude)
{
    const size_t firstNonzero = magnitude.find_first_not_of('0');
    return firstNonzero == string_view::npos ? "0" : string(magnitude.substr(firstNonzero));
}

int compareMagnitudes(const string& left, const string& right)
{
    if (left.size() != right.size()) {
        return left.size() < right.size() ? -1 : 1;
    }
    return left.compare(right);
}

string addMagnitudes(const string& left, const string& right)
{
    string result;
    result.reserve(max(left.size(), right.size()) + 1);
    size_t leftPosition = left.size();
    size_t rightPosition = right.size();
    int carry = 0;
    while (leftPosition || rightPosition || carry) {
        int digit = carry;
        if (leftPosition) {
            digit += left[--leftPosition] - '0';
        }
        if (rightPosition) {
            digit += right[--rightPosition] - '0';
        }
        result.push_back(static_cast<char>('0' + digit % 10));
        carry = digit / 10;
    }
    reverse(result.begin(), result.end());
    return result;
}

string subtractMagnitudes(const string& larger, const string& smaller)
{
    string result;
    result.reserve(larger.size());
    size_t largerPosition = larger.size();
    size_t smallerPosition = smaller.size();
    int borrow = 0;
    while (largerPosition) {
        int digit = larger[--largerPosition] - '0' - borrow;
        if (smallerPosition) {
            digit -= smaller[--smallerPosition] - '0';
        }
        if (digit < 0) {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result.push_back(static_cast<char>('0' + digit));
    }
    while (result.size() > 1 && result.back() == '0') {
        result.pop_back();
    }
    reverse(result.begin(), result.end());
    return result;
}

struct SignedExponent
{
    bool negative = false;
    string magnitude = "0";

    void add(const bool otherNegative, const string& otherMagnitude)
    {
        if (otherMagnitude == "0") {
            return;
        }
        if (magnitude == "0") {
            negative = otherNegative;
            magnitude = otherMagnitude;
            return;
        }
        if (negative == otherNegative) {
            magnitude = addMagnitudes(magnitude, otherMagnitude);
            return;
        }

        const int comparison = compareMagnitudes(magnitude, otherMagnitude);
        if (comparison == 0) {
            negative = false;
            magnitude = "0";
        } else if (comparison > 0) {
            magnitude = subtractMagnitudes(magnitude, otherMagnitude);
        } else {
            negative = otherNegative;
            magnitude = subtractMagnitudes(otherMagnitude, magnitude);
        }
    }

    bool operator==(const SignedExponent&) const = default;
};

struct CanonicalNumber
{
    bool negative = false;
    string digits = "0";
    SignedExponent exponent;

    bool operator==(const CanonicalNumber&) const = default;
};

CanonicalNumber canonicalizeNumber(const string_view number)
{
    CanonicalNumber result;
    size_t valueStart = 0;
    if (number.front() == '-') {
        result.negative = true;
        valueStart = 1;
    }

    const size_t exponentPosition = number.find_first_of("eE", valueStart);
    const size_t decimalPosition = number.find('.', valueStart);
    const size_t integerEnd = min(decimalPosition, exponentPosition);
    result.digits = string(number.substr(valueStart, integerEnd - valueStart));

    size_t fractionalDigits = 0;
    if (decimalPosition != string_view::npos) {
        const size_t fractionEnd = exponentPosition == string_view::npos ? number.size() : exponentPosition;
        fractionalDigits = fractionEnd - decimalPosition - 1;
        result.digits.append(number.substr(decimalPosition + 1, fractionalDigits));
    }

    if (exponentPosition != string_view::npos) {
        size_t exponentStart = exponentPosition + 1;
        bool exponentNegative = false;
        if (number[exponentStart] == '+' || number[exponentStart] == '-') {
            exponentNegative = number[exponentStart] == '-';
            ++exponentStart;
        }
        result.exponent.add(exponentNegative, normalizeMagnitude(number.substr(exponentStart)));
    }

    result.digits = normalizeMagnitude(result.digits);
    if (result.digits == "0") {
        result.negative = false;
        result.exponent = {};
        return result;
    }

    size_t trailingZeros = 0;
    while (result.digits.back() == '0') {
        result.digits.pop_back();
        ++trailingZeros;
    }
    result.exponent.add(true, to_string(fractionalDigits));
    result.exponent.add(false, to_string(trailingZeros));
    return result;
}

struct StrictValue
{
    StrictValueType type = StrictValueType::NIL;
    bool boolValue = false;
    string stringValue;
    CanonicalNumber numberValue;
    map<string, StrictValue> objectValue;
    vector<StrictValue> arrayValue;

    bool operator==(const StrictValue& other) const
    {
        if (type != other.type) {
            return false;
        }
        switch (type) {
            case StrictValueType::INTEGER:
            case StrictValueType::FLOAT:
                return numberValue == other.numberValue;

            case StrictValueType::BOOL:
                return boolValue == other.boolValue;

            case StrictValueType::STRING:
                return stringValue == other.stringValue;

            case StrictValueType::OBJECT:
                return objectValue == other.objectValue;

            case StrictValueType::ARRAY:
                return arrayValue == other.arrayValue;

            case StrictValueType::NIL:
                return true;
        }
        return false;
    }
};

bool isNumberTerminator(const char character)
{
    return character == ' ' || character == '\t' || character == '\n' || character == '\r' || character == ',' ||
           character == ']' || character == '}';
}

bool copyNumber(const string& json, size_t& position, string& transformed, vector<string>& numbers)
{
    const size_t start = position;
    if (json[position] == '-') {
        ++position;
        if (position == json.size()) {
            return false;
        }
    }

    if (json[position] == '0') {
        ++position;
        if (position < json.size() && json[position] >= '0' && json[position] <= '9') {
            return false;
        }
    } else if (json[position] >= '1' && json[position] <= '9') {
        do
        {
            ++position;
        } while (position < json.size() && json[position] >= '0' && json[position] <= '9');
    } else {
        return false;
    }

    if (position < json.size() && json[position] == '.') {
        ++position;
        const size_t fractionStart = position;
        while (position < json.size() && json[position] >= '0' && json[position] <= '9') {
            ++position;
        }
        if (position == fractionStart) {
            return false;
        }
    }

    if (position < json.size() && (json[position] == 'e' || json[position] == 'E')) {
        ++position;
        if (position < json.size() && (json[position] == '+' || json[position] == '-')) {
            ++position;
        }
        const size_t exponentStart = position;
        while (position < json.size() && json[position] >= '0' && json[position] <= '9') {
            ++position;
        }
        if (position == exponentStart) {
            return false;
        }
    }

    if (position < json.size() && !isNumberTerminator(json[position])) {
        return false;
    }
    numbers.emplace_back(json.substr(start, position - start));
    transformed.push_back('0');
    return true;
}

// RapidJSON range-checks positive exponents before calling RawNumber, even with kParseNumbersAsStringsFlag. Validate
// number grammar here, retain each original lexeme, and replace each number with zero before RapidJSON parses it.
// RapidJSON still validates the transformed document's structure, strings, UTF-8, and trailing content.
bool replaceNumbers(const string& json, string& transformed, vector<string>& numbers)
{
    if (json.find('\0') != string::npos) {
        return false;
    }

    transformed.reserve(json.size());
    for (size_t position = 0; position < json.size();) {
        const char character = json[position];
        if (character == '"') {
            transformed.push_back(character);
            ++position;
            while (position < json.size()) {
                const char stringCharacter = json[position++];
                transformed.push_back(stringCharacter);
                if (stringCharacter == '\\' && position < json.size()) {
                    transformed.push_back(json[position++]);
                } else if (stringCharacter == '"') {
                    break;
                }
            }
        } else if (character == '-' || (character >= '0' && character <= '9')) {
            if (!copyNumber(json, position, transformed, numbers)) {
                return false;
            }
        } else {
            transformed.push_back(character);
            ++position;
        }
    }
    return true;
}

optional<StrictValue> convertValue(const rapidjson::Value& source, const vector<string>& numbers, size_t& numberPosition)
{
    StrictValue result;
    if (source.IsNull()) {
        return result;
    }
    if (source.IsBool()) {
        result.type = StrictValueType::BOOL;
        result.boolValue = source.GetBool();
        return result;
    }
    if (source.IsNumber()) {
        if (numberPosition == numbers.size()) {
            return nullopt;
        }
        const string& number = numbers[numberPosition++];
        result.type = number.find_first_of(".eE") == string::npos ? StrictValueType::INTEGER : StrictValueType::FLOAT;
        result.numberValue = canonicalizeNumber(number);
        return result;
    }
    if (source.IsString()) {
        result.type = StrictValueType::STRING;
        result.stringValue.assign(source.GetString(), source.GetStringLength());
        return result;
    }
    if (source.IsArray()) {
        result.type = StrictValueType::ARRAY;
        result.arrayValue.reserve(source.Size());
        for (const auto& item : source.GetArray()) {
            optional<StrictValue> child = convertValue(item, numbers, numberPosition);
            if (!child) {
                return nullopt;
            }
            result.arrayValue.push_back(move(*child));
        }
        return result;
    }

    result.type = StrictValueType::OBJECT;
    for (auto member = source.MemberBegin(); member != source.MemberEnd(); ++member) {
        optional<StrictValue> child = convertValue(member->value, numbers, numberPosition);
        if (!child) {
            return nullopt;
        }
        string key(member->name.GetString(), member->name.GetStringLength());
        if (!result.objectValue.emplace(move(key), move(*child)).second) {
            return nullopt;
        }
    }
    return result;
}

optional<StrictValue> parseStrict(const string& json)
{
    const auto start = chrono::high_resolution_clock::now();
    string transformed;
    vector<string> numbers;
    optional<StrictValue> value;
    if (replaceNumbers(json, transformed, numbers)) {
        rapidjson::Document document;
        constexpr unsigned parseFlags = rapidjson::kParseValidateEncodingFlag;
        const rapidjson::ParseResult result = document.Parse<parseFlags>(transformed.data(), transformed.size());
        if (!result.IsError()) {
            size_t numberPosition = 0;
            value = convertValue(document, numbers, numberPosition);
            if (numberPosition != numbers.size()) {
                value = nullopt;
            }
        }
    }

    const auto end = chrono::high_resolution_clock::now();
    JSON::reportMetrics(JSON::MetricsOperation::PARSE,
                        chrono::duration_cast<chrono::microseconds>(end - start).count(), json.size());
    return value;
}
}

bool SJSONEquals(const string& left, const string& right, const set<string>& ignoredTopLevelKeys)
{
    optional<StrictValue> leftValue = parseStrict(left);
    optional<StrictValue> rightValue = parseStrict(right);
    if (!leftValue || !rightValue) {
        return false;
    }

    // Validate both documents before removing ignored keys. Invalid ignored values and duplicate ignored keys remain errors.
    if (leftValue->type == StrictValueType::OBJECT) {
        for (const string& key : ignoredTopLevelKeys) {
            leftValue->objectValue.erase(key);
        }
    }
    if (rightValue->type == StrictValueType::OBJECT) {
        for (const string& key : ignoredTopLevelKeys) {
            rightValue->objectValue.erase(key);
        }
    }
    return *leftValue == *rightValue;
}

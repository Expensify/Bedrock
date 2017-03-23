#include <libstuff/libstuff.h>
#include "SJSON.h"

SJSON::SJSON() : _type(JSON_NULL), _value(nullptr) {
}

SJSON::SJSON(SJSON::Type t) : _type(t), _value(nullptr) {
    switch(_type) {
        case JSON_NULL:
            _value = nullptr;
            break;
        case JSON_BOOL:
            _value = new bool;
            break;
        case JSON_STRING:
            _value = new string;
            break;
        case JSON_NUMBER:
            _value = new Number;
            break;
        case JSON_ARRAY:
            _value = new list<SJSON>;
            break;
        case JSON_OBJECT:
            _value = new map<string, SJSON>;
            break;
    }
}

SJSON::SJSON(const bool& b) : _type(JSON_BOOL) {
    _value = new bool;
    bool& v = *static_cast<bool*>(_value);
    v = b;
}

SJSON::SJSON(const string& s) : _type(JSON_STRING) {
    _value = new string;
    string& v = *static_cast<string*>(_value);
    v = s;
}

SJSON::SJSON(const char* s) : _type(JSON_STRING) {
    _value = new string;
    string& v = *static_cast<string*>(_value);
    v = s;
}

SJSON& SJSON::operator=(const bool& b) {
    this->~SJSON();
    _type = JSON_BOOL;
    _value = new bool;
    bool& v = *static_cast<bool*>(_value);
    v = b;
    return *this;
}

SJSON& SJSON::operator=(const string& s) {
    this->~SJSON();
    _type = JSON_STRING;
    _value = new string;
    string& v = *static_cast<string*>(_value);
    v = s;
    return *this;
}

SJSON& SJSON::operator=(const char* s) {
    this->~SJSON();
    _type = JSON_STRING;
    _value = new string;
    string& v = *static_cast<string*>(_value);
    v = s;
    return *this;
}

SJSON::SJSON(SJSON&& other) {
    // Copy values from the other object.
    _type = other._type;
    _value = other._value;

    // Zero out the other object's values so they're not deleted at destruction.
    other._value = nullptr;
}

SJSON::SJSON(const SJSON& other) {
    // Copy values from the other object.
    _type = other._type;
    switch(_type) {
        case JSON_NULL:
        {
            _value = nullptr;
        }
        break;
        case JSON_BOOL:
        {
            _value = new bool;
            bool& v = *static_cast<bool*>(_value);
            bool& otherv = *static_cast<bool*>(other._value);
            v = otherv;
        }
        break;
        case JSON_STRING:
        {
            _value = new string;
            string& v = *static_cast<string*>(_value);
            string& otherv = *static_cast<string*>(other._value);
            v = otherv;
        }
        break;
        case JSON_NUMBER:
        {
            _value = new Number;
            Number& v = *static_cast<Number*>(_value);
            Number& otherv = *static_cast<Number*>(other._value);
            v = otherv;
        }
        break;
        case JSON_ARRAY:
        {
            _value = new list<SJSON>;
            list<SJSON>& v = *static_cast<list<SJSON>*>(_value);
            list<SJSON>& otherv = *static_cast<list<SJSON>*>(other._value);
            v = otherv;
        }
        break;
        case JSON_OBJECT:
        {
            _value = new map<string, SJSON>;
            map<string, SJSON>& v = *static_cast<map<string, SJSON>*>(_value);
            map<string, SJSON>& otherv = *static_cast<map<string, SJSON>*>(other._value);
            v = otherv;
        }
        break;
    }
}

SJSON& SJSON::operator=(const SJSON& other) {
    // Free our existing value by explicitly calling the destructor.
    this->~SJSON();

    // Copy values from the other object.
    _type = other._type;
    switch(_type) {
        case JSON_NULL:
        {
            _value = nullptr;
        }
        break;
        case JSON_BOOL:
        {
            _value = new bool;
            bool& v = *static_cast<bool*>(_value);
            bool& otherv = *static_cast<bool*>(other._value);
            v = otherv;
        }
        break;
        case JSON_STRING:
        {
            _value = new string;
            string& v = *static_cast<string*>(_value);
            string& otherv = *static_cast<string*>(other._value);
            v = otherv;
        }
        break;
        case JSON_NUMBER:
        {
            _value = new Number;
            Number& v = *static_cast<Number*>(_value);
            Number& otherv = *static_cast<Number*>(other._value);
            v = otherv;
        }
        break;
        case JSON_ARRAY:
        {
            _value = new list<SJSON>;
            list<SJSON>& v = *static_cast<list<SJSON>*>(_value);
            list<SJSON>& otherv = *static_cast<list<SJSON>*>(other._value);
            v = otherv;
        }
        break;
        case JSON_OBJECT:
        {
            _value = new map<string, SJSON>;
            map<string, SJSON>& v = *static_cast<map<string, SJSON>*>(_value);
            map<string, SJSON>& otherv = *static_cast<map<string, SJSON>*>(other._value);
            v = otherv;
        }
        break;
    }

    return *this;
}

SJSON& SJSON::operator=(SJSON&& other) {
    // Free our existing value by explicitly calling the destructor.
    this->~SJSON();
    
    // Copy values from the other object.
    _type = other._type;
    _value = other._value;

    // Zero out the other object's values so they're not deleted at destruction.
    other._value = nullptr;

    return *this;
}

SJSON& SJSON::operator[](const string& key) {
    if (_type != JSON_OBJECT) {
        throw invalid_argument("Value is not an object");
    }
    return (*static_cast<map<string, SJSON>*>(_value))[key];
}

const SJSON& SJSON::operator[](const string& key) const {
    if (_type != JSON_OBJECT) {
        throw invalid_argument("Value is not an object");
    }
    auto& m = *static_cast<map<string, SJSON>*>(_value);
    auto it = m.find(key);
    if (it != m.end()) {
        return it->second;
    }
    throw out_of_range("Couldn't find key in object");
}

SJSON& SJSON::operator[](size_t pos) {
    if (_type != JSON_ARRAY) {
        throw invalid_argument("Value is not an array");
    }
    list<SJSON>& v = *static_cast<list<SJSON>*>(_value);
    if (pos >= v.size()) {
        throw out_of_range("pos too large");
    }
    auto it = v.begin();
    for (size_t i = 0; i < pos; i++) {
        it++;
    }
    return *it;
}

SJSON::~SJSON() {
    switch(_type) {
        case JSON_NULL:
            // Nothing to do for null.
            break;
        case JSON_BOOL:
        {
            bool* v = static_cast<bool*>(_value);
            delete v;
        }
        break;
        case JSON_STRING:
        {
            string* v = static_cast<string*>(_value);
            delete v;
        }
        break;
        case JSON_NUMBER:
        {
            Number* v = static_cast<Number*>(_value);
            delete v;
        }
        break;
        case JSON_ARRAY:
        {
            list<SJSON>* v = static_cast<list<SJSON>*>(_value);
            delete v;
        }
        break;
        case JSON_OBJECT:
        {
            map<string, SJSON>* v = static_cast<map<string, SJSON>*>(_value);
            delete v;
        }
        break;
    }
    _value = nullptr;
}

bool& SJSON::getBool() {
    if (_type != JSON_BOOL) {
        throw invalid_argument("Value is not a bool");
    }
    return *static_cast<bool*>(_value);
}

string& SJSON::getString() {
    if (_type != JSON_STRING) {
        throw invalid_argument("Value is not a string");
    }
    return *static_cast<string*>(_value);
}

double SJSON::getDouble() const {
    if (_type != JSON_NUMBER) {
        throw invalid_argument("Value is not a number");
    }
    Number& n = *static_cast<Number*>(_value);
    if (!n.isInt) {
        return n.floatingPoint;
    }
    return (double)n.integer;
}

int64_t SJSON::getInt() const {
    if (_type != JSON_NUMBER) {
        throw invalid_argument("Value is not a number");
    }
    Number& n = *static_cast<Number*>(_value);
    if (n.isInt) {
        return n.integer;
    }
    return (int64_t)n.floatingPoint;
}

list<SJSON>& SJSON::getArray() {
    if (_type != JSON_ARRAY) {
        throw invalid_argument("Value is not an array");
    }
    return *static_cast<list<SJSON>*>(_value);
}

map<string, SJSON>& SJSON::getObject() {
    if (_type != JSON_OBJECT) {
        throw invalid_argument("Value is not an object");
    }
    return *static_cast<map<string, SJSON>*>(_value);
}

void SJSON::setDouble(double d) {
    if (_type != JSON_NUMBER) {
        throw invalid_argument("Value is not a number");
    }
    Number& n = *static_cast<Number*>(_value);
    n.isInt = false;
    n.floatingPoint = d;
}

void SJSON::setInt(int64_t i) {
    if (_type != JSON_NUMBER) {
        throw invalid_argument("Value is not a number");
    }
    Number& n = *static_cast<Number*>(_value);
    n.isInt = true;
    n.integer = i;
}

size_t SJSON::size() const {
    switch(_type) {
        case JSON_ARRAY:
        {
            list<SJSON>* v = static_cast<list<SJSON>*>(_value);
            return v->size();
        }
        break;
        case JSON_OBJECT:
        {
            map<string, SJSON>* v = static_cast<map<string, SJSON>*>(_value);
            return v->size();
        }
        break;
        case JSON_STRING:
        {
            string* v = static_cast<string*>(_value);
            return v->size();
        }
        break;
        default:
            throw invalid_argument("Value is incorrect type");
    }
}

void SJSON::push_back(SJSON&& val) {
    switch(_type) {
        case JSON_ARRAY:
        {
            list<SJSON>* v = static_cast<list<SJSON>*>(_value);
            v->push_back(move(val));
        }
        break;
        default:
            throw invalid_argument("Value is not an array");
    }
}

void SJSON::push_back(const SJSON& val) {
    switch(_type) {
        case JSON_ARRAY:
        {
            list<SJSON>* v = static_cast<list<SJSON>*>(_value);
            v->push_back(val);
        }
        break;
        default:
            throw invalid_argument("Value is not an array");
    }
}

string SJSON::serialize() const {
    string out;
    switch (_type) {
        case JSON_NULL:
        {
            out = "null";
        }
        break;
        case JSON_BOOL:
        {
            out += (*(static_cast<bool*>(_value)) ? "true" : "false");
        }
        break;
        case JSON_STRING:
        {
            out += "\"";
            string* v = static_cast<string*>(_value);
            size_t size = v->size();
            for (size_t i = 0; i < size; i++) {
                switch((*v)[i]) {
                    case '"':
                        out += "\\\"";
                        break;
                    case '\\':
                        out += "\\\\";
                        break;
                    case '\b':
                        out += "\\b";
                        break;
                    case '\f':
                        out += "\\f";
                        break;
                    case '\n':
                        out += "\\n";
                        break;
                    case '\r':
                        out += "\\r";
                        break;
                    case '\t':
                        out += "\\t";
                        break;
                    default:
                        out += (*v)[i];
                }
            }
            out += "\"";
        }
        break;
        case JSON_NUMBER:
        {
            Number& n = *static_cast<Number*>(_value);
            if (n.isInt) {
                out += to_string(n.integer);
            } else {
                // Don't use to_string because we get more formatting options this way.
                stringstream temp;
                temp.precision(16);
                temp << n.floatingPoint;
                out += temp.str();
            }
        }
        break;
        case JSON_ARRAY:
        {
            // Cast to appropriate type.
            list<SJSON>* value = static_cast<list<SJSON>*>(_value);
            if (value->empty()) {
                return "[]";
            }
            out = "[";
            for (auto& i : *value) {
                out += i.serialize() + ",";
            }
            out[out.size() - 1] = ']';
        }
        break;
        case JSON_OBJECT:
        {
            // Cast to appropriate type.
            map<string, SJSON>* value = static_cast<map<string, SJSON>*>(_value);
            if (value->empty()) {
                return "{}";
            }
            out = "{";
            for (auto& i : *value) {
                out += "\"" + i.first + "\":" + i.second.serialize() + ",";
            }
            out[out.size() - 1] = '}';
        }
        break;
    }

    return out;
}

SJSON SJSON::deserialize(const string& val) {
    size_t consumed;
    return deserialize(val, 0, consumed);
}

SJSON SJSON::deserialize(const string& val, size_t offset, size_t& consumed) {
    while (isspace(val[offset])) {
        offset++;
    }
    switch (val[offset]) {
        case '{':
            return parseObject(val, offset, consumed);
        case '[':
            return parseArray(val, offset, consumed);
        case '"':
            return parseString(val, offset, consumed);
        case 't':
        case 'f':
            return parseBool(val, offset, consumed);
        case 'n':
            return parseNull(val, offset, consumed);
        case '+':
        case '-':
        case '.':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            return parseNumber(val, offset, consumed);
    };
    throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
}

SJSON SJSON::parseObject(const string& val, size_t offset, size_t& consumed) {
    // Set how many bytes we've consumed.
    consumed = 0;

    // Create the new object.
    SJSON returnVal(JSON_OBJECT);

    // Verify we're starting with the right character.
    if (val[offset] != '{') {
        throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
    }

    // Great, then skip past it.
    offset++;
    consumed++;

    // Loop until we find the end.
    while (true) {
        // skip white space.
        while (isspace(val[offset])) {
            offset++;
            consumed++;
        }

        // If we found the end, we're done.
        if (val[offset] == '}') {
            // Done.
            consumed++;
            return returnVal;
        }

        // Otherwise, we should find a string.
        size_t tempConsumed = 0;
        SJSON key = parseString(val, offset, tempConsumed);
        offset += tempConsumed;
        consumed += tempConsumed;

        // skip white space.
        while (isspace(val[offset])) {
            offset++;
            consumed++;
        }

        // Should be a separator.
        // If we found the end, we're done.
        if (val[offset] == ':') {
            offset++;
            consumed++;
        } else {
            throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
        }

        // skip white space.
        while (isspace(val[offset])) {
            offset++;
            consumed++;
        }

        // Then should be a value.
        tempConsumed = 0;
        auto& map = returnVal.getObject();
        map.insert(pair<string, SJSON>(key.getString(), deserialize(val, offset, tempConsumed)));
        offset += tempConsumed;
        consumed += tempConsumed;

        // skip white space.
        while (isspace(val[offset])) {
            offset++;
            consumed++;
        }

        // If we're at a comma, then skip that, too.
        if (val[offset] == ',') {
            offset++;
            consumed++;
        }
    }

    return returnVal;
}

SJSON SJSON::parseArray(const string& val, size_t offset, size_t& consumed) {
    // Set how many bytes we've consumed.
    consumed = 0;

    // Create the new object.
    SJSON returnVal(JSON_ARRAY);

    // Verify we're starting with the right character.
    if (val[offset] != '[') {
        throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
    }

    // Great, then skip past it.
    offset++;
    consumed++;

    // Loop until we find the end.
    while (true) {
        // skip white space.
        while (isspace(val[offset])) {
            offset++;
            consumed++;
        }

        // If we found the end, we're done.
        if (val[offset] == ']') {
            // Done.
            consumed++;
            return returnVal;
        }

        // Then should be a value.
        size_t tempConsumed = 0;
        auto& array = returnVal.getArray();
        array.push_back(deserialize(val, offset, tempConsumed));
        offset += tempConsumed;
        consumed += tempConsumed;

        // skip white space.
        while (isspace(val[offset])) {
            offset++;
            consumed++;
        }

        // If we're at a comma, then skip that, too.
        if (val[offset] == ',') {
            offset++;
            consumed++;
        }
    }

    return returnVal;
}

SJSON SJSON::parseString(const string& val, size_t offset, size_t& consumed) {
    // Set how many bytes we've consumed;
    consumed = 0;

    // Create the new object.
    SJSON returnVal(JSON_STRING);

    // Verify we're starting with the right character.
    if (val[offset] != '"') {
        throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
    }

    // Great, then skip past it.
    offset++;
    consumed++;

    string& value = returnVal.getString();

    while (true) {
        if (val[offset] == '"') {
            consumed++;
            offset++;
            return returnVal;
        } else if (val[offset] == '\\') {
            consumed++;
            offset++;
            switch (val[offset]) {
                case '"':
                    value += '"';
                    break;
                case '\\':
                    value += '\\';
                    break;
                case '/':
                    value += '/';
                    break;
                case 'b':
                    value += '\b';
                    break;
                case 'f':
                    value += '\f';
                    break;
                case 'n':
                    value += '\n';
                    break;
                case 'r':
                    value += '\r';
                    break;
                case 't':
                    value += '\t';
                    break;
                case 'u':
                {
                    char* end;
                    int64_t number = strtol(val.substr(offset + 1, 4).c_str(), &end, 16);
                    if (number >= 0 && number <= 0x007F) {
                        // One-byte encoding.
                        value += number;
                    } else if (number >= 0x0080 && number <= 0x07FF) {
                        // Two-byte encoding.
                        unsigned char highByte = 0x1F & (number >> 6); // 00011111 turn off the top three bits.
                        highByte |= 0xC0; // 11000000 turn on the top two bits.
                        unsigned char lowByte = 0x3F & number; // 00111111 turn off the top two bits.
                        lowByte |= 0x80; // 10000000 turn on the top bit.
                        value += highByte;
                        value += lowByte;
                    } else if (number >= 0x0800 && number <= 0xFFFF) {
                        // Three-byte encoding.
                        unsigned char byte1 = 0x0F & (number >> 12); // 00001111 turn off the top four bits.
                        byte1 |= 0xE0; // 11100000 turn on the top three bits.
                        unsigned char byte2 = 0x3F & (number >> 6); // 00111111 turn off the top two bits.
                        byte2 |= 0x80; // 10000000 turn on the top bit.
                        unsigned char byte3 = 0x3F & number; // 00111111 turn off the top two bits.
                        byte3 |= 0x80; // 10000000 turn on the top bit.
                        value += byte1;
                        value += byte2;
                        value += byte3;
                    } else if (number >= 0x10000 && number <= 0x10FFFF) {
                        // Four-byte encoding. This is actually impossible to represent in JSON with four hex digits
                        // used to escape values. These can be used unescaped in UTF-8 input.
                    } else {
                        throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
                    }
                    consumed += 4;
                    offset += 4;
                }
                break;
                default:
                    throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
            }
            // And we increment in any case (where we didn't throw);
            consumed++;
            offset++;
        } else {
            value += val[offset];
            consumed++;
            offset++;
        }
    }

    return returnVal;
}

SJSON SJSON::parseNull(const string& val, size_t offset, size_t& consumed) {
    if (val[offset] == 'n' && val[offset + 1] == 'u' && val[offset + 2] == 'l' && val[offset + 3] == 'l') {
        consumed = 4;
        return SJSON(JSON_NULL);
    }
    throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
}

SJSON SJSON::parseBool(const string& val, size_t offset, size_t& consumed) {
    if (val[offset] == 't' && val[offset + 1] == 'r' && val[offset + 2] == 'u' && val[offset + 3] == 'e') {
        consumed = 4;
        SJSON temp(JSON_BOOL);
        temp.getBool() = true;
        return temp;
    }
    if (val[offset] == 'f' && val[offset + 1] == 'a' && val[offset + 2] == 'l' && val[offset + 3] == 's'
        && val[offset + 4] == 'e') {
        consumed = 5;
        SJSON temp(JSON_BOOL);
        temp.getBool() = false;
        return temp;
    }
    throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
}

SJSON SJSON::parseNumber(const string& val, size_t offset, size_t& consumed) {
    // Pointers to the end of parsing for both strings and doubles.
    char* dend = nullptr;
    char* iend = nullptr;

    // Parse in both formats.
    int64_t i = strtoll(&val[offset], &iend, 10);
    double d = strtod(&val[offset], &dend);

    // Both should have succeeded.
    if (!iend || !dend) {
        throw SJSONParseException(val.substr(max((size_t)0, offset - 20), 40), consumed);
    }

    // Store value here.
    SJSON temp(JSON_NUMBER);

    // See how much string each parsed.
    size_t iconsumed = iend - &val[offset];
    size_t dconsumed = dend - &val[offset];

    // If they're the same, it's an int.
    if (iconsumed == dconsumed) {
        temp.setInt(i);
        consumed = iconsumed;
        return temp;
    } else {
        // Otherwise, it was a double.
        temp.setDouble(d);
        consumed = dconsumed;
        return temp;
    }
}

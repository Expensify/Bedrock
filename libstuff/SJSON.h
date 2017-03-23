// Generic parser error.
class SJSONParseException : public exception {
  public:
    SJSONParseException(string msg, size_t where) : _msg(msg), _where(where) {}
    const char* what() const noexcept {
        string temp = "Parse error in: " + _msg + " at offset: " + to_string(_where);
        return temp.c_str();
    }

  private:
    string _msg;
    size_t _where;
};

// The abstract base class.
class SJSONValue {
  public:

    // The possible types of a JSON value.
    enum Type {
        JSON_NULL,
        JSON_BOOL,
        JSON_STRING,
        JSON_NUMBER,
        JSON_ARRAY,
        JSON_OBJECT
    };

    // Serialize this value to a JSON-encoded string.
    string serialize() const;

    // This differs from 'serialize' in that it just returns its value as a C++ string, such that you can create a
    // list<string> (or similar) from a bunch of JSON values.
    string stringValue();

    // Constructor/destructor.
    SJSONValue(Type t);
    ~SJSONValue();

    // This constructor exists mainly to allow operator[] to work as expected when these are stored in maps, by
    // creating a default value (which may immediately be replaced).
    SJSONValue();

    // Move constructor.
    SJSONValue(SJSONValue&& other);

    // Copy constructor.
    SJSONValue(const SJSONValue& other);

    // Various overloaded constructors.
    SJSONValue(const bool& b);
    SJSONValue(const string& s);
    SJSONValue(const char* s);

    // Templated for any integral/floating point type.
    template <typename T>
    SJSONValue(const T& item, typename std::enable_if<std::is_integral<T>::value>::type* t = 0);
    template <typename T>
    SJSONValue(const T& item, typename std::enable_if<std::is_floating_point<T>::value>::type* t = 0);

    // Move assignment operator.
    SJSONValue& operator=(SJSONValue&& other);

    // Copy assignment operator.
    SJSONValue& operator=(const SJSONValue& other);

    // Various overloaded assignment operators.
    template <typename T>
    typename enable_if<is_integral<T>::value, SJSONValue&>::type operator=(const T& item);
    template <typename T>
    typename enable_if<is_floating_point<T>::value, SJSONValue&>::type operator=(const T& item);
    SJSONValue& operator=(const bool& b);
    SJSONValue& operator=(const string& s);
    SJSONValue& operator=(const char* s);

    // This throws on values that are not Object.
    SJSONValue& operator[](const string& key);

    // Const version of above.
    const SJSONValue& operator[](const string& key) const;

    // This throws on values that are not Array.
    SJSONValue& operator[](size_t pos);

    // Create JSON Values from strings.
    static SJSONValue deserialize(const string& val);

    // return the type of the object.
    Type type() const { return _type; }

    // Return the size of the object. This throws except for Object, array, and String.
    size_t size() const;

    // Take ownership of JSON value and push on our array. Throws if not an array, in which case the argument is
    // unaffected.
    void push_back(SJSONValue&& val);

    // Same as above, but makes a copy.
    void push_back(const SJSONValue& val);

    // Return the value of the object. These each throw an exception in the case that the object type doesn't match the
    // accessor being called. There is no 'getNull'.
    bool& getBool();
    string& getString();
    list<SJSONValue>& getArray();
    map<string, SJSONValue>& getObject();

    // The numeric operations do not return references, but values, so that we can maintain type-safety. All
    // conversions are handled automatically. The type still must be JSON_NUMBER or these ill throw.
    double getDouble() const;
    void setDouble(double d);
    int64_t getInt() const;
    void setInt(int64_t i);

    private:

    // Numbers are typically integers, so we use those most of the time, but we still support floating point.
    struct Number {
        bool isInt;
        int64_t integer;
        double floatingPoint;
    };

    // Member values.
    Type _type;
    void* _value;

    // Recursive deserialization functions.
    static SJSONValue deserialize(const string& val, size_t offset, size_t& consumed);
    static SJSONValue parseString(const string& val, size_t offset, size_t& consumed);
    static SJSONValue parseNull(const string& val, size_t offset, size_t& consumed);
    static SJSONValue parseBool(const string& val, size_t offset, size_t& consumed);
    static SJSONValue parseNumber(const string& val, size_t offset, size_t& consumed);
    static SJSONValue parseObject(const string& val, size_t offset, size_t& consumed);
    static SJSONValue parseArray(const string& val, size_t offset, size_t& consumed);
};


// Template function implementations for this class.
template <typename T>
typename enable_if<is_integral<T>::value, SJSONValue&>::type SJSONValue::operator=(const T& item)
{
    // Explicitly call the destructor to free existing values.
    this->~SJSONValue();
    _type = JSON_NUMBER;
    _value = new Number;
    setInt(item);
}
template <typename T>
typename enable_if<is_floating_point<T>::value, SJSONValue&>::type SJSONValue::operator=(const T& item)
{
    // Explicitly call the destructor to free existing values.
    this->~SJSONValue();
    _type = JSON_NUMBER;
    _value = new Number;
    setDouble(item);
}

template <typename T>
SJSONValue::SJSONValue(const T& item, typename std::enable_if<std::is_integral<T>::value>::type* t) {
    _type = JSON_NUMBER;
    _value = new Number;
    Number& v = *static_cast<Number*>(_value);
    v.isInt = true;
    v.integer = item;
}

template <typename T>
SJSONValue::SJSONValue(const T& item, typename std::enable_if<std::is_floating_point<T>::value>::type* t) {
    _type = JSON_NUMBER;
    _value = new Number;
    Number& v = *static_cast<Number*>(_value);
    v.isInt = false;
    v.floatingPoint = item;
}

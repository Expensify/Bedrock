#pragma once

#include <iostream>
#include <cxxabi.h>

using namespace std;

template <typename T>
class Equal {
  public:
    Equal(T v) : val(v) {}

    template<typename U>
    bool operator==(const U& rhs) const {
        return val == rhs;
    }

    template<typename U>
    bool operator!=(const U& rhs) const {
        return !(val == rhs);
    }

  private:

    T val;
};

class EqualComparator {
    public:
        template <typename T, enable_if_t<is_integral<T>::value, bool> = true, typename U>
        EqualComparator(T a, U b)
        {
            // Integral case.
            if (Equal<T>(a) != (T) b) {
                cout << "(Integral): " << a << " != " << b << "\n";
            } else {
                cout << "(Integral): " << a << " == " << b << "\n";
            }
        }

        template <typename T, enable_if_t<!is_integral<T>::value, bool> = true, typename U>
        EqualComparator(T a, U b)
        {
            // Non-integer base case.
            char buffer[1000];
            size_t length = 1000;
            int status;
            abi::__cxa_demangle(typeid(a).name(), buffer, &length, &status);

            if (Equal<T>(a) != b) {
                cout << "Not equal (unhandled type: " << buffer << ")" << "\n";
            } else {
                cout << "equal (unhandled type: " << buffer << ")" << "\n";
            }
        }

        template <typename U>
        EqualComparator(string a, U b)
        {
            // Non-integer base case.
            if (Equal<string>(a) != b) {
                cout << "(string): \"" << a << "\" != \""<< b << "\"" << "\n";
            } else {
                cout << "(string): \"" << a << "\" == \""<< b << "\"" << "\n";
            }
        }

        template <typename U>
        EqualComparator(const char* a, U b) {
            // Non-integer base case.
            if (Equal<const char*>(a) != string(b)) { // Note that `!=` doesn't work correctly on plain `const char *`
                cout << "(const char*): \"" << a << "\" != \""<< b << "\"" << "\n";
            } else {
                cout << "(const char*): \"" << a << "\" == \""<< b << "\"" << "\n";
            }
        }
};

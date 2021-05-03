#pragma once

#include <iostream>
#include <string>
#include <cxxabi.h>

using namespace std;

class PrintEquality {
    public:
        template <typename T, enable_if_t<is_integral<T>::value, bool> = true, typename U>
        PrintEquality(T a, U b, bool isEqual)
        {
            // Integral case.
            if (!isEqual) {
                cout << "(Integral): " << a << " != " << b << "\n";
            } else {
                cout << "(Integral): " << a << " == " << b << "\n";
            }
        }

        template <typename T, enable_if_t<!is_integral<T>::value, bool> = true, typename U>
        PrintEquality(T a, U b, bool isEqual)
        {
            // Non-integral base case.
            char buffer[1000];
            size_t length = 1000;
            int status;
            abi::__cxa_demangle(typeid(a).name(), buffer, &length, &status);

            if (!isEqual) {
                cout << "Not equal (unhandled type: " << buffer << ")" << "\n";
            } else {
                cout << "equal (unhandled type: " << buffer << ")" << "\n";
            }
        }

        template <typename U>
        PrintEquality(string a, U b, bool isEqual)
        {
            if (!isEqual) {
                cout << "(string): \"" << a << "\" != \"" << b << "\"" << "\n";
            } else {
                cout << "(string): \"" << a << "\" == \"" << b << "\"" << "\n";
            }
        }

        template <typename U>
        PrintEquality(const char* a, U b, bool isEqual) {
            if (!isEqual) {
                cout << "(const char*): \"" << a << "\" != \"" << b << "\"" << "\n";
            } else {
                cout << "(const char*): \"" << a << "\" == \"" << b << "\"" << "\n";
            }
        }
};

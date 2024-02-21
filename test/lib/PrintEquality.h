#pragma once

#include <iostream>
#include <string>
#include <set>
#include <map>
#include <list>
#include <cxxabi.h>

using namespace std;

template<typename T>
ostream& operator<<(ostream& output, const list<T>& val)
{
    return output << "[LIST]";
}

template<typename T>
ostream& operator<<(ostream& output, const set<T>& val)
{
    return output << "[SET]";
}

template<typename T, typename U>
ostream& operator<<(ostream& output, const map<T, U>& val)
{
    return output << "[MAP]";
}

class PrintEquality {
    public:
        template <typename U, typename V>
        PrintEquality(U a, V b, bool isEqual) {
            if (!isEqual) {
                cout << "Not equal " << a << ", " << b << "\n";
            } else {
                cout << "equal " << a << ", " << b << "\n";
            }
        }
};

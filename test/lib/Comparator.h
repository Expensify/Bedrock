#pragma once
#include <libstuff/libstuff.h>

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
        return val != rhs;
    }

  private:

    T val;
};

class EqualComparator {
  public:
    template <typename T, enable_if_t<is_integral<T>::value, bool> = true, typename U>
    EqualComparator(T a, U b);

    template <typename T, enable_if_t<!is_integral<T>::value, bool> = true, typename U>
    EqualComparator(T a, U b);

    template <typename U>
    EqualComparator(string a, U b);

    template <typename U>
    EqualComparator(const char* a, U b);
};

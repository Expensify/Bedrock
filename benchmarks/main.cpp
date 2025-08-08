#include <iostream>
#include <test/lib/tpunit++.hpp>

int main(int argc, char* argv[]) {
    std::set<std::string> include;
    std::set<std::string> exclude;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == std::string("-only") && i + 1 < argc) {
            include.insert(argv[++i]);
        } else if (arg == std::string("-except") && i + 1 < argc) {
            exclude.insert(argv[++i]);
        }
    }
    return tpunit::Tests::run(include, exclude, {}, {}, 1);
}



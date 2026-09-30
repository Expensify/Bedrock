#pragma once

#include <cstdlib>
#include <filesystem>
#include <string>
#include <unistd.h>

inline void configureBedrockTestPath()
{
    using namespace std;
    namespace fs = filesystem;
    error_code error;
    fs::path directory = fs::read_symlink("/proc/self/exe", error).parent_path();
    while (!error && !directory.empty()) {
        if (access((directory / "bedrock").c_str(), X_OK) == 0) {
            break;
        }
        if (directory == directory.root_path()) {
            directory.clear();
            break;
        }
        directory = directory.parent_path();
    }
    if (directory.empty()) {
        const char* bedrockDirectory = getenv("BEDROCK_DIR");
        if (!bedrockDirectory || access((fs::path(bedrockDirectory) / "bedrock").c_str(), X_OK) != 0) {
            return;
        }
        directory = bedrockDirectory;
    }

    const char* currentPath = getenv("PATH");
    const string path = directory.string() + ":" + (currentPath ? currentPath : "");
    setenv("PATH", path.c_str(), 1);
}

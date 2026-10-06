#pragma once
#include <string>

// Takes ownership of a locked singleton file and an already-connected bootstrap client.
int runTestPortServer(int lockFD, int bootstrapFD, const std::string& runtimeDirectory);

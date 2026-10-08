#pragma once
#include <string>

using namespace std;

// Takes ownership of a locked singleton file and an already-connected bootstrap client.
int runTestPortServer(int lockFD, int bootstrapFD, const string& runtimeDirectory);

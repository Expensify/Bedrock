#pragma once
#include <chrono>
#include <string>
using namespace std;

// There is a *different* AutoTimer in BedrockCore, which is annoying.
class AutoTimer {
public:
    AutoTimer(const string& name);
    void start();
    void stop();

private:
    string _name;
    chrono::steady_clock::time_point _intervalStart;
    chrono::steady_clock::time_point _instanceStart;
    chrono::steady_clock::duration _countedTime;
};

class AutoTimerTime {
public:
    AutoTimerTime(AutoTimer& t);
    ~AutoTimerTime();

private:
    AutoTimer& _t;
};

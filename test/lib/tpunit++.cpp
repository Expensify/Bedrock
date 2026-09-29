#include "tpunit++.hpp"
#include "ConsoleOutputWriter.h"
#include <string.h>
#include <iostream>
#include <regex>
#include <chrono>
#include <atomic>
#include <utility>
using namespace tpunit;

atomic<bool> tpunit::_TestFixture::exitFlag(false);
thread_local string tpunit::currentTestName;
static thread_local string currentMethodName;
thread_local string tpunit::_TestFixture::testOutputBuffer;
thread_local tpunit::_TestFixture* tpunit::currentTestPtr = nullptr;
thread_local mutex tpunit::currentTestNameMutex;

thread_local int tpunit::_TestFixture::perFixtureStats::_assertions = 0;
thread_local int tpunit::_TestFixture::perFixtureStats::_exceptions = 0;
thread_local int tpunit::_TestFixture::perFixtureStats::_traces = 0;

tpunit::_TestFixture::method::method(_TestFixture* obj, void (_TestFixture::*addr)(), const char* name, unsigned char type)
    : _this(obj)
    , _addr(addr)
    , _type(type)
    , _next(0) {
        char* dest = _name;
        while(name && *name != 0) {
          *dest++ = *name++;
        }
        *dest = 0;
}

tpunit::_TestFixture::method::~method() {
    delete _next;
}

tpunit::_TestFixture::stats::stats()
    : _failures(0)
    , _passes(0)
    {}

tpunit::_TestFixture::perFixtureStats::perFixtureStats()
{
}

tpunit::_TestFixture::_TestFixture(const char* name, bool parallel)
  : _name(name),
    _parallel(parallel)
{
}

void tpunit::_TestFixture::registerTests(method* m0,  method* m1,  method* m2,  method* m3,  method* m4,
                         method* m5,  method* m6,  method* m7,  method* m8,  method* m9,
                         method* m10, method* m11, method* m12, method* m13, method* m14,
                         method* m15, method* m16, method* m17, method* m18, method* m19,
                         method* m20, method* m21, method* m22, method* m23, method* m24,
                         method* m25, method* m26, method* m27, method* m28, method* m29,
                         method* m30, method* m31, method* m32, method* m33, method* m34,
                         method* m35, method* m36, method* m37, method* m38, method* m39,
                         method* m40, method* m41, method* m42, method* m43, method* m44,
                         method* m45, method* m46, method* m47, method* m48, method* m49,
                         method* m50, method* m51, method* m52, method* m53, method* m54,
                         method* m55, method* m56, method* m57, method* m58, method* m59,
                         method* m60, method* m61, method* m62, method* m63, method* m64,
                         method* m65, method* m66, method* m67, method* m68, method* m69,
                         method* m70, method* m71, method* m72, method* m73, method* m74)
{
    tpunit_detail_fixture_list()->push_back(this);

    // DO NOT modify this over 75, you're holding it wrong if you do.
    // Split your test suites/files if you need to!
    method* methods[75] = { m0,  m1,  m2,  m3,  m4,  m5,  m6,  m7,  m8,  m9,
                            m10, m11, m12, m13, m14, m15, m16, m17, m18, m19,
                            m20, m21, m22, m23, m24, m25, m26, m27, m28, m29,
                            m30, m31, m32, m33, m34, m35, m36, m37, m38, m39,
                            m40, m41, m42, m43, m44, m45, m46, m47, m48, m49,
                            m50, m51, m52, m53, m54, m55, m56, m57, m58, m59,
                            m60, m61, m62, m63, m64, m65, m66, m67, m68, m69,
                            m70, m71, m72, m73, m74 };

    for(int i = 0; i < 75; i++) {
       if(methods[i]) {
          method** m = 0;
          switch(methods[i]->_type) {
             case method::AFTER_METHOD:        m = &_afters;         break;
             case method::AFTER_CLASS_METHOD:  m = &_after_classes;  break;
             case method::BEFORE_METHOD:       m = &_befores;        break;
             case method::BEFORE_CLASS_METHOD: m = &_before_classes; break;
             case method::TEST_METHOD:         m = &_tests;          break;
          }
          while(*m && (*m)->_next) {
             m = &(*m)->_next;
          }
          (*m) ? (*m)->_next = methods[i] : *m = methods[i];
       }
    }
    _threadID = 0;
    _mutex    = 0;
}

tpunit::TestFixture::~TestFixture() {
}

tpunit::_TestFixture::~_TestFixture() {
    delete _afters;
    delete _after_classes;
    delete _befores;
    delete _before_classes;
    delete _tests;
}

int tpunit::_TestFixture::tpunit_detail_do_run(int threads, std::function<void()> threadInitFunction, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction, OutputWriter* writer) {
    const std::set<std::string> include, exclude;
    const std::list<std::string> before, after;
    return tpunit_detail_do_run(include, exclude, before, after, threads, threadInitFunction, sortFunction, writer);
}

void tpunit::_TestFixture::tpunit_run_test_class(_TestFixture* f) {
   f->_stats._assertions = 0;
   f->_stats._exceptions = 0;
   tpunit_detail_do_methods(f->_before_classes);
   if (f->_stats._assertions || f->_stats._exceptions) {
       lock_guard<recursive_mutex> lock(*f->_mutex);
       tpunit_detail_stats()._failures++;
       tpunit_detail_stats()._failureNames.emplace(f->_name + "::BEFORE_CLASS"s);
       f->_outputWriter->fixtureSetupFailed(f->_name);
   } else {
       tpunit_detail_do_tests(f);
   }
   f->_stats._assertions = 0;
   f->_stats._exceptions = 0;
   tpunit_detail_do_methods(f->_after_classes);
   if (f->_stats._assertions || f->_stats._exceptions) {
       lock_guard<recursive_mutex> lock(*f->_mutex);
       tpunit_detail_stats()._failures++;
       tpunit_detail_stats()._failureNames.emplace(f->_name + "::AFTER_CLASS"s);
       f->_outputWriter->fixtureTeardownFailed(f->_name);
   }
}
bool tpunit::_TestFixture::sorter(_TestFixture* a, _TestFixture* b) {
   if (a->_name && b->_name) {
      return strcmp(a->_name, b->_name) < 0;
   }
   return false;
}

int tpunit::_TestFixture::tpunit_detail_do_run(const set<string>& include, const set<string>& exclude,
                                              const list<string>& before, const list<string>& after, int threads,
                                              std::function<void()> threadInitFunction, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction, OutputWriter* writer) {
    ConsoleOutputWriter defaultWriter;
    if (!writer) {
        writer = &defaultWriter;
    }
    threadInitFunction();
    /*
    * Run specific tests by name. If 'include' is empty, then every test is
    * run unless it's in 'exclude'. If 'include' has at least one entry,
    * then only tests in 'include' are run, and 'exclude' is ignored.
    */
    std::list<_TestFixture*> testFixtureList = *tpunit_detail_fixture_list();
    testFixtureList.sort(sortFunction);

    // Make local, mutable copies of the include and exclude lists.
    set<string> _include = include;
    set<string> _exclude = exclude;

    // Create a list of threads, and have them each pull tests of the queue.
    list<thread> threadList;
    recursive_mutex m;
    for (auto fixture : testFixtureList) {
        fixture->_outputWriter = writer;
    }

    writer->runStarted(testFixtureList.size());

    // Run the `before` tests
    for (auto name : before) {
        for (auto fixture : testFixtureList) {
            if (fixture->_name && name == fixture->_name) {
               fixture->_threadID = 0;
               fixture->_mutex = &m;
               fixture->_multiThreaded = false;

               // Add to exclude.
               _exclude.insert(name);

                // Run the test.
                writer->fixtureStarted(fixture->_name, true);
                auto start = chrono::steady_clock::now();
                tpunit_run_test_class(fixture);
                writer->fixtureFinished(fixture->_name, chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start));

               continue; // Don't bother checking the rest of the tests.
            }
        }
    }

    // And exclude our `after` tests so they don't get run in the main loop.
    for (auto name : after) {
        _exclude.insert(name);
    }

    list<_TestFixture*> afterTests;
    mutex testTimeLock;
    multimap<chrono::milliseconds, string> testTimes;

    // Track which include patterns matched at least one test
    set<string> includeMatched;

    for (int threadID = 0; threadID < threads; threadID++) {
        // Capture everything by reference except threadID, because we don't want it to be incremented for the
        // next thread in the loop.
        thread t = thread([&, threadID]{
           chrono::steady_clock::time_point start = chrono::steady_clock::now();
           chrono::steady_clock::time_point end = start;

           threadInitFunction();
            try {
                // Do test.
                while (1) {
                    _TestFixture* f = 0;
                    {
                        lock_guard<recursive_mutex> lock(m);
                        if (testFixtureList.empty()) {
                            // Done looping.
                            break;
                        }
                        f = testFixtureList.front();
                        testFixtureList.pop_front();
                    }

                    f->_threadID = threadID;
                    f->_mutex = &m;
                    f->_multiThreaded = threads > 1;

                    // Determine if this test even should run.
                    bool included = true;
                    bool excluded = false;

                    // If there's an include list, run the tests to see if we should include it.
                    if (_include.size()) {
                        included = false;

                        // If there's no name, we can skip the tests.
                        if (f->_name) {
                            for (const string& includedName : _include) {
                                try {
                                    if (regex_match(f->_name, regex("^" + includedName + "$"))) {
                                        included = true;

                                        // Track that this pattern matched at least one test
                                        lock_guard<recursive_mutex> lock(m);
                                        includeMatched.insert(includedName);
                                        break;
                                    }
                                } catch (const regex_error& e) {
                                    lock_guard<recursive_mutex> lock(m);
                                    writer->invalidPattern(includedName);
                                }
                            }
                        }
                    }

                    // Similar for excluding. If it has no name, or there's no exclude list, it's not excluded.
                    else if (f->_name && _exclude.size()) {
                        for (string excludedName : _exclude) {
                            try {
                                if (regex_match(f->_name, regex("^" + excludedName + "$"))) {
                                    excluded = true;
                                    break;
                                }
                            } catch (const regex_error& e) {
                                lock_guard<recursive_mutex> lock(m);
                                writer->invalidPattern(excludedName);
                            }
                        }
                    }

                    if (!included || excluded) {
                        // Put in the after list, in case we want to run it there.
                        lock_guard<recursive_mutex> lock(m);
                        afterTests.push_back(f);
                        continue;
                    }

                    // At this point, we know this test should run.
                    {
                        lock_guard<recursive_mutex> lock(m);
                        writer->fixtureStarted(f->_name ? f->_name : "", !f->_multiThreaded);
                    }
                    {
                        lock_guard<mutex> lock(currentTestNameMutex);
                        currentTestPtr = f;
                        if (f->_name) {
                            currentTestName = f->_name;
                        } else {
                            lock_guard<recursive_mutex> outputLock(m);
                            writer->unnamedFixture();
                            currentTestName = "UNSPECIFIED";
                        }
                    }

                    start = chrono::steady_clock::now();

                    tpunit_run_test_class(f);

                    // Do this to capture the longest test classes, not longest thread.
                    end = chrono::steady_clock::now();
                    {
                        lock_guard<recursive_mutex> lock(m);
                        writer->fixtureFinished(f->_name ? f->_name : "", chrono::duration_cast<chrono::milliseconds>(end - start));
                    }

                   if (currentTestName.size() && currentTestName != "UNSPECIFIED") {
                        lock_guard<mutex> lock(testTimeLock);
                        testTimes.emplace(make_pair(chrono::duration_cast<std::chrono::milliseconds>(end - start), currentTestName));
                    }
                }
            } catch (ShutdownException se) {
                // This will have broken us out of our main loop, so we'll just exit. We also set the exit flag to let
                // other threads know we're trying to exit.
                lock_guard<recursive_mutex> lock(m);
                exitFlag = true;
                writer->threadShutdown(threadID);
            }
        });
        threadList.push_back(move(t));
    }

    // Wait for them all to finish.
    for (thread& currentThread : threadList) {
        currentThread.join();
    }
    threadList.clear();

    // Run the `after` tests
    for (auto name : after) {
        for (auto fixture : afterTests) {
            if (fixture->_name && name == fixture->_name) {
               fixture->_threadID = 0;
               fixture->_mutex = &m;
               fixture->_multiThreaded = false;

                // Run the test.
                writer->fixtureStarted(fixture->_name, true);
                auto start = chrono::steady_clock::now();
                tpunit_run_test_class(fixture);
                writer->fixtureFinished(fixture->_name, chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start));

               continue; // Don't bother checking the rest of the tests.
            }
        }
    }

    // Print message for each include pattern that did not match any test
    for (const auto& pattern : include) {
        if (includeMatched.find(pattern) == includeMatched.end()) {
            writer->unmatchedPattern(pattern);
        }
    }

    if (!exitFlag) {
        vector<pair<chrono::milliseconds, string>> times(testTimes.begin(), testTimes.end());
        stats& results = tpunit_detail_stats();
        writer->runFinished({results._passes, results._failures, results._failureNames, times});

        return tpunit_detail_stats()._failures;
    }
    return 1;
}

bool tpunit::_TestFixture::tpunit_detail_fp_equal(float lhs, float rhs, unsigned char ulps) {
    union {
       float f;
       char  c[4];
    } lhs_u, rhs_u;
    lhs_u.f = lhs;
    rhs_u.f = rhs;

    bool lil_endian = ((unsigned char) 0x00FF) == 0xFF;
    int msb = lil_endian ? 3 : 0;
    int lsb = lil_endian ? 0 : 3;
    if(lhs_u.c[msb] < 0) {
       lhs_u.c[0 ^ lsb] = 0x00 - lhs_u.c[0 ^ lsb];
       lhs_u.c[1 ^ lsb] = (((unsigned char) lhs_u.c[0 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[1 ^ lsb];
       lhs_u.c[2 ^ lsb] = (((unsigned char) lhs_u.c[1 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[2 ^ lsb];
       lhs_u.c[3 ^ lsb] = (((unsigned char) lhs_u.c[2 ^ lsb] > 0x00) ? 0x7F : 0x80) - lhs_u.c[3 ^ lsb];
    }
    if(rhs_u.c[msb] < 0) {
       rhs_u.c[0 ^ lsb] = 0x00 - rhs_u.c[0 ^ lsb];
       rhs_u.c[1 ^ lsb] = (((unsigned char) rhs_u.c[0 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[1 ^ lsb];
       rhs_u.c[2 ^ lsb] = (((unsigned char) rhs_u.c[1 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[2 ^ lsb];
       rhs_u.c[3 ^ lsb] = (((unsigned char) rhs_u.c[2 ^ lsb] > 0x00) ? 0x7F : 0x80) - rhs_u.c[3 ^ lsb];
    }
    return (lhs_u.c[1] == rhs_u.c[1] && lhs_u.c[2] == rhs_u.c[2] && lhs_u.c[msb] == rhs_u.c[msb]) &&
           ((lhs_u.c[lsb] > rhs_u.c[lsb]) ? lhs_u.c[lsb] - rhs_u.c[lsb] : rhs_u.c[lsb] - lhs_u.c[lsb]) <= ulps;
}

bool tpunit::_TestFixture::tpunit_detail_fp_equal(double lhs, double rhs, unsigned char ulps) {
    union {
       double d;
       char   c[8];
    } lhs_u, rhs_u;
    lhs_u.d = lhs;
    rhs_u.d = rhs;

    bool lil_endian = ((unsigned char) 0x00FF) == 0xFF;
    int msb = lil_endian ? 7 : 0;
    int lsb = lil_endian ? 0 : 7;
    if(lhs_u.c[msb] < 0) {
       lhs_u.c[0 ^ lsb] = 0x00 - lhs_u.c[0 ^ lsb];
       lhs_u.c[1 ^ lsb] = (((unsigned char) lhs_u.c[0 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[1 ^ lsb];
       lhs_u.c[2 ^ lsb] = (((unsigned char) lhs_u.c[1 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[2 ^ lsb];
       lhs_u.c[3 ^ lsb] = (((unsigned char) lhs_u.c[2 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[3 ^ lsb];
       lhs_u.c[4 ^ lsb] = (((unsigned char) lhs_u.c[3 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[4 ^ lsb];
       lhs_u.c[5 ^ lsb] = (((unsigned char) lhs_u.c[4 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[5 ^ lsb];
       lhs_u.c[6 ^ lsb] = (((unsigned char) lhs_u.c[5 ^ lsb] > 0x00) ? 0xFF : 0x00) - lhs_u.c[6 ^ lsb];
       lhs_u.c[7 ^ lsb] = (((unsigned char) lhs_u.c[6 ^ lsb] > 0x00) ? 0x7F : 0x80) - lhs_u.c[7 ^ lsb];
    }
    if(rhs_u.c[msb] < 0) {
       rhs_u.c[0 ^ lsb] = 0x00 - rhs_u.c[0 ^ lsb];
       rhs_u.c[1 ^ lsb] = (((unsigned char) rhs_u.c[0 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[1 ^ lsb];
       rhs_u.c[2 ^ lsb] = (((unsigned char) rhs_u.c[1 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[2 ^ lsb];
       rhs_u.c[3 ^ lsb] = (((unsigned char) rhs_u.c[2 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[3 ^ lsb];
       rhs_u.c[4 ^ lsb] = (((unsigned char) rhs_u.c[3 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[4 ^ lsb];
       rhs_u.c[5 ^ lsb] = (((unsigned char) rhs_u.c[4 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[5 ^ lsb];
       rhs_u.c[6 ^ lsb] = (((unsigned char) rhs_u.c[5 ^ lsb] > 0x00) ? 0xFF : 0x00) - rhs_u.c[6 ^ lsb];
       rhs_u.c[7 ^ lsb] = (((unsigned char) rhs_u.c[6 ^ lsb] > 0x00) ? 0x7F : 0x80) - rhs_u.c[7 ^ lsb];
    }
    return (lhs_u.c[1] == rhs_u.c[1] && lhs_u.c[2] == rhs_u.c[2] &&
            lhs_u.c[3] == rhs_u.c[3] && lhs_u.c[4] == rhs_u.c[4] &&
            lhs_u.c[5] == rhs_u.c[5] && lhs_u.c[6] == rhs_u.c[6] &&
            lhs_u.c[msb] == rhs_u.c[msb]) &&
           ((lhs_u.c[lsb] > rhs_u.c[lsb]) ? lhs_u.c[lsb] - rhs_u.c[lsb] : rhs_u.c[lsb] - lhs_u.c[lsb]) <= ulps;
}

namespace tpunit {
void tpunit_report_comparison(const string& lhs, const string& rhs, bool isEqual) {
    _TestFixture* fixture = currentTestPtr;
    lock_guard<recursive_mutex> lock(*fixture->_mutex);
    fixture->_outputWriter->comparisonFailed(fixture->name() ? fixture->name() : "", currentMethodName, lhs, rhs, isEqual);
}

void tpunit_report_diagnostic(const string& message) {
    _TestFixture* fixture = currentTestPtr;
    lock_guard<recursive_mutex> lock(*fixture->_mutex);
    fixture->_outputWriter->diagnostic(fixture->name() ? fixture->name() : "", currentMethodName, message);
}
}

void tpunit::_TestFixture::tpunit_detail_assert(_TestFixture* f, const char* _file, int _line) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    f->_outputWriter->assertionFailed(f->_name ? f->_name : "", currentMethodName,
                                      ++f->_stats._assertions, _file, _line, f->takeTestBuffer());
}

void tpunit::_TestFixture::tpunit_detail_exception(_TestFixture* f, method* _method, const char* _message) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    f->_outputWriter->exceptionCaught(f->_name ? f->_name : "", currentMethodName,
                                      ++f->_stats._exceptions, _method->_name, _message, f->takeTestBuffer());
}

void tpunit::_TestFixture::tpunit_detail_trace(_TestFixture* f, const char* _file, int _line, const char* _message) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    f->_outputWriter->trace(f->_name ? f->_name : "", currentMethodName,
                            ++f->_stats._traces, _file, _line, _message, f->takeTestBuffer());
}

void tpunit::_TestFixture::tpunit_detail_do_method(tpunit::_TestFixture::method* m) {
    currentTestPtr = m->_this;
    currentMethodName = m->_name;
    try {
       // If we're exiting, then don't try and run any more tests.
       if (exitFlag) {
            throw ShutdownException();
       }
       (*m->_this.*m->_addr)();
    } catch(const std::exception& e) {
        lock_guard<recursive_mutex> lock(*(m->_this->_mutex));
       tpunit_detail_exception(m->_this, m, e.what());
    } catch(const char* e) {
        lock_guard<recursive_mutex> lock(*(m->_this->_mutex));
       tpunit_detail_exception(m->_this, m, e);
    } catch(ShutdownException se) {
       // Just re-throw, this exception is special and indicates that a test wants its thread to quit.
       throw;
    } catch(...) {
        lock_guard<recursive_mutex> lock(*(m->_this->_mutex));
       tpunit_detail_exception(m->_this, m, "caught unknown exception type");
    }
}

void tpunit::_TestFixture::tpunit_detail_do_methods(tpunit::_TestFixture::method* m) {
    while (m) {
       tpunit_detail_do_method(m);
       m = m->_next;
    }
}

void tpunit::_TestFixture::tpunit_detail_do_tests(_TestFixture* f) {
    method* t = f->_tests;
    list<thread> testThreads;
    while(t) {
        testThreads.push_back(thread([t, f]() {
            recursive_mutex& m = *(f->_mutex);
            currentTestName = f->_name;
            currentTestPtr = f;
            f->_stats._assertions = 0;
            f->_stats._exceptions = 0;
            f->testOutputBuffer = "";
            {
                lock_guard<recursive_mutex> lock(m);
                f->_outputWriter->testStarted(f->_name ? f->_name : "", t->_name);
            }
            auto start = chrono::steady_clock::now();
            tpunit_detail_do_methods(f->_befores);
            tpunit_detail_do_method(t);
            tpunit_detail_do_methods(f->_afters);
            auto end = chrono::steady_clock::now();
            lock_guard<recursive_mutex> lock(m);
            bool passed = !f->_stats._assertions && !f->_stats._exceptions;
            f->_outputWriter->testFinished(f->_name ? f->_name : "", t->_name, passed,
                                            chrono::duration_cast<chrono::milliseconds>(end - start),
                                            passed ? "" : f->takeTestBuffer());
            if (passed) {
                tpunit_detail_stats()._passes++;
            } else {
                tpunit_detail_stats()._failures++;
                tpunit_detail_stats()._failureNames.emplace(t->_name);
            }
        }));
        if (!f->_parallel) {
            // If it's not set to parallel, we finish each test in order.
            testThreads.front().join();
            testThreads.clear();
        }
        t = t->_next;
    }
    for (auto& thread : testThreads) {
        thread.join();
    }
}

void tpunit::_TestFixture::TESTINFO(const string& newLog) {
    lock_guard<recursive_mutex> lock(*(_mutex));

    // Format the buffer with an indent as we print it out.
    testOutputBuffer += "    " + newLog + "\n";
}

string tpunit::_TestFixture::takeTestBuffer() {
    return std::exchange(testOutputBuffer, ""s);
}

tpunit::_TestFixture::stats& tpunit::_TestFixture::tpunit_detail_stats() {
    static stats _stats;
    return _stats;
}

list<tpunit::_TestFixture*>* tpunit::_TestFixture::tpunit_detail_fixture_list() {
    static list<_TestFixture*>* _fixtureList = new list<_TestFixture*>;
    return _fixtureList;
}

int tpunit::Tests::run(int threads, std::function<void()> threadInitFunction, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction, OutputWriter* writer) {
    return _TestFixture::tpunit_detail_do_run(threads, threadInitFunction, sortFunction, writer);
}

int tpunit::Tests::run(const set<string>& include, const set<string>& exclude,
                       const list<string>& before, const list<string>& after, int threads, std::function<void()> threadInitFunction, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction, OutputWriter* writer) {
    return _TestFixture::tpunit_detail_do_run(include, exclude, before, after, threads, threadInitFunction, sortFunction, writer);
}

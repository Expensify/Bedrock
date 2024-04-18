#include "tpunit++.hpp"
#include <string.h>
#include <iostream>
#include <regex>
#include <chrono>
using namespace tpunit;

bool tpunit::TestFixture::exitFlag = false;
thread_local string tpunit::currentTestName;
thread_local tpunit::TestFixture* tpunit::currentTestPtr = nullptr;
thread_local mutex tpunit::currentTestNameMutex;

thread_local int tpunit::TestFixture::perFixtureStats::_assertions = 0;
thread_local int tpunit::TestFixture::perFixtureStats::_exceptions = 0;
thread_local int tpunit::TestFixture::perFixtureStats::_traces = 0;

tpunit::TestFixture::method::method(TestFixture* obj, void (TestFixture::*addr)(), const char* name, unsigned char type)
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

tpunit::TestFixture::method::~method() {
    delete _next;
}

tpunit::TestFixture::stats::stats()
    : _failures(0)
    , _passes(0)
    {}

tpunit::TestFixture::perFixtureStats::perFixtureStats()
{
}

tpunit::TestFixture::TestFixture(method* m0,  method* m1,  method* m2,  method* m3,  method* m4,
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
                         const char* name, bool parallel)
  : _name(name),
    _parallel(parallel)
{
    tpunit_detail_fixture_list()->push_back(this);

    // DO NOT modify this over 70, you're holding it wrong if you do.
    // Split your test suites/files if you need to!
    method* methods[70] = { m0,  m1,  m2,  m3,  m4,  m5,  m6,  m7,  m8,  m9,
                            m10, m11, m12, m13, m14, m15, m16, m17, m18, m19,
                            m20, m21, m22, m23, m24, m25, m26, m27, m28, m29,
                            m30, m31, m32, m33, m34, m35, m36, m37, m38, m39,
                            m40, m41, m42, m43, m44, m45, m46, m47, m48, m49,
                            m50, m51, m52, m53, m54, m55, m56, m57, m58, m59,
                            m60, m61, m62, m63, m64, m65, m66, m67, m68, m69 };

    for(int i = 0; i < 70; i++) {
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
    delete _afters;
    delete _after_classes;
    delete _befores;
    delete _before_classes;
    delete _tests;
}

int tpunit::TestFixture::tpunit_detail_do_run(int threads, std::function<void()> threadInitFunction) {
    const std::set<std::string> include, exclude;
    const std::list<std::string> before, after;
    return tpunit_detail_do_run(include, exclude, before, after, threads, threadInitFunction);
}

void tpunit::TestFixture::tpunit_run_test_class(TestFixture* f) {
   f->_stats._assertions = 0;
   f->_stats._exceptions = 0;
   tpunit_detail_do_methods(f->_before_classes);
   if (f->_stats._assertions || f->_stats._exceptions) {
      tpunit_detail_stats()._failures++;
      tpunit_detail_stats()._failureNames.emplace(f->_name + "::BEFORE_CLASS"s);
      cout << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C initializing " << f->_name << ". Skipping tests." << endl;
   } else {
       tpunit_detail_do_tests(f);
   }
   f->_stats._assertions = 0;
   f->_stats._exceptions = 0;
   tpunit_detail_do_methods(f->_after_classes);
   if (f->_stats._assertions || f->_stats._exceptions) {
      tpunit_detail_stats()._failures++;
      tpunit_detail_stats()._failureNames.emplace(f->_name + "::AFTER_CLASS"s);
      cout << "\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C cleaning up " << f->_name << "." << endl;
   }
}

int tpunit::TestFixture::tpunit_detail_do_run(const set<string>& include, const set<string>& exclude,
                                              const list<string>& before, const list<string>& after, int threads,
                                              std::function<void()> threadInitFunction) {
   threadInitFunction();
    /*
    * Run specific tests by name. If 'include' is empty, then every test is
    * run unless it's in 'exclude'. If 'include' has at least one entry,
    * then only tests in 'include' are run, and 'exclude' is ignored.
    */
    std::list<TestFixture*> testFixtureList = *tpunit_detail_fixture_list();
    testFixtureList.sort([&](TestFixture* a, TestFixture* b) {
        if (a->_name && b->_name) {
            return strcmp(a->_name, b->_name) < 0;
        }
        return false;
    });

    // Make local, mutable copies of the include and exclude lists.
    set<string> _include = include;
    set<string> _exclude = exclude;

    // Create a list of threads, and have them each pull tests of the queue.
    list<thread> threadList;
    recursive_mutex m;

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
               printf("--------------\n");
               tpunit_run_test_class(fixture);

               continue; // Don't bother checking the rest of the tests.
            }
        }
    }

    // And exclude our `after` tests so they don't get run in the main loop.
    for (auto name : after) {
        _exclude.insert(name);
    }

    list<TestFixture*> afterTests;
    mutex testTimeLock;
    multimap<chrono::milliseconds, string> testTimes;

    for (int threadID = 0; threadID < threads; threadID++) {
        // Capture everything by reference except threadID, because we don't want it to be incremented for the
        // next thread in the loop.
        thread t = thread([&, threadID]{
           auto start = chrono::steady_clock::now();

           threadInitFunction();
            try {
                // Do test.
                while (1) {
                    TestFixture* f = 0;
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
                                        break;
                                    }
                                } catch (const regex_error& e) {
                                    cout << "Invalid pattern: " << includedName << ", skipping." << endl;
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
                                cout << "Invalid pattern: " << excludedName << ", skipping." << endl;
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
                   if (!f->_multiThreaded) {
                       printf("--------------\n");
                   }
                   {
                       lock_guard<mutex> lock(currentTestNameMutex);
                       currentTestPtr = f;
                       if (f->_name) {
                           currentTestName = f->_name;
                       } else {
                           cout << "test has no name???" << endl;
                           currentTestName = "UNSPECIFIED";
                       }
                   }

                   tpunit_run_test_class(f);
                }
            } catch (ShutdownException se) {
                // This will have broken us out of our main loop, so we'll just exit. We also set the exit flag to let
                // other threads know we're trying to exit.
                lock_guard<recursive_mutex> lock(m);
                exitFlag = true;
                printf("Thread %d caught shutdown exception, exiting.\n", threadID);
            }
            auto end = chrono::steady_clock::now();
            if (currentTestName.size()) {
                lock_guard<mutex> lock(testTimeLock);
                testTimes.emplace(make_pair(chrono::duration_cast<std::chrono::milliseconds>(end - start), currentTestName));
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
               printf("--------------\n");
               tpunit_run_test_class(fixture);

               continue; // Don't bother checking the rest of the tests.
            }
        }
    }

    if (!exitFlag) {
        printf("\n[ TEST RESULTS ] Passed: %i, Failed: %i\n", tpunit_detail_stats()._passes, tpunit_detail_stats()._failures);
        if (tpunit_detail_stats()._failureNames.size()) {
            printf("\nFailures:\n");
            for (const auto& failure : tpunit_detail_stats()._failureNames) {
                printf("%s\n", failure.c_str());
            }
        }

        cout << endl;
        cout << "Slowest Test Classes: " << endl;
        auto it = testTimes.rbegin();
        for (size_t i = 0; i < 10; i++) {
            if (it == testTimes.rend()) {
                break;
            }
            cout << it->first << ": " << it->second << endl;
            it++;
        }

        return tpunit_detail_stats()._failures;
    }
    return 1;
}

bool tpunit::TestFixture::tpunit_detail_fp_equal(float lhs, float rhs, unsigned char ulps) {
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

bool tpunit::TestFixture::tpunit_detail_fp_equal(double lhs, double rhs, unsigned char ulps) {
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

void tpunit::TestFixture::tpunit_detail_assert(TestFixture* f, const char* _file, int _line) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    printf("   assertion #%i at %s:%i\n", ++f->_stats._assertions, _file, _line);
    f->printTestBuffer();
}

void tpunit::TestFixture::tpunit_detail_exception(TestFixture* f, method* _method, const char* _message) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    printf("   exception #%i from %s with cause: %s\n", ++f->_stats._exceptions, _method->_name, _message);
    f->printTestBuffer();
}

void tpunit::TestFixture::tpunit_detail_trace(TestFixture* f, const char* _file, int _line, const char* _message) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    printf("   trace #%i at %s:%i: %s\n", ++f->_stats._traces, _file, _line, _message);
    f->printTestBuffer();
}

void tpunit::TestFixture::tpunit_detail_do_method(tpunit::TestFixture::method* m) {
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

void tpunit::TestFixture::tpunit_detail_do_methods(tpunit::TestFixture::method* m) {
    while (m) {
       tpunit_detail_do_method(m);
       m = m->_next;
    }
}

void tpunit::TestFixture::tpunit_detail_do_tests(TestFixture* f) {
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
            auto start = chrono::steady_clock::now();
            tpunit_detail_do_methods(f->_befores);
            tpunit_detail_do_method(t);
            tpunit_detail_do_methods(f->_afters);
            auto end = chrono::steady_clock::now();
            stringstream timeStream;
            timeStream << "(" << chrono::duration_cast<std::chrono::milliseconds>(end - start);
            if (chrono::duration_cast<std::chrono::milliseconds>(end - start) > 5000ms) {
                timeStream << " \xF0\x9F\x90\x8C";
            }
            timeStream << ")";
            string timeStr = timeStream.str();
            const char* time = timeStr.c_str();

            // No new assertions or exceptions. This not currently synchronized correctly. They can cause tests that
            // passed to appear failed when another test failed while this test was running. They cannot cause failed
            // tests to appear to have passed.
            if(!f->_stats._assertions && !f->_stats._exceptions) {
                lock_guard<recursive_mutex> lock(m);
                printf("\xE2\x9C\x85 %s %s\n", t->_name, time);
                tpunit_detail_stats()._passes++;
            } else {
                lock_guard<recursive_mutex> lock(m);

                // Dump the test buffer if the test included any log lines.
                f->printTestBuffer();
                printf("\xE2\x9D\x8C !FAILED! \xE2\x9D\x8C %s %s\n", t->_name, time);
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

void tpunit::TestFixture::TESTINFO(const string& newLog) {
    lock_guard<recursive_mutex> lock(*(_mutex));

    // Format the buffer with an indent as we print it out.
    testOutputBuffer += "    " + newLog + "\n";
}

void tpunit::TestFixture::printTestBuffer() {
    lock_guard<recursive_mutex> lock(*(_mutex));

    cout << testOutputBuffer;
    testOutputBuffer = "";
}

tpunit::TestFixture::stats& tpunit::TestFixture::tpunit_detail_stats() {
    static stats _stats;
    return _stats;
}

list<tpunit::TestFixture*>* tpunit::TestFixture::tpunit_detail_fixture_list() {
    static list<TestFixture*>* _fixtureList = new list<TestFixture*>;
    return _fixtureList;
}

int tpunit::Tests::run(int threads, std::function<void()> threadInitFunction) {
    return TestFixture::tpunit_detail_do_run(threads, threadInitFunction);
}

int tpunit::Tests::run(const set<string>& include, const set<string>& exclude,
                       const list<string>& before, const list<string>& after, int threads, std::function<void()> threadInitFunction) {
    return TestFixture::tpunit_detail_do_run(include, exclude, before, after, threads, threadInitFunction);
}

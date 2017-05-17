#include "tpunit++.hpp"
#include <string.h>
using namespace tpunit;

bool tpunit::TestFixture::exitFlag = false;

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
    : _assertions(0)
    , _exceptions(0)
    , _traces(0)
    {}

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
                         const char* name)
  : _name(name)
{
    tpunit_detail_fixture_list()->push_back(this);

    method* methods[50] = { m0,  m1,  m2,  m3,  m4,  m5,  m6,  m7,  m8,  m9,
                            m10, m11, m12, m13, m14, m15, m16, m17, m18, m19,
                            m20, m21, m22, m23, m24, m25, m26, m27, m28, m29,
                            m30, m31, m32, m33, m34, m35, m36, m37, m38, m39,
                            m40, m41, m42, m43, m44, m45, m46, m47, m48, m49 };

    for(int i = 0; i < 50; i++) {
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
    _name     = 0;
    _mutex    = 0;
}

tpunit::TestFixture::~TestFixture() {
    delete _afters;
    delete _after_classes;
    delete _befores;
    delete _before_classes;
    delete _tests;
}

int tpunit::TestFixture::tpunit_detail_do_run(int threads) {
    const std::set<std::string> include, exclude;
    const std::list<std::string> before, after;
    return tpunit_detail_do_run(include, exclude, before, after, threads);
}

int tpunit::TestFixture::tpunit_detail_do_run(const set<string>& include, const set<string>& exclude,
                                              const list<string>& before, const list<string>& after, int threads) {
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
               printf("[--------------]\n");
               tpunit_detail_do_methods(fixture->_before_classes);
               tpunit_detail_do_tests(fixture);
               tpunit_detail_do_methods(fixture->_after_classes);
               printf("[--------------]\n\n");

               continue; // Don't bother checking the rest of the tests.
            }
        }
    }

    // And exclude our `after` tests so they don't get run in the main loop.
    for (auto name : after) {
        _exclude.insert(name);
    }

    list<TestFixture*> afterTests;

    for (int threadID = 0; threadID < threads; threadID++) {
        // Capture everything by reference except threadID, because we don't want it to be incremented for the
        // next thread in the loop.
        thread t = thread([&, threadID]{
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
                    bool should_run = true;
                    if (_include.size()) {
                       if (!f->_name || (_include.find(std::string(f->_name)) == _include.end())) {
                          should_run = false;
                       }
                    }
                    else if (f->_name && (_exclude.find(std::string(f->_name)) != _exclude.end())) {
                       should_run = false;
                    }

                    // Try the next test.
                    if (!should_run) {
                        // Put in the after list, in case we want to run it there.
                        lock_guard<recursive_mutex> lock(m);
                        afterTests.push_back(f);
                        continue;
                    }

                   // At this point, we know this test should run.
                   if (!f->_multiThreaded) {
                       printf("[--------------]\n");
                   }
                   tpunit_detail_do_methods(f->_before_classes);
                   tpunit_detail_do_tests(f);
                   tpunit_detail_do_methods(f->_after_classes);
                   if (!f->_multiThreaded) {
                       printf("[--------------]\n\n");
                   }
                }
            } catch (ShutdownException se) {
                // This will have broken us out of our main loop, so we'll just exit. We also set the exit flag to let
                // other threads know we're trying to exit.
                lock_guard<recursive_mutex> lock(m);
                exitFlag = true;
                printf("Thread %d caught shutdown exception, exiting.\n", threadID);
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
               printf("[--------------]\n");
               tpunit_detail_do_methods(fixture->_before_classes);
               tpunit_detail_do_tests(fixture);
               tpunit_detail_do_methods(fixture->_after_classes);
               printf("[--------------]\n\n");

               continue; // Don't bother checking the rest of the tests.
            }
        }
    }

    if (!exitFlag) {
        printf("[==============]\n");
        printf("[ TEST RESULTS ] Passed: %i, Failed: %i\n", tpunit_detail_stats()._passes, tpunit_detail_stats()._failures);
        printf("[==============]\n");
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

// TODO: These three functions need to be updated to act on the current TestFixture.
void tpunit::TestFixture::tpunit_detail_assert(TestFixture* f, const char* _file, int _line) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    printf("[              ]    assertion #%i at %s:%i\n", ++f->_stats._assertions, _file, _line);
}

void tpunit::TestFixture::tpunit_detail_exception(TestFixture* f, method* _method, const char* _message) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    printf("[              ]    exception #%i from %s with cause: %s\n", ++f->_stats._exceptions, _method->_name, _message);
}

void tpunit::TestFixture::tpunit_detail_trace(TestFixture* f, const char* _file, int _line, const char* _message) {
    lock_guard<recursive_mutex> lock(*(f->_mutex));
    printf("[              ]    trace #%i at %s:%i: %s\n", ++f->_stats._traces, _file, _line, _message);
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
    recursive_mutex& m = *(f->_mutex);
    while(t) {
       int _prev_assertions = f->_stats._assertions;
       int _prev_exceptions = f->_stats._exceptions;
       if (!f->_multiThreaded) {
           printf("[ RUN          ] %s\n", t->_name);
       }
       tpunit_detail_do_methods(f->_befores);
       tpunit_detail_do_method(t);
       tpunit_detail_do_methods(f->_afters);
       if(_prev_assertions == f->_stats._assertions && _prev_exceptions == f->_stats._exceptions) {
          lock_guard<recursive_mutex> lock(m);
          printf("[       PASSED ] %s\n", t->_name);
          tpunit_detail_stats()._passes++;
       } else {
          lock_guard<recursive_mutex> lock(m);
          printf("[       FAILED ] %s\n", t->_name);
          tpunit_detail_stats()._failures++;
       }
       t = t->_next;
    }
}

tpunit::TestFixture::stats& tpunit::TestFixture::tpunit_detail_stats() {
    static stats _stats;
    return _stats;
}

list<tpunit::TestFixture*>* tpunit::TestFixture::tpunit_detail_fixture_list() {
    static list<TestFixture*>* _fixtureList = new list<TestFixture*>;
    return _fixtureList;
}

int tpunit::Tests::run(int threads) {
    return TestFixture::tpunit_detail_do_run(threads);
}

int tpunit::Tests::run(const set<string>& include, const set<string>& exclude,
                       const list<string>& before, const list<string>& after, int threads) {
    return TestFixture::tpunit_detail_do_run(include, exclude, before, after, threads);
}

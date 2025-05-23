/**
 * Copyright (c) 2011-2015 Trevor Pounds <trevor.pounds@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

#include <cstdio>
#include <set>
#include <string>
#include <list>
#include <thread>
#include <mutex>
#include <algorithm>
#include <functional>
#include <atomic>
#include <test/lib/PrintEquality.h>
using namespace std;

/**
 * This version string has been updated to MAJOR_VERSION 2 by Expensify, as this library has been forked and
 * significantly diverged from the original library. This is basically our own code at that point.
 * In the future, we may even rename this library to avoid confusion.
 *
 * TPUNITPP_VERSION macro contains an integer represented by
 * the value (M*1000000 + N*1000 + P) where M is the major
 * version, N is the minor version, and P is the patch version.
 *
 * TPUNITPP_VERSION_MAJOR is an integer of the major version.
 * TPUNITPP_VERSION_MINOR is an integer of the minor version.
 * TPUNITPP_VERSION_PATCH is an integer of the patch version.
 */
#define TPUNITPP_VERSION 2002001
#define TPUNITPP_VERSION_MAJOR 2
#define TPUNITPP_VERSION_MINOR 2
#define TPUNITPP_VERSION_PATCH 1

/**
 * ABORT(); generates a failure, immediately returning from the
 * currently executing test function.
 * FAIL(); generates a failure, allowing the currently executing
 * test function to continue.
 * PASS(); does nothing, effectively considered a NOP but may be
 * useful for annotating test cases with their desired intent.
 * TRACE(message); adds a trace to the test output with a user
 * specified string message.
 */
#define ABORT() tpunit::_TestFixture::tpunit_detail_assert(tpunit::currentTestPtr, __FILE__, __LINE__); return;
#define FAIL()  tpunit::_TestFixture::tpunit_detail_assert(tpunit::currentTestPtr, __FILE__, __LINE__);
#define PASS()  /* do nothing */
#define TRACE(message) tpunit::_TestFixture::tpunit_detail_trace(tpunit::currentTestPtr, __FILE__, __LINE__, message);

/**
 * The set of core macros for basic predicate testing of boolean
 * expressions and value comparisons.
 *
 * ASSERT_*(...); generates a failure, immediately returning from
 * the currently executing test function if the supplied predicate
 * is not satisfied.
 * EXPECT_*(...); generates a failure, allowing the currently
 * executing test function to continue if the supplied predicate
 * is not satisified.
 */
#define ASSERT_TRUE(condition) if(condition) { PASS(); } else { TESTINFO("assertion failed: " #condition " resolved to FALSE"); ABORT(); }
#define EXPECT_TRUE(condition) if(condition) { PASS(); } else { TESTINFO("expect failed: " #condition " resolved to FALSE"); FAIL(); }
#define ASSERT_FALSE(condition) if(condition) { TESTINFO("assertion failed: " #condition " resolved to TRUE"); ABORT(); } else { PASS(); }
#define EXPECT_FALSE(condition) if(condition) { TESTINFO("expect failed: " #condition " resolved to TRUE"); FAIL(); } else { PASS(); }
#define ASSERT_EQUAL(lhs, rhs) if((lhs) == (rhs)) { PASS(); } else { PrintEquality(lhs, rhs, false); ABORT(); }
#define EXPECT_EQUAL(lhs, rhs) if((lhs) == (rhs)) { PASS(); } else { PrintEquality(lhs, rhs, false); FAIL(); }
#define ASSERT_NOT_EQUAL(lhs, rhs) if((lhs) != (rhs)) { PASS(); } else { PrintEquality(lhs, rhs, true); ABORT(); }
#define EXPECT_NOT_EQUAL(lhs, rhs) if((lhs) != (rhs)) { PASS(); } else { PrintEquality(lhs, rhs, true); FAIL(); }
#define ASSERT_GREATER_THAN(lhs, rhs) if((lhs) > (rhs)) { PASS(); } else { TESTINFO("incorrectly asserted " + to_string(lhs) + " > " + to_string(rhs)); ABORT(); }
#define EXPECT_GREATER_THAN(lhs, rhs) if((lhs) > (rhs)) { PASS(); } else { TESTINFO("incorrectly expected " + to_string(lhs) + " > " + to_string(rhs)); FAIL(); }
#define ASSERT_GREATER_THAN_EQUAL(lhs, rhs) if((lhs) >= (rhs)) { PASS(); } else { TESTINFO("incorrectly asserted " + to_string(lhs) + " >= " + to_string(rhs)); ABORT(); }
#define EXPECT_GREATER_THAN_EQUAL(lhs, rhs) if((lhs) >= (rhs)) { PASS(); } else { TESTINFO("incorrectly expected " + to_string(lhs) + " >= " + to_string(rhs)); FAIL(); }
#define ASSERT_LESS_THAN(lhs, rhs) if((lhs) < (rhs)) { PASS(); } else { TESTINFO("incorrectly asserted " + to_string(lhs) + " < " + to_string(rhs)); ABORT(); }
#define EXPECT_LESS_THAN(lhs, rhs) if((lhs) < (rhs)) { PASS(); } else { TESTINFO("incorrectly expected " + to_string(lhs) + " < " + to_string(rhs)); FAIL(); }
#define ASSERT_LESS_THAN_EQUAL(lhs, rhs) if((lhs) <= (rhs)) { PASS(); } else { TESTINFO("incorrectly asserted " + to_string(lhs) + " <= " + to_string(rhs)); ABORT(); }
#define EXPECT_LESS_THAN_EQUAL(lhs, rhs) if((lhs) <= (rhs)) { PASS(); } else { TESTINFO("incorrectly expected " + to_string(lhs) + " <= " + to_string(rhs)); FAIL(); }

/**
 * The set of floating-point macros used to compare double/float values.
 *
 * ASSERT|EXPECT_FLOAT_EQUAL(lhs, rhs); generates a failure if the given
 * floating-point values are not within 4 ULPs of each other.
 * ASSERT|EXPECT_FLOAT_NEAR(lhs, rhs, abs_error); generates a failure if
 * the given floating-point values exceed the absolute error.
 */
#define ASSERT_FLOAT_EQUAL(lhs, rhs) if(tpunit_detail_fp_equal(lhs, rhs, 4)) { PASS(); } else { PrintEquality(lhs, rhs, false); ABORT(); }
#define EXPECT_FLOAT_EQUAL(lhs, rhs) if(tpunit_detail_fp_equal(lhs, rhs, 4)) { PASS(); } else { PrintEquality(lhs, rhs, false); FAIL(); }
#define ASSERT_FLOAT_NEAR(lhs, rhs, abs_error) if((((lhs) > (rhs)) ? (lhs) - (rhs) : (rhs) - (lhs)) <= (abs_error)) { PASS(); } else { TESTINFO("incorrectly asserted " + SToStr(lhs) + " near " + SToStr(rhs)); ABORT(); }
#define EXPECT_FLOAT_NEAR(lhs, rhs, abs_error) if((((lhs) > (rhs)) ? (lhs) - (rhs) : (rhs) - (lhs)) <= (abs_error)) { PASS(); } else { TESTINFO("incorrectly expected " + SToStr(lhs) + " near " + SToStr(rhs)); FAIL(); }

/**
 * The set of macros for checking whether a statement will throw or not
 * throw an exception. Note, the checked exception macros will generally
 * not work with compilers that do not support exceptions or have them
 * explicitly turned off using a compiler flag (e.g. -fno-exceptions).
 *
 * ASSERT|EXPECT_THROW(statement, exception); generates a failure if
 * the given statement does not throw the supplied excetion.
 * ASSERT|EXPECT_NO_THROW(statement, exception); generates a failure
 * if the given statement throws any exception. Useful for ensuring
 * a statement never throws an exception.
 * ASSERT|EXPECT_ANY_THROW(statement); generates a failure if the
 * given statement does not throw any exceptions.
 */
#define ASSERT_THROW(statement, exception) try { statement; ABORT(); } catch(const exception&) { PASS(); } catch(...) { ABORT(); }
#define EXPECT_THROW(statement, exception) try { statement; FAIL(); } catch(const exception&) { PASS(); } catch(...) { FAIL(); }
#define ASSERT_NO_THROW(statement) try { statement; PASS(); } catch(const exception& e) { cout << "Something threw: " << e.what() << endl; ABORT(); } catch(const char* e) { cout << "Something threw: " << e << endl; ABORT(); } catch(...) { cout << "Something threw, unsure what..." << endl; ABORT(); }
#define EXPECT_NO_THROW(statement) try { statement; PASS(); } catch(const exception& e) { cout << "Something threw: " << e.what() << endl; FAIL(); } catch(const char* e) { cout << "Something threw: " << e << endl; FAIL(); } catch(...) { cout << "Something threw, unsure what..." << endl; FAIL(); }
#define ASSERT_ANY_THROW(statement) try { statement; ABORT(); } catch(...) { PASS(); }
#define EXPECT_ANY_THROW(statement) try { statement; FAIL(); } catch(...) { PASS(); }

/**
 * A macro that can be used to check whether an input matches acceptable
 * values. A matcher implementation is a simple type containing a single
 * boolean function that is applied to the input. The match is considered
 * successful if the function returns true and unsuccessful if it returns
 * false.
 *
 * e.g.
 *
 *    struct AlwaysMatches {
 *       template <typename T>
 *       bool matches(T) { return true; }
 *    };
 *
 *    struct NeverMatches {
 *       template <typename T>
 *       bool matches(T) { return false; }
 *    };
 *
 * ASSERT|EXPECT_THAT(obj, matcher); fail if the matcher evaluates to false.
 */
#define ASSERT_THAT(obj, matcher) if(matcher.matches(obj)) { PASS(); } else { ABORT(); }
#define EXPECT_THAT(obj, matcher) if(matcher.matches(obj)) { PASS(); } else { FAIL(); }

/**
 * The set of convenience macros for registering functions with the test
 * fixture.
 *
 * AFTER(function); registers a function to run once after each subsequent
 * test function within a test fixture.
 * AFTER_CLASS(function); registers a function to run once after all test
 * functions within a test fixture. Useful for cleaning up shared state
 * used by all test functions.
 * BEFORE(function); registers a function to run once before each subsequent
 * test function within a test fixture.
 * BEFORE_CLASS(function); registers a function to run once before all test
 * functions within a test fixture. Useful for initializing shared state
 * used by all test functions.
 * TEST(function); registers a function to run as a test within a test fixture.
 * NAME(testName); Sets a name for the test so it can be included/excluded.
 */
#define AFTER(M)        Method(&M, #M, method::AFTER_METHOD)
#define AFTER_CLASS(M)  Method(&M, #M, method::AFTER_CLASS_METHOD)
#define BEFORE(M)       Method(&M, #M, method::BEFORE_METHOD)
#define BEFORE_CLASS(M) Method(&M, #M, method::BEFORE_CLASS_METHOD)
#define TEST(M)         Method(&M, #M, method::TEST_METHOD)
#define NAME(M)         _name = (#M)

/**
 * Try our best to detect compiler support for exception handling so
 * we can catch and report any unhandled exceptions as normal failures.
 */
#ifndef TPUNITPP_HAS_EXCEPTIONS
   #if defined(__EXCEPTIONS) || defined(_CPPUNWIND)
      #include <exception>
      #define TPUNITPP_HAS_EXCEPTIONS 1
   #endif
#endif

namespace tpunit {
    // Make the current test name in the current thread globally accessible.
    extern thread_local string currentTestName;
    extern thread_local mutex currentTestNameMutex;

    // Doesn't do anything except allow us to detect when the program wants to shutdown.
    class ShutdownException{};

   /**
    * The primary class that provides the integration point for creating user
    * defined test cases. To get started one only needs to derive from _TestFixture,
    * define a few test methods and register them with the base constructor.
    */
   class _TestFixture {
      public:

         static std::atomic<bool> exitFlag;

         struct perFixtureStats {
            perFixtureStats();

            static thread_local int _assertions;
            static thread_local int _exceptions;
            static thread_local int _traces;
         };

         perFixtureStats  _stats;
         recursive_mutex* _mutex;
         int _threadID;

         static bool sorter(_TestFixture* a, _TestFixture* b);

      protected:
         // Test buffer for printing to stdout if a test were to fail.
         string testOutputBuffer;

         /**
          * Internal class encapsulating a registered test method.
          */
         struct method {
            method(_TestFixture* obj, void (_TestFixture::*addr)(), const char* name, unsigned char type);

            ~method();

            _TestFixture* _this;
            void (_TestFixture::*_addr)();
            char _name[1024];

            enum {
               AFTER_METHOD,  AFTER_CLASS_METHOD,
               BEFORE_METHOD, BEFORE_CLASS_METHOD,
               TEST_METHOD
            };
            unsigned char _type;

            method* _next = nullptr;
         };

         /**
          * Internal class encapsulating test statistics.
          */
         struct stats {
            stats();

            int _failures;
            int _passes;
            set<string> _failureNames;
         };

      public:

         _TestFixture(const char* name, bool parallel);

         void registerTests(method* m0,       method* m1 = 0, method* m2  = 0, method* m3  = 0, method* m4  = 0,
                            method* m5  = 0, method* m6  = 0, method* m7  = 0, method* m8  = 0, method* m9  = 0,
                            method* m10 = 0, method* m11 = 0, method* m12 = 0, method* m13 = 0, method* m14 = 0,
                            method* m15 = 0, method* m16 = 0, method* m17 = 0, method* m18 = 0, method* m19 = 0,
                            method* m20 = 0, method* m21 = 0, method* m22 = 0, method* m23 = 0, method* m24 = 0,
                            method* m25 = 0, method* m26 = 0, method* m27 = 0, method* m28 = 0, method* m29 = 0,
                            method* m30 = 0, method* m31 = 0, method* m32 = 0, method* m33 = 0, method* m34 = 0,
                            method* m35 = 0, method* m36 = 0, method* m37 = 0, method* m38 = 0, method* m39 = 0,
                            method* m40 = 0, method* m41 = 0, method* m42 = 0, method* m43 = 0, method* m44 = 0,
                            method* m45 = 0, method* m46 = 0, method* m47 = 0, method* m48 = 0, method* m49 = 0,
                            method* m50 = 0, method* m51 = 0, method* m52 = 0, method* m53 = 0, method* m54 = 0,
                            method* m55 = 0, method* m56 = 0, method* m57 = 0, method* m58 = 0, method* m59 = 0,
                            method* m60 = 0, method* m61 = 0, method* m62 = 0, method* m63 = 0, method* m64 = 0,
                            method* m65 = 0, method* m66 = 0, method* m67 = 0, method* m68 = 0, method* m69 = 0);

         ~_TestFixture();

         /**
          * Create a new method to register with the test fixture.
          *
          * @param[in] _method A method to register with the test fixture.
          * @param[in] _name The internal name of the method used when status messages are displayed.
          */
         template <typename C>
         method* Method(void (C::*_method)(), const char* _name, unsigned char _type) {
            return new method(this, static_cast<void (_TestFixture::*)()>(_method), _name, _type);
         }

         static int tpunit_detail_do_run(int threads = 1, std::function<void()> threadInitFunction = [](){}, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction = &_TestFixture::sorter);

         static int tpunit_detail_do_run(const std::set<std::string>& include, const std::set<std::string>& exclude,
                                         const std::list<std::string>& before, const std::list<std::string>& after, int threads,
                                         std::function<void()> threadInitFunction, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction = &_TestFixture::sorter);

         /**
          * This method writes to a temporary buffer and formats the message nicely for debugging
          * purposes if a test were to fail.
          */
         void TESTINFO(const string& newLog);

         static void tpunit_detail_assert(_TestFixture* f, const char* _file, int _line);

         static void tpunit_detail_exception(_TestFixture* f, method* _method, const char* _message);

         static void tpunit_detail_trace(_TestFixture* f, const char* _file, int _line, const char* _message);

         const char* name() {
            return _name;
         }

      protected:

         /**
          * Determine if two binary32 single precision IEEE 754 floating-point
          * numbers are equal using unit in the last place (ULP) analysis.
          *
          * http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm
          */
         static bool tpunit_detail_fp_equal(float lhs, float rhs, unsigned char ulps);

         /**
          * Determine if two binary64 double precision IEEE 754 floating-point
          * numbers are equal using unit in the last place (ULP) analysis.
          *
          * http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm
          */
         static bool tpunit_detail_fp_equal(double lhs, double rhs, unsigned char ulps);

         const char* _name;

         bool _parallel = false;

      private:

         static void tpunit_run_test_class(_TestFixture*);

         static void tpunit_detail_do_method(method* m);

         static void tpunit_detail_do_methods(method* m);

         static void tpunit_detail_do_tests(_TestFixture* f);

         static stats& tpunit_detail_stats();

         static std::list<_TestFixture*>* tpunit_detail_fixture_list();

         /**
          * Takes the test buffer and outputs it to cout
          */
         void printTestBuffer();

         method* _afters = nullptr;
         method* _after_classes = nullptr;
         method* _befores = nullptr;
         method* _before_classes = nullptr;
         method* _tests = nullptr;

         // True if running multithreaded.
         bool _multiThreaded;
   };

   class TestFixture : public _TestFixture {
public:
         // Use constructor delegation to add an optional default 'name' parameter that works as a first argument.
         // This lets us keep backwards compatibility with existing tests, and add a name to new tests without having
         // to add 70 '0's for a bunch of unused methods.
         // DO NOT ADD MORE THAN 70 METHODS TO A TEST FIXTURE. If you need more, split the test file into multiple files
         // so that you can run these tests more efficiently in parallel.
         TestFixture(const char* name,
                     method* m0,      method* m1  = 0, method* m2  = 0, method* m3  = 0, method* m4  = 0,
                     method* m5  = 0, method* m6  = 0, method* m7  = 0, method* m8  = 0, method* m9  = 0,
                     method* m10 = 0, method* m11 = 0, method* m12 = 0, method* m13 = 0, method* m14 = 0,
                     method* m15 = 0, method* m16 = 0, method* m17 = 0, method* m18 = 0, method* m19 = 0,
                     method* m20 = 0, method* m21 = 0, method* m22 = 0, method* m23 = 0, method* m24 = 0,
                     method* m25 = 0, method* m26 = 0, method* m27 = 0, method* m28 = 0, method* m29 = 0,
                     method* m30 = 0, method* m31 = 0, method* m32 = 0, method* m33 = 0, method* m34 = 0,
                     method* m35 = 0, method* m36 = 0, method* m37 = 0, method* m38 = 0, method* m39 = 0,
                     method* m40 = 0, method* m41 = 0, method* m42 = 0, method* m43 = 0, method* m44 = 0,
                     method* m45 = 0, method* m46 = 0, method* m47 = 0, method* m48 = 0, method* m49 = 0,
                     method* m50 = 0, method* m51 = 0, method* m52 = 0, method* m53 = 0, method* m54 = 0,
                     method* m55 = 0, method* m56 = 0, method* m57 = 0, method* m58 = 0, method* m59 = 0,
                     method* m60 = 0, method* m61 = 0, method* m62 = 0, method* m63 = 0, method* m64 = 0,
                     method* m65 = 0, method* m66 = 0, method* m67 = 0, method* m68 = 0, method* m69 = 0)
                : _TestFixture(name, false)
         {
                    registerTests(m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31, m32, m33, m34, m35, m36, m37, m38, m39, m40, m41, m42, m43, m44, m45, m46, m47, m48, m49, m50, m51, m52, m53, m54, m55, m56, m57, m58, m59, m60, m61, m62, m63, m64, m65, m66, m67, m68, m69);
         }

         TestFixture(bool parallel, const char* name,
                     method* m0,      method* m1  = 0, method* m2  = 0, method* m3  = 0, method* m4  = 0,
                     method* m5  = 0, method* m6  = 0, method* m7  = 0, method* m8  = 0, method* m9  = 0,
                     method* m10 = 0, method* m11 = 0, method* m12 = 0, method* m13 = 0, method* m14 = 0,
                     method* m15 = 0, method* m16 = 0, method* m17 = 0, method* m18 = 0, method* m19 = 0,
                     method* m20 = 0, method* m21 = 0, method* m22 = 0, method* m23 = 0, method* m24 = 0,
                     method* m25 = 0, method* m26 = 0, method* m27 = 0, method* m28 = 0, method* m29 = 0,
                     method* m30 = 0, method* m31 = 0, method* m32 = 0, method* m33 = 0, method* m34 = 0,
                     method* m35 = 0, method* m36 = 0, method* m37 = 0, method* m38 = 0, method* m39 = 0,
                     method* m40 = 0, method* m41 = 0, method* m42 = 0, method* m43 = 0, method* m44 = 0,
                     method* m45 = 0, method* m46 = 0, method* m47 = 0, method* m48 = 0, method* m49 = 0,
                     method* m50 = 0, method* m51 = 0, method* m52 = 0, method* m53 = 0, method* m54 = 0,
                     method* m55 = 0, method* m56 = 0, method* m57 = 0, method* m58 = 0, method* m59 = 0,
                     method* m60 = 0, method* m61 = 0, method* m62 = 0, method* m63 = 0, method* m64 = 0,
                     method* m65 = 0, method* m66 = 0, method* m67 = 0, method* m68 = 0, method* m69 = 0)
                : _TestFixture(name, parallel)
         {
            registerTests(m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31, m32, m33, m34, m35, m36, m37, m38, m39, m40, m41, m42, m43, m44, m45, m46, m47, m48, m49, m50, m51, m52, m53, m54, m55, m56, m57, m58, m59, m60, m61, m62, m63, m64, m65, m66, m67, m68, m69);
         }

         /**
          * Base constructor to register methods with the test fixture. A test
          * fixture can register up to 70 methods. DO NOT expand this beyond 70 tests.
          * Split your tests into more files/suites if you need more than 70 tests
          * so that you can run these tests more efficiently in parallel.
          *
          * @param[in] m0..m49 The methods to register with the test fixture.
          */
         TestFixture(method* m0,      method* m1  = 0, method* m2  = 0, method* m3  = 0, method* m4  = 0,
                     method* m5  = 0, method* m6  = 0, method* m7  = 0, method* m8  = 0, method* m9  = 0,
                     method* m10 = 0, method* m11 = 0, method* m12 = 0, method* m13 = 0, method* m14 = 0,
                     method* m15 = 0, method* m16 = 0, method* m17 = 0, method* m18 = 0, method* m19 = 0,
                     method* m20 = 0, method* m21 = 0, method* m22 = 0, method* m23 = 0, method* m24 = 0,
                     method* m25 = 0, method* m26 = 0, method* m27 = 0, method* m28 = 0, method* m29 = 0,
                     method* m30 = 0, method* m31 = 0, method* m32 = 0, method* m33 = 0, method* m34 = 0,
                     method* m35 = 0, method* m36 = 0, method* m37 = 0, method* m38 = 0, method* m39 = 0,
                     method* m40 = 0, method* m41 = 0, method* m42 = 0, method* m43 = 0, method* m44 = 0,
                     method* m45 = 0, method* m46 = 0, method* m47 = 0, method* m48 = 0, method* m49 = 0,
                     method* m50 = 0, method* m51 = 0, method* m52 = 0, method* m53 = 0, method* m54 = 0,
                     method* m55 = 0, method* m56 = 0, method* m57 = 0, method* m58 = 0, method* m59 = 0,
                     method* m60 = 0, method* m61 = 0, method* m62 = 0, method* m63 = 0, method* m64 = 0,
                     method* m65 = 0, method* m66 = 0, method* m67 = 0, method* m68 = 0, method* m69 = 0,
                     const char* name = 0, bool parallel = false)
                : _TestFixture(name, parallel)
         {
            registerTests(m0, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24, m25, m26, m27, m28, m29, m30, m31, m32, m33, m34, m35, m36, m37, m38, m39, m40, m41, m42, m43, m44, m45, m46, m47, m48, m49, m50, m51, m52, m53, m54, m55, m56, m57, m58, m59, m60, m61, m62, m63, m64, m65, m66, m67, m68, m69);
         }

         ~TestFixture();
   };

   extern thread_local tpunit::_TestFixture* currentTestPtr;

   /**
    * Convenience class containing the entry point to run all registered tests.
    */
   struct Tests {
      /**
       * Run all registered test cases and return the number of failed assertions.
       *
       * @return Number of failed assertions or zero if all tests pass.
       */
      static int run(int threads = 1, std::function<void()> threadInitFunction = [](){}, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction = &_TestFixture::sorter);

      /**
       * Run specific tests by name. If 'include' is empty, then every test is
       * run unless it's in 'exclude'. If 'include' has at least one entry,
       * then only tests in 'include' are run, and 'exclude' is ignored.
       *
       * @return Number of failed assertions or zero if all tests pass.
       */
      static int run(const std::set<std::string>& include, const std::set<std::string>& exclude,
                     const std::list<std::string>& before, const std::list<std::string>& after, int threads = 1,
                     std::function<void()> threadInitFunction = [](){}, std::function<bool(_TestFixture*, _TestFixture*)> sortFunction = &_TestFixture::sorter);
   };
}

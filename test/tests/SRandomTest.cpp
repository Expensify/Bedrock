#include <libstuff/SRandom.h>
#include <test/lib/tpunit++.hpp>

#include <array>
#include <cstdint>
#include <latch>
#include <limits>
#include <set>
#include <string>
#include <thread>

struct SRandomTest : tpunit::TestFixture
{
    SRandomTest()
        : tpunit::TestFixture("SRandom",
                              TEST(SRandomTest::testBounds),
                              TEST(SRandomTest::testStringsAndBooleans),
                              TEST(SRandomTest::testConcurrentThreadWaves))
    {
    }

    static constexpr const char* alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    void testBounds()
    {
        constexpr uint64_t maximum = std::numeric_limits<uint64_t>::max();
        for (uint64_t endpoint : {uint64_t{0}, uint64_t{42}, maximum}) {
            ASSERT_EQUAL(SRandom::limitedRand64(endpoint, endpoint), endpoint);
        }
        const uint64_t ranges[][2] = {{0, 1}, {7, 19}, {maximum - 1, maximum}, {0, maximum}};
        for (const auto& [min, max] : ranges) {
            for (int i = 0; i < 500; ++i) {
                const auto value = SRandom::limitedRand64(min, max);
                ASSERT_EQUAL(value >= min && value <= max, true);
            }
        }
    }

    void testStringsAndBooleans()
    {
        for (unsigned length : {0U, 1U, 16U, 256U}) {
            const auto value = SRandom::randStr(length);
            ASSERT_EQUAL(value.size(), length);
            ASSERT_EQUAL(value.find_first_not_of(alphabet), std::string::npos);
        }
        for (int i = 0; i < 500; ++i) {
            ASSERT_EQUAL(SRandom::randBool(0.0), false);
            ASSERT_EQUAL(SRandom::randBool(1.0), true);
        }
    }

    void testConcurrentThreadWaves()
    {
        struct Result {
            std::array<uint64_t, 4> first{};
            bool valid = true;
        };
        std::set<std::array<uint64_t, 4>> prefixes;
        for (int wave = 0; wave < 2; ++wave) {
            std::array<Result, 16> results{};
            std::array<std::thread, 16> threads;
            std::latch start(threads.size());
            for (size_t t = 0; t < threads.size(); ++t) {
                threads[t] = std::thread([&, t] {
                    start.arrive_and_wait();
                    auto& result = results[t];
                    try {
                        for (auto& value : result.first) {
                            value = SRandom::rand64();
                        }
                        for (unsigned i = 0; i < 500; ++i) {
                            SRandom::rand64();
                            const auto bounded = SRandom::limitedRand64(7, 19);
                            const auto text = SRandom::randStr(i % 33);
                            result.valid &= bounded >= 7 && bounded <= 19;
                            result.valid &= text.size() == i % 33 && text.find_first_not_of(alphabet) == std::string::npos;
                            result.valid &= !SRandom::randBool(0.0);
                            result.valid &= SRandom::randBool(1.0);
                        }
                    } catch (...) {
                        result.valid = false;
                    }
                });
            }
            for (auto& thread : threads) {
                thread.join();
            }
            // Compare prefixes across both waves to catch duplicate seeds and resets on thread recreation.
            for (const auto& result : results) {
                ASSERT_EQUAL(result.valid, true);
                const bool unique = prefixes.insert(result.first).second;
                ASSERT_EQUAL(unique, true);
            }
        }
    }
} __SRandomTest;

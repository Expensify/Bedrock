#include <thread>
#include <vector>
#include <atomic>

#include <libstuff/SRingBuffer.h>
#include <test/lib/BedrockTester.h>

struct SRingBufferTest : tpunit::TestFixture
{
    SRingBufferTest() : tpunit::TestFixture(
        "SRingBuffer",
        TEST(SRingBufferTest::testPushPop),
        TEST(SRingBufferTest::testEmptyPop),
        TEST(SRingBufferTest::testFullBuffer),
        TEST(SRingBufferTest::testFIFOOrder),
        TEST(SRingBufferTest::testMultiProducer),
        TEST(SRingBufferTest::testProducerConsumer),
        TEST(SRingBufferTest::testShutdown),
        TEST(SRingBufferTest::testFlushOnShutdown)
    )
    {
    }

    // Test basic push and pop
    void testPushPop()
    {
        SRingBuffer<int, 10> buffer;

        // Push a value
        int val = 42;
        ASSERT_TRUE(buffer.push(move(val)));

        // Pop returns the value with Ready state
        auto [data, state] = buffer.pop();
        ASSERT_TRUE(state == State::Ready);
        ASSERT_TRUE(data.has_value());
        ASSERT_EQUAL(data.value(), 42);
    }

    // Test pop on empty buffer returns Empty state
    void testEmptyPop()
    {
        SRingBuffer<int, 10> buffer;

        // Pop on empty buffer returns Empty state
        auto [data, state] = buffer.pop();
        ASSERT_TRUE(state == State::Empty);
        ASSERT_FALSE(data.has_value());
    }

    // Test buffer rejects push when full
    void testFullBuffer()
    {
        SRingBuffer<int, 5> buffer;

        // Fill buffer to capacity
        for (int i = 0; i < 5; i++) {
            int val = i;
            ASSERT_TRUE(buffer.push(move(val)));
        }

        // Push fails when full
        int val = 100;
        ASSERT_FALSE(buffer.push(move(val)));

        // Pop one item to make space
        buffer.pop();

        // Push succeeds after making space
        val = 100;
        ASSERT_TRUE(buffer.push(move(val)));
    }

    // Test FIFO ordering
    void testFIFOOrder()
    {
        SRingBuffer<int, 10> buffer;

        // Push 1, 2, 3
        int a = 1, b = 2, c = 3;
        buffer.push(move(a));
        buffer.push(move(b));
        buffer.push(move(c));

        // Pop first item
        auto [firstData, firstState] = buffer.pop();
        ASSERT_EQUAL(firstData.value(), 1);

        // Pop second item
        auto [secondData, secondState] = buffer.pop();
        ASSERT_EQUAL(secondData.value(), 2);

        // Pop third item
        auto [thirdData, thirdState] = buffer.pop();
        ASSERT_EQUAL(thirdData.value(), 3);
    }

    // Test multiple threads pushing concurrently
    void testMultiProducer()
    {
        SRingBuffer<int, 1000> buffer;
        atomic<int> pushCount{0};
        const int numThreads = 4;
        const int pushesPerThread = 100;

        // Spawn producer threads
        vector<thread> producers;
        for (int t = 0; t < numThreads; t++) {
            producers.emplace_back([&buffer, &pushCount]() {
                for (int i = 0; i < 100; i++) {
                    int val = i;
                    if (buffer.push(move(val))) {
                        pushCount++;
                    }
                }
            });
        }

        // Wait for all producers
        for (auto& t : producers) {
            t.join();
        }

        // All pushes succeeded
        ASSERT_EQUAL(pushCount.load(), numThreads * pushesPerThread);

        // Pop all items
        int popCount = 0;
        while (buffer.pop().has_value()) {
            popCount++;
        }

        // All items accounted for
        ASSERT_EQUAL(popCount, numThreads * pushesPerThread);
    }

    // Test producer-consumer pattern with wrap-around
    void testProducerConsumer()
    {
        SRingBuffer<int, 100> buffer;
        atomic<bool> done{false};
        atomic<int> produced{0};
        atomic<int> consumed{0};
        const int totalItems = 1000;

        // Producer pushes 1000 items through size-100 buffer
        thread producer([&]() {
            for (int i = 0; i < totalItems; i++) {
                int val = i;
                while (!buffer.push(move(val))) {
                    val = i;
                    this_thread::yield();
                }
                produced++;
            }
            done = true;
        });

        // Consumer pops until done
        thread consumer([&]() {
            while (!done || consumed < totalItems) {
                auto val = buffer.pop();
                if (val.has_value()) {
                    consumed++;
                } else {
                    this_thread::yield();
                }
            }
        });

        producer.join();
        consumer.join();

        // All items produced and consumed
        ASSERT_EQUAL(produced.load(), totalItems);
        ASSERT_EQUAL(consumed.load(), totalItems);
    }
} __SRingBufferTest;

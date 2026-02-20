#pragma once

#include <array>
#include <atomic>
#include <optional>
#include <string>

using namespace std;

// 10M items, ~1GB at 100 bytes/item, ~200 seconds buffer at 50K items/sec
constexpr size_t SRINGBUFFER_DEFAULT_CAPACITY = 10'000'000;

/*
 * Lock free multi producer, single consumer ring buffer. Used for Fluentd logging
 */
template<typename T, size_t C> class SRingBuffer {
public:
    struct BufferElement
    {
        T data;

        // Prevents consumer from reading partially-written data. Producer reserves slot with CAS, then writes data, then signals ready.
        atomic<bool> isReady{false};
    };

    bool push(T&& data)
    {
        size_t currentTail = tail.load(memory_order_relaxed);

        while (true) {
            if (currentTail - head.load(memory_order_acquire) >= C) {
                return false;
            }
            if (tail.compare_exchange_weak(currentTail, currentTail + 1, memory_order_acq_rel, memory_order_relaxed)) {
                break;
            }
        }

        size_t index = currentTail % C;
        buffer[index].data = move(data);
        buffer[index].isReady.store(true, memory_order_release);
        buffer[index].isReady.notify_one();

        return true;
    }

    optional<T> pop()
    {
        size_t currentHead = head.load();
        size_t index = currentHead % C;

        buffer[index].isReady.wait(false, memory_order_acquire);

        if (!buffer[index].isReady.load(memory_order_acquire)) {
            return nullopt;
        }

        T bufferData = move(buffer[index].data);
        buffer[index].isReady.store(false, memory_order_release);

        head.store(currentHead + 1, memory_order_release);

        return bufferData;
    }

    void notifyConsumer()
    {
        buffer[head.load() % C].isReady.notify_one();
    }

private:
    array<BufferElement, C> buffer;

    // Single consumer reads from here
    atomic<size_t> head{0};

    // Multiple producers write here
    atomic<size_t> tail{0};
};

#pragma once

#include <array>
#include <atomic>
#include <optional>
#include <string>
#include <thread>
#include <utility>

using namespace std;

// 10M items, ~1GB at 100 bytes/item, ~200 seconds buffer at 50K items/sec
constexpr size_t SRINGBUFFER_DEFAULT_CAPACITY = 10'000'000;

enum class State
{
    Empty,
    Ready,
    Shutdown
};

/*
 * Lock free multi producer, single consumer ring buffer. Used for Fluentd logging
 */
template<typename T, size_t C> class SRingBuffer {
public:
    struct BufferElement
    {
        T data;
        atomic<State> state{State::Empty};
    };

    bool push(T&& data)
    {
        size_t currentWriteIndex = writeIndex.load(memory_order_relaxed);

        while (true) {
            if (currentWriteIndex - readIndex.load(memory_order_acquire) >= C) {
                return false;
            }
            if (writeIndex.compare_exchange_weak(currentWriteIndex, currentWriteIndex + 1, memory_order_acq_rel, memory_order_relaxed)) {
                break;
            }
        }

        size_t index = currentWriteIndex % C;
        buffer[index].data = move(data);
        buffer[index].state.store(State::Ready, memory_order_release);
        buffer[index].state.notify_one();

        return true;
    }

    pair<optional<T>, State> pop()
    {
        size_t currentReadIndex = readIndex.load(memory_order_acquire);
        size_t index = currentReadIndex % C;

        State slotState = buffer[index].state.load(memory_order_acquire);

        if (slotState == State::Empty) {
            return {nullopt, State::Empty};
        }

        if (slotState == State::Shutdown) {
            buffer[index].state.store(State::Empty, memory_order_release);
            readIndex.store(currentReadIndex + 1, memory_order_release);
            return {nullopt, State::Shutdown};
        }

        T bufferData = move(buffer[index].data);
        buffer[index].state.store(State::Empty, memory_order_release);

        readIndex.store(currentReadIndex + 1, memory_order_release);

        return {bufferData, State::Ready};
    }

    void wait()
    {
        size_t index = readIndex.load(memory_order_acquire) % C;
        buffer[index].state.wait(State::Empty, memory_order_acquire);
    }

    void shutdown()
    {
        size_t currentWriteIndex = writeIndex.load(memory_order_relaxed);

        while (true) {
            while (currentWriteIndex - readIndex.load(memory_order_acquire) >= C) {
                this_thread::yield();
                currentWriteIndex = writeIndex.load(memory_order_relaxed);
            }
            if (writeIndex.compare_exchange_weak(currentWriteIndex, currentWriteIndex + 1, memory_order_acq_rel, memory_order_relaxed)) {
                break;
            }
        }

        size_t index = currentWriteIndex % C;
        buffer[index].state.store(State::Shutdown, memory_order_release);
        buffer[index].state.notify_one();
    }

private:
    array<BufferElement, C> buffer;

    // Single consumer reads from here
    atomic<size_t> readIndex{0};

    // Multiple producers write here
    atomic<size_t> writeIndex{0};
};

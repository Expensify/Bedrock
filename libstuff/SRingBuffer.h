/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    SRingBuffer.h
 * Path:    libstuff/SRingBuffer.h
 * Pair:    (none - header-only template, no .cpp; sole consumer is SFluentdLogger)
 *
 * INTENT
 *   Lock-free, fixed-capacity, single-producer-notify/multi-producer/single-consumer
 *   ring buffer of T, used to hand log records from arbitrary caller threads to
 *   SFluentdLogger's one sender thread without blocking the caller.
 *
 * OBJECTS
 *   SRINGBUFFER_DEFAULT_CAPACITY - default slot count constant (10M).
 *   State                         - Empty/Ready/Shutdown, tags each slot's occupancy.
 *   SRingBuffer<T, C>             - templated buffer; push()/pop()/wait()/shutdown();
 *     SRingBuffer::BufferElement  - one slot: a T plus its atomic<State>.
 *
 * OUT OF PLACE
 *   [CANDIDATE] `State` is declared at global scope rather than nested inside
 *   SRingBuffer, so a generic five-letter name (`State`) is injected into every
 *   translation unit that includes this header.
 *
 * NAME/LOCATION FIT
 *   Fits; it's a generic buffer, not Fluentd-specific, though today it has one caller.
 *
 * NAMING QUALITY
 *   Consistent aside from the unnested, overly-generic `State` noted above.
 * ─────────────────────────────────────────────────────────────────────*/

#pragma once

#include <array>
#include <atomic>
#include <optional>
#include <string>
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
 * Lock free multi producer, single consumer ring buffer. Used for Fluentd logging.
 * Allocates C+1 slots internally: C for data, 1 reserved for shutdown marker.
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

        size_t index = currentWriteIndex % BufferSize;
        buffer[index].data = move(data);
        buffer[index].state.store(State::Ready, memory_order_release);
        buffer[index].state.notify_one();

        return true;
    }

    pair<optional<T>, State> pop()
    {
        size_t currentReadIndex = readIndex.load(memory_order_acquire);
        size_t index = currentReadIndex % BufferSize;

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
        size_t index = readIndex.load(memory_order_acquire) % BufferSize;
        buffer[index].state.wait(State::Empty, memory_order_acquire);
    }

    void shutdown()
    {
        size_t index = writeIndex.fetch_add(1, memory_order_acq_rel) % BufferSize;
        buffer[index].state.store(State::Shutdown, memory_order_release);
        buffer[index].state.notify_one();
    }

private:
    static constexpr size_t BufferSize = C + 1;
    array<BufferElement, BufferSize> buffer;

    // Single consumer reads from here
    atomic<size_t> readIndex{0};

    // Multiple producers write here
    atomic<size_t> writeIndex{0};
};

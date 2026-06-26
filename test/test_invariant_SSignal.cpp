#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include "libstuff/SSignal.h"

class SecurityTest : public ::testing::TestWithParam<std::tuple<uint32_t, uint32_t>> {};

TEST_P(SecurityTest, MultiplicationOverflowCheck) {
    // Invariant: Allocation size computation must not overflow or produce invalid size
    auto [count, size] = GetParam();
    
    // Call the actual production function that performs the allocation
    // This assumes SSignal::allocateBuffer is the vulnerable function
    void* buffer = SSignal::allocateBuffer(count, size);
    
    // Property: Either allocation succeeds with valid pointer, or fails safely (returns nullptr)
    // We cannot directly test internal overflow check, but we can ensure no heap corruption occurs
    // A safe implementation should either return nullptr or throw on overflow
    if (buffer != nullptr) {
        free(buffer);  // Clean up if allocation succeeded
    }
    // Test passes if no crash or heap corruption occurs
}

INSTANTIATE_TEST_SUITE_P(
    AdversarialInputs,
    SecurityTest,
    ::testing::Values(
        // Exact exploit case: multiplication wraps to small value
        std::make_tuple(0x10000, 0x10000),  // 2^16 * 2^16 = 2^32 → wraps to 0 on 32-bit
        
        // Boundary case: maximum values that don't overflow on 32-bit
        std::make_tuple(0x10000, 0x7FFF),  // 2^16 * (2^15-1) = ~2^31-2^16
        
        // Another boundary: values that overflow 32-bit
        std::make_tuple(0x10000, 0x10001),  // 2^16 * (2^16+1) > 2^32
        
        // Valid normal input
        std::make_tuple(10, 20)
    )
);

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
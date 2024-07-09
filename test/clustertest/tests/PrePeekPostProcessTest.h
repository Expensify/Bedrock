#pragma once
#include <libstuff/libstuff.h>
#include <test/lib/tpunit++.hpp>
#include <test/clustertest/BedrockClusterTester.h>

class BedrockTester;

struct PrePeekPostProcessTest : tpunit::TestFixture
{
    PrePeekPostProcessTest();
    BedrockClusterTester* tester;

    void setup();

    void teardown();

    void checkWithoutThis();

    void prePeek();
    void prePeekThrow();
    void postProcess();
    void prePeekPostProcess();
};

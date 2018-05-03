---
layout: default
---

# Building on OSX

Please make sure you have Xcode / build tools installed.


```
# fetches mbedtls repo
$ git submodule init
$ git submodule update

# overrides the compilers for OSX
$ GXX='g++' CC='gcc' make all


# view help for bedrock
$ ./bedrock -h
```

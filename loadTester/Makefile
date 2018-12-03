# Set the compiler, if it's not set by the environment.
ifndef GXX
	GXX = g++-6
endif

ifndef CC
	CC = gcc-6
endif

all: loadTester

clean:
	rm -rf loadTester

loadTester: loadTester.cpp
	$(GXX) -g -std=gnu++14 -I../ -I../mbedtls/include loadTester.cpp -O2 -o loadTester -L../ -L../mbedtls/include -L../mbedtls/library -rdynamic -lstuff -lmbedtls -lpcrecpp -lmbedx509 -lmbedcrypto -lpthread -ldl -lz

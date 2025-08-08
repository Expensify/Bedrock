# Simple wrapper Makefile so you can run `make` from this directory.

.PHONY: all bench run clean

all: bench

bench:
	$(MAKE) -C .. bench

run: bench
	./bench -only SDeburrBench

clean:
	$(MAKE) -C .. clean



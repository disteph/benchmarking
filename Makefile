.PHONY: all deps build test clean

all: build

deps:
	$(MAKE) -C runner deps

build:
	$(MAKE) -C runner build

test:
	$(MAKE) -C runner test

clean:
	$(MAKE) -C runner clean

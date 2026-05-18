.PHONY: all build test clean

all: build

build:
	$(MAKE) -C runner build

test:
	$(MAKE) -C runner test

clean:
	$(MAKE) -C runner clean

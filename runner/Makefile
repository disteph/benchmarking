.PHONY: all pin-timer deps build install test clean

OPAMFLAGS ?= --yes
TIMER_PIN ?= git+https://github.com/disteph/timer.git

all: build

pin-timer:
	opam pin add $(OPAMFLAGS) --no-action timer $(TIMER_PIN)

deps: pin-timer
	opam install $(OPAMFLAGS) --deps-only .

build: deps
	dune build

install: build
	dune install

test: deps
	dune runtest

clean:
	dune clean

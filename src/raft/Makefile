.PHONY: build test test-3a test-3b test-3c test-3d

SUITE ?=

build:
	go build ./...

test:
ifdef SUITE
	go test -run $(SUITE)
else
	go test
endif

test-3a:
	go test -run 3A

test-3b:
	go test -run 3B

test-3c:
	go test -run 3C

test-3d:
	go test -run 3D

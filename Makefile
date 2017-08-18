all: test

PACKAGES := $(shell go list ./... | grep -v vendor)

init:
	go get -u github.com/AlekSi/gocoverutil

install:
	go install -v $(PACKAGES)

install-race:
	go install -v -race $(PACKAGES)

test: install
	go test -v $(PACKAGES)

bench: install
	go test -bench=. -benchtime=10s -benchmem -v $(PACKAGES)

run: install
	promhouse

run-race: install-race
	promhouse

cover: install
	gocoverutil test -v $(PACKAGES)

env-run:
	docker-compose -f misc/docker-compose.yml -p promhouse up

env-stop:
	docker-compose -f misc/docker-compose.yml -p promhouse stop

env-run-mac:
	docker-compose -f misc/docker-compose-mac.yml -p promhouse up

env-stop-mac:
	docker-compose -f misc/docker-compose-mac.yml -p promhouse stop

all: test

init:
	go get -u github.com/AlekSi/gocoverutil
	go get -u gopkg.in/alecthomas/gometalinter.v2
	go get -u github.com/dvyukov/go-fuzz/...
	gometalinter.v2 --install

protos:
	go install -v ./vendor/github.com/golang/protobuf/protoc-gen-go
	go install -v ./vendor/github.com/gogo/protobuf/protoc-gen-gofast
	go install -v ./vendor/github.com/gogo/protobuf/protoc-gen-gogofast
	go install -v ./vendor/github.com/gogo/protobuf/protoc-gen-gogofaster
	go install -v ./vendor/github.com/gogo/protobuf/protoc-gen-gogoslick
	go install -v ./vendor/github.com/gogo/protobuf/protoc-gen-gogo

	rm -f prompb/*.pb.go
	protoc -Iprompb prompb/*.proto --gofast_out=prompb

install:
	go install -v ./...

install-race:
	go install -v -race ./...

test: install
	go test -v -tags gofuzzgen ./...

test-race: install-race
	go test -v -tags gofuzzgen -race ./...

bench: install
	go test -bench=. -benchtime=10s -benchmem -v ./...

run: install
	promhouse --log.level=info

run-race: install-race
	promhouse --log.level=info

cover: install
	gocoverutil test -v -covermode=count ./...

check: install
	-gometalinter.v2 --tests --vendor --skip=prompb --deadline=300s --sort=linter ./...

gofuzz: test
	go-fuzz-build -func=FuzzJSON -o=json-fuzz.zip github.com/Percona-Lab/PromHouse/storages/clickhouse
	go-fuzz -bin=json-fuzz.zip -workdir=go-fuzz/json

env-run:
	docker-compose -f misc/docker-compose.yml -p promhouse up

env-stop:
	docker-compose -f misc/docker-compose.yml -p promhouse stop

env-run-mac:
	docker-compose -f misc/docker-compose-mac.yml -p promhouse up

env-stop-mac:
	docker-compose -f misc/docker-compose-mac.yml -p promhouse stop

clickhouse-client:
	docker exec -ti -u root promhouse_clickhouse_1 /usr/bin/clickhouse --client --database=prometheus

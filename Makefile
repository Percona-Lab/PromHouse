all: test

init:
	go get -u github.com/AlekSi/gocoverutil
	go get -u gopkg.in/alecthomas/gometalinter.v2
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
	go test -v ./...

test-race: install-race
	go test -v -race ./...

bench: install
	go test -bench=. -benchtime=10s -benchmem -v ./...

run: install
	promhouse -debug

run-race: install-race
	promhouse -debug

cover: install
	gocoverutil test -v -covermode=count ./...

check: install
	-gometalinter.v2 --tests --vendor --skip=prompb --deadline=300s --sort=linter ./...

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

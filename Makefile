name = promhouse
version = 0.1.1
release = 1
package = github.com/Percona-Lab/PromHouse
build_dir = _build
go_version = 1.15

all: promhouse

clean:
	rm -rf $(build_dir)

init:
	go get -u github.com/AlekSi/gocoverutil
	go get -u golang.org/x/perf/cmd/benchstat
	go get -u github.com/dvyukov/go-fuzz/...
	go get -u gopkg.in/alecthomas/gometalinter.v2
	gometalinter.v2 --install

protos:
	go install -v ./vendor/github.com/golang/protobuf/protoc-gen-go
	go install -v ./vendor/github.com/gogo/protobuf/protoc-gen-gogo

	rm -f prompb/*.pb.go
	protoc -Ivendor/github.com/gogo/protobuf -Iprompb prompb/*.proto --gogo_out=prompb

install:
	go install -v ./...

install-race:
	go install -v -race ./...

test: install
	go test -v -tags gofuzzgen ./...

test-race: install-race
	go test -v -tags gofuzzgen -race ./...

bench: install
	go test -run=NONE -bench=. -benchtime=3s -count=5 -benchmem ./... | tee new.txt

run: install
	go run ./cmd/promhouse/*.go --log.level=info

run-race: install-race
	go run -race ./cmd/promhouse/*.go --log.level=info

.PHONY: promhouse docker-promhouse rpm
promhouse:
	mkdir -p $(build_dir)/
	go build -o $(build_dir)/$(name) cmd/$(name)/*.go

rpm_dir = $(build_dir)/rpm
rpm_target = x86_64
rpm: promhouse
	mkdir -p $(rpm_dir)
	cp build/rpm/* $(rpm_dir)
	cp $(build_dir)/$(name) $(rpm_dir)
	git log --format="* %cd %aN%n- (%h) %s%d%n" -n 10 --date local \
		 | sed -r 's/[0-9]+:[0-9]+:[0-9]+ //' >> $(rpm_dir)/$(name).spec
	chmod -R g+w,o+w $(rpm_dir)
	docker run --privileged --rm \
			-v $(shell pwd)/$(rpm_dir):/home/builder/rpm \
			-w /home/builder/rpm rpmbuild/centos7 \
			rpmbuild \
			--define '_name $(name)' \
			--define '_version $(version)' \
			--define '_release $(release)' \
			--define '_arch $(rpm_target)' \
			--target '$(rpm_target)' \
			-bb $(name).spec

docker-promhouse:
	docker run -d --rm \
		-v $(shell pwd):/go/src/$(package) \
		-w /go/src/$(package) \
		golang:$(go_version) \
		make promhouse

cover: install
	gocoverutil test -v -covermode=count ./...

check: install
	-gometalinter.v2 --tests --vendor --skip=prompb --deadline=300s --sort=linter ./...

gofuzz: test
	go-fuzz-build -func=FuzzJSON -o=json-fuzz.zip github.com/Percona-Lab/PromHouse/storages/clickhouse
	go-fuzz -bin=json-fuzz.zip -workdir=go-fuzz/json

up:
	docker-compose -f misc/docker-compose-linux.yml -p promhouse up --force-recreate --abort-on-container-exit --renew-anon-volumes --remove-orphans

up-mac:
	docker-compose -f misc/docker-compose-mac.yml -p promhouse up --force-recreate --abort-on-container-exit --renew-anon-volumes --remove-orphans

down:
	docker-compose -f misc/docker-compose-linux.yml -p promhouse down --volumes --remove-orphans

down-mac:
	docker-compose -f misc/docker-compose-mac.yml -p promhouse down --volumes --remove-orphans

clickhouse-client:
	docker exec -ti -u root promhouse_clickhouse_1 /usr/bin/clickhouse --client --database=prometheus

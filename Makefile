# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOBUILD_LINUX=GO111MODULE=on GOOS=linux go build --mod=vendor
GOTEST=$(GOCMD) test

all: test build
build:
	rm -rf target/
	mkdir target/
	cp cmd/comet/comet-example.toml target/comet.toml
	cp cmd/logic/logic-example.toml target/logic.toml
	cp cmd/job/job-example.toml target/job.toml
	$(GOBUILD) -o target/comet cmd/comet/main.go
	$(GOBUILD) -o target/logic cmd/logic/main.go
	$(GOBUILD) -o target/job cmd/job/main.go

linux:
	rm -rf target/
	mkdir target/
	cp cmd/comet/comet-example.toml target/comet.toml
	cp cmd/logic/logic-example.toml target/logic.toml
	cp cmd/job/job-example.toml target/job.toml
	$(GOBUILD_LINUX) -o target/comet cmd/comet/main.go
	$(GOBUILD_LINUX) -o target/logic cmd/logic/main.go
	$(GOBUILD_LINUX) -o target/job cmd/job/main.go

docker:
	docker build -t goim .

test:
	$(GOTEST) -v ./...

clean:
	rm -rf target/

run:
	nohup target/logic -conf=target/logic.toml -region=sh -zone=sh001 -deploy.env=dev -weight=10 2>&1 > target/logic.log &
	nohup target/comet -conf=target/comet.toml -region=sh -zone=sh001 -deploy.env=dev -weight=10 -addrs=127.0.0.1 -debug=true 2>&1 > target/comet.log &
	nohup target/job -conf=target/job.toml -region=sh -zone=sh001 -deploy.env=dev 2>&1 > target/job.log &

stop:
	pkill -f target/logic
	pkill -f target/job
	pkill -f target/comet


protoc:
	protoc --proto_path=/Users/weisd/go/src --proto_path=/Users/weisd/go/src/github.com/go-kratos/kratos/third_party --proto_path=./  --proto_path=./vendor  --gogofaster_out=plugins=grpc:. ./api/logic/grpc/api.proto

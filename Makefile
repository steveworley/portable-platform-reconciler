VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE   ?= portable-platform-reconciler:$(VERSION)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test vet lint demo docker-build clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/controller ./cmd/controller
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/demo ./cmd/demo

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

lint:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

demo:
	go run ./cmd/demo

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

clean:
	rm -rf bin

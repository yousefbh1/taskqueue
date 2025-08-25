APP?=taskqueue
REG?=ghcr.io/yousefbh1
SHA:=$(shell git rev-parse --short=8 HEAD)

API_IMG=$(REG)/$(APP)-api
WRK_IMG=$(REG)/$(APP)-worker

.PHONY: proto build up down logs test lint fmt images push demo clean

proto:
	protoc --go_out=services/api --go-grpc_out=services/api \
	  --go_opt=module=example.com/taskqueue/api \
	  --go-grpc_opt=module=example.com/taskqueue/api \
	  proto/jobqueue.proto

build:
	cd infra && docker compose build

up:
	cd infra && docker compose up -d

down:
	cd infra && docker compose down

logs:
	cd infra && docker compose logs -f api worker

test:
	cd services/worker && go test ./...
	cd services/api && go test ./...

lint:
	golangci-lint run ./services/... || true

fmt:
	gofmt -s -w services

images:
	docker build -t $(API_IMG):$(SHA) -t $(API_IMG):latest services/api
	docker build -t $(WRK_IMG):$(SHA) -t $(WRK_IMG):latest services/worker

push: images
	@echo "🔐 logging in to ghcr (needs a PAT with write:packages if local)"; \
	echo "$$GHCR_TOKEN" | docker login ghcr.io -u $(REG) --password-stdin
	docker push $(API_IMG):$(SHA) && docker push $(API_IMG):latest
	docker push $(WRK_IMG):$(SHA) && docker push $(WRK_IMG):latest

demo:
	grpcurl -plaintext -d '{"type":"demo","payload":"{\"sleep_ms\":200}","priority":7,"idempotency_key":"demo"}' \
	  localhost:8080 jobqueue.v1.JobApi/SubmitJob

clean:
	git clean -xfd

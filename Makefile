# Convenience targets for Phase 1. Assumes go, sqlc, migrate on PATH.
# If Go tools live outside PATH: export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin

DB_URL=postgresql://ledger:secret@localhost:5433/ledger?sslmode=disable

.PHONY: up down psql sqlc migrateup migratedown newmigration test server tidy proto

MODULE=github.com/1morshed1/distributed-ledger-engine

up:            ## start postgres + redis
	docker compose up -d

down:          ## stop stack
	docker compose down

psql:          ## open psql shell
	docker exec -it ledger-postgres psql -U ledger -d ledger

migrateup:     ## apply all up migrations
	migrate -path db/migration -database "$(DB_URL)" -verbose up

migratedown:   ## roll back all migrations
	migrate -path db/migration -database "$(DB_URL)" -verbose down

newmigration:  ## make NAME=xxx -> new migration pair
	migrate create -ext sql -dir db/migration -seq $(NAME)

sqlc:          ## regenerate type-safe db code
	sqlc generate

proto:         ## regenerate gRPC/protobuf code into internal/pb
	protoc --proto_path=proto --proto_path=$(HOME)/.local/include \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		proto/ledger.proto proto/events.proto

test:          ## run tests with coverage
	go test -v -cover ./...

server:        ## run the api server
	go run ./cmd/api

tidy:
	go mod tidy

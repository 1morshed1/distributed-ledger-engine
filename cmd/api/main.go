package main

import (
	"context"
	"log"

	"github.com/1morshed1/distributed-ledger-engine/internal/api"
	"github.com/1morshed1/distributed-ledger-engine/internal/cache"
	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/util"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatalf("cannot load config: %v", err)
	}

	connPool, err := pgxpool.New(context.Background(), config.DBSource)
	if err != nil {
		log.Fatalf("cannot connect to db: %v", err)
	}
	defer connPool.Close()

	store := db.NewStore(connPool)

	balanceCache := cache.NewRedisCache(config.RedisAddress)
	if err := balanceCache.Ping(context.Background()); err != nil {
		log.Fatalf("cannot connect to redis: %v", err)
	}
	defer balanceCache.Close()

	server, err := api.NewServer(config, store, balanceCache)
	if err != nil {
		log.Fatalf("cannot create server: %v", err)
	}

	log.Printf("starting HTTP server on %s", config.HTTPServerAddress)
	if err := server.Start(config.HTTPServerAddress); err != nil {
		log.Fatalf("cannot start server: %v", err)
	}
}

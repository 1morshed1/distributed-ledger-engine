package main

import (
	"context"
	"log"
	"net"

	"github.com/1morshed1/distributed-ledger-engine/internal/api"
	"github.com/1morshed1/distributed-ledger-engine/internal/cache"
	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/gapi"
	"github.com/1morshed1/distributed-ledger-engine/internal/pb"
	"github.com/1morshed1/distributed-ledger-engine/internal/util"
	"github.com/1morshed1/distributed-ledger-engine/internal/worker"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

	// Asynq shares the same Redis. Distributor enqueues; processor consumes.
	redisOpt := asynq.RedisClientOpt{Addr: config.RedisAddress}
	taskDistributor := worker.NewRedisTaskDistributor(redisOpt)
	go runTaskProcessor(redisOpt)

	// gRPC server runs in its own goroutine; HTTP server blocks main.
	go runGRPCServer(config, store, balanceCache, taskDistributor)
	runHTTPServer(config, store, balanceCache, taskDistributor)
}

func runTaskProcessor(redisOpt asynq.RedisClientOpt) {
	processor := worker.NewRedisTaskProcessor(redisOpt)
	log.Println("starting task processor")
	if err := processor.Start(); err != nil {
		log.Fatalf("cannot start task processor: %v", err)
	}
}

func runGRPCServer(config util.Config, store db.Store, balanceCache cache.BalanceCache, taskDistributor worker.TaskDistributor) {
	grpcServer := grpc.NewServer()
	pb.RegisterLedgerServiceServer(grpcServer, gapi.NewServer(config, store, balanceCache, taskDistributor))
	reflection.Register(grpcServer) // lets grpcurl/ghz introspect without the .proto

	listener, err := net.Listen("tcp", config.GRPCServerAddress)
	if err != nil {
		log.Fatalf("cannot create grpc listener: %v", err)
	}
	log.Printf("starting gRPC server on %s", config.GRPCServerAddress)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("cannot start grpc server: %v", err)
	}
}

func runHTTPServer(config util.Config, store db.Store, balanceCache cache.BalanceCache, taskDistributor worker.TaskDistributor) {
	server, err := api.NewServer(config, store, balanceCache, taskDistributor)
	if err != nil {
		log.Fatalf("cannot create http server: %v", err)
	}
	log.Printf("starting HTTP server on %s", config.HTTPServerAddress)
	if err := server.Start(config.HTTPServerAddress); err != nil {
		log.Fatalf("cannot start http server: %v", err)
	}
}

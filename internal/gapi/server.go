package gapi

import (
	"github.com/1morshed1/distributed-ledger-engine/internal/cache"
	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/pb"
	"github.com/1morshed1/distributed-ledger-engine/internal/util"
	"github.com/1morshed1/distributed-ledger-engine/internal/worker"
)

// Server implements the generated pb.LedgerServiceServer. It reuses the exact
// same db.Store, cache, and task distributor as the REST API — business logic
// lives in one place.
type Server struct {
	pb.UnimplementedLedgerServiceServer // forward-compatible: new RPCs won't break builds
	config                              util.Config
	store                               db.Store
	cache                               cache.BalanceCache
	taskDistributor                     worker.TaskDistributor
}

func NewServer(config util.Config, store db.Store, balanceCache cache.BalanceCache, taskDistributor worker.TaskDistributor) *Server {
	return &Server{config: config, store: store, cache: balanceCache, taskDistributor: taskDistributor}
}

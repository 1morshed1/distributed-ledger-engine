package api

import (
	"fmt"

	"github.com/1morshed1/distributed-ledger-engine/internal/cache"
	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/observability"
	"github.com/1morshed1/distributed-ledger-engine/internal/token"
	"github.com/1morshed1/distributed-ledger-engine/internal/util"
	"github.com/1morshed1/distributed-ledger-engine/internal/worker"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// Server holds everything HTTP handlers need: config, the DB store, the balance
// cache, the task distributor, the token maker, and the Gin router.
type Server struct {
	config          util.Config
	store           db.Store
	cache           cache.BalanceCache
	taskDistributor worker.TaskDistributor
	tokenMaker      token.Maker
	router          *gin.Engine
}

func NewServer(config util.Config, store db.Store, balanceCache cache.BalanceCache, taskDistributor worker.TaskDistributor) (*Server, error) {
	tokenMaker, err := token.NewPasetoMaker(config.TokenSymmetricKey)
	if err != nil {
		return nil, fmt.Errorf("cannot create token maker: %w", err)
	}

	server := &Server{config: config, store: store, cache: balanceCache, taskDistributor: taskDistributor, tokenMaker: tokenMaker}

	// Register the custom `currency` binding tag.
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterValidation("currency", validCurrency)
	}

	server.setupRouter()
	return server, nil
}

func (server *Server) setupRouter() {
	router := gin.Default()

	// Observability middleware: OTel tracing span per request + Prometheus metrics.
	router.Use(otelgin.Middleware("ledger-api"))
	router.Use(observability.GinMetrics())

	// Operational endpoints (unauthenticated).
	router.GET("/metrics", observability.MetricsHandler())
	router.GET("/healthz", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })

	// Public routes.
	router.POST("/api/v1/users", server.createUser)
	router.POST("/api/v1/auth/login", server.loginUser)
	router.POST("/api/v1/auth/refresh", server.renewAccessToken)

	// Protected routes: everything below requires a valid access token.
	auth := router.Group("/").Use(authMiddleware(server.tokenMaker))
	auth.POST("/api/v1/accounts", server.createAccount)
	auth.GET("/api/v1/accounts/:id", server.getAccount)
	auth.GET("/api/v1/accounts/:id/balance", server.getAccountBalance)
	auth.GET("/api/v1/accounts", server.listAccounts)
	auth.POST("/api/v1/transfers", server.createTransfer)
	auth.GET("/api/v1/transfers", server.listTransfers)
	auth.GET("/api/v1/fraud-alerts", server.listFraudAlerts)

	server.router = router
}

// Start runs the HTTP server. Blocks.
func (server *Server) Start(address string) error {
	return server.router.Run(address)
}

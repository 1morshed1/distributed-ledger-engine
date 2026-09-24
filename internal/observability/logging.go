package observability

import (
	"log/slog"
	"os"
)

// InitLogging installs a structured JSON logger as the slog default. JSON logs
// are what Loki/ELK ingest and query by field (trace_id, account_id, etc.).
func InitLogging(environment string) *slog.Logger {
	level := slog.LevelInfo
	if environment == "development" {
		level = slog.LevelDebug
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

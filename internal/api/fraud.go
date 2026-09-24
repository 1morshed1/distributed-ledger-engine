package api

import (
	"encoding/json"
	"net/http"
	"time"

	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/gin-gonic/gin"
)

type listFraudAlertsRequest struct {
	AfterID  int64 `form:"after_id" binding:"min=0"`
	PageSize int32 `form:"page_size" binding:"required,min=1,max=100"`
}

// fraudAlertResponse renders Detail (stored as JSONB / []byte) as embedded JSON
// rather than a base64 blob.
type fraudAlertResponse struct {
	ID          int64           `json:"id"`
	AccountID   int64           `json:"account_id"`
	Rule        string          `json:"rule"`
	Severity    string          `json:"severity"`
	Detail      json.RawMessage `json:"detail"`
	WindowStart *time.Time      `json:"window_start,omitempty"`
	WindowEnd   *time.Time      `json:"window_end,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

func toFraudAlertResponse(a db.FraudAlert) fraudAlertResponse {
	resp := fraudAlertResponse{
		ID:        a.ID,
		AccountID: a.AccountID,
		Rule:      a.Rule,
		Severity:  a.Severity,
		Detail:    json.RawMessage(a.Detail),
		CreatedAt: a.CreatedAt.Time,
	}
	if a.WindowStart.Valid {
		resp.WindowStart = &a.WindowStart.Time
	}
	if a.WindowEnd.Valid {
		resp.WindowEnd = &a.WindowEnd.Time
	}
	return resp
}

// listFraudAlerts returns fraud alerts newest-first with cursor pagination.
func (server *Server) listFraudAlerts(ctx *gin.Context) {
	var req listFraudAlertsRequest
	if err := ctx.ShouldBindQuery(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	alerts, err := server.store.ListFraudAlerts(ctx, db.ListFraudAlertsParams{
		AfterID:  req.AfterID,
		PageSize: req.PageSize,
	})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	resp := make([]fraudAlertResponse, 0, len(alerts))
	var nextCursor int64
	for _, a := range alerts {
		resp = append(resp, toFraudAlertResponse(a))
		nextCursor = a.ID // DESC order -> last is smallest id seen
	}
	ctx.JSON(http.StatusOK, gin.H{"alerts": resp, "next_after_id": nextCursor})
}

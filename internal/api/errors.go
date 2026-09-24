package api

import "github.com/gin-gonic/gin"

// errorResponse wraps an error into the JSON shape all handlers return on failure.
func errorResponse(err error) gin.H {
	return gin.H{"error": err.Error()}
}

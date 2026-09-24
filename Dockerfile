# Multi-stage build: compile a static binary, ship it on a tiny base.

# ---- build stage ----
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Cache deps first (only re-runs when go.mod/go.sum change).
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off -> fully static binary, portable across minimal images.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/server ./cmd/api

# ---- runtime stage ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /out/server .
COPY app.env .
USER app
EXPOSE 8080 9090
ENTRYPOINT ["/app/server"]

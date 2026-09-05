FROM docker.io/golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-X main.version=0.1.0-draft" -o bisync ./cmd/bisync

FROM scratch

COPY --from=builder /app/bisync /bisync

ENTRYPOINT ["/bisync"]

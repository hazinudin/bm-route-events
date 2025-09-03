# Stage 1: Build the binary
FROM golang:1.24.6 AS builder
WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire project
COPY . .

# Build the server binary from ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/server

# Stage 2: Minimal runtime image
FROM alpine:3.20
WORKDIR /app

# Copy just the binary
COPY --from=builder /app/server .

# Expose API port
EXPOSE 8080

# Run the binary
CMD ["./server"]

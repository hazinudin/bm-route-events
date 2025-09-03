# Build stage
FROM golang:1.24.6 AS builder
WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the eventlistener binary
RUN CGO_ENABLED=0 GOOS=linux go build -o eventlistener ./cmd/eventlistener

# Runtime stage
FROM alpine:3.20
WORKDIR /app

# Copy the built binary
COPY --from=builder /app/eventlistener .

# Run the event listener
CMD ["./eventlistener"]

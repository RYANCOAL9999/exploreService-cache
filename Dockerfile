# Stage 1: Build the Go binary
FROM golang:1.22-alpine AS builder

# Install git and build tools if required
RUN apk add --no-cache git

# Set working directory inside the container
WORKDIR /app

# Copy go mod files first to leverage Docker cache layers
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source tree
COPY . .

# Build the highly optimized Go binary for alpine execution
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o explore-service-cache main.go

# Stage 2: Create the final minimal production image
FROM alpine:3.19

WORKDIR /app

# Copy the pre-compiled binary from the builder stage
COPY --from=builder /app/explore-service-cache .

# 💡 Added: Default environment variables for defense.
# If they run "docker run" without any parameters, these values will be used.
ENV PORT=50052

# Configure continuous health checks using standard gRPC check boundaries
ENV REDIS_ADDR="localhost:6379"

# Expose the gRPC port
EXPOSE 50052
ENV KAFKA_BROKERS="localhost:9092"
ENV KAFKA_TOPIC="myQueue"

# Run the service
CMD ["./explore-service-cache"]
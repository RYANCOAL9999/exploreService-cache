package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"exploreService-cache/internal"
	pb "exploreService-cache/pb"

	redis "github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {

	log.Println("[Bootstrap] Initializing CacheService Microservice cluster node...")

	// Initialize Redis Client Cluster Connection Only
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		PoolSize:     100, // Limit network connection threads to prevent high concurrency from overwhelming Redis.
		MinIdleConns: 20,  // Maintaining a consistently available idle connection significantly improves RPC response speed.
	})
	// The ctx here has a single function: it is used only for connectivity testing during startup.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rClient.Ping(pingCtx).Err(); err != nil {
		pingCancel() // Preventing memory leakage
		log.Fatalf("[Fatal] Redis cluster ping failure: %v", err)
	}
	pingCancel() // Release this short-lived context immediately upon success.
	log.Println("[MAIN] Redis Cluster health check passed (Ping Success)")

	// Initialize Kafka cluster address
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	var brokers []string
	if brokersEnv != "" {
		brokers = strings.Split(brokersEnv, ",")
	} else {
		brokers = []string{"localhost:9092"}
	}

	// Instantiating stateless CacheServer
	cacheServer := internal.NewCacheServer(rClient, brokers)

	// When the system sends a shutdown notification, we call `cancel()`, which notifies all background tasks to prepare for cleanup.
	mainCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheServer.StartScheduledGoroutine(mainCtx)

	// Setup gRPC engine directly
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("[Fatal] Failed to bind local TCP port 50052: %v", err)
	}
	// Instantiate gRPC Server with performance KeepAlive safety configs
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Minute,
			Time:              5 * time.Minute,
			Timeout:           20 * time.Second,
		}),
	)

	// Thanks to stateless and dynamic routing design, you can launch multiple different schedules based on different services, all sharing the same connection pool.
	pb.RegisterCacheServiceServer(grpcServer, cacheServer)

	// Run gRPC in a separate background Goroutine to avoid blocking the main execution thread.
	go func() {
		log.Println("[Server Running] CacheService gRPC engine active on port :50052")
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("[Fatal] Server execution failure: %v", err)
		}
	}()

	// [Core Security Feature] Monitors the shutdown signal of the operating system.
	// SIGINT  = Ctrl+C
	quit := make(chan os.Signal, 1)

	// SIGTERM = Kubernetes The standard notification issued when preparing to delete this Pod.
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// This will remain blocked until someone shuts down their computer.
	sig := <-quit
	log.Printf("[MAIN] System shutdown signal detected: [%s]. Graceful Shutdown procedure initiated ...", sig)

	// Notify the background mission in the early morning to be completed immediately.
	cancel()

	// GracefulStop will wait until all currently executing RPC requests have been processed before closing the connection.
	log.Println("[MAIN] gRPC is stopping accepting new requests and waiting for ongoing requests to finish ....")
	grpcServer.GracefulStop()

	// Elegant Shutdown Step 3: Shut down the global Kafka connection pool to ensure all messages in the buffer are flushed to AWS Kafka.
	log.Println("[MAIN] Closing the Kafka connection pool ...")
	if err := cacheServer.Close(); err != nil {
		log.Printf("[MAIN-ERROR] An error occurred while closing the Kafka connection pool: %v", err)
	}

	// 11. Close Redis
	_ = rClient.Close()

	log.Println("[MAIN] The exploreService-cache has been safely and completely shut down with zero data loss. Goodbye.！")

}

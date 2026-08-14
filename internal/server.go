package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "exploreService-cache/pb"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel value to efficiently deflect cache penetration at the memory layer.
const CachePenetrationSentinel = "-1"

type CacheServer struct {
	pb.UnimplementedCacheServiceServer // Ensures backward compatibility
	redisClient                        *redis.Client
	writer                             *kafka.Writer
}

// NewCacheServer instantiates the handler strictly requiring a Redis client cluster wrapper.
// Database link components are completely eliminated from this architectural domain.
func NewCacheServer(rc *redis.Client, brokers []string) *CacheServer {
	return &CacheServer{
		redisClient: rc,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Balancer: &kafka.LeastBytes{},
			Async:    false,
		},
	}
}

func (s *CacheServer) Close() error {
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}

// AppendNewLikeToCache: Pipelines atomic ZSET appends and counter increments.
func (s *CacheServer) AppendNewLikeToCache(ctx context.Context, req *pb.AppendRequest) (*pb.EmptyResponse, error) {
	actorID := req.GetActorUserId()
	recipientID := req.GetRecipientUserId()
	likedRecipient := req.GetLikedRecipient()
	if actorID == "" || recipientID == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_user_id and recipient_user_id are required")
	}

	var listKey string = fmt.Sprintf("user:new_likes:%s", recipientID)

	var countKey string = fmt.Sprintf("user:likes_count:%s", recipientID)

	var memberValue string = fmt.Sprintf("%s:%t", actorID, likedRecipient)

	// Encapsulated pipeline writes to optimize network round-trip limits
	pipe := s.redisClient.Pipeline()
	pipe.ZAdd(ctx, listKey, &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: memberValue,
	})
	pipe.Incr(ctx, countKey)

	pipe.SAdd(ctx, GlobalIndexKey, recipientID)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "redis pipeline replication execution failed: %v", err)
	}

	return &pb.EmptyResponse{}, nil
}

// GetLikedYouCount: Pure O(1) in-memory string metrics lookup. No DB fallbacks allowed.
func (s *CacheServer) GetLikedYouCount(ctx context.Context, req *pb.CountRequest) (*pb.CountResponse, error) {
	recipientID := req.GetRecipientUserId()
	if recipientID == "" {
		return nil, status.Error(codes.InvalidArgument, "recipient_user_id is required")
	}

	cacheKey := fmt.Sprintf("user:likes_count:%s", recipientID)

	// Fetch element from the high-speed Redis cluster
	val, err := s.redisClient.Get(ctx, cacheKey).Result()
	if err != nil {
		if err == redis.Nil {
			// Cache Miss Scenario: Return 0 count back to the upstream business orchestrator (exploreServiceRead).
			// The orchestrator will handle the database fallback query and asynchronously populate the cache later.
			return &pb.CountResponse{Count: 0}, nil
		}
		return nil, status.Errorf(codes.Internal, "redis storage backend connection error: %v", err)
	}

	// Intercept and short-circuit query if it hits the penetration placeholder sentinel
	if val == CachePenetrationSentinel {
		return &pb.CountResponse{Count: 0}, nil
	}

	var count int64
	if _, fmtErr := fmt.Sscanf(val, "%d", &count); fmtErr != nil {
		return nil, status.Errorf(codes.Internal, "cache payload serialization corrupted: %v", fmtErr)
	}

	return &pb.CountResponse{Count: uint64(count)}, nil
}

// RemoveMutualMatchFromCache: Atomic cache eviction when a match criteria is achieved.
func (s *CacheServer) RemoveMutualMatchFromCache(ctx context.Context, req *pb.EvictRequest) (*pb.EmptyResponse, error) {
	userA := req.GetUserA()
	userB := req.GetUserB()
	if userA == "" || userB == "" {
		return nil, status.Error(codes.InvalidArgument, "user_a and user_b identifiers are required")
	}

	listKeyA := fmt.Sprintf("user:new_likes:%s", userA)
	listKeyB := fmt.Sprintf("user:new_likes:%s", userB)

	pipe := s.redisClient.Pipeline()
	pipe.ZRem(ctx, listKeyA, userB)
	pipe.ZRem(ctx, listKeyB, userA)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "redis pipeline cache eviction failed: %v", err)
	}

	return &pb.EmptyResponse{}, nil
}

func (s *CacheServer) ListNewLikedYouFromCache(ctx context.Context, req *pb.CacheListRequest) (*pb.CacheListResponse, error) {
	recipientID := req.GetRecipientUserId()

	if recipientID == "" {
		return nil, status.Error(codes.InvalidArgument, "recipient_user_id is required")
	}

	key := fmt.Sprintf("user:new_likes:%s", recipientID)
	var limit int64 = 20

	zObjects, err := s.redisClient.ZRevRangeWithScores(ctx, key, 0, limit-1).Result()

	if err != nil || len(zObjects) == 0 {
		return &pb.CacheListResponse{
			Likers:              []*pb.CacheListResponse_Liker{},
			NextPaginationToken: nil,
		}, nil
	}

	var likers []*pb.CacheListResponse_Liker

	for _, zObj := range zObjects {
		rawMember, ok := zObj.Member.(string)
		if !ok {
			continue
		}

		parts := strings.Split(rawMember, ":")
		if len(parts) < 2 {
			continue
		}

		actualActorID := parts[0]
		likedStatusStr := parts[1]

		if likedStatusStr == "true" {
			likers = append(likers, &pb.CacheListResponse_Liker{
				ActorId:       actualActorID,
				UnixTimestamp: uint64(zObj.Score),
			})
		}
	}

	var nextToken *string
	if int64(len(zObjects)) == limit && len(likers) > 0 {
		lastLiker := likers[len(likers)-1]
		tokenStr := lastLiker.GetActorId()
		nextToken = &tokenStr
	}

	return &pb.CacheListResponse{
		Likers:              likers,
		NextPaginationToken: nextToken,
	}, nil
}

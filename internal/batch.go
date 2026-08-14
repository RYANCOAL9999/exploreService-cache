package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
)

const GlobalIndexKey = "global:active_recipients"

// specifies the maximum number of data items that can be popped from Redis and written to Kafka in each batch.
const BatchSize int64 = int64(100)

type Decision struct {
	ActorUserID     string `gorm:"type:varchar(64);not null;uniqueIndex:idx_actor_recipient,priority:1;index:idx_actor_liked"`
	RecipientUserID string `gorm:"type:varchar(64);not null;uniqueIndex:idx_actor_recipient,priority:2;index:idx_recipient_liked_created,priority:1"`
	LikedRecipient  bool   `gorm:"not null;index:idx_actor_liked;index:idx_recipient_liked_created,priority:2"`
}

// Calculates the remaining time until the next 2:00 AM
func timeUntilNext2AM() time.Timer {
	var now time.Time = time.Now()

	var duration time.Duration

	// Create a time object for 2:00 AM today
	next2AM := time.Date(now.Year(), now.Month(), now.Day(), 2, 0, 0, 0, now.Location())

	// Create a time object for 6:00 AM today
	next6AM := time.Date(now.Year(), now.Month(), now.Day(), 6, 0, 0, 0, now.Location())

	if (now.After(next2AM) || now.Equal(next2AM)) && now.Before(next6AM) {
		duration = 0 // Section A: If the timeframe is between 2:00 AM and 6:00 AM (inclusive), return 0 and execute immediately.
	} else if now.Before(next2AM) {
		duration = time.Until(next2AM) // Section B: Before 2:00 AM, calculate the remaining time for the day as usual.
	} else {
		duration = time.Until(next2AM.AddDate(0, 0, 1)) // Section C: It's past 6:00 AM today, so the target is postponed to 2:00 AM tomorrow.
	}

	fmt.Printf("[INFO] Sleeping for %s until next 2:00 AM...\n", duration)

	return *time.NewTimer(duration)
}

func generativeKakfaMessage(record string, recipientUserID string) (*string, []byte, error) {

	if len(record) <= 0 {
		return nil, nil, fmt.Errorf("record length is less than 0")
	}

	if recipientUserID == "" {
		return nil, nil, fmt.Errorf("recipientUserID is empty")
	}

	parts := strings.Split(record, ":")
	if len(parts) < 2 {
		return nil, nil, fmt.Errorf("parts is less than 2")
	}

	decision := Decision{
		ActorUserID:     parts[0],
		RecipientUserID: recipientUserID,
		LikedRecipient:  parts[1] == "true",
	}

	payload, err := json.Marshal(decision)

	return &parts[0], payload, err
}

func generativeKafkaMessageArray(topic string, recipientID string, records []string) []kafka.Message {

	if len(records) <= 0 {
		return nil
	}

	if recipientID == "" {
		return nil
	}

	if topic == "" {
		return nil
	}

	var kafkaMessages []kafka.Message

	for _, record := range records {
		partsZero, payload, err := generativeKakfaMessage(record, recipientID)
		if err != nil {
			log.Printf("generativeKafkaMessage failed: %v", err)
			continue
		}
		kafkaMessages = append(kafkaMessages, kafka.Message{
			Topic: topic,
			Key:   []byte(*partsZero),
			Value: payload,
		})
	}

	return kafkaMessages
}

func (s *CacheServer) FireMessage(ctx context.Context, kafkaMessages []kafka.Message, redisIndexKey string, recipientIDs []string) error {
	if len(kafkaMessages) <= 0 {
		return fmt.Errorf("[KAFKA-CRITICAL] Writing to Kafka failed without data")
	}

	err := s.writer.WriteMessages(ctx, kafkaMessages...)

	if err == nil {
		return nil
	}

	log.Printf("[KAFKA-CRITICAL] Writing to Kafka failed: %v. Pushing the ID back into the Redis index.", err)

	if redisIndexKey == "" {
		return fmt.Errorf("[KAFKA-CRITICAL] Push Back Fail without redisIndexKey")
	}

	if len(recipientIDs) <= 0 {
		return fmt.Errorf("[KAFKA-CRITICAL] Push Back Fail without recipientIDs")
	}

	_, err = s.redisClient.SAdd(ctx, redisIndexKey, recipientIDs).Result()

	return fmt.Errorf("[KAFKA-CRITICAL] Push Back Fail: %v", err)

}

func (s *CacheServer) CleanUpMessage(ctx context.Context, processedListKeys []string) error {
	if len(processedListKeys) <= 0 {
		return fmt.Errorf("[ERROR] Clean Redis old cache without data")
	}

	err := s.redisClient.Del(ctx, processedListKeys...)

	return fmt.Errorf("[ERROR] Clean Redis old cache failed: %v", err)

}

func (s *CacheServer) handleSPopN(ctx context.Context, redisIndexKey string) ([]string, error) {

	if redisIndexKey == "" {
		return nil, fmt.Errorf("[KAFKA-MIGRATION] redisIndexKey is empty.")
	}

	recipientIDs, err := s.redisClient.SPopN(ctx, redisIndexKey, BatchSize).Result()

	if len(recipientIDs) == 0 {
		return nil, fmt.Errorf("[KAFKA-MIGRATION] The index %s has been fully processed.", redisIndexKey)
	}

	if err == redis.Nil {
		return nil, fmt.Errorf("[KAFKA-MIGRATION] The index %s has been fully processed.", redisIndexKey)
	}

	if err != nil {
		return nil, fmt.Errorf("[KAFKA-MIGRATION-ERROR] SPOP failed: %v", err)
	}

	return recipientIDs, nil
}

func (s *CacheServer) MigrateActiveUsersToKafka(ctx context.Context, redisIndexKey string) {

	if redisIndexKey == "" {
		return
	}

	log.Printf("[SCHEDULER] Start moving %s data.", redisIndexKey)

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "myQueue"
	}

	// executes Redis's "SPOP key count" command, which is also atomic.
	recipientIDs, err := s.handleSPopN(ctx, redisIndexKey)
	if err != nil {
		log.Printf("%s", err.Error())
		return
	}

	var kafkaMessages []kafka.Message
	var processedListKeys []string

	// Take the safe pop-up ID and retrieve the corresponding ZSET data from Redis
	for _, recipientID := range recipientIDs {

		listKey := fmt.Sprintf("user:new_likes:%s", recipientID)

		records, err := s.redisClient.ZRange(ctx, listKey, 0, -1).Result()
		if err != nil || len(records) == 0 {
			log.Printf("ZRange insert error: %s", err.Error())
			continue
		}

		processedListKeys = append(processedListKeys, listKey)
		kafkaMessages = generativeKafkaMessageArray(topic, recipientID, records)
	}

	// fire message to kafka
	err = s.FireMessage(ctx, kafkaMessages, redisIndexKey, recipientIDs)
	if err != nil {
		fmt.Printf("%s", err.Error())
	}

	// Clean up the Redis cache after success.
	err = s.CleanUpMessage(ctx, processedListKeys)
	if err != nil {
		fmt.Printf("%s", err.Error())
	}
}

func (s *CacheServer) StartScheduledGoroutine(ctx context.Context) {

	// Con job
	// after do the migration on the databases.
	// May be 2:00 am to 6:00 am
	// Using goroutines to call AWS Kafka with this three data
	// ActorUserId:     req.ActorUserID
	// RecipientUserId: req.RecipientUserID
	// LikedRecipient:  req.LikedRecipient
	// Pub topic to Kafka
	// exploreService - exploreServiceWrite - Kafka Sub Services
	// sub topic for Kafka, after topic will call PutDecision
	// exploreService - exploreServiceRead
	// keep continue with MutualLikes, ListLikedYou, ListNewLikedYou, CountLikedYou
	// Start the background schedule loop
	// s.startScheduledGoroutine(ctx)

	go func() {
		for {
			timer := timeUntilNext2AM()
			select {
			case <-timer.C:
				s.MigrateActiveUsersToKafka(ctx, GlobalIndexKey)
				time.Sleep(5 * time.Second)
			case <-ctx.Done():
				log.Println("[SCHEDULER] Received system shutdown signal, canceled timer, safely exited background scheduler.。")
				timer.Stop()
				return
			}
		}
	}()
}

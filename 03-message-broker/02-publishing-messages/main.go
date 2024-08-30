package main

import (
	"encoding/json"
	"os"

	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := watermill.NewStdLogger(false, false)
	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	payload, err := json.Marshal(50)
	if err != nil {
		panic(err)
	}
	msg := message.NewMessage(watermill.NewUUID(), payload)

	if err = publisher.Publish("progress", msg); err != nil {
		panic(err)
	}

	payload, err = json.Marshal(100)
	if err != nil {
		panic(err)
	}
	msg = message.NewMessage(watermill.NewUUID(), payload)

	if err = publisher.Publish("progress", msg); err != nil {
		panic(err)
	}
}

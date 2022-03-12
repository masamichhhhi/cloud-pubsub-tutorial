package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
)

const TOPIC_NAME = "avrotopic"

func main() {
	loadEnv()
	ctx := context.Background()

	c, err := pubsub.NewClient(ctx, os.Getenv("GCLOUD_PROJECT_ID"))
	if err != nil {
		log.Fatal(err)
	}
	topic := c.Topic(TOPIC_NAME)

	data := []string{
		`{"StringField":"hello", "FloatField":123.45, "BooleanField":true}`,
		`{"StringField":"world", "FloatField":0, "BooleanField":false}`,
		`{"NGField":"dummy"}`,
	}

	for _, v := range data {
		res := topic.Publish(ctx, &pubsub.Message{
			Data: []byte(v),
		})

		if _, err := res.Get(ctx); err != nil {
			log.Fatal(err)
		}

		fmt.Println("success publish", v)
	}
}

func loadEnv() {
	err := godotenv.Load(".env")

	if err != nil {
		panic("Error loading .env file")
	}
}

package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Couldn't connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Successful connection")

	connChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	err = pubsub.Subscribe(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		HandlerLogs(),
		pubsub.DecodeGob[routing.GameLog],
	)
	if err != nil {
		log.Fatalf("Couldn't subscribe to game_logs: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		inputs := gamelogic.GetInput()
		switch inputs[0] {
		case "pause":
			fmt.Println("Publishing paused game state")
			err = pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Fatalf("could not publish time: %v", err) // ???
			}
		case "resume":
			fmt.Println("Publishing resume game state")
			err = pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Fatalf("Couldn't publish time: %v", err)
			}
		case "quit":
			fmt.Println("Exiting the game")
			return
		default:
			fmt.Printf("Unknown command: %s\n", inputs[0])
		}
	}
}

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
	fmt.Println("Starting Peril client...")

	connString := "amqp://guest:guest@localhost:5672"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Could not get username: %v", err)
	}

	gs := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("Couldn't subscribe to pause: %v", err)
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		switch inputs[0] {
		case "spawn":
			err = gs.CommandSpawn(inputs)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			_, err = gs.CommandMove(inputs)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Sapmming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("Unknown command: %s\n", inputs[0])
		}

	}
}

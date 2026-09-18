package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Couldn't create channel: %v", err)
	}
	defer ch.Close()

	err = pubsub.Subscribe(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.Transient,
		HandlerPause(gs),
		pubsub.UnmarshalJSON[routing.PlayingState],
	)
	if err != nil {
		log.Fatalf("Couldn't subscribe to pause: %v", err)
	}

	err = pubsub.Subscribe(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gs.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		HandlerMove(gs, ch),
		pubsub.UnmarshalJSON[gamelogic.ArmyMove],
	)
	if err != nil {
		log.Fatalf("Couldn't subscribe to armymove: %v", err)
	}

	err = pubsub.Subscribe(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
		pubsub.Durable,
		HandlerWar(gs, ch),
		pubsub.UnmarshalJSON[gamelogic.RecognitionOfWar],
	)
	if err != nil {
		log.Fatalf("Couldn't subscribe to war: %v", err)
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
			mv, err := gs.CommandMove(inputs)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+".*",
				mv,
			)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(inputs) != 2 {
				fmt.Println("Not enough inputs. Follow spam with a number")
				continue
			}
			n, err := strconv.Atoi(inputs[1])
			if err != nil {
				fmt.Printf("error: %s is not a valid number\n", inputs[1])
				continue
			}
			for i := 0; i < n; i++ {
				mlog := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGameLog(ch, routing.GameLog{
					CurrentTime: time.Now(),
					Message:     mlog,
					Username:    gs.GetUsername(),
				})
				if err != nil {
					fmt.Println(err)
					continue
				}
			}
			fmt.Printf("Published %v malicious logs\n", n)
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("Unknown command: %s\n", inputs[0])
		}
	}
}

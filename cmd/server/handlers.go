package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func HandlerLogs() func(gl routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Println("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Printf("Failed to write log to disk: %v", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

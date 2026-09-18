package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func Subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	ch.Qos(10, 0, false)
	msgChan, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		defer ch.Close()
		for msg := range msgChan {
			output, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Println("Failed to decode message")
				continue
			}
			switch handler(output) {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Could not create channel: %v", err)
	}

	isDurable := false
	autoDelete := true
	exclusive := true
	if queueType == Durable {
		isDurable = true
		autoDelete = false
		exclusive = false
	}

	queue, err := ch.QueueDeclare(queueName, isDurable, autoDelete, exclusive, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Couldn't bind queue: %v", err)
	}

	return ch, queue, nil
}

func UnmarshalJSON[T any](input []byte) (T, error) {
	var output T
	err := json.Unmarshal(input, &output)
	return output, err
}

func DecodeGob[T any](input []byte) (T, error) {
	var output T
	buffer := bytes.NewBuffer(input)
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&output)
	return output, err
}

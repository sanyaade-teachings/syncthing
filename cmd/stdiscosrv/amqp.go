package main

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type amqpReplicator struct {
	dst      string
	clientID string
	db       database
	outbox   chan ReplicationRecord
}

func newAMQPReplicator(dst, clientID string, db database) *amqpReplicator {
	return &amqpReplicator{
		dst:      dst,
		clientID: clientID,
		db:       db,
		outbox:   make(chan ReplicationRecord, replicationOutboxSize),
	}
}

func (s *amqpReplicator) Serve(ctx context.Context) error {
	conn, err := amqp.Dial(s.dst)
	if err != nil {
		log.Println("RabbitMQ connect:", err)
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Println("RabbitMQ channel:", err)
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"discovery", // name
		"fanout",    // type
		false,       // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Println("RabbitMQ exchange:", err)
		return err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Println("RabbitMQ queue:", err)
		return err
	}

	err = ch.QueueBind(
		q.Name,      // queue name
		"",          // routing key
		"discovery", // exchange
		false,
		nil,
	)
	if err != nil {
		log.Println("RabbitMQ bind:", err)
		return err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Println("RabbitMQ consume:", err)
		return err
	}

	// Send records.
	buf := make([]byte, 1024)
	for {
		select {
		case rec := <-s.outbox:
			size := rec.Size()
			if len(buf) < size {
				buf = make([]byte, size)
			}

			n, err := rec.MarshalTo(buf)
			if err != nil {
				// odd to get an error here, but we haven't sent anything
				// yet so it's not fatal
				replicationSendsTotal.WithLabelValues("error").Inc()
				log.Println("Replication marshal:", err)
				continue
			}

			err = ch.PublishWithContext(ctx,
				"discovery", // exchange
				"",          // routing key
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        buf[:n],
					AppId:       s.clientID,
				})
			if err != nil {
				replicationSendsTotal.WithLabelValues("error").Inc()
				log.Println("RabbitMQ publish:", err)
				continue
			}
			replicationSendsTotal.WithLabelValues("success").Inc()

		case msg := <-msgs:
			if msg.AppId == s.clientID {
				continue
			}

			var rec ReplicationRecord
			if err := rec.Unmarshal(msg.Body); err != nil {
				log.Println("Replication unmarshal:", err)
				continue
			}

			s.db.merge(rec.Key, rec.Addresses, rec.Seen)
			replicationRecvsTotal.WithLabelValues("success").Inc()

		case <-ctx.Done():
			return nil
		}
	}
}

func (s *amqpReplicator) String() string {
	return fmt.Sprintf("kubeMQReplicator(%q)", s.dst)
}

func (s *amqpReplicator) send(key string, ps []DatabaseAddress, _ int64) {
	item := ReplicationRecord{
		Key:       key,
		Addresses: ps,
	}

	// The send should never block. The inbox is suitably buffered for at
	// least a few seconds of stalls, which shouldn't happen in practice.
	select {
	case s.outbox <- item:
	default:
		replicationSendsTotal.WithLabelValues("drop").Inc()
	}
}

package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type RabbitMQQueue struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

var instance *RabbitMQQueue
var once sync.Once

var rabbitMQConnection *amqp.Connection
var rabbitMQChannel *amqp.Channel

// Singleton para RabbitMQQueue
func GetRabbitMQInstance(amqpURL string, queueName string) (*RabbitMQQueue, error) {
	var err error
	once.Do(func() {
		instance, err = NewRabbitMQQueue(amqpURL, queueName)
	})
	return instance, err
}

// Inicializa a fila no RabbitMQ
func NewRabbitMQQueue(amqpURL string, queueName string) (*RabbitMQQueue, error) {

	if rabbitMQConnection == nil || rabbitMQConnection.IsClosed() {
		var err error
		rabbitMQConnection, err = amqp.DialConfig(amqpURL, amqp.Config{
			Heartbeat: 10 * time.Second,
		})
		if err != nil {
			log.Error().Msg("Falha ao conectar ao RabbitMQ:" + err.Error())
			return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
		}
	}

	if rabbitMQChannel == nil {
		var err error
		rabbitMQChannel, err = rabbitMQConnection.Channel()
		if err != nil {
			log.Error().Msg("Falha ao abrir um canal:" + err.Error())
			rabbitMQConnection.Close()
			return nil, fmt.Errorf("failed to open channel: %v", err)
		}
	}

	q, err := rabbitMQChannel.QueueDeclare(
		queueName,
		true,                             // Persistente
		false,                            // Não deletar automaticamente
		false,                            // Não exclusiva
		false,                            // Sem espera
		amqp.Table{"x-max-priority": 10}, // Habilita prioridade
	)
	if err != nil {
		log.Error().Msg("Falha ao declarar a fila:" + err.Error())
		return nil, fmt.Errorf("failed to declare queue: %v", err)
	}

	return &RabbitMQQueue{
		conn:    rabbitMQConnection,
		channel: rabbitMQChannel,
		queue:   q,
	}, nil
}

// Adiciona uma mensagem na fila com prioridade
func (q *RabbitMQQueue) Enqueue(message string, priority uint8) error {
	return q.channel.Publish(
		"",
		q.queue.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Priority:     priority,
			ContentType:  "text/plain",
			Body:         []byte(message),
		},
	)
}

// Consome uma mensagem da fila
func (q *RabbitMQQueue) Dequeue() (<-chan amqp.Delivery, error) {
	return q.channel.Consume(
		q.queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

// Close fecha o canal e a conexão AMQP
func (q *RabbitMQQueue) Close() error {
	if q == nil {
		return nil
	}
	var err error
	if q.channel != nil {
		if closeErr := q.channel.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close RabbitMQ channel")
			err = closeErr
		}
	}
	if q.conn != nil && !q.conn.IsClosed() {
		if closeErr := q.conn.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close RabbitMQ connection")
			err = closeErr
		}
	}
	return err
}

package main

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type RabbitMQQueue struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// Inicializa a fila no RabbitMQ
func NewRabbitMQQueue(amqpURL string, queueName string) (*RabbitMQQueue, error) {
	conn, err := amqp.DialConfig(amqpURL, amqp.Config{
		Heartbeat: 10 * time.Second,
	})
	if err != nil {
		log.Error().Msg("Falha ao conectar ao RabbitMQ:" + err.Error())
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Error().Msg("Falha ao abrir um canal:" + err.Error())
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %v", err)
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,                             // Persistente
		false,                            // Não deletar automaticamente
		false,                            // Não exclusiva
		false,                            // Sem espera
		amqp.Table{"x-max-priority": 10}, // Habilita prioridade
	)
	if err != nil {
		ch.Close()   // Fecha o canal se falhar ao declarar a fila
		conn.Close() // Fecha a conexão se falhar ao declarar a fila
		log.Error().Msg("Falha ao declarar a fila:" + err.Error())
	}

	return &RabbitMQQueue{
		conn:    conn,
		channel: ch,
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
		return nil // Retorna nil se o queue for nil para evitar pânico
	}
	var err error
	if q.channel != nil {
		if closeErr := q.channel.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close RabbitMQ channel")
			err = closeErr // Armazena o último erro
		}
	}
	if q.conn != nil {
		if closeErr := q.conn.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Failed to close RabbitMQ connection")
			err = closeErr // Armazena o último erro
		}
	}
	return err
}

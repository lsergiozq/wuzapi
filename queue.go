package main

import (
	"log"

	"github.com/streadway/amqp"
)

type RabbitMQQueue struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// Inicializa a fila no RabbitMQ
func NewRabbitMQQueue(amqpURL string, queueName string) *RabbitMQQueue {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatal("Falha ao conectar ao RabbitMQ:", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Falha ao abrir um canal:", err)
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
		log.Fatal("Falha ao declarar a fila:", err)
	}

	return &RabbitMQQueue{
		conn:    conn,
		channel: ch,
		queue:   q,
	}
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

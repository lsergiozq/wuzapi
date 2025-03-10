package main

import (
	"encoding/json"
	"errors"
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

type UserConsumer struct {
	queue      *RabbitMQQueue
	cancelChan chan struct{}
}

type MessageData struct {
	Id         string          `json:"Id"`
	Phone      string          `json:"Phone"`
	MsgProto   json.RawMessage `json:"MsgProto"`
	Userid     int             `json:"Userid"`
	RetryCount int             `json:"RetryCount,omitempty"`
}

var (
	queueInstance  *RabbitMQQueue
	once           sync.Once
	userConsumers  = make(map[int]*UserConsumer)
	consumersMutex sync.Mutex
)

// Singleton para RabbitMQQueue
// Inicializa a conexão global RabbitMQ
func GetRabbitMQInstance(amqpURL string) (*RabbitMQQueue, error) {
	var initErr error
	once.Do(func() {
		conn, err := amqp.DialConfig(amqpURL, amqp.Config{Heartbeat: 10 * time.Second})
		if err != nil {
			initErr = fmt.Errorf("failed to connect to RabbitMQ: %v", err)
			log.Error().Err(err).Msg("Failed to connect to RabbitMQ")
			return
		}
		ch, err := conn.Channel()
		if err != nil {
			initErr = fmt.Errorf("failed to open global channel: %v", err)
			log.Error().Err(err).Msg("Failed to open global channel")
			conn.Close()
			return
		}
		err = ch.ExchangeDeclare(
			"WuzAPI_DLX",
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			initErr = fmt.Errorf("failed to declare DLX: %v", err)
			log.Error().Err(err).Msg("Failed to declare DLX")
			ch.Close()
			conn.Close()
			return
		}
		dlq, err := ch.QueueDeclare(
			"WuzAPI_Dead_Letter_Queue",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			initErr = fmt.Errorf("failed to declare DLQ: %v", err)
			log.Error().Err(err).Msg("Failed to declare DLQ")
			ch.Close()
			conn.Close()
			return
		}
		err = ch.QueueBind(
			dlq.Name,
			"",
			"WuzAPI_DLX",
			false,
			nil,
		)
		if err != nil {
			initErr = fmt.Errorf("failed to bind DLQ to DLX: %v", err)
			log.Error().Err(err).Msg("Failed to bind DLQ to DLX")
			ch.Close()
			conn.Close()
			return
		}
		queueInstance = &RabbitMQQueue{conn: conn, channel: ch}
	})

	if initErr != nil {
		return nil, initErr
	}
	if queueInstance == nil {
		return nil, fmt.Errorf("RabbitMQ instance not initialized")
	}
	return queueInstance, nil
}

// Cria fila e canal por usuário
// GetUserQueue initializes and returns a RabbitMQ queue for a specific user.
// It connects to the RabbitMQ instance using the provided AMQP URL, opens a channel,
// and declares a queue with a name based on the user ID. The queue is configured with
// a maximum priority and a dead-letter exchange.
//
// Parameters:
//   - amqpURL: The URL to connect to the RabbitMQ instance.
//   - userID: The ID of the user for whom the queue is being created.
//
// Returns:
//   - *RabbitMQQueue: A pointer to the RabbitMQQueue struct containing the connection,
//     channel, and declared queue.
//   - error: An error if any step in the process fails, otherwise nil.
func GetUserQueue(amqpURL string, userID int) (*RabbitMQQueue, error) {
	globalQueue, err := GetRabbitMQInstance(amqpURL)
	if err != nil {
		return nil, err
	}
	if globalQueue == nil || globalQueue.conn == nil {
		return nil, fmt.Errorf("RabbitMQ instance not initialized")
	}

	consumersMutex.Lock()
	if existingConsumer, exists := userConsumers[userID]; exists {
		existingConsumer.queue.Close()
		delete(userConsumers, userID)
		log.Warn().Int("userID", userID).Msg("Canal antigo fechado antes de criar um novo")
	}
	consumersMutex.Unlock()

	ch, err := globalQueue.conn.Channel()
	if err != nil {
		log.Error().Err(err).Int("userID", userID).Msg("Falha ao abrir canal para o usuário")
		return nil, err
	}

	queueName := fmt.Sprintf("WuzAPI_Messages_Queue_%d", userID)
	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-max-priority":         10,
			"x-dead-letter-exchange": "WuzAPI_DLX",
		},
	)
	if err != nil {
		log.Error().Err(err).Int("userID", userID).Msg("Falha ao declarar fila do usuário")
		ch.Close()
		return nil, err
	}

	return &RabbitMQQueue{conn: globalQueue.conn, channel: ch, queue: q}, nil
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
			ContentType:  "application/json",
			Body:         []byte(message),
		},
	)
}

func (q *RabbitMQQueue) Dequeue() (<-chan amqp.Delivery, error) {
	return q.channel.Consume(
		q.queue.Name,
		fmt.Sprintf("consumer-%d", time.Now().UnixNano()),
		false,
		false,
		false,
		false,
		nil,
	)
}

func (q *RabbitMQQueue) Close() error {
	if q.channel != nil {
		return q.channel.Close()
	}
	return nil
}

// getValidNumber checks if a given phone number is registered on WhatsApp for a specific user.
// It returns the JID (WhatsApp ID) of the phone number if it is valid, or an error if the number is not found or if there is an issue with the verification process.
//
// Parameters:
//   - userid: An integer representing the user ID.
//   - phone: A string containing the phone number to be checked.
//
// Returns:
//   - A string containing the JID of the phone number if it is valid.
//   - An error if the phone number is not found on WhatsApp or if there is an error during the verification process.
func GetValidNumber(userid int, phone string) (string, error) {
	// Cria um array de string com o número original
	phones := []string{phone}

	// Verifica se o número está no WhatsApp
	resp, err := clientPointer[userid].IsOnWhatsApp(phones)
	if err != nil {
		return "", fmt.Errorf("erro ao verificar número no WhatsApp: %v", err)
	}

	// Verifica se a resposta está vazia
	if len(resp) == 0 {
		return "", errors.New("número de telefone " + phone + " não encontrado no WhatsApp")
	}

	// Extrai o JID do primeiro item (ou todos se preferir concatenar)``
	jid := resp[0].JID.User + "@" + resp[0].JID.Server

	// Retorna o JID formatado
	return jid, nil
}

// Pool de Consumidores
func StartUserConsumers(s *server, amqpURL string, globalCancelChan <-chan struct{}) {
	const maxQueues = 150 // Adiciona limite
	go func() {
		for {
			select {
			case <-globalCancelChan:
				consumersMutex.Lock()
				for userID, consumer := range userConsumers {
					close(consumer.cancelChan)
					if err := consumer.queue.Close(); err != nil {
						log.Warn().Err(err).Int("userID", userID).Msg("Failed to close user queue")
					}
					delete(userConsumers, userID)
				}
				consumersMutex.Unlock()
				return
			case <-time.After(5 * time.Second):
				consumersMutex.Lock()
				activeUsers := make(map[int]bool)
				queueCount := len(userConsumers)

				for userID := range clientPointer {
					activeUsers[userID] = true
					if queueCount >= maxQueues {
						log.Warn().Msgf("Max queues reached (%d/%d), skipping new consumers", queueCount, maxQueues)
						continue
					}
					if _, exists := userConsumers[userID]; !exists {
						queue, err := GetUserQueue(amqpURL, userID)
						if err != nil {
							log.Error().Err(err).Int("userID", userID).Msg("Failed to create user queue")
							continue
						}
						userCancelChan := make(chan struct{})
						userConsumers[userID] = &UserConsumer{queue: queue, cancelChan: userCancelChan}
						go ProcessUserMessages(queue, s, userID, userCancelChan)
						queueCount++
						log.Info().Int("userID", userID).Msg("Started consumer for user")
					}
				}

				for userID, consumer := range userConsumers {
					if !activeUsers[userID] {
						close(consumer.cancelChan)
						if err := consumer.queue.Close(); err != nil {
							log.Warn().Err(err).Int("userID", userID).Msg("Failed to close user queue")
						}
						delete(userConsumers, userID)
						queueCount--
						log.Info().Int("userID", userID).Msg("Stopped consumer for inactive user")
					}
				}
				consumersMutex.Unlock()
			}
		}
	}()
}

func ProcessUserMessages(queue *RabbitMQQueue, s *server, userID int, cancelChan <-chan struct{}) {
	for {
		deliveries, err := queue.Dequeue()
		if err != nil {
			log.Error().Err(err).Int("userID", userID).Msg("Failed to start consuming messages, retrying in 5 seconds")
			select {
			case <-cancelChan:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		for {
			select {
			case delivery, ok := <-deliveries:
				if !ok {
					log.Info().Int("userID", userID).Msg("Delivery channel closed")
					return
				}

				processMessage(delivery, s, queue)

			case <-cancelChan:
				log.Info().Int("userID", userID).Msg("Shutting down user consumer")
				return
			}
		}
	}
}

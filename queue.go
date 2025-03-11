package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
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
	userChannels   = make(map[int]*amqp.Channel) // Mapa para armazenar canais por userID
	channelsMutex  sync.Mutex                    // Protege o mapa
	consumersMutex sync.Mutex
)

const maxRetries = 3

// Singleton para RabbitMQQueue
// Inicializa a conexão global RabbitMQ
func GetRabbitMQInstance(amqpURL string) (*RabbitMQQueue, error) {
	var err error
	once.Do(func() {
		conn, err := amqp.DialConfig(amqpURL, amqp.Config{Heartbeat: 10 * time.Second})
		if err != nil {
			log.Error().Err(err).Msg("Failed to connect to RabbitMQ")
			return
		}
		ch, err := conn.Channel()
		if err != nil {
			log.Error().Err(err).Msg("Failed to open global channel")
			conn.Close()
			return
		}
		// Configura Dead Letter Exchange
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
			log.Error().Err(err).Msg("Failed to declare DLQ")
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
			log.Error().Err(err).Msg("Failed to bind DLQ to DLX")
			return
		}
		queueInstance = &RabbitMQQueue{conn: conn, channel: ch}
	})
	return queueInstance, err
}

func GetUserQueue(amqpURL string, userID int) (*RabbitMQQueue, error) {
	globalQueue, err := GetRabbitMQInstance(amqpURL)
	if err != nil {
		return nil, err
	}

	// Verifica se já existe um consumidor ativo
	if consumer, exists := userConsumers[userID]; exists && consumer.queue.channel != nil {
		log.Info().Int("userID", userID).Msg("Reusing existing queue for user")
		return consumer.queue, nil
	}

	ch, err := globalQueue.conn.Channel()
	if err != nil {
		log.Error().Err(err).Int("userID", userID).Msg("Failed to open user channel")
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
		log.Error().Err(err).Int("userID", userID).Msg("Failed to declare user queue")
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
	deliveries, err := q.channel.Consume(
		q.queue.Name,
		fmt.Sprintf("consumer-%d", time.Now().UnixNano()),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Error().Err(err).Str("queue", q.queue.Name).Msg("Failed to start consuming")
		return nil, err
	}

	log.Info().Str("queue", q.queue.Name).Msg("Waiting for messages...")
	return deliveries, nil
}

// Close fecha o canal explicitamente (chamado manualmente, se necessário)
func (q *RabbitMQQueue) Close() error {
	if q.channel != nil {
		err := q.channel.Close()
		if err != nil {
			log.Error().Err(err).Str("queue", q.queue.Name).Msg("Failed to close channel")
			return err
		}
		log.Info().Str("queue", q.queue.Name).Msg("Channel closed")
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
		return "", errors.New("número de telefone não encontrado no WhatsApp")
	}

	// Extrai o JID do primeiro item (ou todos se preferir concatenar)``
	jid := resp[0].JID.User + "@" + resp[0].JID.Server

	// Retorna o JID formatado
	return jid, nil
}

// Pool de Consumidores
func StartUserConsumers(s *server, amqpURL string, globalCancelChan chan struct{}) {
	go func() {
		for {
			select {
			case <-globalCancelChan:
				consumersMutex.Lock()
				for userID, consumer := range userConsumers {
					log.Warn().Int("userID", userID).Msg("Shutting down consumer")
					close(consumer.cancelChan)
					if err := consumer.queue.Close(); err != nil {
						log.Warn().Err(err).Int("userID", userID).Msg("Failed to close user queue")
					}
					delete(userConsumers, userID)
				}
				consumersMutex.Unlock()
				return

			case <-time.After(5 * time.Second):
				activeUsers := make(map[int]bool)

				for userID := range clientPointer {
					activeUsers[userID] = true

					// Evita bloqueios desnecessários
					consumersMutex.Lock()
					_, exists := userConsumers[userID]
					consumersMutex.Unlock()

					if !exists {
						queue, err := GetUserQueue(amqpURL, userID)
						if err != nil {
							log.Error().Err(err).Int("userID", userID).Msg("Failed to create user queue")
							continue
						}

						userCancelChan := make(chan struct{})

						// Agora protegemos a escrita no `userConsumers`
						consumersMutex.Lock()
						userConsumers[userID] = &UserConsumer{queue: queue, cancelChan: userCancelChan}
						consumersMutex.Unlock()

						go processUserMessages(queue, s, userID, userCancelChan)
						log.Info().Int("userID", userID).Msg("Started consumer for user")
					}
				}

				// Remover consumidores inativos sem bloquear o mutex por muito tempo
				for userID, consumer := range userConsumers {
					if !activeUsers[userID] {
						log.Warn().Int("userID", userID).Msg("Closing inactive consumer")
						close(consumer.cancelChan)

						consumersMutex.Lock()
						consumer.queue.Close()
						delete(userConsumers, userID)
						consumersMutex.Unlock()
					}
				}
			}
		}
	}()
}

func processUserMessages(queue *RabbitMQQueue, s *server, userID int, cancelChan chan struct{}) {
	deliveries, err := queue.Dequeue()
	if err != nil {
		log.Error().Err(err).Int("userID", userID).Msg("Failed to start consuming messages")
		return
	}

	log.Info().Int("userID", userID).Msg("Consumer started successfully")

	// Adicionando o consumidor ao userConsumers
	consumersMutex.Lock()
	userConsumers[userID] = &UserConsumer{queue: queue, cancelChan: cancelChan}
	consumersMutex.Unlock()

	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				log.Warn().Int("userID", userID).Msg("Delivery channel closed. Cleaning up resources...")
				consumersMutex.Lock()
				queue.Close() // Fecha o canal ao sair
				delete(userConsumers, userID)
				consumersMutex.Unlock()
				return
			}

			ProcessMessage(delivery, s, MessageData{Userid: userID}, queue)

		case <-cancelChan:
			log.Info().Int("userID", userID).Msg("Shutting down user consumer")
			consumersMutex.Lock()
			queue.Close() // Fecha o canal ao encerrar
			delete(userConsumers, userID)
			consumersMutex.Unlock()
			return
		}
	}
}

func ProcessMessage(delivery amqp.Delivery, s *server, msgData MessageData, queue *RabbitMQQueue) {

	if err := json.Unmarshal(delivery.Body, &msgData); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal message")
		delivery.Nack(false, true)
		return
	}

	if msgData.RetryCount >= maxRetries {
		log.Warn().Str("id", msgData.Id).Msg("Max retries reached, moving to DLQ")
		delivery.Nack(false, false)
		return
	}

	client, exists := clientPointer[msgData.Userid]
	if !exists || client == nil {
		log.Warn().Int("userID", msgData.Userid).Msg("No active session for user")
		delivery.Nack(false, false)
		return
	}

	var msgProto waProto.Message
	if err := json.Unmarshal(msgData.MsgProto, &msgProto); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal MsgProto")
		delivery.Nack(false, true)
		return
	}

	jid, err := GetValidNumber(msgData.Userid, msgData.Phone)
	if err != nil {
		log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to get valid number")
		delivery.Nack(false, true)
		return
	}

	recipient, ok := parseJID(jid)
	if !ok {
		log.Error().Int("userID", msgData.Userid).Msg("Invalid JID")
		delivery.Nack(false, true)
		return
	}

	resp, err := client.SendMessage(context.Background(), recipient, &msgProto, whatsmeow.SendRequestExtra{ID: msgData.Id})

	// Define status e detalhes do envio
	status := "success"
	details := "Mensagem enviada com sucesso"
	timestamp := int64(0)

	if err != nil {
		status = "error"
		details = err.Error()

		log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to send message")
		msgData.RetryCount++
		updatedMessage, err := json.Marshal(msgData)
		if err != nil {
			log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to marshal updated message")
			delivery.Nack(false, true)
			return
		}
		if err := queue.Enqueue(string(updatedMessage), delivery.Priority); err != nil {
			log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to re-enqueue message")
		}
		delivery.Ack(false)
	} else {
		timestamp = resp.Timestamp.Unix()
		log.Info().Str("id", msgData.Id).Str("timestamp", fmt.Sprintf("%d", resp.Timestamp)).Msg("Message sent")
		delivery.Ack(false)
	}

	sendWebhookNotification(s, msgData, timestamp, status, details)
}

func sendWebhookNotification(s *server, msgData MessageData, timestamp int64, status, details string) {
	// Obtém o webhook do usuário
	webhookurl := ""
	myuserinfo, found := userinfocache.Get(s.getTokenByUserId(msgData.Userid))
	if found {
		webhookurl = myuserinfo.(Values).Get("Webhook")
	}

	if webhookurl != "" {
		events := strings.Split(myuserinfo.(Values).Get("Events"), ",")

		// Após o envio, verificar se o webhook deve ser chamado
		if !Find(events, "CallBack") && !Find(events, "All") {
			log.Warn().Msg("Usuário não está inscrito para CallBack. Ignorando webhook.")
		} else {
			// Criar estrutura de evento no mesmo formato do wmiau.go
			postmap := map[string]interface{}{
				"type": "CallBack",
				"event": map[string]interface{}{
					"id":        msgData.Id,
					"phone":     msgData.Phone,
					"status":    status,
					"details":   details,
					"timestamp": timestamp,
				},
			}

			// Enviar para o webhook

			values, _ := json.Marshal(postmap)
			data := map[string]string{
				"jsonData": string(values),
				"token":    myuserinfo.(Values).Get("Token"),
			}
			go callHook(webhookurl, data, msgData.Userid)

			log.Info().Str("id", msgData.Id).Str("status", status).Msg("CallBack processado")
		}
	} else {
		log.Warn().Str("userid", fmt.Sprintf("%d", msgData.Userid)).Msg("Nenhum webhook configurado para este usuário")
	}
}

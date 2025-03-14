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
	Id            string          `json:"Id"`
	Phone         string          `json:"Phone"`
	MsgProto      json.RawMessage `json:"MsgProto"`
	Userid        int             `json:"Userid"`
	RetryCount    int             `json:"RetryCount,omitempty"`
	DLQRetryCount int             `json:"DLQRetryCount,omitempty"`
}

var (
	queueInstance  *RabbitMQQueue
	once           sync.Once
	userConsumers  = make(map[int]*UserConsumer)
	userChannels   = make(map[int]*amqp.Channel) // Mapa para armazenar canais por userID
	channelsMutex  sync.Mutex                    // Protege o mapa
	consumersMutex sync.Mutex
)

const maxRetries = 10
const maxDLQRetries = 10

// Singleton para RabbitMQQueue
// Inicializa a conexão global RabbitMQ
func GetRabbitMQInstance(amqpURL string) (*RabbitMQQueue, error) {
	var err error
	once.Do(func() {
		//log.Info().Msg("Initializing RabbitMQ connection...")

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

		//log.Info().Msg("RabbitMQ channel opened")

		// 🔹 Declara Exchange DLX
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
			log.Fatal().Err(err).Msg("Failed to declare DLX")
			ch.Close()
			conn.Close()
			return
		}

		// 🔹 Declara Exchange para mensagens com atraso
		err = ch.ExchangeDeclare(
			"WuzAPI_Delayed_Exchange",
			"direct", // ✅ RabbitMQ 4.0+ NÃO suporta mais x-delayed-message
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			log.Error().Err(err).Msg("Failed to declare Delayed Exchange")
			ch.Close()
			conn.Close()
			return
		}

		// 🔹 Declara Dead Letter Queue (DLQ)
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
			ch.Close()
			conn.Close()
			return
		}

		// 🔹 Declara a Retry Queue
		_, err = ch.QueueDeclare(
			"WuzAPI_Retry_Queue",
			true,
			false,
			false,
			false,
			amqp.Table{
				"x-message-ttl":          300000, // 5 minutos de TTL
				"x-dead-letter-exchange": "WuzAPI_Delayed_Exchange",
			},
		)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to declare Retry Queue")
			ch.Close()
			conn.Close()
			return
		}

		// 🔹 Associa a DLQ ao DLX (somente se a fila foi criada com sucesso)
		if dlq.Name != "" {
			err = ch.QueueBind(
				dlq.Name,
				"",
				"WuzAPI_DLX",
				false,
				nil,
			)
			if err != nil {
				log.Error().Err(err).Msg("Failed to bind DLQ to DLX")
				ch.Close()
				conn.Close()
				return
			}
		} else {
			log.Fatal().Msg("DLQ Name is empty, cannot bind to DLX")
			ch.Close()
			conn.Close()
			return
		}

		queueInstance = &RabbitMQQueue{conn: conn, channel: ch}
		//log.Info().Msg("RabbitMQ instance successfully initialized")
	})

	// 🔹 Se a conexão caiu, tente reconectar automaticamente
	if queueInstance == nil || queueInstance.conn == nil || queueInstance.conn.IsClosed() {
		log.Warn().Msg("RabbitMQ connection lost, reconnecting...")

		// Tenta reconectar até 3 vezes
		for i := 1; i <= 3; i++ {
			log.Warn().Int("attempt", i).Msg("Attempting RabbitMQ reconnection...")

			conn, err := amqp.DialConfig(amqpURL, amqp.Config{Heartbeat: 10 * time.Second})
			if err == nil {
				ch, err := conn.Channel()
				if err == nil {
					queueInstance = &RabbitMQQueue{conn: conn, channel: ch}
					//log.Info().Msg("RabbitMQ connection re-established")
					return queueInstance, nil
				}
				conn.Close()
			}

			// Aguarda antes de tentar novamente
			time.Sleep(5 * time.Second)
		}

		log.Fatal().Msg("Failed to reconnect to RabbitMQ after 3 attempts")
		return nil, fmt.Errorf("failed to reconnect to RabbitMQ")
	}

	return queueInstance, err
}

func GetUserQueue(amqpURL string, userID int) (*RabbitMQQueue, error) {
	globalQueue, err := GetRabbitMQInstance(amqpURL)
	if err != nil {
		return nil, err
	}

	// 🔹 Verifica se a conexão com RabbitMQ ainda está ativa
	if globalQueue.conn == nil || globalQueue.conn.IsClosed() {
		log.Warn().Int("userID", userID).Msg("RabbitMQ connection lost, reconnecting...")
		globalQueue, err = GetRabbitMQInstance(amqpURL)
		if err != nil {
			return nil, err
		}
	}

	// 🔹 Abre um novo canal para evitar erro "channel/connection is not open"
	ch, err := globalQueue.conn.Channel()
	if err != nil {
		log.Error().Err(err).Int("userID", userID).Msg("Failed to open user channel")
		return nil, err
	}

	queueName := fmt.Sprintf("WuzAPI_Messages_Queue_%d", userID)
	qUser, err := ch.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Auto-delete
		false, // Exclusive
		false, // No-wait
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

	// 🔹 Verifica se a fila foi criada corretamente antes de retornar
	if qUser.Name == "" {
		log.Error().Int("userID", userID).Msg("User queue name is empty, something went wrong")
		ch.Close()
		return nil, fmt.Errorf("failed to declare queue for user %d", userID)
	}

	// 🔹 Vincula a fila do usuário à `WuzAPI_Delayed_Exchange` para receber mensagens atrasadas
	err = ch.QueueBind(
		qUser.Name,
		fmt.Sprintf("user-%d", userID), // Routing key específica do usuário
		"WuzAPI_Delayed_Exchange",
		false,
		nil,
	)
	if err != nil {
		log.Error().Err(err).Int("userID", userID).Msg("Failed to bind user queue")
		ch.Close()
		return nil, err
	}

	return &RabbitMQQueue{conn: globalQueue.conn, channel: ch, queue: qUser}, nil
}

// Adiciona uma mensagem na fila com prioridade
func (q *RabbitMQQueue) Enqueue(message string, priority uint8, userID int) error {

	// 	interval := userSendIntervals[userID] // Obtém o intervalo definido para o usuário
	// if interval == 0 {
	//     interval = 5 * time.Second // Valor padrão, caso não tenha sido configurado
	// }

	interval := 5 * time.Second // Valor padrão, caso não tenha sido configurado

	err := q.channel.Publish(
		"WuzAPI_Delayed_Exchange",
		fmt.Sprintf("user-%d", userID), // Routing key consistente com o QueueBind
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Priority:     priority,
			ContentType:  "application/json",
			Body:         []byte(message),
			Headers: amqp.Table{
				"x-delay": int(interval.Milliseconds()),
			},
		},
	)
	if err != nil {
		log.Error().Err(err).Str("queue", q.queue.Name).Msg("Failed to enqueue message")
	}
	return err
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

	//log.Info().Str("queue", q.queue.Name).Msg("Waiting for messages...")
	return deliveries, nil
}

// Close fecha o canal explicitamente (chamado manualmente, se necessário)
func (q *RabbitMQQueue) Close() error {
	if q.channel != nil {
		err := q.channel.Close()
		if err != nil {
			return nil // ✅ Ignora erros ao fechar o canal
		}

		//log.Info().Str("queue", q.queue.Name).Msg("Channel closed")
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
						//log.Info().Int("userID", userID).Msg("Started consumer for user")
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

	//log.Info().Int("userID", userID).Msg("Consumer started successfully")

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
			//log.Info().Int("userID", userID).Msg("Shutting down user consumer")
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
		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", "Erro ao tentar decodificar a mensagem")
		delivery.Ack(false)
		return
	}

	if msgData.RetryCount >= maxRetries {
		log.Warn().Str("id", msgData.Id).Msg("Max retries reached, moving to DLQ")
		//sendWebhookNotification(s, msgData, time.Now().Unix(), "error", "Realizada a quantidade "+fmt.Sprintf("%d", maxRetries)+" de tentativas de envio")
		delivery.Nack(false, false)
		return
	}

	client, exists := clientPointer[msgData.Userid]
	if !exists || client == nil {
		log.Warn().Int("userID", msgData.Userid).Msg("No active session for user")
		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", "Nenhuma sessão ativa no WhatsApp")
		delivery.Ack(false)
		return
	}

	var msgProto waProto.Message
	if err := json.Unmarshal(msgData.MsgProto, &msgProto); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal MsgProto")

		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", "Erro ao tentar decodificar a mensagem MsgProto")

		delivery.Ack(false)
		return
	}

	jid, err := GetValidNumber(msgData.Userid, msgData.Phone)
	if err != nil {
		// Dispara webhook com erro
		errMsg := "Erro ao converter ao validar o telefone " + msgData.Phone

		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", errMsg)

		delivery.Ack(false)
		return
	}

	recipient, ok := parseJID(jid)
	if !ok {
		log.Error().Int("userID", msgData.Userid).Msg("Invalid JID")
		// Dispara webhook com erro
		errMsg := "Erro ao converter telefone para JID " + jid

		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", errMsg)

		delivery.Ack(false)
		return
	}

	//log.Info().Str("id", msgData.Id).Str("phone", msgData.Phone).Msg("Processing message from queue")

	resp, err := client.SendMessage(context.Background(), recipient, &msgProto, whatsmeow.SendRequestExtra{ID: msgData.Id})

	// Define status e detalhes do envio
	status := "success"
	details := "Mensagem enviada com sucesso"
	timestamp := int64(0)

	if err != nil {
		status = "error"
		details = err.Error()

		log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to send message")

		//verifica a mensagem de erro é "server returned error 479", se sim, reinicia a sessão
		if strings.Contains(err.Error(), "server returned error 479") {
			log.Warn().Int("userID", msgData.Userid).Msg("Reiniciando sessão do usuário")
			s.DisconnectUser(msgData.Userid)
			//tempo para reconectar de 2 segundos
			time.Sleep(2 * time.Second)
			s.ConnectUser(msgData.Userid)
		}

		msgData.RetryCount++
		updatedMessage, err := json.Marshal(msgData)
		if err != nil {
			log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to marshal updated message")
			delivery.Nack(false, true)
			return
		}
		if err := queue.Enqueue(string(updatedMessage), delivery.Priority, msgData.Userid); err != nil {
			log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to re-enqueue message")
			sendWebhookNotification(s, msgData, time.Now().Unix(), status, "Falha ao reenfileirar mensagem")
			delivery.Nack(false, false) // ✅ Só manda para DLQ se falhou ao reenfileirar
		} else {
			delivery.Ack(false) // ✅ Confirma a mensagem como processada se foi reenfileirada corretamente
		}

	} else {
		timestamp = resp.Timestamp.Unix()
		//log.Info().Str("id", msgData.Id).Str("timestamp", fmt.Sprintf("%d", resp.Timestamp)).Msg("Message sent")
		delivery.Ack(false) //Processada com sucesso
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

			//log.Info().Str("id", msgData.Id).Str("status", status).Msg("CallBack processado")
		}
	} else {
		log.Warn().Str("userid", fmt.Sprintf("%d", msgData.Userid)).Msg("Nenhum webhook configurado para este usuário")
	}
}

func StartDLQConsumer(s *server, amqpURL string) {
	go func() {
		queue, err := GetRabbitMQInstance(amqpURL)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize DLQ consumer")
			return
		}

		ch := queue.channel
		dlqName := "WuzAPI_Dead_Letter_Queue"

		msgs, err := ch.Consume(
			dlqName,
			"dlq-consumer",
			false, // ✨ Não auto-ack para podermos reenviar manualmente
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			log.Fatal().Err(err).Msg("Failed to consume DLQ messages")
			return
		}

		//log.Info().Msg("DLQ Consumer started, waiting for messages...")

		for msg := range msgs {
			go handleDLQMessage(s, msg, queue) // ✨ Processa cada mensagem separadamente
		}
	}()
}

func handleDLQMessage(s *server, msg amqp.Delivery, queue *RabbitMQQueue) {
	log.Warn().Str("message_id", string(msg.Body)).Msg("Message received from DLQ, checking retry attempts...")

	var msgData MessageData
	if err := json.Unmarshal(msg.Body, &msgData); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal DLQ message")
		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", "Erro ao processar mensagem da DLQ")
		msg.Ack(false) // ❌ Remove a mensagem da DLQ
		return
	}

	// 📌 Verifica se atingiu o limite de tentativas
	if msgData.DLQRetryCount >= maxDLQRetries {
		log.Error().Str("id", msgData.Id).Msg("Max retry attempts reached, discarding message")
		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", "Mensagem descartada após várias tentativas")
		msg.Ack(false) // ❌ Remove a mensagem da DLQ permanentemente
		return
	}

	// ✨ Incrementa o número de tentativas antes de reenviar
	msgData.DLQRetryCount++

	updatedMessage, _ := json.Marshal(msgData)

	err := queue.channel.Publish(
		"WuzAPI_Delayed_Exchange", // ✅ Certifique-se de que a mensagem será processada corretamente
		"WuzAPI_Retry_Queue",
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         updatedMessage,
		},
	)

	if err != nil {
		log.Error().Err(err).Str("id", msgData.Id).Msg("Failed to enqueue message in Retry Queue")
		sendWebhookNotification(s, msgData, time.Now().Unix(), "error", "Falha ao mover mensagem para Retry Queue")
	} else {
		//log.Info().Str("id", msgData.Id).Int("attempts", msgData.DLQRetryCount).Msg("Message moved to Retry Queue for 5 minutes")
	}

	msg.Ack(false) // ✨ Remove a mensagem da DLQ
}

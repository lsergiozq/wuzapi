package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/streadway/amqp"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
)

const maxRetries = 3 // Valor único e consistente em todo o código

func processQueue(queue *RabbitMQQueue, s *server, cancelChan <-chan struct{}) {
	var msgs <-chan amqp.Delivery
	var err error

	for {
		select {
		case <-cancelChan:
			log.Info().Msg("Shutting down RabbitMQ consumer")
			if queue != nil {
				queue.Close()
			}
			return
		default:
			if queue == nil || queue.conn == nil || queue.conn.IsClosed() {
				log.Warn().Msg("RabbitMQ connection lost, attempting to reconnect...")
				if queue != nil {
					queue.Close()
				}
				rabbitMQURL := getRabbitMQURL()
				queue, err = GetRabbitMQInstance(rabbitMQURL)
				if err != nil {
					log.Error().Err(err).Msg("Failed to reconnect, retrying in 5 seconds...")
					time.Sleep(5 * time.Second)
					continue
				}
			}

			if msgs == nil {
				msgs, err = queue.Dequeue()
				if err != nil {
					log.Error().Err(err).Msg("Error starting consumer, retrying...")
					queue.Close()
					queue = nil
					continue
				}
			}

			for msg := range msgs {
				processMessage(msg, s, queue)
			}
		}
	}
}

// Obtém o token do usuário pelo ID
func (s *server) getTokenByUserId(userid int) string {
	var token string
	err := s.db.QueryRow("SELECT token FROM users WHERE id = ?", userid).Scan(&token)
	if err != nil {
		log.Warn().Msg(fmt.Sprintf("Falha ao buscar token para o usuário ID %d", userid))
		return ""
	}
	return token
}

func processMessage(msg amqp.Delivery, s *server, queue *RabbitMQQueue) {
	var msgData struct {
		Id         string          `json:"Id"`
		Phone      string          `json:"Phone"`
		MsgProto   json.RawMessage `json:"MsgProto"`
		Userid     int             `json:"Userid"`
		RetryCount int             `json:"RetryCount,omitempty"`
	}

	if err := json.Unmarshal(msg.Body, &msgData); err != nil {
		log.Error().Msg("Erro ao decodificar mensagem: " + err.Error())
		msg.Nack(false, false)
		return
	}

	if msgData.Userid == 0 {
		log.Error().Msg("Nenhuma sessão ativa no WhatsApp")
		msg.Nack(false, false)
		return
	}

	var msgProto waProto.Message
	if err := json.Unmarshal(msgData.MsgProto, &msgProto); err != nil {
		log.Error().Msg("Erro ao decodificar MsgProto: " + err.Error())
		msg.Nack(false, false)
		return
	}

	// 🚀 Nova validação: Verifica se o número está no WhatsApp antes de enviar
	jid, err := GetValidNumber(msgData.Userid, msgData.Phone)
	if err != nil {
		log.Error().Err(err).Str("id", msgData.Id).Msg("Número inválido no WhatsApp")
		msg.Nack(false, false)
		return
	}

	recipient, ok := parseJID(jid)
	if !ok {
		log.Error().Msg("Erro ao converter telefone para JID")
		msg.Nack(false, false)
		return
	}

	client, exists := clientPointer[msgData.Userid]
	if !exists || client == nil {
		log.Warn().Int("userID", msgData.Userid).Msg("No active session for user")
		return
	}

	resp, err := client.SendMessage(context.Background(), recipient, &msgProto, whatsmeow.SendRequestExtra{ID: msgData.Id})

	if err != nil {
		msgData.RetryCount++
		if msgData.RetryCount >= maxRetries {
			log.Warn().Str("id", msgData.Id).Msg("Max retries reached, moving to DLQ")
			msg.Nack(false, false)
			return
		}

		updatedMessage, _ := json.Marshal(msgData)
		if err := queue.Enqueue(string(updatedMessage), msg.Priority); err != nil {
			log.Error().Err(err).Str("id", msgData.Id).Msg("Falha ao reenfileirar mensagem")
		}
		msg.Ack(false)
		return
	}

	msg.Ack(false)

	log.Info().
		Str("id", msgData.Id).
		Str("phone", msgData.Phone).
		Str("timestamp", fmt.Sprintf("%d", resp.Timestamp)).
		Msg("Mensagem enviada com sucesso")

	sendWebhookNotification(s, msgData, resp.Timestamp.Unix(), "success", "Mensagem enviada com sucesso")
}

func sendWebhookNotification(s *server, msgData struct {
	Id         string          `json:"Id"`
	Phone      string          `json:"Phone"`
	MsgProto   json.RawMessage `json:"MsgProto"`
	Userid     int             `json:"Userid"`
	RetryCount int             `json:"RetryCount,omitempty"`
}, timestamp int64, status, details string) {
	webhookurl := ""
	myuserinfo, found := userinfocache.Get(s.getTokenByUserId(msgData.Userid))
	if found {
		webhookurl = myuserinfo.(Values).Get("Webhook")
	}

	if webhookurl != "" {
		events := strings.Split(myuserinfo.(Values).Get("Events"), ",")

		if !Find(events, "CallBack") && !Find(events, "All") {
			log.Warn().Msg("Usuário não está inscrito para CallBack. Ignorando webhook.")
		} else {
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

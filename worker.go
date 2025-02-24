package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
)

func processQueue(queue *RabbitMQQueue, s *server, cancelChan <-chan struct{}) {
	maxRetries := 10
	retries := 0

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
				rabbitMQURL := getRabbitMQURL()
				var err error
				queue, err = GetRabbitMQInstance(rabbitMQURL)
				if err != nil {
					log.Error().Err(err).Msg("Failed to reconnect to RabbitMQ, retrying in 5 seconds...")
					retries++
					if retries > maxRetries {
						log.Fatal().Msg("Exceeded maximum number of reconnection attempts, exiting...")
						os.Exit(1)
					}
					time.Sleep(5 * time.Second)
					continue
				}
				retries = 0
			}

			msgs, err := queue.Dequeue()
			if err != nil {
				log.Error().Err(err).Msg("Error consuming messages from queue, attempting to reconnect...")
				if queue != nil {
					queue.Close()
				}
				queue = nil
				continue
			}

			for msg := range msgs {
				// Decodifica a mensagem JSON da fila
				var msgData struct {
					Id       string          `json:"Id"`
					Phone    string          `json:"Phone"`
					MsgProto json.RawMessage `json:"MsgProto"` // Armazena a mensagem como JSON bruto
					Userid   int             `json:"Userid"`
				}

				if err := json.Unmarshal(msg.Body, &msgData); err != nil {
					log.Error().Msg("Erro ao decodificar mensagem da fila: " + err.Error())
					continue
				}

				if msgData.Userid == 0 {
					log.Error().Msg("Nenhuma sessão ativa no WhatsApp")
					continue
				}

				// Decodifica `msgProto` diretamente da fila
				var msgProto waProto.Message
				if err := json.Unmarshal(msgData.MsgProto, &msgProto); err != nil {
					log.Error().Msg("Erro ao decodificar MsgProto: " + err.Error())
					continue
				}

				recipient, ok := parseJID(msgData.Phone)
				if !ok {
					log.Error().Msg("Erro ao converter telefone para JID")
					continue
				}

				// Envia a mensagem e captura o resultado
				resp, err := clientPointer[msgData.Userid].SendMessage(context.Background(), recipient, &msgProto, whatsmeow.SendRequestExtra{ID: msgData.Id})

				// Define status e detalhes do envio
				status := "success"
				details := "Mensagem enviada com sucesso"
				timestamp := int64(0)

				if err != nil {
					status = "error"
					details = err.Error()
				} else {
					timestamp = resp.Timestamp.Unix()
				}

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

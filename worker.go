package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
)

func processQueue(queue *RabbitMQQueue, s *server) {
	msgs, err := queue.Dequeue()
	if err != nil {
		log.Error().Msg("Erro ao consumir mensagens da fila: " + err.Error())
		return
	}

	for msg := range msgs {
		// Decodifica a mensagem JSON da fila
		var msgData struct {
			Id       string          `json:"Id"`
			Phone    string          `json:"Phone"`
			MsgProto json.RawMessage `json:"MsgProto"` // Armazena a mensagem como JSON bruto
		}

		if err := json.Unmarshal(msg.Body, &msgData); err != nil {
			log.Error().Msg("Erro ao decodificar mensagem da fila: " + err.Error())
			continue
		}

		// Recupera o usuário correto
		userid := 0
		for uid, client := range clientPointer {
			if client.IsConnected() {
				userid = uid
				break
			}
		}

		if userid == 0 {
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
		resp, err := clientPointer[userid].SendMessage(context.Background(), recipient, &msgProto, whatsmeow.SendRequestExtra{ID: msgData.Id})

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
		myuserinfo, found := userinfocache.Get(s.getTokenByUserId(userid))
		if found {
			webhookurl = myuserinfo.(Values).Get("Webhook")
		}

		if webhookurl != "" {
			events := strings.Split(myuserinfo.(Values).Get("Events"), ",")

			// Após o envio, verificar se o webhook deve ser chamado
			if !Find(events, "Callback") && !Find(events, "All") {
				log.Warn().Msg("Usuário não está inscrito para Callback. Ignorando webhook.")
			} else {
				// Criar estrutura de evento no mesmo formato do wmiau.go
				postmap := map[string]interface{}{
					"type": "Callback",
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
				go callHook(webhookurl, data, userid)

				log.Info().Str("id", msgData.Id).Str("status", status).Msg("Callback processado")
			}
		} else {
			log.Warn().Str("userid", fmt.Sprintf("%d", userid)).Msg("Nenhum webhook configurado para este usuário")
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

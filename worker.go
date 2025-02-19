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
		log.Error().Msg("Erro ao consumir mensagens:")
	}

	for msg := range msgs {
		// Decodifica a mensagem JSON
		var msgData map[string]string
		if err := json.Unmarshal(msg.Body, &msgData); err != nil {
			log.Error().Msg("Erro ao decodificar mensagem: " + err.Error())
			continue
		}

		phone := msgData["Phone"]
		body := msgData["Body"]
		image := msgData["Image"]
		id := msgData["Id"]

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

		// Criando a mensagem
		var msgProto *waProto.Message
		if image != "" {
			msgProto = &waProto.Message{
				ImageMessage: &waProto.ImageMessage{
					Caption: &body,
				},
			}
		} else {
			msgProto = &waProto.Message{
				ExtendedTextMessage: &waProto.ExtendedTextMessage{
					Text: &body,
				},
			}
		}

		recipient, ok := parseJID(phone)
		if !ok {
			log.Error().Msg("Erro ao converter telefone para JID")
			continue
		}

		// Envia a mensagem e captura o resultado
		resp, err := clientPointer[userid].SendMessage(context.Background(), recipient, msgProto, whatsmeow.SendRequestExtra{ID: id})

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
		events := strings.Split(myuserinfo.(Values).Get("Events"), ",")

		// Após o envio, verificar se o webhook deve ser chamado
		if !Find(events, "Callback") && !Find(events, "All") {
			log.Warn().Msg("Usuário não está inscrito para Callback. Ignorando webhook.")
			return
		}

		// Criar estrutura de evento no mesmo formato do wmiau.go
		postmap := make(map[string]interface{})
		postmap["type"] = "Callback"
		postmap["event"] = map[string]interface{}{
			"id":        id,
			"phone":     phone,
			"status":    status,
			"details":   details,
			"timestamp": timestamp,
		}

		// Enviar para o webhook
		if webhookurl != "" {
			values, _ := json.Marshal(postmap)
			data := map[string]string{
				"jsonData": string(values),
				"token":    myuserinfo.(Values).Get("Token"),
			}
			go callHook(webhookurl, data, userid)
		} else {
			log.Warn().Str("userid", fmt.Sprintf("%d", userid)).Msg("Nenhum webhook configurado para este usuário")
		}

		log.Info().Str("id", id).Str("status", status).Msg("Callback processado")
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

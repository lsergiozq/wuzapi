package main

import (
	"context"
	"encoding/json"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
)

func processQueue(queue *RabbitMQQueue) {
	msgs, err := queue.Dequeue()
	if err != nil {
		log.Error().Msg("Erro ao consumir mensagens:")
	}

	for msg := range msgs {
		// Decodifica a mensagem JSON
		var msgData map[string]string
		if err := json.Unmarshal(msg.Body, &msgData); err != nil {
			log.Println("Erro ao decodificar mensagem:", err)
			continue
		}

		phone := msgData["Phone"]
		body := msgData["Body"]
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
			log.Println("Nenhuma sessão ativa no WhatsApp")
			continue
		}

		// Criando e enviando a mensagem
		recipient, ok := parseJID(phone)
		if !ok {
			log.Println("Erro ao converter telefone para JID")
			continue
		}

		msgProto := &waProto.Message{
			ExtendedTextMessage: &waProto.ExtendedTextMessage{
				Text: &body,
			},
		}

		resp, err := clientPointer[userid].SendMessage(context.Background(), recipient, msgProto, whatsmeow.SendRequestExtra{ID: id})
		if err != nil {
			log.Println("Erro ao enviar mensagem:", err)
			continue
		}

		log.Println("Mensagem enviada! ID:", id, "Timestamp:", resp.Timestamp)
	}
}

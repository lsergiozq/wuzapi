package main

import (
	"context"
	"encoding/json"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
)

func processQueue(queue *RedisQueue) {
	for {
		// Pega a mensagem com maior prioridade
		msgData, err := queue.Dequeue()
		if err != nil || msgData == "" {
			time.Sleep(1 * time.Second) // Evita consumo excessivo de CPU
			continue
		}

		// Decodifica a mensagem JSON
		var msg map[string]string
		if err := json.Unmarshal([]byte(msgData), &msg); err != nil {
			log.Println("Erro ao decodificar mensagem:", err)
			continue
		}

		phone := msg["Phone"]
		body := msg["Body"]
		id := msg["Id"]

		// Recupera o usuário correto
		userid := 0
		for uid, client := range clientPointer {
			if client.IsConnected() {
				userid = uid
				break
			}
		}

		if userid == 0 {
			log.Println("Nenhuma sessão do WhatsApp ativa")
			continue
		}

		// Criando a mensagem para envio
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

		// Envio da mensagem
		resp, err := clientPointer[userid].SendMessage(context.Background(), recipient, msgProto, whatsmeow.SendRequestExtra{ID: id})
		if err != nil {
			log.Println("Erro ao enviar mensagem:", err)
			continue
		}

		log.Println("Mensagem enviada! ID:", id, "Timestamp:", resp.Timestamp)
	}
}

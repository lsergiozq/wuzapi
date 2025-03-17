package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nfnt/resize"
	"github.com/vincent-petithory/dataurl"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

func processQueue(queue *RabbitMQQueue, s *server, cancelChan <-chan struct{}) {
	maxRetries := 10
	retries := 0

	for {
		select {
		case <-cancelChan:
			//log.Info().Msg("Shutting down RabbitMQ consumer")
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

			msg := <-msgs
			// Decodifica a mensagem JSON da fila
			var msgData struct {
				Id        string `json:"Id"`
				Phone     string `json:"Phone"`
				Userid    int    `json:"Userid"`
				SendImage bool   `json:"SendImage"`
				Image     string `json:"Image"`
				Text      string `json:"Text"`
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

			if msgData.SendImage {
				var uploaded whatsmeow.UploadResponse
				var filedata []byte
				var thumbnailBytes []byte

				if strings.HasPrefix(msgData.Image, "https://") {
					if imageBase64, err := ImageToBase64(msgData.Image); err == nil {
						msgData.Image = fmt.Sprintf("data:image/jpeg;base64,%s", imageBase64)
					} else {
						log.Error().Msg("Erro ao converter imagem para base64: " + err.Error())
						continue
					}
				}

				// Caso ainda não tenha imagem, retornar erro
				if msgData.Image == "" {
					log.Error().Msg("Imagem obrigatória no Payload ou no usuário")
					continue
				}

				if strings.HasPrefix(msgData.Image, "data:image") {
					dataURL, err := dataurl.DecodeString(msgData.Image)
					if err != nil {
						log.Error().Msg("Erro ao decodificar a imagem base64: " + err.Error())
						continue
					}

					filedata = dataURL.Data
					if len(filedata) == 0 {
						log.Error().Msg("Imagem inválida ou corrompida")
						continue
					}
					uploaded, err = clientPointer[msgData.Userid].Upload(context.Background(), filedata, whatsmeow.MediaImage)
					if err != nil {
						log.Error().Msg("Erro ao fazer upload da imagem: " + err.Error())
						continue
					}

					reader := bytes.NewReader(filedata)
					img, _, err := image.Decode(reader)
					if err != nil {
						log.Error().Msg("Erro ao decodificar imagem, enviando sem thumbnail")
						thumbnailBytes = nil
					} else {
						thumbnail := resize.Thumbnail(72, 72, img, resize.Lanczos3)

						var thumbnailBuffer bytes.Buffer
						if err := jpeg.Encode(&thumbnailBuffer, thumbnail, nil); err == nil {
							thumbnailBytes = thumbnailBuffer.Bytes()
						}
					}

					msgProto = waProto.Message{
						ImageMessage: &waProto.ImageMessage{
							Caption:       proto.String(msgData.Text),
							URL:           proto.String(uploaded.URL),
							DirectPath:    proto.String(uploaded.DirectPath),
							MediaKey:      uploaded.MediaKey,
							Mimetype:      proto.String(http.DetectContentType(filedata)),
							FileEncSHA256: uploaded.FileEncSHA256,
							FileSHA256:    uploaded.FileSHA256,
							FileLength:    proto.Uint64(uint64(len(filedata))),
							JPEGThumbnail: thumbnailBytes,
						},
					}

				} else {
					log.Error().Msg("Formato de imagem inválido")
					continue
				}

			} else {
				msgProto = waProto.Message{
					ExtendedTextMessage: &waProto.ExtendedTextMessage{
						Text: proto.String(msgData.Text),
					},
				}
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

			} else {
				log.Warn().Str("userid", fmt.Sprintf("%d", msgData.Userid)).Msg("Nenhum webhook configurado para este usuário")
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

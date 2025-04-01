package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/patrickmn/go-cache"
	"github.com/vincent-petithory/dataurl"
	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type Values struct {
	m map[string]string
}

func (v Values) Get(key string) string {
	return v.m[key]
}

var messageTypes = []string{"Message", "ReadReceipt", "Presence", "HistorySync", "ChatPresence", "CallBack", "All"}

func (s *server) authadmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		if token != *adminToken {
			s.Respond(w, r, http.StatusUnauthorized, errors.New("Unauthorized"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) authalice(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		var ctx context.Context
		userid := 0
		txtid := ""
		webhook := ""
		jid := ""
		events := ""
		imagebase64 := ""

		// Get token from headers or uri parameters
		token := r.Header.Get("token")
		if token == "" {
			token = strings.Join(r.URL.Query()["token"], "")
		}

		myuserinfo, found := userinfocache.Get(token)
		if !found {
			//log.Info().Msg("Looking for user information in DB")
			// Checks DB from matching user and store user values in context
			rows, err := s.db.Query("SELECT id,webhook,jid,events,imagebase64 FROM users WHERE token=? LIMIT 1", token)
			if err != nil {
				s.Respond(w, r, http.StatusInternalServerError, err)
				return
			}
			defer rows.Close()
			for rows.Next() {
				err = rows.Scan(&txtid, &webhook, &jid, &events, &imagebase64)
				if err != nil {
					s.Respond(w, r, http.StatusInternalServerError, err)
					return
				}
				userid, _ = strconv.Atoi(txtid)
				v := Values{map[string]string{
					"Id":          txtid,
					"Jid":         jid,
					"Webhook":     webhook,
					"Token":       token,
					"Events":      events,
					"ImageBase64": imagebase64,
				}}

				userinfocache.Set(token, v, cache.NoExpiration)
				ctx = context.WithValue(r.Context(), "userinfo", v)
			}
		} else {
			ctx = context.WithValue(r.Context(), "userinfo", myuserinfo)
			//log.Info().Msg(myuserinfo.(Values).Get("Id"))
			userid, _ = strconv.Atoi(myuserinfo.(Values).Get("Id"))
		}

		if userid == 0 {
			s.Respond(w, r, http.StatusUnauthorized, errors.New("Unauthorized"))
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Middleware: Authenticate connections based on Token header/uri parameter
func (s *server) auth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var ctx context.Context
		userid := 0
		txtid := ""
		webhook := ""
		jid := ""
		events := ""
		imagebase64 := ""

		// Get token from headers or uri parameters
		token := r.Header.Get("token")
		if token == "" {
			token = strings.Join(r.URL.Query()["token"], "")
		}

		myuserinfo, found := userinfocache.Get(token)
		if !found {
			//log.Info().Msg("Looking for user information in DB")
			// Checks DB from matching user and store user values in context
			rows, err := s.db.Query("SELECT id,webhook,jid,events,imagebase64 FROM users WHERE token=? LIMIT 1", token)
			if err != nil {
				s.Respond(w, r, http.StatusInternalServerError, err)
				return
			}
			defer rows.Close()
			for rows.Next() {
				err = rows.Scan(&txtid, &webhook, &jid, &events, &imagebase64)
				if err != nil {
					s.Respond(w, r, http.StatusInternalServerError, err)
					return
				}
				userid, _ = strconv.Atoi(txtid)
				v := Values{map[string]string{
					"Id":          txtid,
					"Jid":         jid,
					"Webhook":     webhook,
					"Token":       token,
					"Events":      events,
					"ImageBase64": imagebase64,
				}}

				userinfocache.Set(token, v, cache.NoExpiration)
				ctx = context.WithValue(r.Context(), "userinfo", v)
			}
		} else {
			ctx = context.WithValue(r.Context(), "userinfo", myuserinfo)
			userid, _ = strconv.Atoi(myuserinfo.(Values).Get("Id"))
		}

		if userid == 0 {
			s.Respond(w, r, http.StatusUnauthorized, errors.New("Unauthorized"))
			return
		}
		handler(w, r.WithContext(ctx))
	}
}

// Connects to Whatsapp Servers
// Connect handles the HTTP request and response
func (s *server) Connect() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		jid := r.Context().Value("userinfo").(Values).Get("Jid")
		webhook := r.Context().Value("userinfo").(Values).Get("Webhook")
		token := r.Context().Value("userinfo").(Values).Get("Token")
		userid, _ := strconv.Atoi(txtid)

		decoder := json.NewDecoder(r.Body)
		var t struct {
			Subscribe []string
			Immediate bool
		}
		if err := decoder.Decode(&t); err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode Payload"))
			return
		}

		err := s.ConnectUserCore(userid, jid, token, t.Subscribe, t.Immediate)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
			return
		}

		eventstring := strings.Join(t.Subscribe, ",")
		v := updateUserInfo(r.Context().Value("userinfo"), "Events", eventstring)
		userinfocache.Set(token, v, cache.NoExpiration)

		response := map[string]interface{}{"webhook": webhook, "jid": jid, "events": eventstring, "details": "Connected!"}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
			return
		}
		s.Respond(w, r, http.StatusOK, string(responseJson))
	}
}

func (s *server) ConnectUser(userid int) error {
	txtid := fmt.Sprintf("%d", userid)
	userinfo, found := userinfocache.Get(txtid) // Fallback if token isn’t available
	if !found {
		return errors.New("User info not found in cache")
	}
	jid := userinfo.(Values).Get("Jid")
	token := userinfo.(Values).Get("Token")
	subscribe := strings.Split(userinfo.(Values).Get("Events"), ",")
	if len(subscribe) == 0 || (len(subscribe) == 1 && subscribe[0] == "") {
		subscribe = []string{"All"}
	}
	return s.ConnectUserCore(userid, jid, token, subscribe, false)
}

// ConnectUserCore handles the core connection logic
func (s *server) ConnectUserCore(userid int, jid, token string, subscribe []string, immediate bool) error {
	if clientPointer[userid] != nil && clientPointer[userid].IsLoggedIn() && clientPointer[userid].IsConnected() {
		return errors.New("Already Connected")
	}

	var subscribedEvents []string
	if len(subscribe) < 1 {
		if !Find(subscribedEvents, "All") {
			subscribedEvents = append(subscribedEvents, "All")
		}
	} else {
		for _, arg := range subscribe {
			if !Find(messageTypes, arg) {
				log.Warn().Str("Type", arg).Msg("Message type discarded")
				continue
			}
			if !Find(subscribedEvents, arg) {
				subscribedEvents = append(subscribedEvents, arg)
			}
		}
	}

	killchannel[userid] = make(chan bool)
	go s.startClient(userid, jid, token, subscribedEvents)

	if !immediate {
		log.Warn().Msg("Waiting 10 seconds")
		time.Sleep(10 * time.Second)
		if clientPointer[userid] == nil || !clientPointer[userid].IsConnected() {
			return errors.New("Failed to Connect")
		}
	}
	return nil
}

// Disconnects from Whatsapp websocket, does not log out device
// DisconnectUserCore handles the core disconnection logic for a given userid
func (s *server) DisconnectUserCore(userid int) error {
	if clientPointer[userid] == nil {
		return errors.New("No session")
	}
	if !clientPointer[userid].IsConnected() {
		return errors.New("Cannot disconnect because it is not connected")
	}
	if !clientPointer[userid].IsLoggedIn() {
		return errors.New("Cannot disconnect because it is not logged in")
	}

	killchannel[userid] <- true
	log.Info().Int("userid", userid).Msg("Disconnection successful")
	return nil
}

// DisconnectUser wraps DisconnectUserCore with additional context handling
func (s *server) DisconnectUser(userid int) error {
	err := s.DisconnectUserCore(userid)
	if err != nil {
		log.Warn().Int("userid", userid).Msg(err.Error())
		return err
	}
	return nil
}

// Disconnect handles the HTTP request and response
func (s *server) Disconnect() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)

		err := s.DisconnectUser(userid)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
			return
		}

		response := map[string]interface{}{"Details": "Disconnected"}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
			return
		}
		s.Respond(w, r, http.StatusOK, string(responseJson))
	}
}

// Gets WebHook
func (s *server) GetWebhook() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		webhook := ""
		events := ""
		txtid := r.Context().Value("userinfo").(Values).Get("Id")

		rows, err := s.db.Query("SELECT webhook,events FROM users WHERE id=? LIMIT 1", txtid)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Could not get webhook: %v", err)))
			return
		}
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&webhook, &events)
			if err != nil {
				s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Could not get webhook: %s", fmt.Sprintf("%s", err))))
				return
			}
		}
		err = rows.Err()
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Could not get webhook: %s", fmt.Sprintf("%s", err))))
			return
		}

		eventarray := strings.Split(events, ",")

		response := map[string]interface{}{"webhook": webhook, "subscribe": eventarray}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Sets WebHook
func (s *server) SetWebhook() http.HandlerFunc {
	type webhookStruct struct {
		WebhookURL string
	}
	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		token := r.Context().Value("userinfo").(Values).Get("Token")
		userid, _ := strconv.Atoi(txtid)

		decoder := json.NewDecoder(r.Body)
		var t webhookStruct
		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Could not set webhook: %v", err)))
			return
		}
		var webhook = t.WebhookURL

		dbMutex.Lock()
		defer dbMutex.Unlock()
		_, err = s.db.Exec("UPDATE users SET webhook=? WHERE id=?", webhook, userid)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("%s", err)))
			return
		}

		v := updateUserInfo(r.Context().Value("userinfo"), "Webhook", webhook)
		userinfocache.Set(token, v, cache.NoExpiration)

		response := map[string]interface{}{"webhook": webhook}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Gets QR code encoded in Base64
func (s *server) GetQR() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)
		code := ""

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		} else {
			if clientPointer[userid].IsConnected() == false {
				s.Respond(w, r, http.StatusInternalServerError, errors.New("Not connected"))
				return
			}
			rows, err := s.db.Query("SELECT qrcode AS code FROM users WHERE id=? LIMIT 1", userid)
			if err != nil {
				s.Respond(w, r, http.StatusInternalServerError, err)
				return
			}
			defer rows.Close()
			for rows.Next() {
				err = rows.Scan(&code)
				if err != nil {
					s.Respond(w, r, http.StatusInternalServerError, err)
					return
				}
			}
			err = rows.Err()
			if err != nil {
				s.Respond(w, r, http.StatusInternalServerError, err)
				return
			}
			if clientPointer[userid].IsLoggedIn() == true {
				s.Respond(w, r, http.StatusInternalServerError, errors.New("Already Loggedin"))
				return
			}
		}

		//log.Info().Str("userid", txtid).Str("qrcode", code).Msg("Get QR successful")
		response := map[string]interface{}{"QRCode": fmt.Sprintf("%s", code)}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Logs out device from Whatsapp (requires to scan QR next time)
func (s *server) Logout() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		jid := r.Context().Value("userinfo").(Values).Get("Jid")
		userid, _ := strconv.Atoi(txtid)

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		} else {
			if clientPointer[userid].IsLoggedIn() == true && clientPointer[userid].IsConnected() == true {
				err := clientPointer[userid].Logout()
				if err != nil {
					log.Error().Str("jid", jid).Msg("Could not perform logout")
					s.Respond(w, r, http.StatusInternalServerError, errors.New("Could not perform logout"))
					return
				} else {
					//log.Info().Str("jid", jid).Msg("Logged out")
					killchannel[userid] <- true
				}
			} else {
				if clientPointer[userid].IsConnected() == true {
					log.Warn().Str("jid", jid).Msg("Ignoring logout as it was not logged in")
					s.Respond(w, r, http.StatusInternalServerError, errors.New("Could not disconnect as it was not logged in"))
					return
				} else {
					log.Warn().Str("jid", jid).Msg("Ignoring logout as it was not connected")
					s.Respond(w, r, http.StatusInternalServerError, errors.New("Could not disconnect as it was not connected"))
					return
				}
			}
		}

		response := map[string]interface{}{"Details": "Logged out"}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Pair by Phone. Retrieves the code to pair by phone number instead of QR
func (s *server) PairPhone() http.HandlerFunc {

	type pairStruct struct {
		Phone string
	}

	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		}

		decoder := json.NewDecoder(r.Body)
		var t pairStruct
		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode Payload"))
			return
		}

		if t.Phone == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Phone in Payload"))
			return
		}

		isLoggedIn := clientPointer[userid].IsLoggedIn()
		if isLoggedIn {
			log.Error().Msg(fmt.Sprintf("%s", "Already paired"))
			s.Respond(w, r, http.StatusBadRequest, errors.New("Already paired"))
			return
		}

		linkingCode, err := clientPointer[userid].PairPhone(t.Phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
		if err != nil {
			log.Error().Msg(fmt.Sprintf("%s", err))
			s.Respond(w, r, http.StatusBadRequest, err)
			return
		}

		response := map[string]interface{}{"LinkingCode": linkingCode}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Gets Connected and LoggedIn Status
func (s *server) GetStatus() http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		}

		isConnected := clientPointer[userid].IsConnected()
		isLoggedIn := clientPointer[userid].IsLoggedIn()

		response := map[string]interface{}{"Connected": isConnected, "LoggedIn": isLoggedIn}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Sends a document/attachment message
func (s *server) SendDocument() http.HandlerFunc {

	type documentStruct struct {
		Caption     string
		Phone       string
		Document    string
		FileName    string
		Id          string
		ContextInfo waE2E.ContextInfo
	}

	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)
		msgid := ""
		var resp whatsmeow.SendResponse

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		}

		decoder := json.NewDecoder(r.Body)
		var t documentStruct
		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode Payload"))
			return
		}

		if t.Phone == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Phone in Payload"))
			return
		}

		if t.Document == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Document in Payload"))
			return
		}

		if t.FileName == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing FileName in Payload"))
			return
		}

		recipient, err := validateMessageFields(t.Phone, t.ContextInfo.StanzaID, t.ContextInfo.Participant)
		if err != nil {
			log.Error().Msg(fmt.Sprintf("%s", err))
			s.Respond(w, r, http.StatusBadRequest, err)
			return
		}

		if t.Id == "" {
			msgid = whatsmeow.GenerateMessageID()
		} else {
			msgid = t.Id
		}

		var uploaded whatsmeow.UploadResponse
		var filedata []byte

		if t.Document[0:29] == "data:application/octet-stream" {
			dataURL, err := dataurl.DecodeString(t.Document)
			if err != nil {
				s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode base64 encoded data from payload"))
				return
			} else {
				filedata = dataURL.Data
				uploaded, err = clientPointer[userid].Upload(context.Background(), filedata, whatsmeow.MediaDocument)
				if err != nil {
					s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Failed to upload file: %v", err)))
					return
				}
			}
		} else {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Document data should start with \"data:application/octet-stream;base64,\""))
			return
		}

		msg := &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
			URL:           proto.String(uploaded.URL),
			FileName:      &t.FileName,
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			Mimetype:      proto.String(http.DetectContentType(filedata)),
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(filedata))),
			Caption:       proto.String(t.Caption),
		}}

		if t.ContextInfo.StanzaID != nil {
			msg.ExtendedTextMessage.ContextInfo = &waE2E.ContextInfo{
				StanzaID:      proto.String(*t.ContextInfo.StanzaID),
				Participant:   proto.String(*t.ContextInfo.Participant),
				QuotedMessage: &waE2E.Message{Conversation: proto.String("")},
			}
		}
		if t.ContextInfo.MentionedJID != nil {
			if msg.ExtendedTextMessage.ContextInfo == nil {
				msg.ExtendedTextMessage.ContextInfo = &waE2E.ContextInfo{}
			}
			msg.ExtendedTextMessage.ContextInfo.MentionedJID = t.ContextInfo.MentionedJID
		}

		resp, err = clientPointer[userid].SendMessage(context.Background(), recipient, msg, whatsmeow.SendRequestExtra{ID: msgid})
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Error sending message: %v", err)))
			return
		}

		//log.Info().Str("timestamp", fmt.Sprintf("%d", resp.Timestamp)).Str("id", msgid).Msg("Message sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp, "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Sends an audio message
func (s *server) SendAudio() http.HandlerFunc {

	type audioStruct struct {
		Phone       string
		Audio       string
		Caption     string
		Id          string
		ContextInfo waE2E.ContextInfo
	}

	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)
		msgid := ""
		var resp whatsmeow.SendResponse

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		}

		decoder := json.NewDecoder(r.Body)
		var t audioStruct
		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode Payload"))
			return
		}

		if t.Phone == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Phone in Payload"))
			return
		}

		if t.Audio == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Audio in Payload"))
			return
		}

		recipient, err := validateMessageFields(t.Phone, t.ContextInfo.StanzaID, t.ContextInfo.Participant)
		if err != nil {
			log.Error().Msg(fmt.Sprintf("%s", err))
			s.Respond(w, r, http.StatusBadRequest, err)
			return
		}

		if t.Id == "" {
			msgid = whatsmeow.GenerateMessageID()
		} else {
			msgid = t.Id
		}

		var uploaded whatsmeow.UploadResponse
		var filedata []byte

		if t.Audio[0:14] == "data:audio/ogg" {
			dataURL, err := dataurl.DecodeString(t.Audio)
			if err != nil {
				s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode base64 encoded data from payload"))
				return
			} else {
				filedata = dataURL.Data
				uploaded, err = clientPointer[userid].Upload(context.Background(), filedata, whatsmeow.MediaAudio)
				if err != nil {
					s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Failed to upload file: %v", err)))
					return
				}
			}
		} else {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Audio data should start with \"data:audio/ogg;base64,\""))
			return
		}

		ptt := true
		mime := "audio/ogg; codecs=opus"

		msg := &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
			URL:        proto.String(uploaded.URL),
			DirectPath: proto.String(uploaded.DirectPath),
			MediaKey:   uploaded.MediaKey,
			//Mimetype:      proto.String(http.DetectContentType(filedata)),
			Mimetype:      &mime,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(filedata))),
			PTT:           &ptt,
		}}

		if t.ContextInfo.StanzaID != nil {
			msg.ExtendedTextMessage.ContextInfo = &waE2E.ContextInfo{
				StanzaID:      proto.String(*t.ContextInfo.StanzaID),
				Participant:   proto.String(*t.ContextInfo.Participant),
				QuotedMessage: &waE2E.Message{Conversation: proto.String("")},
			}
		}
		if t.ContextInfo.MentionedJID != nil {
			if msg.ExtendedTextMessage.ContextInfo == nil {
				msg.ExtendedTextMessage.ContextInfo = &waE2E.ContextInfo{}
			}
			msg.ExtendedTextMessage.ContextInfo.MentionedJID = t.ContextInfo.MentionedJID
		}

		resp, err = clientPointer[userid].SendMessage(context.Background(), recipient, msg, whatsmeow.SendRequestExtra{ID: msgid})
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Error sending message: %v", err)))
			return
		}

		//log.Info().Str("timestamp", fmt.Sprintf("%d", resp.Timestamp)).Str("id", msgid).Msg("Message sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp, "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// ImageToBase64 recebe a URL de uma imagem e retorna a string em Base64
func ImageToBase64(url string) (string, error) {
	// Cria um client HTTP otimizado
	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	// Faz o GET na URL da imagem
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("erro ao baixar a imagem: %v", err)
	}
	defer resp.Body.Close()

	// Verifica se a resposta foi OK (200)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("erro: status da resposta %s", resp.Status)
	}

	// Lê todos os bytes da imagem
	imageBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("erro ao ler a imagem: %v", err)
	}

	// Converte os bytes da imagem para uma string Base64
	base64Image := base64.StdEncoding.EncodeToString(imageBytes)
	return base64Image, nil
}

func (s *server) SendImage() http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")

		//log.Info().Str("txtid", txtid).Msg("Conteúdo do txtid")

		userid, _ := strconv.Atoi(txtid)

		//log.Info().Str("userid", strconv.Itoa(userid)).Msg("Conteúdo do userid")
		msgid := ""

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Nenhuma sessão ativa"))
			return
		}

		decoder := json.NewDecoder(r.Body)
		var t struct {
			Phone       string
			Image       string
			Caption     string
			Id          string
			Priority    int
			ContextInfo waE2E.ContextInfo
		}

		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Erro ao decodificar Payload"))
			return
		}

		if t.Phone == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Número de telefone obrigatório"))
			return
		}

		_, err = validateMessageFields(t.Phone, t.ContextInfo.StanzaID, t.ContextInfo.Participant)
		if err != nil {
			log.Error().Msg(fmt.Sprintf("%s", err))
			s.Respond(w, r, http.StatusBadRequest, err)
			return
		}

		// Verificar se a imagem está no Payload ou no usuário
		if t.Image == "" {
			imageBase64 := r.Context().Value("userinfo").(Values).Get("ImageBase64")
			t.Image = imageBase64
		}

		if t.Id == "" {
			msgid = whatsmeow.GenerateMessageID()
		} else {
			msgid = t.Id
		}

		// Enfileirar no RabbitMQ
		queue, err := GetUserQueue(getRabbitMQURL(), userid)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Failed to get user queue"))
			return
		}

		// Cria msgData no formato original
		msgData, err := json.Marshal(map[string]interface{}{
			"Id":        msgid,
			"Phone":     t.Phone,
			"Userid":    userid,
			"SendImage": true,
			"Image":     t.Image,
			"Text":      t.Caption,
			"Priority":  t.Priority,
		})
		if err != nil {
			log.Error().Err(err).Str("msgid", msgid).Msg("Failed to marshal msgData")
			s.Respond(w, r, http.StatusInternalServerError, fmt.Errorf("failed to marshal msgData: %v", err))
			return
		}

		// Valida e aplica Priority
		priority := uint8(t.Priority)
		if t.Priority < 0 || t.Priority > 255 {
			priority = 0
			log.Warn().Int("priority", t.Priority).Str("msgid", msgid).Msg("Priority out of range, defaulting to 0")
		}

		err = queue.Enqueue(string(msgData), priority, userid)
		if err != nil { // ✅ Correto: verifica se houve erro no enfileiramento
			log.Error().Err(err).Str("msgid", msgid).Msg("Erro ao enfileirar mensagem")
		}

		//log.Info().Str("id", msgid).Str("phone", t.Phone).Msg("Imagem enfileirada para envio")

		response := map[string]interface{}{"Details": "Imagem enfileirada", "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

// Sends Video message
func (s *server) SendVideo() http.HandlerFunc {

	type imageStruct struct {
		Phone         string
		Video         string
		Caption       string
		Id            string
		JPEGThumbnail []byte
		ContextInfo   waE2E.ContextInfo
	}

	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)
		msgid := ""
		var resp whatsmeow.SendResponse

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		}

		decoder := json.NewDecoder(r.Body)
		var t imageStruct
		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode Payload"))
			return
		}

		if t.Phone == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Phone in Payload"))
			return
		}

		if t.Video == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Video in Payload"))
			return
		}

		recipient, err := validateMessageFields(t.Phone, t.ContextInfo.StanzaID, t.ContextInfo.Participant)
		if err != nil {
			log.Error().Msg(fmt.Sprintf("%s", err))
			s.Respond(w, r, http.StatusBadRequest, err)
			return
		}

		if t.Id == "" {
			msgid = whatsmeow.GenerateMessageID()
		} else {
			msgid = t.Id
		}

		var uploaded whatsmeow.UploadResponse
		var filedata []byte

		if t.Video[0:4] == "data" {
			dataURL, err := dataurl.DecodeString(t.Video)
			if err != nil {
				s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode base64 encoded data from payload"))
				return
			} else {
				filedata = dataURL.Data
				uploaded, err = clientPointer[userid].Upload(context.Background(), filedata, whatsmeow.MediaVideo)
				if err != nil {
					s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Failed to upload file: %v", err)))
					return
				}
			}
		} else {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Data should start with \"data:mime/type;base64,\""))
			return
		}

		msg := &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			Caption:       proto.String(t.Caption),
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			Mimetype:      proto.String(http.DetectContentType(filedata)),
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(filedata))),
			JPEGThumbnail: t.JPEGThumbnail,
		}}

		if t.ContextInfo.StanzaID != nil {
			msg.ExtendedTextMessage.ContextInfo = &waE2E.ContextInfo{
				StanzaID:      proto.String(*t.ContextInfo.StanzaID),
				Participant:   proto.String(*t.ContextInfo.Participant),
				QuotedMessage: &waE2E.Message{Conversation: proto.String("")},
			}
		}
		if t.ContextInfo.MentionedJID != nil {
			if msg.ExtendedTextMessage.ContextInfo == nil {
				msg.ExtendedTextMessage.ContextInfo = &waE2E.ContextInfo{}
			}
			msg.ExtendedTextMessage.ContextInfo.MentionedJID = t.ContextInfo.MentionedJID
		}

		resp, err = clientPointer[userid].SendMessage(context.Background(), recipient, msg, whatsmeow.SendRequestExtra{ID: msgid})
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Error sending message: %v", err)))
			return
		}

		//log.Info().Str("timestamp", fmt.Sprintf("%d", resp.Timestamp)).Str("id", msgid).Msg("Message sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp, "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

func (s *server) SendMessage() http.HandlerFunc {

	type textStruct struct {
		Phone       string
		Body        string
		Id          string
		Priority    int
		ContextInfo waE2E.ContextInfo
	}

	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Nenhuma sessão ativa"))
			return
		}

		msgid := ""
		decoder := json.NewDecoder(r.Body)
		var t textStruct
		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode Payload"))
			return
		}

		if t.Phone == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Phone in Payload"))
			return
		}

		_, err = validateMessageFields(t.Phone, t.ContextInfo.StanzaID, t.ContextInfo.Participant)
		if err != nil {
			log.Error().Msg(fmt.Sprintf("%s", err))
			s.Respond(w, r, http.StatusBadRequest, err)
			return
		}

		if t.Body == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Body in Payload"))
			return
		}

		queue, err := GetUserQueue(getRabbitMQURL(), userid)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Failed to get user queue"))
			return
		}

		if t.Id == "" {
			msgid = whatsmeow.GenerateMessageID()
		} else {
			msgid = t.Id
		}

		// Cria msgData no formato original
		msgData, err := json.Marshal(map[string]interface{}{
			"Id":        msgid,
			"Phone":     t.Phone,
			"Userid":    userid,
			"SendImage": false,
			"Image":     "",
			"Text":      t.Body,
			"Priority":  t.Priority,
		})
		if err != nil {
			log.Error().Err(err).Str("msgid", msgid).Msg("Failed to marshal msgData")
			s.Respond(w, r, http.StatusInternalServerError, fmt.Errorf("failed to marshal msgData: %v", err))
			return
		}

		// Valida e aplica Priority
		priority := uint8(t.Priority)
		if t.Priority < 0 || t.Priority > 255 {
			priority = 0
			log.Warn().Int("priority", t.Priority).Str("msgid", msgid).Msg("Priority out of range, defaulting to 0")
		}

		err = queue.Enqueue(string(msgData), priority, userid)
		if err != nil { // ✅ Correto: verifica se houve erro no enfileiramento
			log.Error().Err(err).Str("msgid", msgid).Msg("Erro ao enfileirar mensagem")
			return
		}

		response := map[string]interface{}{"Details": "Mensagem enfileirada", "Id": t.Id}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

// checks if users/phones are on Whatsapp
func (s *server) CheckUser() http.HandlerFunc {

	type checkUserStruct struct {
		Phone []string
	}

	type User struct {
		Query        string
		IsInWhatsapp bool
		JID          string
		VerifiedName string
	}

	type UserCollection struct {
		Users []User
	}

	return func(w http.ResponseWriter, r *http.Request) {

		txtid := r.Context().Value("userinfo").(Values).Get("Id")
		userid, _ := strconv.Atoi(txtid)

		if clientPointer[userid] == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("No session"))
			return
		}

		decoder := json.NewDecoder(r.Body)
		var t checkUserStruct
		err := decoder.Decode(&t)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Could not decode Payload"))
			return
		}

		if len(t.Phone) < 1 {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Missing Phone in Payload"))
			return
		}

		resp, err := clientPointer[userid].IsOnWhatsApp(t.Phone)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("Failed to check if users are on WhatsApp: %s", err)))
			return
		}

		uc := new(UserCollection)
		for _, item := range resp {
			if item.VerifiedName != nil {
				var msg = User{Query: item.Query, IsInWhatsapp: item.IsIn, JID: fmt.Sprintf("%s", item.JID), VerifiedName: item.VerifiedName.Details.GetVerifiedName()}
				uc.Users = append(uc.Users, msg)
			} else {
				var msg = User{Query: item.Query, IsInWhatsapp: item.IsIn, JID: fmt.Sprintf("%s", item.JID), VerifiedName: ""}
				uc.Users = append(uc.Users, msg)
			}
		}
		responseJson, err := json.Marshal(uc)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
		return
	}
}

// Admin List users
func (s *server) ListUsers() http.HandlerFunc {

	type usersStruct struct {
		Id        int
		Name      string
		Connected bool
		Events    string
	}

	return func(w http.ResponseWriter, r *http.Request) {

		// Query the database to get the list of users
		rows, err := s.db.Query("SELECT id, name, token, webhook, jid, connected, expiration, events FROM users order by name")
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem accessing DB"))
			return
		}
		defer rows.Close()

		// Create a slice to store the user data
		users := []map[string]interface{}{}

		// Iterate over the rows and populate the user data
		for rows.Next() {
			var id int
			var name, token, webhook, jid string
			var connectedNull sql.NullInt64
			var expiration int
			var events string

			err := rows.Scan(&id, &name, &token, &webhook, &jid, &connectedNull, &expiration, &events)
			if err != nil {
				s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem accessing DB"))
				return
			}

			connected := int(0)
			if connectedNull.Valid {
				connected = int(connectedNull.Int64)
			}

			user := map[string]interface{}{
				"id":         id,
				"name":       name,
				"token":      token,
				"webhook":    webhook,
				"jid":        jid,
				"connected":  connected == 1,
				"expiration": expiration,
				"events":     events,
			}

			users = append(users, user)
		}
		// Check for any error that occurred during iteration
		if err := rows.Err(); err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem accessing DB"))
			return
		}

		// Set the response content type to JSON
		w.Header().Set("Content-Type", "application/json")

		// Encode the user data as JSON and write the response
		err = json.NewEncoder(w).Encode(users)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem encodingJSON"))
			return
		}
	}
}

func (s *server) AddUser() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		// Parse the request body
		var user struct {
			Name        string `json:"name"`
			Token       string `json:"token"`
			Webhook     string `json:"webhook"`
			Expiration  int    `json:"expiration"`
			Events      string `json:"events"`
			ImageBase64 string `json:"imagebase64"`
		}

		err := json.NewDecoder(r.Body).Decode(&user)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Incomplete data in Payload. Required name,token,webhook,expiration,events, imagebase64"))
			return
		}

		// Check if a user with the same token already exists
		var count int
		err = s.db.QueryRow("SELECT COUNT(*) FROM users WHERE token = ?", user.Token).Scan(&count)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem accessing DB"))
			return
		}
		if count > 0 {
			s.Respond(w, r, http.StatusConflict, errors.New("User with the same token already exists"))
			return
		}

		// Validate the events input
		validEvents := []string{"Message", "ReadReceipt", "Presence", "HistorySync", "ChatPresence", "CallBack", "All"}
		eventList := strings.Split(user.Events, ",")
		for _, event := range eventList {
			event = strings.TrimSpace(event)
			if !contains(validEvents, event) {
				s.Respond(w, r, http.StatusBadRequest, errors.New("Invalid event: "+event))
				return
			}
		}

		// Insert the user into the database
		result, err := s.db.Exec("INSERT INTO users (name, token, webhook, expiration, events, imagebase64, jid, qrcode) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			user.Name, user.Token, user.Webhook, user.Expiration, user.Events, user.ImageBase64, "", "")
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem accessing DB"))
			log.Error().Str("error", fmt.Sprintf("%v", err)).Msg("Admin DB Error")
			return
		}

		// Get the ID of the inserted user
		id, _ := result.LastInsertId()

		// Return the inserted user ID
		response := map[string]interface{}{
			"id": id,
		}
		json.NewEncoder(w).Encode(response)
	}
}

func (s *server) DeleteUser() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		// Get the user ID from the request URL
		vars := mux.Vars(r)
		userID := vars["id"]

		//antes de excluir, fechar a conexão com whatsapp
		userid, _ := strconv.Atoi(userID)
		if clientPointer[userid] != nil {
			if clientPointer[userid].IsLoggedIn() && clientPointer[userid].IsConnected() {
				clientPointer[userid].Logout()
			}
		}

		// Delete the user from the database
		result, err := s.db.Exec("DELETE FROM users WHERE id = ?", userID)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem accessing DB"))
			return
		}

		// Check if the user was deleted
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			s.Respond(w, r, http.StatusNotFound, errors.New("User not found"))
			return
		}

		// Return a success response
		response := map[string]interface{}{"Details": "User deleted successfully"}
		responseJson, err := json.Marshal(response)

		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

func (s *server) UpdateUserImage() http.HandlerFunc {
	type updateImageStruct struct {
		ImageBase64 string `json:"imagebase64"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		// Obtém o token da URL
		vars := mux.Vars(r)
		token := vars["token"]

		if token == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Token é obrigatório na URL"))
			return
		}

		userid, err := s.GetUserIdByToken(token)
		if err != nil {
			s.Respond(w, r, http.StatusNotFound, errors.New(err.Error()))
		}

		var requestData updateImageStruct
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&requestData)
		if err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Erro ao decodificar JSON"))
			return
		}

		dbMutex.Lock()
		defer dbMutex.Unlock()
		// Atualiza a imagem no banco de dados pelo token
		result, err := s.db.Exec("UPDATE users SET imagebase64 = ? WHERE id = ?", requestData.ImageBase64, userid)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Falha ao atualizar a imagem do usuário"))
			return
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			s.Respond(w, r, http.StatusNotFound, errors.New("Usuário não encontrado ou token inválido"))
			return
		}

		//log.Info().Str("token", token).Msg("Imagem do usuário atualizada com sucesso")
		response := map[string]interface{}{"message": "Imagem atualizada com sucesso"}
		responseJson, _ := json.Marshal(response)

		//atualiza a imagem em r.Context().Value("userinfo").(Values).Get("ImageBase64")
		v := updateUserInfo(r.Context().Value("userinfo"), "ImageBase64", requestData.ImageBase64)
		userinfocache.Set(token, v, cache.NoExpiration)

		s.Respond(w, r, http.StatusOK, string(responseJson))
	}
}

func (s *server) GetUserIdByToken(token string) (int, error) {
	var userId int

	// Consulta o banco de dados usando o token
	err := s.db.QueryRow("SELECT id FROM users WHERE token = ?", token).Scan(&userId)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("Usuário não encontrado para o token: %s", token)
		}
		return 0, err
	}

	return userId, nil
}

func (s *server) DeleteUserByToken() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		// Pegar o token do parâmetro da URL
		token := r.URL.Query().Get("token")
		if token == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("Token is required"))
			return
		}

		userid, err := s.GetUserIdByToken(token)
		if err != nil {
			s.Respond(w, r, http.StatusNotFound, errors.New(err.Error()))
			return
		}

		//antes de excluir, fechar a conexão com whatsapp
		if clientPointer[userid] != nil {
			if clientPointer[userid].IsLoggedIn() && clientPointer[userid].IsConnected() {
				clientPointer[userid].Logout()
			}
		}

		// Excluir o usuário do banco de dados com base no token
		result, err := s.db.Exec("DELETE FROM users WHERE id = ?", userid)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("Problem accessing DB"))
			return
		}

		// Verificar se o usuário foi deletado
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			s.Respond(w, r, http.StatusNotFound, errors.New("User not found"))
			return
		}

		// Retornar resposta de sucesso
		response := map[string]interface{}{"Details": "User deleted successfully"}
		responseJson, err := json.Marshal(response)

		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

// Writes JSON response to API clients
func (s *server) Respond(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	dataenvelope := map[string]interface{}{"code": status}
	if err, ok := data.(error); ok {
		dataenvelope["error"] = err.Error()
		dataenvelope["success"] = false
	} else {
		mydata := make(map[string]interface{})
		err = json.Unmarshal([]byte(data.(string)), &mydata)
		if err != nil {
			log.Error().Str("error", fmt.Sprintf("%v", err)).Msg("Error unmarshalling JSON")
		}
		dataenvelope["data"] = mydata
		dataenvelope["success"] = true
	}
	data = dataenvelope

	if err := json.NewEncoder(w).Encode(data); err != nil {
		panic("respond: " + err.Error())
	}
}

func validateMessageFields(phone string, stanzaid *string, participant *string) (types.JID, error) {

	recipient, ok := parseJID(phone)
	if !ok {
		return types.NewJID("", types.DefaultUserServer), errors.New("Could not parse Phone")
	}

	if stanzaid != nil {
		if participant == nil {
			return types.NewJID("", types.DefaultUserServer), errors.New("Missing Participant in ContextInfo")
		}
	}

	if participant != nil {
		if stanzaid == nil {
			return types.NewJID("", types.DefaultUserServer), errors.New("Missing StanzaID in ContextInfo")
		}
	}

	return recipient, nil
}

func contains(slice []string, item string) bool {
	for _, value := range slice {
		if value == item {
			return true
		}
	}
	return false
}

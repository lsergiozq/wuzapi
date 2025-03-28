package main

import (
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/justinas/alice"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

type Middleware = alice.Constructor

func (s *server) routes() {

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)

	if *logType == "json" {
		log = zerolog.New(os.Stdout).With().Timestamp().Str("role", filepath.Base(os.Args[0])).Str("host", *address).Logger()
	} else {
		output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339, NoColor: !*colorOutput}
		log = zerolog.New(output).With().Timestamp().Str("role", filepath.Base(os.Args[0])).Str("host", *address).Logger()
	}

	adminRoutes := s.router.PathPrefix("/admin").Subrouter()
	adminRoutes.Use(s.authadmin)
	adminRoutes.Handle("/users", s.ListUsers()).Methods("GET")
	adminRoutes.Handle("/users", s.AddUser()).Methods("POST")
	adminRoutes.Handle("/users/{id}", s.DeleteUser()).Methods("DELETE")
	adminRoutes.Handle("/users", s.DeleteUserByToken()).Methods("DELETE")
	//criar rota para atualizar o usuário, iremos atualizar uma imagem de perfil, que seria a logomarca da empresa
	adminRoutes.Handle("/users/image/{token}", s.UpdateUserImage()).Methods("POST")
	adminRoutes.HandleFunc("/listusers", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, s.exPath+"/static/admin.html")
	}).Methods("GET")

	c := alice.New()
	c = c.Append(s.authalice)
	c = c.Append(hlog.NewHandler(log))

	c = c.Append(hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
		// log desativado
		if (status != 200 && status != 201) || size == 0 {
			hlog.FromRequest(r).Error().
				Str("method", r.Method).
				Stringer("url", r.URL).
				Int("status", status).
				Int("size", size).
				Dur("duration", duration).
				Str("userid", r.Context().Value("userinfo").(Values).Get("Id")).
				Msg("Got API Request")
			return
		}
	}))
	c = c.Append(hlog.RemoteAddrHandler("ip"))
	c = c.Append(hlog.UserAgentHandler("user_agent"))
	c = c.Append(hlog.RefererHandler("referer"))
	c = c.Append(hlog.RequestIDHandler("req_id", "Request-Id"))

	s.router.Handle("/session/connect", c.Then(s.Connect())).Methods("POST")
	s.router.Handle("/session/disconnect", c.Then(s.Disconnect())).Methods("POST")
	s.router.Handle("/session/logout", c.Then(s.Logout())).Methods("POST")
	s.router.Handle("/session/status", c.Then(s.GetStatus())).Methods("GET")
	s.router.Handle("/session/qr", c.Then(s.GetQR())).Methods("GET")
	s.router.Handle("/session/pairphone", c.Then(s.PairPhone())).Methods("POST")

	s.router.Handle("/webhook", c.Then(s.SetWebhook())).Methods("POST")
	s.router.Handle("/webhook", c.Then(s.GetWebhook())).Methods("GET")

	s.router.Handle("/chat/send/text", c.Then(s.SendMessage())).Methods("POST")
	s.router.Handle("/chat/send/image", c.Then(s.SendImage())).Methods("POST")
	s.router.Handle("/chat/send/audio", c.Then(s.SendAudio())).Methods("POST")
	s.router.Handle("/chat/send/document", c.Then(s.SendDocument())).Methods("POST")
	//	s.router.Handle("/chat/send/template", c.Then(s.SendTemplate())).Methods("POST")
	s.router.Handle("/chat/send/video", c.Then(s.SendVideo())).Methods("POST")

	s.router.Handle("/user/check", c.Then(s.CheckUser())).Methods("POST")

	s.router.PathPrefix("/").Handler(http.FileServer(http.Dir(exPath + "/static/")))
}

package main

import (
	"github.com/stayfatal/Networking_Application_Layer/internal/adapter/httpapi"
	"github.com/stayfatal/Networking_Application_Layer/internal/adapter/httpclient"
	"github.com/stayfatal/Networking_Application_Layer/internal/config"
	"github.com/stayfatal/Networking_Application_Layer/internal/usecase"
	"log"
	"net/http"
)

func main() {
	s := usecase.New(httpclient.New(), config.Env("TRANSPORT_URL", "http://localhost:8082"))
	log.Fatal(http.ListenAndServe(":"+config.Env("PORT", "8081"), httpapi.New(s, config.Env("STATIC_DIR", "frontend/dist"))))
}

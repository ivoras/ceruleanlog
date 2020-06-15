package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gorilla/handlers"
	"github.com/ivoras/ceruleanlog/logcore"
	"github.com/rs/cors"
)

const (
	wwwBind         = ":2020"
	jsonContentType = "application/json; charset=utf-8"
)

// Goroutine which serves HTTP & WS for the main client-facing API
func webServer() {
	http.HandleFunc("/", wwwRoot)
	http.HandleFunc("/gelf", wwwGelf)
	http.HandleFunc("/query", wwwQuery)

	log.Println("Web server listening on", wwwBind)

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "DELETE", "PUT"},
		AllowCredentials: true,
	})

	err := http.ListenAndServe(wwwBind, handlers.CombinedLoggingHandler(logOutput, corsHandler.Handler(http.DefaultServeMux)))
	if err != nil {
		log.Panic("Cannot listen on ", wwwBind, " for the web server")
	}
}

// wwwError sends a HTTP error with a JSON description (WwwRespDefault struct)
// to the client with the Internel Server Error (500) code.
func wwwError(w http.ResponseWriter, r *http.Request, msg string) {
	log.Println(msg)
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(http.StatusInternalServerError)
	_, err := w.Write(jsonifyWhateverToBytes(WwwRespDefault{Ok: false, Message: msg}))
	if err != nil {
		log.Println(err)
	}
	debug.PrintStack()
}

// wwwError sends a HTTP error with a JSON description (WwwRespDefault struct)
// to the client an arbitrary HTTP code.
func wwwErrorWithCode(w http.ResponseWriter, r *http.Request, msg string, code int) {
	log.Println(msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, err := w.Write(jsonifyWhateverToBytes(WwwRespDefault{Ok: false, Message: msg}))
	if err != nil {
		log.Println(err)
	}
}

// wwwJSON JSON-ifies and sends the given msg to the HTTP client.
func wwwJSON(w http.ResponseWriter, r *http.Request, msg interface{}) {
	jsonb, err := json.Marshal(msg)
	if err != nil {
		wwwError(w, r, fmt.Sprintf("Error marshalling response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(jsonb)
	if err != nil {
		log.Println(err)
	}
}

// Handles index.html
func wwwRoot(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("<html><body>Equinox API. Nothing here.</body></html>"))
}

// Handles the /gelf API
func wwwGelf(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		wwwError(w, r, "HTTP POST method expected")
		return
	}
	defer r.Body.Close()
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		wwwErrorWithCode(w, r, "Cannot read data", http.StatusBadRequest)
		return
	}
	msg, err := logcore.ParseGelfMessage(data)
	if err != nil {
		wwwErrorWithCode(w, r, fmt.Sprintf("Error parsing GELF message: %v", err), http.StatusBadRequest)
		return
	}
	err = instance.AddMessage(msg)
	if err != nil {
		wwwError(w, r, fmt.Sprintf("Error ingesting message: %v", err))
		return
	}
	wwwJSON(w, r, WwwRespDefault{Ok: true, Message: "Saved."})
}

// Handles the /query API
func wwwQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		wwwError(w, r, "HTTP GET method expected")
		return
	}
	if len(r.URL.Query()["time_from"]) == 0 {
		wwwErrorWithCode(w, r, "Missing time_from", http.StatusBadRequest)
		return
	}
	if len(r.URL.Query()["time_to"]) == 0 {
		wwwErrorWithCode(w, r, "Missing time_to", http.StatusBadRequest)
		return
	}

	strTimeFrom := r.URL.Query()["time_from"][0]
	timeFrom, err := time.ParseInLocation("2006-01-02T15:04", strTimeFrom, time.UTC)
	if err != nil {
		wwwErrorWithCode(w, r, fmt.Sprintf("Invalid time_from: %v", err), http.StatusBadRequest)
		return
	}
	strTimeTo := r.URL.Query()["time_to"][0]
	timeTo, err := time.ParseInLocation("2006-01-02T15:04", strTimeTo, time.UTC)
	if err != nil {
		wwwErrorWithCode(w, r, fmt.Sprintf("Invalid time_to: %v", err), http.StatusBadRequest)
		return
	}
	query := r.URL.Query()["query"][0]
	if query == "" {
		wwwErrorWithCode(w, r, "Invalid query", http.StatusBadRequest)
		return
	}

	result, err := instance.Query(uint32(timeFrom.Unix()), uint32(timeTo.Unix()), query)
	wwwJSON(w, r, WwwRespQuery{Ok: true, Result: result})
}

package main

import (
	"log"
	"net/http"

	"project/handlers"
	"project/middleware"
	"project/store"

	"github.com/elastic/go-elasticsearch/v8"
)

var jwtKey = []byte("my_secret_key")

func main() {
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	err = store.DeleteIndexIfExists(esClient, "places")
	if err != nil {
		log.Fatalf("Error deleting index: %s", err)
	}

	err = store.CreateIndex(esClient)
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}

	err = store.LoadData(esClient, "data/data.csv")
	if err != nil {
		log.Fatalf("Error loading data: %s", err)
	}

	store := store.NewElasticsearchStore(esClient)

	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		handlers.HandleRequest(w, req, store, handlers.RenderHTML)
	})

	http.HandleFunc("/api/places", func(w http.ResponseWriter, req *http.Request) {
		handlers.HandleRequest(w, req, store, handlers.RenderJSON)
	})

	http.HandleFunc("/api/recommend", middleware.WithJWTAuth(handlers.HandleGeoRequest(store), jwtKey))

	http.HandleFunc("/api/get_token", handlers.HandleGetToken(jwtKey))

	log.Fatal(http.ListenAndServe(":8888", nil))
}

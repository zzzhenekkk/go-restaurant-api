package handlers

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"

	"project/store"
)

type PageData struct {
	Places   []map[string]interface{}
	PrevPage int
	NextPage int
	LastPage int
}

var tmpl = template.Must(template.ParseFiles("templates/index.html"))

func HandleRequest(w http.ResponseWriter, req *http.Request, store store.Store, renderFunc func(http.ResponseWriter, []map[string]interface{}, int, int, int)) {
	page := req.URL.Query().Get("page")
	if page == "" {
		page = "1"
	}

	pageNumber, err := strconv.Atoi(page)
	if err != nil || pageNumber < 1 {
		http.Error(w, fmt.Sprintf(`{"error": "Invalid 'page' value: '%s'"}`, page), http.StatusBadRequest)
		return
	}
	size := 10
	from := (pageNumber - 1) * size

	places, total, err := store.GetPlaces(size, from)
	if err != nil {
		http.Error(w, "Error fetching places", http.StatusInternalServerError)
		return
	}

	lastPage := (total + size - 1) / size
	if pageNumber > lastPage {
		http.Error(w, fmt.Sprintf(`{"error": "Invalid 'page' value: '%s'"}`, page), http.StatusBadRequest)
		return
	}

	renderFunc(w, places, total, pageNumber, size)
}

func RenderHTML(w http.ResponseWriter, places []map[string]interface{}, total, pageNumber, size int) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	lastPage := (total + size - 1) / size
	prevPage := 0
	if pageNumber > 1 {
		prevPage = pageNumber - 1
	}
	nextPage := lastPage
	if pageNumber*size < total {
		nextPage = pageNumber + 1
	}

	pageData := PageData{
		Places:   places,
		PrevPage: prevPage,
		NextPage: nextPage,
		LastPage: lastPage,
	}

	if err := tmpl.Execute(w, pageData); err != nil {
		fmt.Fprintf(w, "Error executing template: %s", err)
	}
}

func RenderJSON(w http.ResponseWriter, places []map[string]interface{}, total, pageNumber, size int) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"name":   "Places",
		"total":  total,
		"places": places,
	}

	if pageNumber > 1 {
		response["prev_page"] = pageNumber - 1
	}
	if pageNumber*size < total {
		response["next_page"] = pageNumber + 1
	}
	response["last_page"] = (total + size - 1) / size

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
	}
}

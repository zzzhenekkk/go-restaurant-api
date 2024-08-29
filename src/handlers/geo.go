package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"project/store"
)

func HandleGeoRequest(store store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		latStr := req.URL.Query().Get("lat")
		lonStr := req.URL.Query().Get("lon")
		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			http.Error(w, "Invalid latitude value", http.StatusBadRequest)
			return
		}
		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			http.Error(w, "Invalid longitude value", http.StatusBadRequest)
			return
		}

		places, err := store.GetNearestPlaces(lat, lon, 3)
		if err != nil {
			http.Error(w, "Error fetching nearest places", http.StatusInternalServerError)
			return
		}

		response := map[string]interface{}{
			"name":   "Recommendation",
			"places": places,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
		}
	}
}

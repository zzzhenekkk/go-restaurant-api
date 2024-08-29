package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dgrijalva/jwt-go"
)

func HandleGetToken(jwtKey []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expirationTime := time.Now().Add(24 * time.Hour)
		claims := &jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, err := token.SignedString(jwtKey)
		if err != nil {
			http.Error(w, "Error generating token", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"token": tokenString,
		})
	}
}

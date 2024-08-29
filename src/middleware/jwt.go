package middleware

import (
	"net/http"
	"strings"

	"github.com/dgrijalva/jwt-go"
)

func WithJWTAuth(next http.HandlerFunc, jwtKey []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tokenStr := r.Header.Get("Authorization")
		if tokenStr == "" {
			http.Error(w, "Missing token", http.StatusUnauthorized)
			return
		}

		tokenStr = strings.TrimPrefix(tokenStr, "Bearer ")

		claims := &jwt.StandardClaims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
			return jwtKey, nil
		})

		if err != nil || !token.Valid {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

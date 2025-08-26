package middleware

import (
	"net/http"
	"time"

	"validation-gateway/internal"
)

func Auth(next http.HandlerFunc, secret_key []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")

		if authHeader == "" {
			http.Error(w, "Authorization header required.", http.StatusUnauthorized)
			return
		}

		tokenString := authHeader[len("Bearer "):]

		token, err := internal.ValidateToken(tokenString, secret_key)

		if err != nil {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		if token.ExpiresAt.Unix() < time.Now().Unix() {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		r.Header.Set("X-Username", token.Username)
		r.Header.Set("X-Role", token.Role)
		next(w, r)
	}
}

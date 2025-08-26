package internal

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type CustomClaims struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type LoginRequest struct {
	Username string `json:"username" validate:"required"`
	Password string `json:"password" validate:"required"`
}

type LoginResponse struct {
	Token      string `json:"token"`
	Expiration int64  `json:"expiration"`
}

func CreateToken(username string, secret_key []byte) (string, int64, error) {
	expiration := time.Now().Add(time.Hour * 24).Unix()
	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		jwt.MapClaims{
			"username": username,
			"exp":      expiration,
			"role":     "atmin",
		},
	)

	tokenString, err := token.SignedString(secret_key)

	if err != nil {
		return "", 0, err
	}

	return tokenString, expiration, nil
}

func ValidateToken(tokenString string, secret_key []byte) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return secret_key, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token")
}

func GenerateToken(request *LoginRequest, secret_key []byte) (LoginResponse, error) {
	token, exprtime, err := CreateToken(request.Username, secret_key)

	if err != nil {
		return LoginResponse{}, err
	}

	return LoginResponse{Token: token, Expiration: exprtime}, nil
}

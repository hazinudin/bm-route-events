package user

import (
	"fmt"
	"time"
	"validation-gateway/infra"
	"validation-gateway/pkg/repo"
	"validation-gateway/pkg/user"

	"golang.org/x/crypto/bcrypt"
)

type UserService struct {
	repo *repo.UserRepository
}

func NewUserService(db *infra.Database) *UserService {
	var service UserService

	service.repo = repo.NewUserRepository("users", db)

	return &service
}

func (u *UserService) CreateNewUser(username string, password string) error {
	pbyte := []byte(password)

	hash, err := bcrypt.GenerateFromPassword(pbyte, bcrypt.DefaultCost)

	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	user := user.User{
		Username:     username,
		PasswordHash: string(hash),
		CreatedAt:    time.Now().Unix(),
	}

	err = u.repo.CreateNewUser(&user)

	if err != nil {
		return err
	}

	return nil
}

func (u *UserService) ComparePasswordHash(username string, password string) bool {
	user, err := u.repo.FindUser(username)

	if err != nil {
		return false
	}

	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password))

	return err == nil
}

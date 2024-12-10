package auth

import (
	"context"
	"fmt"

	"github.com/konstellation-io/kai-sdk/go-sdk/v1/internal/common"

	"github.com/Nerzal/gocloak/v13"
	"github.com/go-logr/logr"
	"github.com/spf13/viper"
)

const (
	_authLoggerName = "[AUTHENTICATION]"
)

type Auth struct {
	logger       logr.Logger
	authEndpoint string
	clientID     string
	clientSecret string
	realm        string
	username     string
	password     string
	jwt          *gocloak.JWT
}

func New(logger logr.Logger) *Auth {
	user := viper.GetString(common.ConfigMinioClientUserKey)
	password := viper.GetString(common.ConfigMinioClientPasswordKey)
	authEndpoint := viper.GetString(common.ConfigAuthEndpointKey)
	realm := viper.GetString(common.ConfigAuthRealmKey)
	clientID := viper.GetString(common.ConfigAuthClientKey)
	clientSecret := viper.GetString(common.ConfigAuthClientSecretKey)

	return &Auth{
		logger:       logger,
		authEndpoint: authEndpoint,
		clientID:     clientID,
		clientSecret: clientSecret,
		realm:        realm,
		username:     user,
		password:     password,
		jwt:          nil,
	}
}

func (a *Auth) GetToken() (*gocloak.JWT, error) {
	client := gocloak.NewClient(a.authEndpoint)
	ctx := context.Background()

	if a.jwt != nil {
		token, err := client.RefreshToken(ctx, a.jwt.RefreshToken, a.clientID, a.clientSecret, a.realm)

		if err != nil {
			a.logger.WithName(_authLoggerName).V(2).Info("Couldn't refresh token")
		} else {
			a.jwt = token

			return token, nil
		}
	}

	token, err := client.Login(
		ctx,
		a.clientID,
		a.clientSecret,
		a.realm,
		a.username,
		a.password,
	)
	if err != nil {
		a.logger.WithName(_authLoggerName).Info(fmt.Sprintf("Error getting token: %s", err.Error()))
		return nil, err
	}

	a.jwt = token

	return a.jwt, nil
}

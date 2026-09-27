package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	Addr           string
	DatabaseURL    string
	JWTSecret      []byte
	AllowedOrigins map[string]struct{}
}

func Load() (Config, error) {
	c := Config{
		Addr:           envOr("HTTP_ADDR", ":8080"),
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		JWTSecret:      []byte(os.Getenv("JWT_SECRET")),
		AllowedOrigins: make(map[string]struct{}),
	}
	if c.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if len(c.JWTSecret) < 32 || string(c.JWTSecret) == "replace_with_a_random_secret_at_least_32_bytes" {
		return Config{}, errors.New("JWT_SECRET must be a random value of at least 32 bytes")
	}
	for _, origin := range strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",") {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			c.AllowedOrigins[origin] = struct{}{}
		}
	}
	return c, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

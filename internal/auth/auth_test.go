package auth

import (
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestPasswordHashAndJWT(t *testing.T) {
	hash, err := HashPassword("correct horse battery")
	if err != nil || hash == "correct horse battery" {
		t.Fatalf("password hash failed: %v", err)
	}
	if !CheckPassword(hash, "correct horse battery") || CheckPassword(hash, "wrong password") {
		t.Fatal("password verification gave incorrect result")
	}
	manager := New([]byte(strings.Repeat("x", 32)))
	token, expires, err := manager.Issue(42)
	if err != nil || expires.IsZero() {
		t.Fatalf("issue token: %v", err)
	}
	id, err := manager.Verify(token)
	if err != nil || id != 42 {
		t.Fatalf("verify token: id=%d err=%v", id, err)
	}
	forged, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{Issuer: "go-im", Subject: "42"}).SignedString([]byte(strings.Repeat("y", 32)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Verify(forged); err == nil {
		t.Fatal("forged token accepted")
	}
}

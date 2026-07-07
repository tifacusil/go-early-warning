package publisher

import (
	"context"
	"testing"
)

func TestNewFirebasePusher_NoCredentials_ReturnsNoop(t *testing.T) {
	p, err := NewFirebasePusher(context.Background(), FirebaseConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !IsNoop(p) {
		t.Fatal("expected noop pusher when no credentials are configured")
	}
}

func TestNewFirebasePusher_InvalidJSON_ReturnsNoop(t *testing.T) {
	p, err := NewFirebasePusher(context.Background(), FirebaseConfig{CredentialsJSON: "not-json"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !IsNoop(p) {
		t.Fatal("expected noop pusher for invalid credentials JSON")
	}
}

func TestIsNoop_FalseForOtherPushers(t *testing.T) {
	if IsNoop(&fcmPusher{}) {
		t.Fatal("fcmPusher must not be reported as noop")
	}
}

// Firebase Cloud Messaging adapter implementing publisher.Pusher, ported from
// the FCM half of alert_publisher.py. Initialized lazily; if credentials are
// absent the constructor returns a no-op pusher so the service still runs.
package publisher

import (
	"context"
	"encoding/json"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

// FirebaseConfig holds FCM credentials (externalized, CLAUDE.md §1.7).
type FirebaseConfig struct {
	CredentialsPath string // path to service-account JSON; takes precedence
	CredentialsJSON string // raw service-account JSON
}

type fcmPusher struct {
	client *messaging.Client
}

// noopPusher is used when Firebase is not configured.
type noopPusher struct{}

func (noopPusher) Push(context.Context, string, string, map[string]string, []string) error {
	return nil
}

// NewFirebasePusher builds an FCM pusher. If no credentials are present it
// returns a no-op pusher (nil error), matching the Python "skip if unconfigured".
func NewFirebasePusher(ctx context.Context, cfg FirebaseConfig) (Pusher, error) {
	var opt option.ClientOption
	switch {
	case cfg.CredentialsPath != "":
		opt = option.WithCredentialsFile(cfg.CredentialsPath)
	case cfg.CredentialsJSON != "":
		if !json.Valid([]byte(cfg.CredentialsJSON)) {
			return noopPusher{}, nil
		}
		opt = option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))
	default:
		return noopPusher{}, nil
	}

	app, err := firebase.NewApp(ctx, nil, opt)
	if err != nil {
		return nil, err
	}
	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, err
	}
	return &fcmPusher{client: client}, nil
}

// Push sends a high-priority multicast notification to the given tokens.
func (f *fcmPusher) Push(ctx context.Context, title, body string, data map[string]string, tokens []string) error {
	if len(tokens) == 0 {
		return nil
	}
	msg := &messaging.MulticastMessage{
		Tokens:       tokens,
		Notification: &messaging.Notification{Title: title, Body: body},
		Data:         data,
		Android:      &messaging.AndroidConfig{Priority: "high"},
		APNS:         &messaging.APNSConfig{Headers: map[string]string{"apns-priority": "10"}},
	}
	_, err := f.client.SendEachForMulticast(ctx, msg)
	return err
}

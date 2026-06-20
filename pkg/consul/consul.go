// Package consul handles service registration/deregistration and health-check
// registration, per CLAUDE.md §1.6 / §6.5: names <service-name>-<n>, tags
// version-/lang-/env-, HTTP health check every 10s.
package consul

import "time"

// Registration describes how a service announces itself to a Consul agent.
type Registration struct {
	ServiceName string
	ServiceID   string // <service-name>-<addr>-<port> when empty
	Address     string
	Port        int
	Tags        []string // version-<v>, lang-go, env-<env>

	HealthURL       string // http://addr:port/health
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	DeregisterAfter time.Duration
}

// TODO(SE-practice): implement Register/Deregister against hashicorp/consul/api
// (port of consul_registrar.py), Hito 2. Skeleton only.

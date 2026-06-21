package consul

import (
	"fmt"

	capi "github.com/hashicorp/consul/api"
)

// Client wraps a Consul agent client for service registration.
type Client struct {
	agent *capi.Client
	id    string
}

// AgentConfig holds how to reach the local Consul agent (externalized §1.7).
type AgentConfig struct {
	Host   string
	Port   int
	Scheme string
	Token  string
}

// NewClient connects to the Consul agent.
func NewClient(cfg AgentConfig) (*Client, error) {
	c := capi.DefaultConfig()
	c.Address = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	if cfg.Scheme != "" {
		c.Scheme = cfg.Scheme
	}
	c.Token = cfg.Token
	cl, err := capi.NewClient(c)
	if err != nil {
		return nil, err
	}
	return &Client{agent: cl}, nil
}

// Register self-registers the service with an HTTP health check (port of
// consul_registrar.py). Health check runs every HealthInterval (CLAUDE.md §1.6).
func (c *Client) Register(r Registration) error {
	id := r.ServiceID
	if id == "" {
		id = fmt.Sprintf("%s-%s-%d", r.ServiceName, r.Address, r.Port)
	}
	c.id = id
	reg := &capi.AgentServiceRegistration{
		ID:      id,
		Name:    r.ServiceName,
		Address: r.Address,
		Port:    r.Port,
		Tags:    r.Tags,
		Check: &capi.AgentServiceCheck{
			HTTP:                           r.HealthURL,
			Interval:                       r.HealthInterval.String(),
			Timeout:                        r.HealthTimeout.String(),
			DeregisterCriticalServiceAfter: r.DeregisterAfter.String(),
		},
	}
	return c.agent.Agent().ServiceRegister(reg)
}

// Deregister removes the service from the agent (call on shutdown).
func (c *Client) Deregister() error {
	if c.id == "" {
		return nil
	}
	return c.agent.Agent().ServiceDeregister(c.id)
}

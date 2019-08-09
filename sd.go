package gorillaz

import (
	"github.com/hashicorp/consul/api"
	"time"
)

type AgentServiceRegistrationOpt func(sd *api.AgentServiceRegistration)

type ServiceDefinition struct {
	ServiceName                    string
	Addr                           string
	Port                           int
	Tags                           []string
	Meta                           map[string]string
	Opt                            AgentServiceRegistrationOpt
	DeregisterCriticalServiceAfter time.Duration
	CheckInterval                  time.Duration
	CheckTimeout                   time.Duration
}

type ServiceEndpoint struct {
	Addr string
	Port int
	Tags []string
	Meta map[string]string
}

type ServiceRegistry interface {
	Register(d *ServiceDefinition, httpPort int) (string, error)
	DeRegister(serviceId string) error
	Resolve(serviceName string) ([]ServiceEndpoint, error)
}

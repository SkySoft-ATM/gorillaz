package gorillaz

import (
	"errors"
	"fmt"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
	"strings"
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

type ServiceDiscovery interface {
	Register(d *ServiceDefinition, httpPort int) (string, error)
	DeRegister(serviceId string) error
	Resolve(serviceName string) ([]ServiceEndpoint, error)
}

// gorillazResolverBuilder is a
// ResolverBuilder(https://godoc.org/google.golang.org/grpc/resolver#Builder).
type gorillazResolverBuilder struct {
	gaz *Gaz
}

func (g *gorillazResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	var result resolver.Resolver
	if strings.HasPrefix(target.Endpoint, "sd://") {
		if g.gaz.ServiceDiscovery == nil {
			return nil, errors.New("service discovery not initialized in gorillaz")
		}
		r := &serviceDiscoveryResolver{
			serviceDiscovery: g.gaz.ServiceDiscovery,
			name:             strings.TrimPrefix(target.Endpoint, "sd://"),
			cc:               cc,
			tick:             time.NewTicker(1 * time.Second),
		}
		go r.updater()

		result = r
	} else {
		split := strings.Split(target.Endpoint, ",")
		addrs := make([]resolver.Address, len(split))
		for i, s := range split {
			addrs[i] = resolver.Address{Addr: s}
		}
		r := &gorillazDefaultResolver{
			cc:        cc,
			addresses: addrs,
		}
		r.start()
		result = r
	}

	return result, nil
}
func (*gorillazResolverBuilder) Scheme() string { return "gorillaz" }

// serviceDiscoveryResolver is a
// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type serviceDiscoveryResolver struct {
	serviceDiscovery ServiceDiscovery
	name             string
	cc               resolver.ClientConn
	tick             *time.Ticker
}

func (r *serviceDiscoveryResolver) updater() {
	r.sendUpdate()
	for {
		_, ok := <-r.tick.C
		if !ok {
			break
		}
		r.sendUpdate()
	}
}

func (r *serviceDiscoveryResolver) sendUpdate() {
	endpoints, err := r.serviceDiscovery.Resolve(r.name)
	if err != nil {
		Log.Warn("Error while resolving ", zap.String("name", r.name), zap.Error(err))
		return
	}
	addrs := make([]resolver.Address, len(endpoints))
	for i, e := range endpoints {
		addrs[i] = resolver.Address{Addr: fmt.Sprintf("%s:%d", e.Addr, e.Port)}
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs})
}

func (*serviceDiscoveryResolver) ResolveNow(o resolver.ResolveNowOption) {}
func (r *serviceDiscoveryResolver) Close() {
	r.tick.Stop()
}

// gorillazDefaultResolver is a
// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type gorillazDefaultResolver struct {
	cc        resolver.ClientConn
	addresses []resolver.Address
}

func (r *gorillazDefaultResolver) start() {
	r.cc.UpdateState(resolver.State{Addresses: r.addresses})
}
func (*gorillazDefaultResolver) ResolveNow(o resolver.ResolveNowOption) {}
func (*gorillazDefaultResolver) Close()                                 {}

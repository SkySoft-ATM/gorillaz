package gorillaz

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
	"strings"
	"sync"
	"time"
)

const SdPrefix = "sd://"

type ServiceDefinition struct {
	ServiceName string
	Addr        string
	GrpcPort    int
	HttpPort    int
	Tags        []string
	Meta        map[string]string
}

type ServiceDiscovery interface {
	Register(d *ServiceDefinition) (RegistrationHandle, error)
	Resolve(serviceName string) ([]ServiceDefinition, error)
	ResolveWithTag(serviceName, tag string) ([]ServiceDefinition, error)
	ResolveTags(tag string) (map[string][]ServiceDefinition, error)
}

type RegistrationHandle interface {
	DeRegister(context.Context) error
}

// gorillazResolverBuilder is a
// ResolverBuilder(https://godoc.org/google.golang.org/grpc/resolver#Builder).
type gorillazResolverBuilder struct {
	gaz *Gaz
}

func (g *gorillazResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	var result resolver.Resolver

	if strings.HasPrefix(target.Endpoint, SdPrefix) {
		if g.gaz.ServiceDiscovery == nil {
			return nil, errors.New("service discovery not initialized in gorillaz")
		}
		r := &serviceDiscoveryResolver{
			serviceDiscovery: g.gaz.ServiceDiscovery,
			name:             strings.TrimPrefix(target.Endpoint, SdPrefix),
			cc:               cc,
			tick:             time.NewTicker(1 * time.Second),
			env:              g.gaz.Env,
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
	env              string
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
	endpoints, err := r.serviceDiscovery.ResolveWithTag(r.name, r.env)
	if err != nil {
		Log.Warn("Error while resolving ", zap.String("name", r.name), zap.Error(err))
		return
	}
	addrs := make([]resolver.Address, len(endpoints))
	for i, e := range endpoints {
		addrs[i] = resolver.Address{Addr: fmt.Sprintf("%s:%d", e.Addr, e.GrpcPort)}
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

func (g *Gaz) Register(d *ServiceDefinition) (RegistrationHandle, error) {
	if g.ServiceDiscovery == nil {
		return nil, errors.New("no service registry configured")
	}
	d.Tags = append(d.Tags, g.Env)
	return g.ServiceDiscovery.Register(d)
}

func (g *Gaz) Resolve(serviceName string) ([]ServiceDefinition, error) {
	return g.ServiceDiscovery.Resolve(serviceName)
}

func (g *Gaz) ResolveWithTag(serviceName, tag string) ([]ServiceDefinition, error) {
	return g.ServiceDiscovery.ResolveWithTag(serviceName, tag)
}

func NewMockedServiceDiscovery() (*MockedServiceDiscoveryToLocalGrpcServer, Option) {
	mock := MockedServiceDiscoveryToLocalGrpcServer{}
	return &mock, Option{Opt: func(gaz *Gaz) error {
		mock.mu.Lock()
		mock.g = gaz
		mock.mu.Unlock()
		gaz.ServiceDiscovery = &mock
		return nil
	}}
}

type MockedServiceDiscoveryToLocalGrpcServer struct {
	g  *Gaz
	mu sync.RWMutex
}

func (m *MockedServiceDiscoveryToLocalGrpcServer) UpdateGaz(g *Gaz) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.g = g
}

type MockedRegistrationHandle struct {
}

func (m MockedRegistrationHandle) DeRegister(ctx context.Context) error {
	return nil
}

func (m *MockedServiceDiscoveryToLocalGrpcServer) Register(d *ServiceDefinition) (RegistrationHandle, error) {
	return MockedRegistrationHandle{}, nil
}

func (m *MockedServiceDiscoveryToLocalGrpcServer) Resolve(serviceName string) ([]ServiceDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := ServiceDefinition{
		ServiceName: serviceName,
		Addr:        "localhost",
		GrpcPort:    m.g.GrpcPort(),
		HttpPort:    m.g.HttpPort(),
		Tags:        []string{},
		Meta: map[string]string{
			ServiceName: serviceName,
		},
	}
	return []ServiceDefinition{result}, nil
}

func (m *MockedServiceDiscoveryToLocalGrpcServer) ResolveWithTag(serviceName, tag string) ([]ServiceDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := ServiceDefinition{
		ServiceName: serviceName,
		Addr:        "localhost",
		GrpcPort:    m.g.GrpcPort(),
		HttpPort:    m.g.HttpPort(),
		Tags:        []string{tag},
		Meta: map[string]string{
			ServiceName: serviceName,
		},
	}
	return []ServiceDefinition{result}, nil
}

func (m *MockedServiceDiscoveryToLocalGrpcServer) ResolveTags(tag string) (map[string][]ServiceDefinition, error) {
	return nil, errors.New("unimplemented")
}

func WithMockedServiceDiscovery() Option {
	_, r := NewMockedServiceDiscovery()
	return r
}

package gorillaz

import (
	"context"
	"fmt"
	_ "github.com/coreos/etcd/clientv3"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/resolver"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var initialized = false

type Gaz struct {
	Router            *mux.Router
	ServiceDiscovery  ServiceDiscovery
	GrpcServer        *grpc.Server
	ServiceName       string
	ViperRemoteConfig bool
	Env               string
	Viper             *viper.Viper
	// use int32 because sync.atomic package doesn't support boolean out of the box
	isReady               *int32
	isLive                *int32
	streamRegistry        *streamRegistry
	grpcListener          net.Listener
	grpcServerOptions     []grpc.ServerOption
	configPath            string
	serviceAddress        string // optional address of the service that will be used for service discovery
	streamConsumers       *streamConsumerRegistry
	streamEndpointOptions []StreamEndpointConfigOpt
	httpListener          net.Listener
	httpSrv               *http.Server
	prometheusRegistry    *prometheus.Registry
}

type streamConsumerRegistry struct {
	sync.Mutex
	g                 *Gaz
	endpointsByName   map[string]*StreamEndpoint
	endpointConsumers map[*StreamEndpoint]map[*registeredConsumer]struct{}
}

func (g *Gaz) createConsumer(endpoints []string, streamName string, opts ...ConsumerConfigOpt) (StreamConsumer, error) {
	r := g.streamConsumers
	target := strings.Join(endpoints, ",")
	r.Lock()
	defer r.Unlock()
	e, ok := r.endpointsByName[target]
	if !ok {
		var err error
		Log.Debug("Creating stream endpoint", zap.String("target", target))
		e, err = r.g.NewStreamEndpoint(endpoints, g.streamEndpointOptions...)
		if err != nil {
			return nil, errors.Wrapf(err, "error while creating stream endpoint for target %s", target)
		}
		r.endpointsByName[e.target] = e
	}
	sc := e.ConsumeStream(streamName, opts...)
	rc := registeredConsumer{g: r.g, StreamConsumer: sc}
	consumers := r.endpointConsumers[e]
	if consumers == nil {
		consumers = make(map[*registeredConsumer]struct{})
		r.endpointConsumers[e] = consumers
	}
	consumers[&rc] = struct{}{}
	return &rc, nil
}

func (g *Gaz) deregister(c *registeredConsumer) {
	r := g.streamConsumers
	e := c.StreamConsumer.streamEndpoint()
	r.Lock()
	defer r.Unlock()
	consumers, ok := r.endpointConsumers[e]
	if !ok {
		Log.Warn("Stream consumers not found", zap.String("stream name", c.StreamName()),
			zap.String("target", e.target))
		return
	}
	delete(consumers, c)
	if len(consumers) == 0 {
		Log.Debug("Closing endpoint", zap.String("target", e.target))
		err := e.Close()
		if err != nil {
			Log.Warn("Error while closing endpoint", zap.String("target", e.target), zap.Error(err))
		}
		delete(r.endpointsByName, e.target)
		delete(r.endpointConsumers, e)
	} else {
		r.endpointConsumers[e] = consumers
	}
}

type GazOption interface {
	apply(*Gaz) error
}

type InitOption struct {
	Init func(g *Gaz) error
}

func (i InitOption) apply(g *Gaz) error {
	return i.Init(g)
}

type Option struct {
	Opt func(g *Gaz) error
}

func (i Option) apply(g *Gaz) error {
	return i.Opt(g)
}

func WithConfigPath(configPath string) InitOption {
	return InitOption{func(g *Gaz) error {
		g.configPath = configPath
		return nil
	}}
}

func WithGrpcServerOptions(o ...grpc.ServerOption) Option {
	return Option{func(g *Gaz) error {
		g.grpcServerOptions = o
		return nil
	}}
}

// New initializes the different modules (Logger, Tracing, Metrics, ready and live Probes and Properties)
// It takes root at the current folder for properties file and a map of properties
func New(options ...GazOption) *Gaz {
	if initialized {
		Log.Warn("gorillaz is already initialized")
	}
	initialized = true
	GracefulStop()
	gaz := Gaz{Router: mux.NewRouter(), isReady: new(int32), isLive: new(int32), Viper: viper.New()}
	gaz.httpSrv = &http.Server{Handler: gaz.Router}
	gaz.streamConsumers = &streamConsumerRegistry{
		g:                 &gaz,
		endpointsByName:   make(map[string]*StreamEndpoint),
		endpointConsumers: make(map[*StreamEndpoint]map[*registeredConsumer]struct{}),
	}

	// first apply only init options
	for _, o := range options {
		_, ok := o.(InitOption)
		if ok {
			err := o.apply(&gaz)
			if err != nil {
				panic(err)
			}
		}
	}

	// then parse configuration
	if gaz.ViperRemoteConfig {
		err := gaz.Viper.ReadRemoteConfig()
		if err != nil {
			panic(err)
		}
	}
	parseConfiguration(&gaz, gaz.configPath)

	// then apply non-init options
	for _, o := range options {
		_, ok := o.(InitOption)
		if !ok {
			err := o.apply(&gaz)
			if err != nil {
				panic(err)
			}
		}
	}

	serviceName := gaz.Viper.GetString("service.name")
	if serviceName == "" {
		panic(errors.New("please provide a service name with the \"service.name\" configuration key"))
	}
	gaz.ServiceName = serviceName

	env := gaz.Viper.GetString("env")
	if env == "" {
		panic(errors.New("please provide an environment with the \"env\" configuration key"))
	}
	gaz.Env = env

	serviceAddress := gaz.Viper.GetString("service.address")
	gaz.serviceAddress = serviceAddress

	err := gaz.InitLogs(gaz.Viper.GetString("log.level"))
	if err != nil {
		panic(err)
	}

	if gaz.Viper.GetBool("tracing.enabled") {
		gaz.InitTracingFromConfig()
	}

	// necessary to avoid weird 'transport closing' errors
	// see https://github.com/grpc/grpc-go/issues/2443
	// see https://github.com/grpc/grpc-go/issues/2160
	// https://stackoverflow.com/questions/52993259/problem-with-grpc-setup-getting-an-intermittent-rpc-unavailable-error/54703234#54703234
	ka := grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 1 * time.Minute,
		Time:              20 * time.Second, // ping client connection every 20 seconds
	})

	keepalivePolicy := grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second, // Allow the client to send ping every 10 seconds max
		PermitWithoutStream: true,             // Allow the client to send pings when no streams are created
	})

	serverOptions := make([]grpc.ServerOption, 3+len(gaz.grpcServerOptions))
	serverOptions[0] = grpc.CustomCodec(&binaryCodec{})
	serverOptions[1] = ka
	serverOptions[2] = keepalivePolicy

	for i, o := range gaz.grpcServerOptions {
		serverOptions[3+i] = o
	}

	gaz.GrpcServer = grpc.NewServer(serverOptions...)
	reflection.Register(gaz.GrpcServer)
	gaz.streamRegistry = &streamRegistry{
		providers:  make(map[string]*StreamProvider),
		serviceIds: make(map[string]RegistrationHandle),
	}
	stream.RegisterStreamServer(gaz.GrpcServer, gaz.streamRegistry)

	Log.Info("Registering gRPC health server")
	healthServer := health.NewServer()
	healthServer.SetServingStatus("Stream", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gaz.GrpcServer, healthServer)

	Log.Info("Registering gorillaz gRPC resolver")
	resolver.Register(&gorillazResolverBuilder{gaz: &gaz})

	grpcPort := gaz.Viper.GetInt("grpc.port")
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		panic(err)
	}
	gaz.grpcListener = grpcListener

	return &gaz
}

// Starts the router, once Run is launched, you should no longer add new handlers on the router
func (g *Gaz) Run() <-chan struct{} {

	if he := g.Viper.GetBool("healthcheck.enabled"); he {
		Sugar.Info("Activating health check")
		g.InitHealthcheck()
	}

	if prom := g.Viper.GetBool("prometheus.enabled"); prom {
		promPath := g.Viper.GetString("prometheus.endpoint")
		g.InitPrometheus(promPath)
	}

	if pprof := g.Viper.GetBool("pprof.enabled"); pprof {
		g.InitPprof(g.Viper.GetInt("pprof.port"))
	}

	grpcNotif := make(chan struct{})
	go g.serveGrpc(grpcNotif)

	httpNotif := make(chan struct{})
	go func() {
		// register /version to return the build version
		g.Router.HandleFunc("/version", VersionHTML).Methods("GET")
		port := g.Viper.GetInt("http.port")
		httpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			panic(err)
		}
		g.httpListener = httpListener
		httpPort := g.HttpPort()
		Sugar.Infof("Starting HTTP server on :%d", httpPort)
		httpNotif <- struct{}{}

		err = g.httpSrv.Serve(httpListener)
		if err != nil {
			if err != http.ErrServerClosed {
				Sugar.Errorf("HTTP server server stopped on :%d, %v", httpPort, err)
				panic(err)
			} else {
				Sugar.Infof("HTTP server server stopped on :%d", httpPort)
			}
		}
	}()
	gazReady := make(chan struct{})
	go g.notifyWhenReady(grpcNotif, httpNotif, gazReady)
	return gazReady
}

func (g *Gaz) notifyWhenReady(grpcNotif, httpNotif <-chan struct{}, gazReady chan<- struct{}) {
	var grpcReady, httpReady bool
	for {
		select {
		case <-httpNotif:
			httpReady = true
		case <-grpcNotif:
			grpcReady = true
		}
		if grpcReady && httpReady {
			gazReady <- struct{}{}
			return
		}
	}
}

func (g *Gaz) GrpcPort() int {
	return g.grpcListener.Addr().(*net.TCPAddr).Port
}

func (g *Gaz) HttpPort() int {
	return g.httpListener.Addr().(*net.TCPAddr).Port
}

func (g *Gaz) serveGrpc(ready chan<- struct{}) {
	port := g.GrpcPort()
	Log.Info("Starting gRPC server on port", zap.Int("port", port))

	if g.ServiceDiscovery != nil {
		h, err := g.Register(&ServiceDefinition{ServiceName: g.ServiceName,
			Addr: g.serviceAddress,
			Port: port})
		if err != nil {
			panic(err)
		}
		defer h.DeRegister()
	}
	ready <- struct{}{}
	err := g.GrpcServer.Serve(g.grpcListener)
	if err != nil {
		Log.Warn("gRPC server stopped", zap.Error(err))
	}
}

func (g *Gaz) GrpcDialService(serviceName string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return g.GrpcDial(SdPrefix+serviceName, opts...)
}

func (g *Gaz) GrpcDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	clientKeepAlive := grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                15 * time.Second,
		PermitWithoutStream: true,
	})
	options := make([]grpc.DialOption, len(opts)+3)
	options[0] = grpc.WithBalancerName(roundrobin.Name)
	options[1] = grpc.WithBackoffMaxDelay(5 * time.Second)
	options[2] = clientKeepAlive
	for i, o := range opts {
		options[3+i] = o
	}
	return grpc.Dial("gorillaz:///"+target, options...)
}

func (g *Gaz) Shutdown() {
	Log.Info("Stopping gRPC server")
	g.GrpcServer.Stop()
	Log.Info("Closing gRPC listener")
	err := g.grpcListener.Close()
	if err != nil {
		Log.Warn("Error while closing gRPC listener", zap.Error(err))
	}
	Log.Info("Shutting down http server")
	err = g.httpSrv.Shutdown(context.Background())
	if err != nil {
		Log.Warn("Error while shutting down http server", zap.Error(err))
	}
	if g.httpListener != nil {
		Log.Info("Closing http listener")
		err = g.httpListener.Close()
		if err != nil {
			Log.Warn("Error while closing http listener", zap.Error(err))
		}
	}

}

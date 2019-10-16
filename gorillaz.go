package gorillaz

import (
	"context"
	"errors"
	"fmt"
	_ "github.com/coreos/etcd/clientv3"
	"github.com/gorilla/mux"
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
	"strconv"
	"sync"
	"time"
)

type Gaz struct {
	Router             *mux.Router
	ServiceDiscovery   ServiceDiscovery
	registrationHandle RegistrationHandle
	GrpcServer         *grpc.Server
	ServiceName        string
	ViperRemoteConfig  func(g *Gaz) error
	Env                string
	Viper              *viper.Viper
	// use int32 because sync.atomic package doesn't support boolean out of the box
	isReady               *int32
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
	bindConfigKeysAsFlag  bool
	streamDefinitions     *GetAndWatchStreamProvider
}

type streamConsumerRegistry struct {
	sync.Mutex
	g                 *Gaz
	endpointsByName   map[string]*streamEndpoint
	endpointConsumers map[*streamEndpoint]map[StoppableStream]struct{}
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

// with this option the configuration keys define in the config file will be declared as flags,
// and the flag default value will be the value from the config file.
func BindConfigKeysAsFlags() InitOption {
	return InitOption{func(g *Gaz) error {
		g.bindConfigKeysAsFlag = true
		return nil
	}}
}

func WithConfigPath(configPath string) InitOption {
	return InitOption{func(g *Gaz) error {
		g.configPath = configPath
		return nil
	}}
}

func WithServiceName(sn string) InitOption {
	return InitOption{func(g *Gaz) error {
		g.Viper.Set("service.name", sn)
		return nil
	}}
}

func WithTracingEnabled() InitOption {
	return InitOption{func(g *Gaz) error {
		g.Viper.Set("tracing.enabled", true)
		return nil
	}}
}

func WithGrpcServerOptions(o ...grpc.ServerOption) Option {
	return Option{func(g *Gaz) error {
		g.grpcServerOptions = o
		return nil
	}}
}

func (g *Gaz) tracingEnabled() bool {
	return g.Viper.GetBool("tracing.enabled")
}

// New initializes the different modules (Logger, Tracing, Metrics, ready and live Probes and Properties)
// It takes root at the current folder for properties file and a map of properties
func New(options ...GazOption) *Gaz {
	GracefulStop()
	gaz := Gaz{Router: mux.NewRouter(), isReady: new(int32), Viper: viper.New(), prometheusRegistry: prometheus.NewRegistry()}

	// expose Go metrics and process metrics as Prometheus DefaultRegistry would
	// https://github.com/prometheus/client_golang/blob/v1.1.0/prometheus/registry.go#L60
	gaz.prometheusRegistry.Register(prometheus.NewGoCollector())
	gaz.prometheusRegistry.Register(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	gaz.httpSrv = &http.Server{Handler: gaz.Router}
	gaz.streamConsumers = &streamConsumerRegistry{
		g:                 &gaz,
		endpointsByName:   make(map[string]*streamEndpoint),
		endpointConsumers: make(map[*streamEndpoint]map[StoppableStream]struct{}),
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
	parseConfiguration(&gaz, gaz.configPath)

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

	if gaz.ViperRemoteConfig != nil {
		err := gaz.ViperRemoteConfig(&gaz)
		if err != nil {
			panic(err)
		}
	}

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

	serviceAddress := gaz.Viper.GetString("service.address")
	gaz.serviceAddress = serviceAddress

	err := gaz.InitLogs(gaz.Viper.GetString("log.level"))
	if err != nil {
		panic(err)
	}

	if gaz.tracingEnabled() {
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

	if gaz.tracingEnabled() {
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(TracingServerInterceptor()))
	}

	gaz.GrpcServer = grpc.NewServer(serverOptions...)
	reflection.Register(gaz.GrpcServer)
	gaz.streamRegistry = NewStreamRegistry(&gaz)
	sdProvider, err := gaz.NewGetAndWatchStreamProvider(streamDefinitions, "stream.StreamDefinition")
	if err != nil {
		Log.Fatal("Cannot create stream definitions provider", zap.Error(err))
	}
	gaz.streamDefinitions = sdProvider
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

const (
	grpcTag          = "grpc"
	httpTag          = "http"
	httpPortMetadata = "http_port"
)

// Starts the router, once Run is launched, you should no longer add new handlers on the router.
// It returns a channel that will be notified once the gRPC and http servers have been started.
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

	var waitgroup sync.WaitGroup
	waitgroup.Add(2) // wait for gRPC + http
	go g.serveGrpc(&waitgroup)

	port := g.Viper.GetInt("http.port")
	httpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		Log.Panic("HTTP Listen failed", zap.Error(err))
	}
	g.httpListener = httpListener

	go func() {
		// register /version to return the build version
		g.Router.HandleFunc("/version", VersionHTML).Methods("GET")
		httpPort := g.HttpPort()
		Sugar.Infof("Starting HTTP server on :%d", httpPort)
		waitgroup.Done()

		err = g.httpSrv.Serve(httpListener)
		if err != nil {
			if err != http.ErrServerClosed {
				Log.Panic("HTTP serve stopped unexpectedly", zap.Error(err))
			}
			Sugar.Infof("HTTP server server stopped on :%d", httpPort)
		}
	}()
	if g.ServiceDiscovery != nil {
		Log.Info("registering service", zap.String("serviceName", g.ServiceName), zap.String("serviceAddr", g.serviceAddress), zap.Int("port", g.GrpcPort()))
		var err error
		serviceDefinition := &ServiceDefinition{ServiceName: g.ServiceName,
			Addr: g.serviceAddress,
			Port: g.GrpcPort(),
			Tags: []string{grpcTag, httpTag, StreamProviderTag, g.Env},
			Meta: map[string]string{httpPortMetadata: strconv.Itoa(g.HttpPort()), "env": g.Env},
		}

		g.registrationHandle, err = g.Register(serviceDefinition)
		if err != nil {
			Log.Panic("failed to register service", zap.Error(err))
		}
	}
	gazReady := make(chan struct{})
	go g.notifyWhenReady(&waitgroup, gazReady)
	return gazReady
}

func (g *Gaz) notifyWhenReady(waitgroup *sync.WaitGroup, gazReady chan<- struct{}) {
	waitgroup.Wait()
	gazReady <- struct{}{}
}

func (g *Gaz) GrpcPort() int {
	return g.grpcListener.Addr().(*net.TCPAddr).Port
}

func (g *Gaz) HttpPort() int {
	return g.httpListener.Addr().(*net.TCPAddr).Port
}

func (g *Gaz) serveGrpc(waitgroup *sync.WaitGroup) {
	port := g.GrpcPort()
	Log.Info("Starting gRPC server on port", zap.Int("port", port))

	waitgroup.Done()
	err := g.GrpcServer.Serve(g.grpcListener)
	if err != nil {
		Log.Fatal("gRPC Serve in error", zap.Error(err))
	}
	Log.Info("gRPC server stopped")
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
	if g.tracingEnabled() {
		options = append(options, grpc.WithUnaryInterceptor(TracingClientInterceptor()))
	}

	return grpc.Dial("gorillaz:///"+target, options...)
}

func (g *Gaz) Shutdown() {
	Log.Info("Deregister the service")
	// wait max 1 second for deregistering the service
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if g.registrationHandle != nil {
		err := g.registrationHandle.DeRegister(ctx)
		if err != nil {
			Log.Error("Failed to deregister the service", zap.Error(err))
		}
	}

	Log.Info("Stopping gRPC server")
	g.GrpcServer.Stop()

	Log.Info("Closing http server")
	err := g.httpSrv.Close()
	if err != nil {
		Log.Warn("Error while closing http server", zap.Error(err))
	}
}

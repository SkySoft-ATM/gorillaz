package gorillaz

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"net"
	"net/http"
	"time"
)

var initialized = false

type Gaz struct {
	Router           *mux.Router
	ServiceDiscovery ServiceDiscovery
	GrpcServer       *grpc.Server
	ServiceName      string
	// use int32 because sync.atomic package doesn't support boolean out of the box
	isReady        *int32
	isLive         *int32
	streamRegistry *streamRegistry
	context        map[string]interface{}
	grpcListener   net.Listener
}

type Option func(*Gaz) error

func WithContext(context map[string]interface{}) Option {
	return func(g *Gaz) error {
		g.context = context
		return nil
	}
}

// New initializes the different modules (Logger, Tracing, Metrics, ready and live Probes and Properties)
// It takes root at the current folder for properties file and a map of properties
func New(options ...Option) *Gaz {
	if initialized {
		panic("gorillaz is already initialized")
	}
	initialized = true
	gaz := Gaz{Router: mux.NewRouter(), isReady: new(int32), isLive: new(int32)}
	for _, o := range options {
		err := o(&gaz)
		if err != nil {
			panic(err)
		}
	}

	parseConfiguration(gaz.context)
	err := gaz.InitLogs(viper.GetString("log.level"))
	if err != nil {
		panic(err)
	}

	if viper.GetBool("tracing.enabled") {
		gaz.InitTracingFromConfig()
	}

	serviceName := viper.GetString("service.name")
	if serviceName == "" {
		panic(errors.New("please provide a service name with the \"service.name\" configuration key"))
	}
	gaz.ServiceName = serviceName

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

	gaz.GrpcServer = grpc.NewServer(grpc.CustomCodec(&binaryCodec{}), ka, keepalivePolicy)
	gaz.streamRegistry = &streamRegistry{
		providers:  make(map[string]*StreamProvider),
		serviceIds: make(map[string]string),
	}
	stream.RegisterStreamServer(gaz.GrpcServer, gaz.streamRegistry)

	Log.Info("Registering gRPC health server")
	healthServer := health.NewServer()
	healthServer.SetServingStatus("Stream", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gaz.GrpcServer, healthServer)

	Log.Info("Registering gorillaz gRPC resolver")
	resolver.Register(&gorillazResolverBuilder{gaz: &gaz})

	grpcPort := viper.GetInt("grpc.port")
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		panic(err)
	}
	gaz.grpcListener = grpcListener

	return &gaz
}

// Starts the router, once Run is launched, you should no longer add new handlers on the router
func (g Gaz) Run() {

	go g.serveGrpc()

	go func() {
		if he := viper.GetBool("healthcheck.enabled"); he {
			Sugar.Info("Activating health check")
			g.InitHealthcheck()
		}

		if prom := viper.GetBool("prometheus.enabled"); prom {
			promPath := viper.GetString("prometheus.endpoint")
			g.InitPrometheus(promPath)
		}

		if pprof := viper.GetBool("pprof.enabled"); pprof {
			g.InitPprof(viper.GetInt("pprof.port"))
		}

		// register /version to return the build version
		g.Router.HandleFunc("/version", VersionHTML).Methods("GET")
		port := viper.GetInt("http.port")
		httpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			panic(err)
		}
		httpPort := httpListener.Addr().(*net.TCPAddr).Port
		Sugar.Infof("Starting HTTP server on :%d", httpPort)
		err = http.Serve(httpListener, g.Router)
		if err != nil {
			Sugar.Errorf("Cannot start HTTP server on :%d, %+v", httpPort, err)
			panic(err)
		}
	}()
}

func (g Gaz) serveGrpc() {
	port := g.grpcListener.Addr().(*net.TCPAddr).Port
	Log.Info("Starting gRPC server on port", zap.Int("port", port))

	sid, err := g.Register(&ServiceDefinition{ServiceName: g.ServiceName, Port: port})
	if err != nil {
		panic(err)
	}

	defer g.DeRegister(sid)

	err = g.GrpcServer.Serve(g.grpcListener)
	Log.Warn("gRPC server stopper", zap.Error(err))
}

func (g Gaz) Register(d *ServiceDefinition) (string, error) {
	if g.ServiceDiscovery == nil {
		return "", errors.New("no service registry configured")
	}
	return g.ServiceDiscovery.Register(d)
}

func (g Gaz) DeRegister(serviceId string) error {
	return g.ServiceDiscovery.DeRegister(serviceId)
}

func (g Gaz) Resolve(serviceName string) ([]ServiceDefinition, error) {
	return g.ServiceDiscovery.Resolve(serviceName)
}

func (g Gaz) GrpcDial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
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

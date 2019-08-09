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
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var initialized = false

type Gaz struct {
	Router             *mux.Router
	ServiceDiscovery   ServiceDiscovery
	StreamProviderPort int
	// use int32 because sync.atomic package doesn't support boolean out of the box
	isReady        *int32
	isLive         *int32
	grpcServer     *grpc.Server
	streamRegistry *streamRegistry
	httpListener   *net.Listener
	httpPort       int
	context        map[string]interface{}
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

	gaz.grpcServer = grpc.NewServer(grpc.CustomCodec(&binaryCodec{}), ka, keepalivePolicy)
	gaz.streamRegistry = &streamRegistry{
		providers: make(map[string]*StreamProvider),
	}
	stream.RegisterStreamServer(gaz.grpcServer, gaz.streamRegistry)

	port := viper.GetInt("http.port")
	httpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	gaz.httpPort = httpListener.Addr().(*net.TCPAddr).Port
	gaz.httpListener = &httpListener

	Log.Info("Registering gorillaz gRPC resolver")
	resolver.Register(&gorillazResolverBuilder{gaz: &gaz})

	return &gaz
}

// Starts the router, once Run is launched, you should no longer add new handlers on the router
func (g *Gaz) Run() {
	streamPort := viper.GetInt("stream.provider.port")

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", streamPort))
	if err != nil {
		panic(err)
	}
	split := strings.Split(l.Addr().String(), ":")
	port, err := strconv.Atoi(split[len(split)-1])
	if err != nil {
		panic(err)
	}
	Log.Info("Listening for streaming request on port", zap.Int("port", port))
	g.StreamProviderPort = port
	go g.grpcServer.Serve(l)

	go func() {
		if health := viper.GetBool("healthcheck.enabled"); health {
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
		Sugar.Infof("Starting HTTP server on :%d", g.httpPort)
		err := http.Serve(*g.httpListener, g.Router)
		if err != nil {
			Sugar.Errorf("Cannot updater HTTP server on :%d, %+v", g.httpPort, err)
			panic(err)
		}
	}()
}

func (g Gaz) Register(d *ServiceDefinition) (string, error) {
	if g.ServiceDiscovery == nil {
		return "", errors.New("no service registry configured")
	}
	return g.ServiceDiscovery.Register(d, g.httpPort)
}

func (g Gaz) DeRegister(serviceId string) error {
	return g.ServiceDiscovery.DeRegister(serviceId)
}

func (g Gaz) Resolve(serviceName string) ([]ServiceEndpoint, error) {
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

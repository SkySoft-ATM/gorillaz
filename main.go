package gorillaz

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"net/http"
)

var initialized = false

type Gaz struct {
	Router *mux.Router
	// use int32 because sync.atomic package doesn't support boolean out of the box
	isReady        *int32
	isLive         *int32
	grpcServer     *grpc.Server
	streamRegistry *streamRegistry
}

// New initializes the different modules (Logger, Tracing, Metrics, ready and live Probes and Properties)
// It takes root at the current folder for properties file and a map of properties
func New(context map[string]interface{}) *Gaz {
	if initialized {
		panic("gorillaz is already initialized")
	}
	initialized = true

	parseConfiguration(context)
	gaz := Gaz{Router: mux.NewRouter(), isReady: new(int32), isLive: new(int32)}
	err := gaz.InitLogs(viper.GetString("log.level"))
	if err != nil {
		panic(err)
	}

	if viper.GetBool("tracing.enabled") {
		gaz.InitTracingFromConfig()
	}

	gaz.grpcServer = grpc.NewServer(grpc.CustomCodec(&binaryCodec{}))
	gaz.streamRegistry = &streamRegistry{
		providers: make(map[string]*StreamProvider),
	}
	stream.RegisterStreamServer(gaz.grpcServer, gaz.streamRegistry)

	return &gaz
}

// Starts the router, once Run is launched, you should no longer add new handlers on the router
func (g Gaz) Run() {
	if viper.IsSet("stream.provider.port") {
		streamPort := viper.GetInt("stream.provider.port")
		Log.Info("Listening for streaming request on port", zap.Int("port", streamPort))
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", streamPort))
		if err != nil {
			panic(err)
		}
		go g.grpcServer.Serve(l)
	}

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

		port := viper.GetInt("http.port")
		Sugar.Infof("Starting http server on port %d", port)
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), g.Router)
		if err != nil {
			Sugar.Errorf("Cannot start HTTP server on :%d, %+v", port, err)
			panic(err)
		}
	}()
}

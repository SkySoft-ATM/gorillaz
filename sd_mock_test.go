package gorillaz

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/skysoft-atm/gorillaz/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"net"
	"testing"
)

const serviceName = "MyServiceName"

type testService struct {
	t *testing.T
}

func (t testService) Send(ctx context.Context, ping *test.Ping) (*test.Pong, error) {
	span := opentracing.SpanFromContext(ctx)
	assert.NotNil(t.t, span)
	return &test.Pong{Name: ping.Name, TraceId: GetTraceId(span)}, nil
}

func TestMockedServiceDiscoveryOfExternalService(t *testing.T) {
	grpcListener := startExternalService(t)
	defer grpcListener.Close()
	port := grpcListener.Addr().(*net.TCPAddr).Port
	sd := ServiceDefinition{
		ServiceName: serviceName,
		Addr:        "127.0.0.1",
		Port:        port,
		Tags:        []string{"dev"}, // default environment
	}
	zipkin := ServiceDefinition{ //necessary since we will enable tracing
		ServiceName: "zipkin",
		Addr:        "127.0.0.1",
		Port:        1234,
		Tags:        []string{"dev"}, // default environment
		Meta:        map[string]string{"url": "fake"},
	}

	g := New(WithServiceName("test"), WithMockedServiceDiscoveryDefinitions([]ServiceDefinition{sd, zipkin}), WithTracingEnabled())

	conn, err := g.GrpcDialService(serviceName, grpc.WithInsecure())
	failIf(t, err)

	cli := test.NewTestServiceClient(conn)
	span, ctx := StartChildSpan(context.Background(), "ping")
	name := "piiing"
	pong, err := cli.Send(ctx, &test.Ping{Name: name})
	failIf(t, err)
	assert.Equal(t, GetTraceId(span), pong.GetTraceId())
	assert.Equal(t, name, pong.GetName())
}

func startExternalService(t *testing.T) net.Listener {
	grpcListener, err := net.Listen("tcp", ":0")
	failIf(t, err)
	server := grpc.NewServer(grpc.UnaryInterceptor(TracingServerInterceptor())) // this is done automatically for the gorillaz gRPC server
	test.RegisterTestServiceServer(server, testService{t: t})
	go server.Serve(grpcListener)
	return grpcListener
}

func failIf(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

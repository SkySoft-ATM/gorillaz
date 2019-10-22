package gorillaz

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
)

func TestVersionInfo(t *testing.T) {
	description := "wonderful_application"
	name := "srv-tasty"
	version := "v0.3.1-rc1"

	// in a normal setup, these values will be set at build time
	ApplicationDescription = description
	ApplicationName = name
	ApplicationVersion = version

	g := New(WithServiceName("tasty-service"))
	<-g.Run()

	url := fmt.Sprintf("http://localhost:%d/info", g.HttpPort())

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("unexpected error, %+v", err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body payload, %+v", err)
	}

	expectedInfo := Info{App: App{
		Description: description,
		Name:        name,
		Version:     version,
	}}

	var info Info
	err = json.Unmarshal(b, &info)
	if err != nil {
		t.Fatalf("failed to unmarshal response into Json, %+v", err)
	}

	if !reflect.DeepEqual(info, expectedInfo) {
		t.Errorf("expected response %+v but got %+v", expectedInfo, info)
	}
}

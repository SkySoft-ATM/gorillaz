package gorillaz

import (
	"encoding/json"
	"go.uber.org/zap"
	"net/http"
)

// set at build time, go build  -ldflags "-X github.com/skysoft-atm/gorillaz.ApplicationVersion=v0.3.2 -X github.com/skysoft-atm/gorillaz.ApplicationName=srv-cheesy-cheese -X github.com/skysoft-atm/gorillaz.ApplicationDescription=bla_pepito"
var (
	ApplicationVersion     string
	ApplicationDescription string
	ApplicationName        string
)

func versionInfoHandler() func(http.ResponseWriter, *http.Request) {
	// generate the info value from ApplicationVersion, ApplicationDescription and ApplicationName
	inf := Info{
		App: App{
			Version:     ApplicationVersion,
			Description: ApplicationDescription,
			Name:        ApplicationName,
		},
	}
	info, err := json.MarshalIndent(inf, "", " ")

	// there is no reason for this to happen, so panic
	if err != nil {
		panic("failed to marshal build information data")
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		_, err := w.Write(info)
		if err != nil {
			Log.Error("failed to write response", zap.Error(err))
		}
	}
}

type Info struct {
	App App `json:"app"`
}

type App struct {
	Version     string `json:"version"`
	Description string `json:"description"`
	Name        string `json:"name"`
}

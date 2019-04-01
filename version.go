package gorillaz

import (
	"fmt"
	"net/http"
)

var BuildVersion string
var GoVersion string

func VersionHTML(w http.ResponseWriter, r *http.Request) {
	body := fmt.Sprintf("<html><body>build version: %s<br>go version: %s", BuildVersion, GoVersion)
	w.Write([]byte(body))
	w.WriteHeader(http.StatusOK)
}

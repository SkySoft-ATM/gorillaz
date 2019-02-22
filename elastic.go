package gorillaz

import (
	"bytes"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"time"
)

func UploadElasticSearchTemplate() {
	elasticServers := viper.GetString("elasticsearch.servers")
	template := viper.GetString("elasticsearch.template.name")
	path := GetConfigPath(nil)

	templateFile := path + "/" + template

	fileContent, err := ioutil.ReadFile(templateFile)
	if err != nil {
		Sugar.Warnf("Failed to Read the File %v Error: %v", templateFile, err)
		return
	}

	Sugar.Infof("Going to upload template file: %v", templateFile)
	client := &http.Client{}
	client.Timeout = time.Second * 15

	uri := elasticServers + "/_template/" + template
	body := bytes.NewBuffer(fileContent)
	req, err := http.NewRequest(http.MethodPut, uri, body)
	if err != nil {
		Sugar.Warnf("http.NewRequest() failed with %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		Sugar.Warnf("client.Do() failed with %v", err)
		return
	}

	defer resp.Body.Close()
	var response []byte
	response, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		Sugar.Warnf("ioutil.ReadAll() failed with %v", err)
		return
	}

	Sugar.Infof("Response status code: %v, text:%v", resp.StatusCode, string(response))
	if resp.StatusCode == 200 {
		Sugar.Infof("Template has been uploaded to ES: %v", string(fileContent))
	} else {
		Sugar.Warn("Template has NOT been uploaded to ES")
	}

}

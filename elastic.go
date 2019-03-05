package gorillaz

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

// UploadElasticSearchTemplate uploads the template file from configFolder/template to Elasticsearch
func UploadElasticSearchTemplate(hostname string, client *http.Client, template string) error {
	//TODO: GetConfigPath of nil probably panic since we try to read a nil map
	configPath := GetConfigPath(nil)

	templateFile := configPath + "/" + template

	fileContent, err := ioutil.ReadFile(templateFile)
	if err != nil {
		Sugar.Errorf("Failed to Read the File %s Error: %+v", templateFile, err)
		return err
	}

	Sugar.Infof("Going to upload template file: %s", templateFile)

	uri := hostname + "/_template/" + template
	body := bytes.NewReader(fileContent)
	req, err := http.NewRequest(http.MethodPut, uri, body)

	if err != nil {
		Sugar.Errorf("http.NewRequest() failed with %+v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		Sugar.Errorf("client.Do() failed with %+v", err)
		return err
	}

	defer resp.Body.Close()
	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		Sugar.Errorf("ioutil.ReadAll() failed with %+v", err)
		return err
	}

	Sugar.Infof("Response status code: %d, text:%+v", resp.StatusCode, string(response))
	if resp.StatusCode == 200 {
		Sugar.Infof("Template has been uploaded to ES: %s", string(fileContent))
		return nil
	}
	Sugar.Errorf("Template has NOT been uploaded to ES")
	return fmt.Errorf("bad response from elastic search, status: %d, response %s", resp.StatusCode, string(response))
}

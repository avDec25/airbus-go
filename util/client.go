package util

import (
	"bitbucket.mynt.myntra.com/plt/airbus-go/constants"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
)

func client() *http.Client {
	return &http.Client{
		Timeout: constants.Timeout,
	}
}

func responseHandler(resp *http.Response) (data interface{}, err error) {
	if Contains(resp.StatusCode, []interface{}{http.StatusOK, http.StatusAccepted, http.StatusNotModified}) {
		var byteBody []byte
		if byteBody, err = ioutil.ReadAll(resp.Body); err == nil {
			err = json.Unmarshal(byteBody, &data)
		}
	} else {
		var byteBody []byte
		if byteBody, err = ioutil.ReadAll(resp.Body); err == nil {
			log.Errorf("Error from Airbus: %s\n", byteBody)
		} else {
			log.Errorf("Error during reading response: %s\n", err)
		}
		err = errors.New(constants.InternalServerError)
	}
	return
}

func HttpGet(url string, header map[string]string) (data interface{}, err error) {
	var resp *http.Response
	var req *http.Request

	if req, err = http.NewRequest("GET", url, nil); err == nil {
		for key, value := range header {
			req.Header.Set(key, value)
		}
		if resp, err = client().Do(req); err == nil {
			defer resp.Body.Close()
			data, err = responseHandler(resp)
		}
	}
	return
}

func HttpPost(url string, header map[string]string, body io.Reader) (data interface{}, err error) {
	var resp *http.Response
	var req *http.Request

	if req, err = http.NewRequest("POST", url, body); err == nil {
		for key, value := range header {
			req.Header.Set(key, value)
		}
		if resp, err = client().Do(req); err == nil {
			defer resp.Body.Close()
			data, err = responseHandler(resp)
		}
	}
	return
}

func HttpPut(url string, header map[string]string, body io.Reader) (data interface{}, err error) {
	var resp *http.Response
	var req *http.Request

	if req, err = http.NewRequest("PUT", url, body); err == nil {
		for key, value := range header {
			req.Header.Set(key, value)
		}
		if resp, err = client().Do(req); err == nil {
			defer resp.Body.Close()
			data, err = responseHandler(resp)
		}
	}
	return
}

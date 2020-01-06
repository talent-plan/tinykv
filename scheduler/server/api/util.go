// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
)

// dialClient used to dail http request.
var dialClient = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives: true,
	},
}

var (
	errNoImplement    = errors.New("no implement")
	errOptionNotExist = func(name string) error { return errors.Errorf("the option %s does not exist", name) }
)

func collectEscapeStringOption(option string, input map[string]interface{}, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		value, err := url.QueryUnescape(v)
		if err != nil {
			return err
		}
		for _, c := range collectors {
			c(value)
		}
		return nil
	}
	return errOptionNotExist(option)
}

func collectStringOption(option string, input map[string]interface{}, collectors ...func(v string)) error {
	if v, ok := input[option].(string); ok {
		for _, c := range collectors {
			c(v)
		}
		return nil
	}
	return errOptionNotExist(option)
}

func readJSON(r io.ReadCloser, data interface{}) error {
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}
	err = json.Unmarshal(b, data)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func postJSON(url string, data []byte, checkOpts ...func(res []byte) bool) error {
	resp, err := dialClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return errors.WithStack(err)
	}
	res, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New(string(res))
	}
	for _, opt := range checkOpts {
		if !opt(res) {
			return errors.New("check failed")
		}
	}
	return nil
}

func doDelete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	res, err := dialClient.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	return nil
}

func doGet(url string) (*http.Response, error) {
	resp, err := dialClient.Get(url)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("http get url %s return code %d", url, resp.StatusCode)
	}
	return resp, nil
}

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
	"io/ioutil"
	"net/http/httptest"

	. "github.com/pingcap/check"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/apiutil"
	"github.com/unrolled/render"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestJsonRespondErrorOk(c *C) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := ioutil.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input map[string]string
	output := map[string]string{"zone": "cn", "host": "local"}
	err := apiutil.ReadJSONRespondError(rd, response, body, &input)
	c.Assert(err, IsNil)
	c.Assert(input["zone"], Equals, output["zone"])
	c.Assert(input["host"], Equals, output["host"])
	result := response.Result()
	c.Assert(result.StatusCode, Equals, 200)
}

func (s *testUtilSuite) TestJsonRespondErrorBadInput(c *C) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	response := httptest.NewRecorder()
	body := ioutil.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\", \"host\":\"local\"}"))
	var input []string
	err := apiutil.ReadJSONRespondError(rd, response, body, &input)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "json: cannot unmarshal object into Go value of type []string")
	result := response.Result()
	c.Assert(result.StatusCode, Equals, 400)

	{
		body := ioutil.NopCloser(bytes.NewBufferString("{\"zone\":\"cn\","))
		var input []string
		err := apiutil.ReadJSONRespondError(rd, response, body, &input)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "unexpected end of JSON input")
		result := response.Result()
		c.Assert(result.StatusCode, Equals, 400)
	}
}

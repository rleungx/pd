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

package apiutil

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"

	"github.com/juju/errors"
)

// DeferClose captures the error returned from closing (if an error occurs).
// This is designed to be used in a defer statement.
func DeferClose(c io.Closer, err *error) {
	if cerr := c.Close(); cerr != nil && *err == nil {
		*err = errors.Trace(cerr)
	}
}

// JSONError lets callers check for just one error type
type JSONError struct {
	Err error
}

func (e JSONError) Error() string {
	return e.Err.Error()
}

func tagJSONError(err error) error {
	switch err.(type) {
	case *json.SyntaxError, *json.UnmarshalTypeError:
		return JSONError{err}
	}
	return err
}

// ReadJSON reads a JSON data from r and then closes it.
// An error due to invalid json will be returned as a JSONError
func ReadJSON(r io.ReadCloser, data interface{}) error {
	var err error
	defer DeferClose(r, &err)
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Trace(err)
	}

	err = json.Unmarshal(b, data)
	if err != nil {
		return tagJSONError(err)
	}

	return err
}

// FieldError connects an error to a particular field
type FieldError struct {
	error
	field string
}

// ParseVarUint wraps strconv.ParseUint with detailed error reporting
func ParseVarUint(vars map[string]string, varName string, base int, bitsize int) (uint64, error) {
	str, ok := vars[varName]
	if !ok {
		return 0, FieldError{field: varName, error: fmt.Errorf("field %s not present", varName)}
	}
	parsed, err := strconv.ParseUint(str, base, bitsize)
	if err == nil {
		return parsed, nil
	}
	return parsed, FieldError{field: varName, error: err}
}

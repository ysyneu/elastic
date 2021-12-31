// Copyright 2012-2018 Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// XPackSqlQueryService stops the watcher service if it is running.
// See https://www.elastic.co/guide/en/elasticsearch/reference/7.0/watcher-api-stop.html.
type XPackSqlQueryService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	filterClauses           []Query
	fetchSize               int
	cursor                  string
	sql                     string
	requestTimeout          string
	pageTimeout             string
	timeZone                string
	fieldMultiValueLeniency bool
}

// NewXPackSqlQueryService creates a new XPackSqlQueryService.
func NewXPackSqlQueryService(client *Client) *XPackSqlQueryService {
	return &XPackSqlQueryService{
		client: client,
	}
}

// Pretty tells Elasticsearch whether to return a formatted JSON response.
func (s *XPackSqlQueryService) Pretty(pretty bool) *XPackSqlQueryService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *XPackSqlQueryService) Human(human bool) *XPackSqlQueryService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *XPackSqlQueryService) ErrorTrace(errorTrace bool) *XPackSqlQueryService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *XPackSqlQueryService) FilterPath(filterPath ...string) *XPackSqlQueryService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *XPackSqlQueryService) Header(name string, value string) *XPackSqlQueryService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *XPackSqlQueryService) Headers(headers http.Header) *XPackSqlQueryService {
	s.headers = headers
	return s
}

// SQL specifies the sql of the request
func (s *XPackSqlQueryService) SQL(sql string) *XPackSqlQueryService {
	s.sql = sql
	return s
}

// SQL specifies the sql of the request
func (s *XPackSqlQueryService) Cursor(cursor string) *XPackSqlQueryService {
	s.cursor = cursor
	return s
}

// Filter specifies the filter of the request
func (s *XPackSqlQueryService) Filter(filters ...Query) *XPackSqlQueryService {
	s.filterClauses = append(s.filterClauses, filters...)
	return s
}

// FetchSize specifies the fetch_size of the request
func (s *XPackSqlQueryService) FetchSize(size int) *XPackSqlQueryService {
	s.fetchSize = size
	return s
}

// RequestTimeout specifies the request_timeout of the request
func (s *XPackSqlQueryService) RequestTimeout(timeout string) *XPackSqlQueryService {
	s.requestTimeout = timeout
	return s
}

// PageTimeout specifies the page_timeout of the request
func (s *XPackSqlQueryService) PageTimeout(timeout string) *XPackSqlQueryService {
	s.pageTimeout = timeout
	return s
}

// TimeZone specifies the sql of the request
func (s *XPackSqlQueryService) TimeZone(zone string) *XPackSqlQueryService {
	s.timeZone = zone
	return s
}

// SetFieldMultiValueLeniency specifies the field_multi_value_leniency of the request
func (s *XPackSqlQueryService) SetFieldMultiValueLeniency(leniency bool) *XPackSqlQueryService {
	s.fieldMultiValueLeniency = leniency
	return s
}

// Source allows the user to set the request body manually without using
// any of the structs and interfaces in Elastic.
func (s *XPackSqlQueryService) Source() (interface{}, error) {
	source := make(map[string]interface{})
	if len(s.cursor) > 0 {
		source["cursor"] = s.cursor
		return source, nil
	}

	if len(s.sql) == 0 && len(s.cursor) == 0 {
		return nil, errors.New("query and cursor must be not both empty")
	}
	source["query"] = s.sql
	if s.fetchSize > 0 {
		source["fetch_size"] = s.fetchSize
	}
	if len(s.pageTimeout) > 0 {
		source["page_timeout"] = s.pageTimeout
	}
	if len(s.requestTimeout) > 0 {
		source["request_timeout"] = s.requestTimeout
	}
	if len(s.timeZone) > 0 {
		source["time_zone"] = s.timeZone
	}
	if s.fieldMultiValueLeniency {
		source["field_multi_value_leniency"] = s.fieldMultiValueLeniency
	}

	// filter
	if len(s.filterClauses) == 1 {
		src, err := s.filterClauses[0].Source()
		if err != nil {
			return nil, err
		}
		source["filter"] = src
	} else if len(s.filterClauses) > 1 {
		var clauses []interface{}
		for _, subQuery := range s.filterClauses {
			src, err := subQuery.Source()
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, src)
		}
		source["filter"] = clauses
	}

	return source, nil
}

// buildURL builds the URL for the operation.
func (s *XPackSqlQueryService) buildURL() (string, url.Values, error) {
	// Build URL path
	path := "/_sql"

	// Add query string parameters
	params := url.Values{}
	if v := s.pretty; v != nil {
		params.Set("pretty", fmt.Sprint(*v))
	}
	if v := s.human; v != nil {
		params.Set("human", fmt.Sprint(*v))
	}
	if v := s.errorTrace; v != nil {
		params.Set("error_trace", fmt.Sprint(*v))
	}
	if len(s.filterPath) > 0 {
		params.Set("filter_path", strings.Join(s.filterPath, ","))
	}

	// Support json format only for now
	params.Set("format", "json")

	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *XPackSqlQueryService) Validate() error {
	return nil
}

// Do executes the operation.
func (s *XPackSqlQueryService) Do(ctx context.Context) (*XPackSqlQueryResponse, error) {
	// Check pre-conditions
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return nil, err
	}

	// Get body for request
	body, err := s.Source()
	if err != nil {
		return nil, err
	}

	// Get HTTP response
	res, err := s.client.PerformRequest(ctx, PerformRequestOptions{
		Method:  "POST",
		Path:    path,
		Params:  params,
		Body:    body,
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(XPackSqlQueryResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// XPackSqlQueryResponse is the response of XPackSqlQueryService.Do.
type XPackSqlQueryResponse struct {
	Columns []*Column       `json:"columns,omitempty"`
	Rows    [][]interface{} `json:"rows,omitempty"`
	Cursor  string          `json:"cursor,omitempty"`
}

type Column struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

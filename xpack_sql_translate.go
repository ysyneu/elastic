// Copyright 2012-2018 Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// XPackSqlTranslateService translate sql into dsl.
// See https://www.elastic.co/guide/en/elasticsearch/reference/7.0/sql-translate.html.
type XPackSqlTranslateService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	filterClauses           []Query
	fetchSize               int
	sql                     string
	requestTimeout          string
	pageTimeout             string
	timeZone                string
	fieldMultiValueLeniency bool
}

// NewXPackSqlTranslateService creates a new XPackSqlTranslateService.
func NewXPackSqlTranslateService(client *Client) *XPackSqlTranslateService {
	return &XPackSqlTranslateService{
		client: client,
	}
}

// Pretty tells Elasticsearch whether to return a formatted JSON response.
func (s *XPackSqlTranslateService) Pretty(pretty bool) *XPackSqlTranslateService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *XPackSqlTranslateService) Human(human bool) *XPackSqlTranslateService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *XPackSqlTranslateService) ErrorTrace(errorTrace bool) *XPackSqlTranslateService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *XPackSqlTranslateService) FilterPath(filterPath ...string) *XPackSqlTranslateService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *XPackSqlTranslateService) Header(name string, value string) *XPackSqlTranslateService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *XPackSqlTranslateService) Headers(headers http.Header) *XPackSqlTranslateService {
	s.headers = headers
	return s
}

// SQL specifies the sql of the request
func (s *XPackSqlTranslateService) SQL(sql string) *XPackSqlTranslateService {
	s.sql = sql
	return s
}

// Filter specifies the filter of the request
func (s *XPackSqlTranslateService) Filter(filters ...Query) *XPackSqlTranslateService {
	s.filterClauses = append(s.filterClauses, filters...)
	return s
}

// FetchSize specifies the fetch_size of the request
func (s *XPackSqlTranslateService) FetchSize(size int) *XPackSqlTranslateService {
	s.fetchSize = size
	return s
}

// RequestTimeout specifies the request_timeout of the request
func (s *XPackSqlTranslateService) RequestTimeout(timeout string) *XPackSqlTranslateService {
	s.requestTimeout = timeout
	return s
}

// PageTimeout specifies the page_timeout of the request
func (s *XPackSqlTranslateService) PageTimeout(timeout string) *XPackSqlTranslateService {
	s.pageTimeout = timeout
	return s
}

// TimeZone specifies the sql of the request
func (s *XPackSqlTranslateService) TimeZone(zone string) *XPackSqlTranslateService {
	s.timeZone = zone
	return s
}

// SetFieldMultiValueLeniency specifies the field_multi_value_leniency of the request
func (s *XPackSqlTranslateService) SetFieldMultiValueLeniency(leniency bool) *XPackSqlTranslateService {
	s.fieldMultiValueLeniency = leniency
	return s
}

// Source allows the user to set the request body manually without using
// any of the structs and interfaces in Elastic.
func (s *XPackSqlTranslateService) Source() (interface{}, error) {
	source := make(map[string]interface{})

	if len(s.sql) == 0 {
		return nil, errors.New("query must be not empty")
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
func (s *XPackSqlTranslateService) buildURL() (string, url.Values, error) {
	// Build URL path
	path := "/_sql/translate"

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
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *XPackSqlTranslateService) Validate() error {
	return nil
}

// Do executes the operation.
func (s *XPackSqlTranslateService) Do(ctx context.Context) (string, error) {
	// Check pre-conditions
	if err := s.Validate(); err != nil {
		return "", err
	}

	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return "", err
	}

	// Get body for request
	body, err := s.Source()
	if err != nil {
		return "", err
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
		return "", err
	}

	// Return operation response
	return string(res.Body), nil
}

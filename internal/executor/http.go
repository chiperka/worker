// Package executor provides test execution implementations.
package executor

import "net/http"

// HTTPResponse holds the response data from an HTTP request.
type HTTPResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

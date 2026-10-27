package instruction

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"chiperka-worker/internal/dag"
	"chiperka-worker/internal/docker"
)

// HTTPRequestParams are the parameters for the http_request instruction.
type HTTPRequestParams struct {
	Target  string            `json:"target"`
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	UUID    string            `json:"uuid,omitempty"`
}

// HTTPRequestHandler executes an HTTP request via curl inside the Docker network.
type HTTPRequestHandler struct{}

const (
	respBodyPath    = "/tmp/chiperka-resp-body"
	respHeadersPath = "/tmp/chiperka-resp-headers"
)

func (h *HTTPRequestHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p HTTPRequestParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid http_request params: %w", err)
	}

	manager := ictx.Manager()
	if manager == nil {
		return nil, fmt.Errorf("http_request: no Docker manager (start_services must run first)")
	}

	method := p.Method
	if method == "" {
		method = "GET"
	}

	// Curl writes response body and headers to files inside the container,
	// only the status code is returned via stdout. The body file is then
	// copied out via the Docker tar stream — bypassing the json-file logging
	// driver, which would otherwise mangle invalid UTF-8 bytes in binary
	// responses.
	curlArgs := []string{
		"-s",
		"-o", respBodyPath,
		"-D", respHeadersPath,
		"-w", "%{http_code}",
		"-X", method,
	}

	if p.UUID != "" {
		curlArgs = append(curlArgs, "-H", fmt.Sprintf("X-Chiperka-Test-Id: %s", p.UUID))
	}

	for key, value := range p.Headers {
		curlArgs = append(curlArgs, "-H", fmt.Sprintf("%s: %s", key, value))
	}

	if p.Body != "" {
		curlArgs = append(curlArgs, "-d", p.Body)
	}

	url := p.Target + p.URL
	curlArgs = append(curlArgs, url)

	ictx.Log("info", "http_request", "", fmt.Sprintf("%s %s%s", method, p.Target, p.URL), "")

	stdout, outFiles, err := manager.RunInNetworkAndCopyOut(ctx, docker.CurlImage(), curlArgs, []string{respBodyPath, respHeadersPath})
	if err != nil {
		ictx.Log("fail", "http_request", "", fmt.Sprintf("Request failed: %v", err), "")
		return nil, fmt.Errorf("request failed: %w\n%s", err, string(stdout))
	}

	statusCode, headers, body, err := buildHTTPResult(stdout, outFiles[respBodyPath], outFiles[respHeadersPath])
	if err != nil {
		return nil, err
	}

	ictx.Log("info", "http_response", "", fmt.Sprintf("%s %s%s → %d", method, p.Target, p.URL, statusCode), "")

	headerMap := make(map[string]interface{})
	for k, v := range headers {
		if len(v) == 1 {
			headerMap[k] = v[0]
		} else {
			iface := make([]interface{}, len(v))
			for i, s := range v {
				iface[i] = s
			}
			headerMap[k] = iface
		}
	}

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"status_code": float64(statusCode),
			"headers":     headerMap,
			"body":        body,
		},
	}, nil
}

// buildHTTPResult assembles the (status, headers, body) triple from the three
// pieces curl produces when invoked with `-w "%{http_code}" -o <body> -D
// <headers>`. body is returned as a Go string — note that DAG step results
// JSON-marshal their data, so true binary bodies (PDF, etc.) still cannot
// round-trip through this handler intact. For binary HTTP responses, prefer
// the CLI snapshot path.
func buildHTTPResult(stdout, bodyFile, headersFile []byte) (int, http.Header, string, error) {
	statusCode, err := strconv.Atoi(strings.TrimSpace(string(stdout)))
	if err != nil {
		return 0, nil, "", fmt.Errorf("failed to parse status code from %q: %w", string(stdout), err)
	}
	return statusCode, parseCurlHeaderDump(headersFile), string(bodyFile), nil
}

// parseCurlHeaderDump parses the headers file produced by `curl -D <file>`.
// The file may contain multiple HTTP response header blocks (e.g. when the
// server sends 100 Continue followed by the real response); only the last
// block is returned.
func parseCurlHeaderDump(data []byte) http.Header {
	headers := make(http.Header)
	if len(data) == 0 {
		return headers
	}

	text := string(data)
	lastHTTPIdx := strings.LastIndex(text, "HTTP/")
	if lastHTTPIdx > 0 {
		text = text[lastHTTPIdx:]
	}

	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "HTTP/") {
			continue
		}
		colonIdx := strings.Index(line, ":")
		if colonIdx <= 0 {
			continue
		}
		name := strings.TrimSpace(line[:colonIdx])
		value := strings.TrimSpace(line[colonIdx+1:])
		headers[name] = append(headers[name], value)
	}
	return headers
}

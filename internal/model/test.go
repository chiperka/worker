// Package model defines core domain types for the test runner.
package model

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// ShellCommand is a []string that can be unmarshaled from either a YAML string or a list.
// When given a string, it splits using shell-like tokenization (respects single/double quotes).
// This matches Docker Compose behavior where command accepts both forms.
type ShellCommand []string

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (s *ShellCommand) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.SequenceNode:
		var list []string
		if err := value.Decode(&list); err != nil {
			return err
		}
		*s = list
		return nil
	case yaml.ScalarNode:
		*s = shellSplit(value.Value)
		return nil
	default:
		return fmt.Errorf("command must be a string or list of strings")
	}
}

// shellSplit splits a string into tokens respecting single and double quotes.
func shellSplit(s string) []string {
	var tokens []string
	var current strings.Builder
	inSingle := false
	inDouble := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '\'' && !inDouble:
			inSingle = !inSingle
		case ch == '"' && !inSingle:
			inDouble = !inDouble
		case ch == ' ' && !inSingle && !inDouble:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// ExecutorType defines the type of test executor.
type ExecutorType string

const (
	ExecutorHTTP ExecutorType = "http"
	ExecutorCLI  ExecutorType = "cli"
)

// Body represents a request body that can be a raw string, a file reference, or multipart form data.
// YAML usage:
//   - Inline string: body: '{"key": "value"}'
//   - File reference: body: { file: ./data/payload.json }
//   - Multipart form: body: { multipart: { field: value, file_field: { file: ./photo.jpg } } }
type Body struct {
	Raw       string
	File      string
	Multipart map[string]MultipartField
}

// MultipartField represents a single field in a multipart form body.
// Can be a simple text value or a file upload.
type MultipartField struct {
	Value    string `yaml:"-"`
	File     string `yaml:"file" json:"file,omitempty"`
	Filename string `yaml:"filename,omitempty" json:"filename,omitempty"`
}

// IsZero returns true if the body is empty (for omitempty support).
func (b Body) IsZero() bool {
	return b.Raw == "" && b.File == "" && len(b.Multipart) == 0
}

// IsFile returns true if the body is a file reference.
func (b Body) IsFile() bool {
	return b.File != ""
}

// IsMultipart returns true if the body is multipart form data.
func (b Body) IsMultipart() bool {
	return len(b.Multipart) > 0
}

// DisplayString returns a human-readable representation of the body for logs and reports.
func (b Body) DisplayString() string {
	if b.Raw != "" {
		return b.Raw
	}
	if b.File != "" {
		return fmt.Sprintf("[file: %s]", b.File)
	}
	if len(b.Multipart) > 0 {
		names := make([]string, 0, len(b.Multipart))
		for name := range b.Multipart {
			names = append(names, name)
		}
		sort.Strings(names)
		return fmt.Sprintf("[multipart: %s]", strings.Join(names, ", "))
	}
	return ""
}

// UnmarshalYAML implements yaml.Unmarshaler for Body.
func (b *Body) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		b.Raw = value.Value
		return nil
	case yaml.MappingNode:
		keys := make(map[string]bool)
		for i := 0; i < len(value.Content)-1; i += 2 {
			keys[value.Content[i].Value] = true
		}
		if keys["file"] && keys["multipart"] {
			return fmt.Errorf("body: cannot specify both 'file' and 'multipart'")
		}
		if keys["file"] {
			var obj struct {
				File string `yaml:"file"`
			}
			if err := value.Decode(&obj); err != nil {
				return err
			}
			b.File = obj.File
			return nil
		}
		if keys["multipart"] {
			var obj struct {
				Multipart map[string]MultipartField `yaml:"multipart"`
			}
			if err := value.Decode(&obj); err != nil {
				return err
			}
			b.Multipart = obj.Multipart
			return nil
		}
		return fmt.Errorf("body: mapping must have 'file' or 'multipart' key")
	default:
		return fmt.Errorf("body must be a string or mapping")
	}
}

// UnmarshalYAML implements yaml.Unmarshaler for MultipartField.
func (f *MultipartField) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		f.Value = value.Value
		return nil
	case yaml.MappingNode:
		type alias MultipartField
		var a alias
		if err := value.Decode(&a); err != nil {
			return err
		}
		*f = MultipartField(a)
		return nil
	default:
		return fmt.Errorf("multipart field must be a string or mapping")
	}
}

// MarshalJSON implements json.Marshaler for Body.
func (b Body) MarshalJSON() ([]byte, error) {
	if b.Raw != "" {
		return json.Marshal(b.Raw)
	}
	if b.File != "" {
		return json.Marshal(map[string]string{"file": b.File})
	}
	if len(b.Multipart) > 0 {
		return json.Marshal(map[string]interface{}{"multipart": b.Multipart})
	}
	return []byte(`""`), nil
}

// UnmarshalJSON implements json.Unmarshaler for Body.
func (b *Body) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		b.Raw = s
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("body must be a string or object")
	}
	if fileData, ok := obj["file"]; ok {
		return json.Unmarshal(fileData, &b.File)
	}
	if mpData, ok := obj["multipart"]; ok {
		return json.Unmarshal(mpData, &b.Multipart)
	}
	return fmt.Errorf("body object must have 'file' or 'multipart' key")
}

// MarshalJSON implements json.Marshaler for MultipartField.
func (f MultipartField) MarshalJSON() ([]byte, error) {
	if f.Value != "" {
		return json.Marshal(f.Value)
	}
	type alias MultipartField
	return json.Marshal(alias(f))
}

// UnmarshalJSON implements json.Unmarshaler for MultipartField.
func (f *MultipartField) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		f.Value = s
		return nil
	}
	type alias MultipartField
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*f = MultipartField(a)
	return nil
}

// HTTPRequest defines the HTTP request configuration.
type HTTPRequest struct {
	Method  string            `yaml:"method" json:"method"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    Body              `yaml:"body,omitempty" json:"body,omitempty"`
}

// CLICommand defines the CLI command configuration.
type CLICommand struct {
	// Service is the name of the service container to execute the command in
	Service string `yaml:"service" json:"service"`
	// Command is the command to execute (passed to sh -c)
	Command string `yaml:"command" json:"command"`
	// WorkingDir is the working directory for command execution (optional)
	WorkingDir string `yaml:"workingDir,omitempty" json:"working_dir,omitempty"`
}

// Execution defines how a test should be executed.
type Execution struct {
	Executor ExecutorType `yaml:"executor" json:"executor"`
	Target   string       `yaml:"target,omitempty" json:"target,omitempty"`
	Request  HTTPRequest  `yaml:"request,omitempty" json:"request,omitempty"`
	CLI      *CLICommand  `yaml:"cli,omitempty" json:"cli,omitempty"`
}

// HealthCheckTest is a string that can be unmarshaled from a YAML string or list.
// Supports Docker Compose test formats:
//   - string: "curl -f http://localhost/" -> used as-is for --health-cmd
//   - ["CMD-SHELL", "curl -f http://localhost/"] -> shell command
//   - ["CMD", "curl", "-f", "http://localhost/"] -> joined with spaces
//   - ["NONE"] -> empty (disable)
type HealthCheckTest string

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (t *HealthCheckTest) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		*t = HealthCheckTest(value.Value)
		return nil
	case yaml.SequenceNode:
		var list []string
		if err := value.Decode(&list); err != nil {
			return err
		}
		if len(list) == 0 {
			return fmt.Errorf("healthcheck test: empty list")
		}
		switch list[0] {
		case "CMD-SHELL":
			if len(list) != 2 {
				return fmt.Errorf("CMD-SHELL expects exactly one argument")
			}
			*t = HealthCheckTest(list[1])
		case "CMD":
			*t = HealthCheckTest(strings.Join(list[1:], " "))
		case "NONE":
			*t = ""
		default:
			*t = HealthCheckTest(strings.Join(list, " "))
		}
		return nil
	default:
		return fmt.Errorf("healthcheck test must be a string or list")
	}
}

// HealthCheck defines how to verify a service is ready.
// Fields map 1:1 to Docker's --health-* flags on docker run.
// Two modes:
//   - "healthcheck: true": wait for the image's built-in HEALTHCHECK (no flags added).
//   - "test" field: healthcheck command (maps to --health-cmd), with optional interval/timeout/retries.
type HealthCheck struct {
	// Test is the healthcheck command (maps to --health-cmd).
	// Accepts string or list (Docker Compose style: CMD-SHELL, CMD, NONE).
	// Empty when using "healthcheck: true" (image's built-in HEALTHCHECK).
	Test HealthCheckTest `yaml:"test,omitempty" json:"test,omitempty"`
	// Docker --health-interval (default: "1s")
	Interval string `yaml:"interval,omitempty" json:"interval,omitempty"`
	// Docker --health-timeout (default: "3s")
	Timeout string `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	// Docker --health-retries (default: 30)
	Retries int `yaml:"retries,omitempty" json:"retries,omitempty"`
	// Docker --health-start-period (default: "0s")
	StartPeriod string `yaml:"startPeriod,omitempty" json:"start_period,omitempty"`
	// Docker --health-start-interval
	StartInterval string `yaml:"startInterval,omitempty" json:"start_interval,omitempty"`
}

// UnmarshalYAML allows HealthCheck to be specified as either a boolean (true) or a mapping.
// "healthcheck: true" means "wait for the image's built-in HEALTHCHECK".
func (hc *HealthCheck) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode && value.Tag == "!!bool" {
		if value.Value == "true" {
			// Empty HealthCheck: no --health-* flags, but waitForHealthy will poll docker inspect
			return nil
		}
		return fmt.Errorf("healthcheck: only 'true' is supported as boolean value")
	}

	// Default struct unmarshaling (alias avoids infinite recursion)
	type healthCheckAlias HealthCheck
	var alias healthCheckAlias
	if err := value.Decode(&alias); err != nil {
		return err
	}
	*hc = HealthCheck(alias)
	return nil
}

// Hook defines an action that runs at a specific point in the test lifecycle.
// Hooks are defined on service templates and inherited by tests using those services.
type Hook struct {
	Slot        string    `yaml:"slot" json:"slot"`                               // "beforeExecution", "afterExecution"
	Priority    int       `yaml:"priority,omitempty" json:"priority,omitempty"`    // lower = earlier, default 50
	CLI         *HookCLI  `yaml:"cli,omitempty" json:"cli,omitempty"`             // CLI command in service container
	Diff        *HookDiff `yaml:"diff,omitempty" json:"diff,omitempty"`           // compute diff between two artifacts
	ServiceName string    `yaml:"-" json:"service_name,omitempty"`                // set during compilation from owning service
}

// HookCLI runs a command inside a service container.
type HookCLI struct {
	Command    string `yaml:"command" json:"command"`
	WorkingDir string `yaml:"workingDir,omitempty" json:"working_dir,omitempty"`
}

// HookDiff computes a unified diff between two artifact files.
type HookDiff struct {
	Source string `yaml:"source" json:"source"` // source artifact name
	Target string `yaml:"target" json:"target"` // target artifact name
	Output string `yaml:"output" json:"output"` // output artifact name
}

// ServiceArtifact defines an external artifact to collect from a service container.
type ServiceArtifact struct {
	Name string `yaml:"name,omitempty" json:"name,omitempty"`
	Path string `yaml:"path" json:"path"`
}

// Service defines a Docker service to start before test execution.
type Service struct {
	// Ref references a service template by name (mutually exclusive with inline definition)
	Ref string `yaml:"ref,omitempty" json:"ref,omitempty"`
	// Name is a unique identifier for this service instance (also used as hostname in network)
	Name string `yaml:"name,omitempty" json:"name,omitempty"`
	// ContainerName is the explicit container name (optional)
	ContainerName string `yaml:"containerName,omitempty" json:"container_name,omitempty"`
	// Image is the Docker image to use
	Image string `yaml:"image,omitempty" json:"image,omitempty"`
	// Command overrides the default command of the image (string or list)
	Command ShellCommand `yaml:"command,omitempty" json:"command,omitempty"`
	// WorkingDir is the working directory inside the container
	WorkingDir string `yaml:"workingDir,omitempty" json:"working_dir,omitempty"`
	// Environment variables
	Environment map[string]string `yaml:"environment,omitempty" json:"environment,omitempty"`
	// Health check configuration
	HealthCheck *HealthCheck `yaml:"healthcheck,omitempty" json:"healthcheck,omitempty"`
	// Artifacts to collect from the container after test execution
	Artifacts []ServiceArtifact `yaml:"artifacts,omitempty" json:"artifacts,omitempty"`
	// Weight represents the resource cost of running this service (default 1)
	Weight int `yaml:"weight,omitempty" json:"weight,omitempty"`
	// Hooks inherited from service template
	Hooks []Hook `yaml:"-" json:"hooks,omitempty"`
}

// ServiceTemplate defines a reusable service configuration.
type ServiceTemplate struct {
	// Type must be "service" for service templates
	Type string `yaml:"type"`
	// Name is the unique identifier for this template
	Name string `yaml:"name"`
	// Image is the Docker image to use
	Image string `yaml:"image,omitempty"`
	// Command overrides the default command of the image (string or list)
	Command ShellCommand `yaml:"command,omitempty"`
	// WorkingDir is the working directory inside the container
	WorkingDir string `yaml:"workingDir,omitempty"`
	// Environment variables
	Environment map[string]string `yaml:"environment,omitempty"`
	// Health check configuration
	HealthCheck *HealthCheck `yaml:"healthcheck,omitempty"`
	// Artifacts to collect from the container after test execution
	Artifacts []ServiceArtifact `yaml:"artifacts,omitempty"`
	// Weight represents the resource cost of running this service (default 1)
	Weight int `yaml:"weight,omitempty"`
	// Hooks define actions at specific points in the test lifecycle
	Hooks []Hook `yaml:"hooks,omitempty"`
	// FilePath stores the source file path (not from YAML, set by parser)
	FilePath string `yaml:"-"`
}

// ServiceTemplateCollection holds all discovered service templates.
type ServiceTemplateCollection struct {
	Templates map[string]*ServiceTemplate
}

// NewServiceTemplateCollection creates an empty service template collection.
func NewServiceTemplateCollection() *ServiceTemplateCollection {
	return &ServiceTemplateCollection{
		Templates: make(map[string]*ServiceTemplate),
	}
}

// AddTemplate adds a template to the collection.
func (c *ServiceTemplateCollection) AddTemplate(template *ServiceTemplate) {
	c.Templates[template.Name] = template
}

// GetTemplate returns a template by name, or nil if not found.
func (c *ServiceTemplateCollection) GetTemplate(name string) *ServiceTemplate {
	return c.Templates[name]
}

// HasTemplates returns true if there are any templates in the collection.
func (c *ServiceTemplateCollection) HasTemplates() bool {
	return len(c.Templates) > 0
}

// ResolveService resolves a service reference to a full service definition.
// If the service has a Ref, it merges the template with any overrides.
// Returns error if the referenced template doesn't exist.
func (c *ServiceTemplateCollection) ResolveService(svc Service) (Service, error) {
	if svc.Ref == "" {
		// No reference, return as-is
		return svc, nil
	}

	template := c.GetTemplate(svc.Ref)
	if template == nil {
		return svc, fmt.Errorf("service template '%s' not found", svc.Ref)
	}

	// Start with template values
	resolved := Service{
		Name:        svc.Ref, // Default name to ref name
		Image:       template.Image,
		Command:     append(ShellCommand{}, template.Command...),
		WorkingDir:  template.WorkingDir,
		HealthCheck: template.HealthCheck,
		Artifacts:   append([]ServiceArtifact{}, template.Artifacts...),
		Weight:      template.Weight,
		Hooks:       append([]Hook{}, template.Hooks...),
	}

	// Copy environment from template
	if template.Environment != nil {
		resolved.Environment = make(map[string]string)
		for k, v := range template.Environment {
			resolved.Environment[k] = v
		}
	}

	// Apply overrides from the service definition
	if svc.Name != "" {
		resolved.Name = svc.Name
	}
	if svc.ContainerName != "" {
		resolved.ContainerName = svc.ContainerName
	}
	if svc.Image != "" {
		resolved.Image = svc.Image
	}
	if len(svc.Command) > 0 {
		resolved.Command = svc.Command
	}
	if svc.WorkingDir != "" {
		resolved.WorkingDir = svc.WorkingDir
	}
	if svc.HealthCheck != nil {
		resolved.HealthCheck = svc.HealthCheck
	}
	if svc.Weight > 0 {
		resolved.Weight = svc.Weight
	}

	// Merge environment (override wins)
	if svc.Environment != nil {
		if resolved.Environment == nil {
			resolved.Environment = make(map[string]string)
		}
		for k, v := range svc.Environment {
			resolved.Environment[k] = v
		}
	}

	// Append service-level artifacts after template artifacts
	if len(svc.Artifacts) > 0 {
		resolved.Artifacts = append(resolved.Artifacts, svc.Artifacts...)
	}

	return resolved, nil
}

// HeaderMatcher checks a single response header value.
type HeaderMatcher struct {
	Equals   string `yaml:"equals,omitempty" json:"equals,omitempty"`
	Contains string `yaml:"contains,omitempty" json:"contains,omitempty"`
	Exists   *bool  `yaml:"exists,omitempty" json:"exists,omitempty"`
}

// JsonPathCheck checks a single JSONPath expression against the response body.
type JsonPathCheck struct {
	Path     string `yaml:"path" json:"path"`
	Expected string `yaml:"expected" json:"expected"`
}

// ResponseBodyAssertion checks properties of the HTTP response body.
type ResponseBodyAssertion struct {
	JsonPath []JsonPathCheck `yaml:"jsonPath,omitempty" json:"json_path,omitempty"`
	Contains string          `yaml:"contains,omitempty" json:"contains,omitempty"`
	Equals   string          `yaml:"equals,omitempty" json:"equals,omitempty"`
	MinSize  *int64          `yaml:"minSize,omitempty" json:"min_size,omitempty"`
	Snapshot string          `yaml:"snapshot,omitempty" json:"snapshot,omitempty"`
}

// ResponseTimeAssertion checks HTTP response timing.
type ResponseTimeAssertion struct {
	MaxMs int `yaml:"maxMs" json:"max_ms"`
}

// ResponseAssertion groups all HTTP response checks.
type ResponseAssertion struct {
	StatusCode *int                     `yaml:"statusCode,omitempty" json:"status_code,omitempty"`
	Headers    map[string]HeaderMatcher `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body       *ResponseBodyAssertion   `yaml:"body,omitempty" json:"body,omitempty"`
	Time       *ResponseTimeAssertion   `yaml:"time,omitempty" json:"time,omitempty"`
}

// CLIOutputAssertion checks stdout or stderr content.
type CLIOutputAssertion struct {
	Contains string `yaml:"contains,omitempty" json:"contains,omitempty"`
	Equals   string `yaml:"equals,omitempty" json:"equals,omitempty"`
	Snapshot string `yaml:"snapshot,omitempty" json:"snapshot,omitempty"`
}

// CLIAssertion groups all CLI command checks.
type CLIAssertion struct {
	ExitCode *int                `yaml:"exitCode,omitempty" json:"exit_code,omitempty"`
	Stdout   *CLIOutputAssertion `yaml:"stdout,omitempty" json:"stdout,omitempty"`
	Stderr   *CLIOutputAssertion `yaml:"stderr,omitempty" json:"stderr,omitempty"`
}

// ArtifactAssertion checks properties of a collected artifact (service logs, files extracted from containers).
type ArtifactAssertion struct {
	Name     string `yaml:"name" json:"name"`
	Exists   *bool  `yaml:"exists,omitempty" json:"exists,omitempty"`
	MinSize  *int64 `yaml:"minSize,omitempty" json:"min_size,omitempty"`
	MaxSize  *int64 `yaml:"maxSize,omitempty" json:"max_size,omitempty"`
	Snapshot string `yaml:"snapshot,omitempty" json:"snapshot,omitempty"`
}

// Assertion defines what to verify after test execution.
type Assertion struct {
	Response *ResponseAssertion `yaml:"response,omitempty" json:"response,omitempty"`
	CLI      *CLIAssertion      `yaml:"cli,omitempty" json:"cli,omitempty"`
	Artifact *ArtifactAssertion `yaml:"artifact,omitempty" json:"artifact,omitempty"`
}

// SetupHTTP defines an HTTP request for setup.
type SetupHTTP struct {
	Target  string      `yaml:"target" json:"target"`
	Request HTTPRequest `yaml:"request" json:"request"`
}

// SetupInstruction defines a single setup step that runs after healthchecks but before execution.
type SetupInstruction struct {
	// HTTP request to execute (mutually exclusive with CLI)
	HTTP *SetupHTTP `yaml:"http,omitempty" json:"http,omitempty"`
	// CLI command to execute (mutually exclusive with HTTP)
	CLI *CLICommand `yaml:"cli,omitempty" json:"cli,omitempty"`
}

// Test represents a single test case loaded from a chiperka.yaml file.
type Test struct {
	Name        string             `yaml:"name" json:"name"`
	Description string             `yaml:"description,omitempty" json:"description,omitempty"`
	Tags        []string           `yaml:"tags,omitempty" json:"tags,omitempty"`
	Skipped     bool               `yaml:"skipped,omitempty" json:"skipped,omitempty"`
	Services    []Service          `yaml:"services,omitempty" json:"services,omitempty"`
	Setup       []SetupInstruction `yaml:"setup,omitempty" json:"setup,omitempty"`
	Execution   Execution          `yaml:"execution" json:"execution"`
	Assertions  []Assertion        `yaml:"assertions" json:"assertions"`
	Teardown    []SetupInstruction `yaml:"teardown,omitempty" json:"teardown,omitempty"`
}

// Weight returns the total weight of all services in this test.
// Each service defaults to weight 1 if not specified.
func (t Test) Weight() int {
	w := 0
	for _, svc := range t.Services {
		if svc.Weight > 0 {
			w += svc.Weight
		} else {
			w += 1
		}
	}
	if w == 0 {
		w = 1
	}
	return w
}

// ContainerCount returns the number of Docker containers this test will run.
func (t Test) ContainerCount() int {
	if len(t.Services) == 0 {
		return 1
	}
	return len(t.Services)
}

// CollectHooks gathers all hooks from test services for a given slot, sorted by priority.
func (t *Test) CollectHooks(slot string) []Hook {
	var hooks []Hook
	for _, svc := range t.Services {
		for _, h := range svc.Hooks {
			if h.Slot == slot {
				hook := h
				// Set service name on CLI hooks so the DAG knows which container to exec in
				if hook.CLI != nil && hook.CLI.Command != "" {
					hook.ServiceName = svc.Name
				}
				hooks = append(hooks, hook)
			}
		}
	}
	// Sort by priority (lower first, default 50)
	sort.Slice(hooks, func(i, j int) bool {
		pi, pj := hooks[i].Priority, hooks[j].Priority
		if pi == 0 { pi = 50 }
		if pj == 0 { pj = 50 }
		return pi < pj
	})
	return hooks
}

// Suite represents a collection of tests from a single chiperka.yaml file.
type Suite struct {
	Name  string `yaml:"name" json:"name"`
	Tests []Test `yaml:"tests" json:"tests"`
	// FilePath stores the source file path (not from YAML, set by parser).
	FilePath string `yaml:"-" json:"file_path,omitempty"`
}

// TestCollection holds all discovered test suites.
type TestCollection struct {
	Suites []Suite
}

// NewTestCollection creates an empty test collection.
func NewTestCollection() *TestCollection {
	return &TestCollection{
		Suites: make([]Suite, 0),
	}
}

// AddSuite appends a suite to the collection.
func (c *TestCollection) AddSuite(suite Suite) {
	c.Suites = append(c.Suites, suite)
}

// TotalTests returns the total number of tests across all suites.
func (c *TestCollection) TotalTests() int {
	total := 0
	for _, suite := range c.Suites {
		total += len(suite.Tests)
	}
	return total
}

// FilterByTags returns a new TestCollection containing only tests that match any of the given tags.
// If tags is empty, returns a copy of the original collection (no filtering).
func (c *TestCollection) FilterByTags(tags []string) *TestCollection {
	if len(tags) == 0 {
		return c
	}

	tagSet := make(map[string]bool)
	for _, tag := range tags {
		tagSet[tag] = true
	}

	filtered := NewTestCollection()
	for _, suite := range c.Suites {
		var matchingTests []Test
		for _, test := range suite.Tests {
			if testMatchesTags(test, tagSet) {
				matchingTests = append(matchingTests, test)
			}
		}
		if len(matchingTests) > 0 {
			filteredSuite := Suite{
				Name:     suite.Name,
				Tests:    matchingTests,
				FilePath: suite.FilePath,
			}
			filtered.AddSuite(filteredSuite)
		}
	}
	return filtered
}

// testMatchesTags returns true if the test has at least one tag from the tagSet.
func testMatchesTags(test Test, tagSet map[string]bool) bool {
	for _, tag := range test.Tags {
		if tagSet[tag] {
			return true
		}
	}
	return false
}

// FilterByName returns a new TestCollection containing only tests whose names match the pattern.
// The pattern supports simple glob matching (* for any characters).
// If pattern is empty, returns the original collection (no filtering).
func (c *TestCollection) FilterByName(pattern string) *TestCollection {
	if pattern == "" {
		return c
	}

	filtered := NewTestCollection()
	for _, suite := range c.Suites {
		var matchingTests []Test
		for _, test := range suite.Tests {
			if matchesPattern(test.Name, pattern) {
				matchingTests = append(matchingTests, test)
			}
		}
		if len(matchingTests) > 0 {
			filteredSuite := Suite{
				Name:     suite.Name,
				Tests:    matchingTests,
				FilePath: suite.FilePath,
			}
			filtered.AddSuite(filteredSuite)
		}
	}
	return filtered
}

// matchesPattern checks if name matches the glob pattern.
// Supports * as wildcard for any characters.
func matchesPattern(name, pattern string) bool {
	// Simple glob matching - convert to case-insensitive contains/prefix/suffix
	pattern = strings.ToLower(pattern)
	name = strings.ToLower(name)

	// Handle common patterns
	if pattern == "*" {
		return true
	}

	// No wildcards - exact substring match
	if !strings.Contains(pattern, "*") {
		return strings.Contains(name, pattern)
	}

	// *suffix - ends with
	if strings.HasPrefix(pattern, "*") && !strings.Contains(pattern[1:], "*") {
		return strings.HasSuffix(name, pattern[1:])
	}

	// prefix* - starts with
	if strings.HasSuffix(pattern, "*") && !strings.Contains(pattern[:len(pattern)-1], "*") {
		return strings.HasPrefix(name, pattern[:len(pattern)-1])
	}

	// *middle* - contains
	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
		middle := pattern[1 : len(pattern)-1]
		if !strings.Contains(middle, "*") {
			return strings.Contains(name, middle)
		}
	}

	// Complex pattern - split by * and check parts in order
	parts := strings.Split(pattern, "*")
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(name[pos:], part)
		if idx == -1 {
			return false
		}
		// First part must be at start if pattern doesn't start with *
		if i == 0 && !strings.HasPrefix(pattern, "*") && idx != 0 {
			return false
		}
		pos += idx + len(part)
	}
	// Last part must be at end if pattern doesn't end with *
	if !strings.HasSuffix(pattern, "*") && len(parts) > 0 {
		lastPart := parts[len(parts)-1]
		if lastPart != "" && !strings.HasSuffix(name, lastPart) {
			return false
		}
	}
	return true
}

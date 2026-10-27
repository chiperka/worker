package model

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// --- ShellCommand ---

func TestShellCommand_UnmarshalYAML_String(t *testing.T) {
	input := `command: "echo hello world"`
	var out struct {
		Command ShellCommand `yaml:"command"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Command) != 3 || out.Command[0] != "echo" || out.Command[1] != "hello" || out.Command[2] != "world" {
		t.Fatalf("expected [echo hello world], got %v", out.Command)
	}
}

func TestShellCommand_UnmarshalYAML_List(t *testing.T) {
	input := "command:\n  - echo\n  - hello world"
	var out struct {
		Command ShellCommand `yaml:"command"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Command) != 2 || out.Command[0] != "echo" || out.Command[1] != "hello world" {
		t.Fatalf("expected [echo, hello world], got %v", out.Command)
	}
}

func TestShellCommand_UnmarshalYAML_QuotedString(t *testing.T) {
	input := `command: "echo 'hello world' \"foo bar\""`
	var out struct {
		Command ShellCommand `yaml:"command"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	expected := []string{"echo", "hello world", "foo bar"}
	if len(out.Command) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, out.Command)
	}
	for i, v := range expected {
		if out.Command[i] != v {
			t.Fatalf("index %d: expected %q, got %q", i, v, out.Command[i])
		}
	}
}

func TestShellCommand_UnmarshalYAML_InvalidType(t *testing.T) {
	input := "command:\n  key: value"
	var out struct {
		Command ShellCommand `yaml:"command"`
	}
	err := yaml.Unmarshal([]byte(input), &out)
	if err == nil {
		t.Fatal("expected error for mapping node")
	}
}

// --- Body ---

func TestBody_UnmarshalYAML_RawString(t *testing.T) {
	input := `body: '{"key": "value"}'`
	var out struct {
		Body Body `yaml:"body"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if out.Body.Raw != `{"key": "value"}` {
		t.Fatalf("expected raw body, got %q", out.Body.Raw)
	}
	if out.Body.IsZero() || out.Body.IsFile() || out.Body.IsMultipart() {
		t.Fatal("wrong type flags")
	}
}

func TestBody_UnmarshalYAML_File(t *testing.T) {
	input := "body:\n  file: ./data/payload.json"
	var out struct {
		Body Body `yaml:"body"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if out.Body.File != "./data/payload.json" {
		t.Fatalf("expected file ref, got %q", out.Body.File)
	}
	if !out.Body.IsFile() || out.Body.IsMultipart() {
		t.Fatal("wrong type flags")
	}
}

func TestBody_UnmarshalYAML_Multipart(t *testing.T) {
	input := "body:\n  multipart:\n    name: test\n    avatar:\n      file: ./photo.jpg\n      filename: photo.jpg"
	var out struct {
		Body Body `yaml:"body"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Body.IsMultipart() {
		t.Fatal("expected multipart")
	}
	if out.Body.Multipart["name"].Value != "test" {
		t.Fatalf("expected text field 'test', got %q", out.Body.Multipart["name"].Value)
	}
	if out.Body.Multipart["avatar"].File != "./photo.jpg" {
		t.Fatalf("expected file field, got %+v", out.Body.Multipart["avatar"])
	}
	if out.Body.Multipart["avatar"].Filename != "photo.jpg" {
		t.Fatalf("expected filename, got %q", out.Body.Multipart["avatar"].Filename)
	}
}

func TestBody_UnmarshalYAML_ErrorBothFileAndMultipart(t *testing.T) {
	input := "body:\n  file: ./f.json\n  multipart:\n    x: y"
	var out struct {
		Body Body `yaml:"body"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err == nil {
		t.Fatal("expected error for both file and multipart")
	}
}

func TestBody_UnmarshalYAML_ErrorNoKey(t *testing.T) {
	input := "body:\n  unknown: value"
	var out struct {
		Body Body `yaml:"body"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err == nil {
		t.Fatal("expected error for missing file/multipart key")
	}
}

func TestBody_IsZero(t *testing.T) {
	b := Body{}
	if !b.IsZero() {
		t.Fatal("empty body should be zero")
	}
}

func TestBody_DisplayString(t *testing.T) {
	tests := []struct {
		name string
		body Body
		want string
	}{
		{"raw", Body{Raw: "hello"}, "hello"},
		{"file", Body{File: "./f.json"}, "[file: ./f.json]"},
		{"multipart", Body{Multipart: map[string]MultipartField{"a": {Value: "1"}, "b": {Value: "2"}}}, "[multipart: a, b]"},
		{"empty", Body{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.body.DisplayString()
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

// --- HealthCheckTest ---

func TestHealthCheckTest_UnmarshalYAML_String(t *testing.T) {
	input := `test: "curl -f http://localhost/"`
	var out struct {
		Test HealthCheckTest `yaml:"test"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Test) != "curl -f http://localhost/" {
		t.Fatalf("got %q", out.Test)
	}
}

func TestHealthCheckTest_UnmarshalYAML_CMDShell(t *testing.T) {
	input := "test:\n  - CMD-SHELL\n  - curl -f http://localhost/"
	var out struct {
		Test HealthCheckTest `yaml:"test"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Test) != "curl -f http://localhost/" {
		t.Fatalf("got %q", out.Test)
	}
}

func TestHealthCheckTest_UnmarshalYAML_CMD(t *testing.T) {
	input := "test:\n  - CMD\n  - curl\n  - -f\n  - http://localhost/"
	var out struct {
		Test HealthCheckTest `yaml:"test"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Test) != "curl -f http://localhost/" {
		t.Fatalf("got %q", out.Test)
	}
}

func TestHealthCheckTest_UnmarshalYAML_NONE(t *testing.T) {
	input := "test:\n  - NONE"
	var out struct {
		Test HealthCheckTest `yaml:"test"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Test) != "" {
		t.Fatalf("expected empty, got %q", out.Test)
	}
}

func TestHealthCheckTest_UnmarshalYAML_PlainList(t *testing.T) {
	input := "test:\n  - curl\n  - -f\n  - http://localhost/"
	var out struct {
		Test HealthCheckTest `yaml:"test"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Test) != "curl -f http://localhost/" {
		t.Fatalf("got %q", out.Test)
	}
}

func TestHealthCheckTest_UnmarshalYAML_EmptyList(t *testing.T) {
	input := "test: []"
	var out struct {
		Test HealthCheckTest `yaml:"test"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err == nil {
		t.Fatal("expected error for empty list")
	}
}

func TestHealthCheckTest_UnmarshalYAML_CMDShellWrongArgs(t *testing.T) {
	input := "test:\n  - CMD-SHELL\n  - a\n  - b"
	var out struct {
		Test HealthCheckTest `yaml:"test"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err == nil {
		t.Fatal("expected error for CMD-SHELL with multiple args")
	}
}

// --- HealthCheck boolean unmarshal ---

func TestHealthCheck_UnmarshalYAML_True(t *testing.T) {
	input := "healthcheck: true"
	var out struct {
		HealthCheck *HealthCheck `yaml:"healthcheck"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if out.HealthCheck == nil {
		t.Fatal("expected non-nil healthcheck")
	}
	if string(out.HealthCheck.Test) != "" {
		t.Fatalf("expected empty test, got %q", out.HealthCheck.Test)
	}
}

func TestHealthCheck_UnmarshalYAML_False(t *testing.T) {
	input := "healthcheck: false"
	var out struct {
		HealthCheck *HealthCheck `yaml:"healthcheck"`
	}
	err := yaml.Unmarshal([]byte(input), &out)
	if err == nil {
		t.Fatal("expected error for false boolean")
	}
}

func TestHealthCheck_UnmarshalYAML_Mapping(t *testing.T) {
	input := "healthcheck:\n  test: \"curl localhost\"\n  interval: 2s\n  retries: 5"
	var out struct {
		HealthCheck *HealthCheck `yaml:"healthcheck"`
	}
	if err := yaml.Unmarshal([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
	if string(out.HealthCheck.Test) != "curl localhost" {
		t.Fatalf("got test %q", out.HealthCheck.Test)
	}
	if out.HealthCheck.Interval != "2s" {
		t.Fatalf("got interval %q", out.HealthCheck.Interval)
	}
	if out.HealthCheck.Retries != 5 {
		t.Fatalf("got retries %d", out.HealthCheck.Retries)
	}
}

// --- TestCollection.FilterByTags ---

func makeTestCollection() *TestCollection {
	tc := NewTestCollection()
	tc.AddSuite(Suite{
		Name:     "suite1",
		FilePath: "s1.yaml",
		Tests: []Test{
			{Name: "test-api", Tags: []string{"api", "smoke"}},
			{Name: "test-db", Tags: []string{"db"}},
			{Name: "test-no-tags"},
		},
	})
	tc.AddSuite(Suite{
		Name:     "suite2",
		FilePath: "s2.yaml",
		Tests: []Test{
			{Name: "test-ui", Tags: []string{"ui"}},
		},
	})
	return tc
}

func TestTestCollection_TotalTests(t *testing.T) {
	tc := makeTestCollection()
	if tc.TotalTests() != 4 {
		t.Fatalf("expected 4, got %d", tc.TotalTests())
	}
}

func TestFilterByTags_EmptyTags(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByTags(nil)
	if result.TotalTests() != 4 {
		t.Fatalf("expected no filtering, got %d tests", result.TotalTests())
	}
}

func TestFilterByTags_SingleTag(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByTags([]string{"api"})
	if result.TotalTests() != 1 {
		t.Fatalf("expected 1, got %d", result.TotalTests())
	}
	if result.Suites[0].Tests[0].Name != "test-api" {
		t.Fatalf("wrong test: %s", result.Suites[0].Tests[0].Name)
	}
}

func TestFilterByTags_MultipleTags(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByTags([]string{"db", "ui"})
	if result.TotalTests() != 2 {
		t.Fatalf("expected 2, got %d", result.TotalTests())
	}
}

func TestFilterByTags_NoMatch(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByTags([]string{"nonexistent"})
	if result.TotalTests() != 0 {
		t.Fatalf("expected 0, got %d", result.TotalTests())
	}
}

func TestFilterByTags_PreservesSuiteMetadata(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByTags([]string{"api"})
	if len(result.Suites) != 1 {
		t.Fatalf("expected 1 suite, got %d", len(result.Suites))
	}
	if result.Suites[0].Name != "suite1" || result.Suites[0].FilePath != "s1.yaml" {
		t.Fatalf("suite metadata not preserved")
	}
}

// --- TestCollection.FilterByName ---

func TestFilterByName_Empty(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("")
	if result.TotalTests() != 4 {
		t.Fatalf("expected no filtering, got %d", result.TotalTests())
	}
}

func TestFilterByName_Wildcard(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("*")
	if result.TotalTests() != 4 {
		t.Fatalf("expected all, got %d", result.TotalTests())
	}
}

func TestFilterByName_ExactSubstring(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("test-api")
	if result.TotalTests() != 1 {
		t.Fatalf("expected 1, got %d", result.TotalTests())
	}
}

func TestFilterByName_Prefix(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("test-a*")
	if result.TotalTests() != 1 {
		t.Fatalf("expected 1, got %d", result.TotalTests())
	}
}

func TestFilterByName_Suffix(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("*-db")
	if result.TotalTests() != 1 {
		t.Fatalf("expected 1, got %d", result.TotalTests())
	}
}

func TestFilterByName_Contains(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("*-no-*")
	if result.TotalTests() != 1 {
		t.Fatalf("expected 1, got %d", result.TotalTests())
	}
}

func TestFilterByName_CaseInsensitive(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("TEST-API")
	if result.TotalTests() != 1 {
		t.Fatalf("expected 1, got %d", result.TotalTests())
	}
}

func TestFilterByName_ComplexGlob(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("test*api")
	if result.TotalTests() != 1 {
		t.Fatalf("expected 1, got %d", result.TotalTests())
	}
}

func TestFilterByName_NoMatch(t *testing.T) {
	tc := makeTestCollection()
	result := tc.FilterByName("nonexistent*")
	if result.TotalTests() != 0 {
		t.Fatalf("expected 0, got %d", result.TotalTests())
	}
}

// --- ServiceTemplate resolution ---

func TestResolveService_NoRef(t *testing.T) {
	coll := NewServiceTemplateCollection()
	svc := Service{Name: "mydb", Image: "postgres:16"}
	resolved, err := coll.ResolveService(svc)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "mydb" || resolved.Image != "postgres:16" {
		t.Fatalf("unexpected: %+v", resolved)
	}
}

func TestResolveService_WithRef(t *testing.T) {
	coll := NewServiceTemplateCollection()
	coll.AddTemplate(&ServiceTemplate{
		Name:        "postgres",
		Image:       "postgres:16",
		Environment: map[string]string{"POSTGRES_DB": "test"},
		Command:     ShellCommand{"postgres", "-c", "log_statement=all"},
	})

	svc := Service{Ref: "postgres"}
	resolved, err := coll.ResolveService(svc)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "postgres" {
		t.Fatalf("expected name=postgres, got %q", resolved.Name)
	}
	if resolved.Image != "postgres:16" {
		t.Fatalf("expected postgres:16, got %q", resolved.Image)
	}
	if resolved.Environment["POSTGRES_DB"] != "test" {
		t.Fatal("environment not inherited")
	}
	if len(resolved.Command) != 3 {
		t.Fatalf("command not inherited: %v", resolved.Command)
	}
}

func TestResolveService_WithOverrides(t *testing.T) {
	coll := NewServiceTemplateCollection()
	coll.AddTemplate(&ServiceTemplate{
		Name:        "postgres",
		Image:       "postgres:16",
		Environment: map[string]string{"POSTGRES_DB": "test", "POSTGRES_USER": "admin"},
	})

	svc := Service{
		Ref:         "postgres",
		Name:        "custom-pg",
		Image:       "postgres:17",
		Environment: map[string]string{"POSTGRES_DB": "override"},
	}
	resolved, err := coll.ResolveService(svc)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Name != "custom-pg" {
		t.Fatalf("name override failed: %q", resolved.Name)
	}
	if resolved.Image != "postgres:17" {
		t.Fatalf("image override failed: %q", resolved.Image)
	}
	if resolved.Environment["POSTGRES_DB"] != "override" {
		t.Fatalf("env override failed: %q", resolved.Environment["POSTGRES_DB"])
	}
	if resolved.Environment["POSTGRES_USER"] != "admin" {
		t.Fatal("env merge failed: POSTGRES_USER missing")
	}
}

func TestResolveService_MissingTemplate(t *testing.T) {
	coll := NewServiceTemplateCollection()
	svc := Service{Ref: "missing"}
	_, err := coll.ResolveService(svc)
	if err == nil {
		t.Fatal("expected error for missing template")
	}
}

func TestResolveService_ArtifactsMerged(t *testing.T) {
	coll := NewServiceTemplateCollection()
	coll.AddTemplate(&ServiceTemplate{
		Name:      "app",
		Image:     "app:latest",
		Artifacts: []ServiceArtifact{{Name: "logs", Path: "/var/log"}},
	})

	svc := Service{
		Ref:       "app",
		Artifacts: []ServiceArtifact{{Name: "data", Path: "/data"}},
	}
	resolved, err := coll.ResolveService(svc)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved.Artifacts) != 2 {
		t.Fatalf("expected 2 artifacts, got %d", len(resolved.Artifacts))
	}
}

func TestServiceTemplateCollection_HasTemplates(t *testing.T) {
	coll := NewServiceTemplateCollection()
	if coll.HasTemplates() {
		t.Fatal("should be empty")
	}
	coll.AddTemplate(&ServiceTemplate{Name: "x"})
	if !coll.HasTemplates() {
		t.Fatal("should have templates")
	}
}

// --- Body JSON round-trip ---

func TestBody_JSONRoundTrip_Raw(t *testing.T) {
	b := Body{Raw: "hello"}
	data, err := b.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var b2 Body
	if err := b2.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if b2.Raw != "hello" {
		t.Fatalf("got %q", b2.Raw)
	}
}

func TestBody_JSONRoundTrip_File(t *testing.T) {
	b := Body{File: "./f.json"}
	data, err := b.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var b2 Body
	if err := b2.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if b2.File != "./f.json" {
		t.Fatalf("got %q", b2.File)
	}
}

func TestBody_JSONRoundTrip_Multipart(t *testing.T) {
	b := Body{Multipart: map[string]MultipartField{
		"name": {Value: "test"},
		"pic":  {File: "./pic.png"},
	}}
	data, err := b.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var b2 Body
	if err := b2.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if b2.Multipart["name"].Value != "test" {
		t.Fatalf("got %+v", b2.Multipart["name"])
	}
	if b2.Multipart["pic"].File != "./pic.png" {
		t.Fatalf("got %+v", b2.Multipart["pic"])
	}
}

func TestBody_JSON_Empty(t *testing.T) {
	b := Body{}
	data, err := b.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `""` {
		t.Fatalf("expected empty string JSON, got %s", data)
	}
}

func TestBody_UnmarshalJSON_InvalidType(t *testing.T) {
	var b Body
	err := b.UnmarshalJSON([]byte(`{"unknown": "value"}`))
	if err == nil {
		t.Fatal("expected error")
	}
}

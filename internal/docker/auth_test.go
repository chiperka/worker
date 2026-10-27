package docker

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/docker/docker/api/types/registry"
)

// --------------------------------------------------------------------------
// registryHost tests
// --------------------------------------------------------------------------

func TestRegistryHost_DockerHubShortName(t *testing.T) {
	// Short names like "nginx" resolve to docker.io
	got := registryHost("nginx")
	if got != "docker.io" {
		t.Errorf("registryHost(\"nginx\") = %q, want \"docker.io\"", got)
	}
}

func TestRegistryHost_DockerHubWithLibrary(t *testing.T) {
	got := registryHost("library/nginx:latest")
	if got != "docker.io" {
		t.Errorf("registryHost(\"library/nginx:latest\") = %q, want \"docker.io\"", got)
	}
}

func TestRegistryHost_DockerHubExplicit(t *testing.T) {
	got := registryHost("docker.io/library/nginx:latest")
	if got != "docker.io" {
		t.Errorf("registryHost(\"docker.io/library/nginx:latest\") = %q, want \"docker.io\"", got)
	}
}

func TestRegistryHost_CustomRegistry(t *testing.T) {
	got := registryHost("ghcr.io/my-org/my-image:v1.0")
	if got != "ghcr.io" {
		t.Errorf("registryHost(\"ghcr.io/my-org/my-image:v1.0\") = %q, want \"ghcr.io\"", got)
	}
}

func TestRegistryHost_CustomRegistryWithPort(t *testing.T) {
	got := registryHost("registry.example.com:5000/my-image:latest")
	if got != "registry.example.com:5000" {
		t.Errorf("registryHost(\"registry.example.com:5000/my-image:latest\") = %q, want \"registry.example.com:5000\"", got)
	}
}

func TestRegistryHost_EmptyString(t *testing.T) {
	got := registryHost("")
	if got != "" {
		t.Errorf("registryHost(\"\") = %q, want \"\"", got)
	}
}

func TestRegistryHost_InvalidReference(t *testing.T) {
	got := registryHost("INVALID:://image")
	if got != "" {
		t.Errorf("registryHost(\"INVALID:://image\") = %q, want \"\"", got)
	}
}

func TestRegistryHost_UserRepo(t *testing.T) {
	got := registryHost("curlimages/curl:latest")
	if got != "docker.io" {
		t.Errorf("registryHost(\"curlimages/curl:latest\") = %q, want \"docker.io\"", got)
	}
}

func TestRegistryHost_ECR(t *testing.T) {
	got := registryHost("123456789.dkr.ecr.us-east-1.amazonaws.com/my-app:latest")
	if got != "123456789.dkr.ecr.us-east-1.amazonaws.com" {
		t.Errorf("registryHost ECR = %q, want \"123456789.dkr.ecr.us-east-1.amazonaws.com\"", got)
	}
}

// --------------------------------------------------------------------------
// dockerConfigPath tests
// --------------------------------------------------------------------------

func TestDockerConfigPath_Default(t *testing.T) {
	// Clear DOCKER_CONFIG to test default behavior
	t.Setenv("DOCKER_CONFIG", "")

	got := dockerConfigPath()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("could not get home dir: %v", err)
	}
	want := filepath.Join(home, ".docker", "config.json")
	if got != want {
		t.Errorf("dockerConfigPath() = %q, want %q", got, want)
	}
}

func TestDockerConfigPath_CustomViaEnv(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", "/tmp/my-docker-config")

	got := dockerConfigPath()
	want := "/tmp/my-docker-config/config.json"
	if got != want {
		t.Errorf("dockerConfigPath() = %q, want %q", got, want)
	}
}

// --------------------------------------------------------------------------
// encodeAuth tests
// --------------------------------------------------------------------------

func TestEncodeAuth_RoundTrip(t *testing.T) {
	input := registry.AuthConfig{
		Username:      "myuser",
		Password:      "mypass",
		ServerAddress: "ghcr.io",
	}

	encoded := encodeAuth(input)
	if encoded == "" {
		t.Fatal("encodeAuth returned empty string")
	}

	// Decode and verify
	decoded, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("failed to base64-decode: %v", err)
	}

	var got registry.AuthConfig
	if err := json.Unmarshal(decoded, &got); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if got.Username != input.Username {
		t.Errorf("Username = %q, want %q", got.Username, input.Username)
	}
	if got.Password != input.Password {
		t.Errorf("Password = %q, want %q", got.Password, input.Password)
	}
	if got.ServerAddress != input.ServerAddress {
		t.Errorf("ServerAddress = %q, want %q", got.ServerAddress, input.ServerAddress)
	}
}

func TestEncodeAuth_EmptyFields(t *testing.T) {
	encoded := encodeAuth(registry.AuthConfig{})
	if encoded == "" {
		t.Fatal("encodeAuth returned empty string for zero-value AuthConfig")
	}
	// Should still be valid base64-encoded JSON
	decoded, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("failed to base64-decode: %v", err)
	}
	var got registry.AuthConfig
	if err := json.Unmarshal(decoded, &got); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
}

// --------------------------------------------------------------------------
// CurlImage tests
// --------------------------------------------------------------------------

func TestCurlImage_Default(t *testing.T) {
	t.Setenv("CHIPERKA_INTERNAL_CURL_IMAGE", "")

	got := CurlImage()
	if got != defaultCurlImage {
		t.Errorf("CurlImage() = %q, want %q", got, defaultCurlImage)
	}
}

func TestCurlImage_EnvOverride(t *testing.T) {
	t.Setenv("CHIPERKA_INTERNAL_CURL_IMAGE", "my-registry.io/curl:v1")

	got := CurlImage()
	if got != "my-registry.io/curl:v1" {
		t.Errorf("CurlImage() = %q, want \"my-registry.io/curl:v1\"", got)
	}
}

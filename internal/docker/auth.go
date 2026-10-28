package docker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/distribution/reference"
	"github.com/docker/docker/api/types/registry"
)

// RegistryCredential is a job-scoped registry login, e.g. a CI job token
// the submitting pipeline passed through the hub.
type RegistryCredential struct {
	Host     string
	Username string
	Password string
}

type registryCredentialsKey struct{}

// WithRegistryCredentials returns a context whose image pulls use creds for matching registries.
func WithRegistryCredentials(ctx context.Context, creds []RegistryCredential) context.Context {
	if len(creds) == 0 {
		return ctx
	}
	return context.WithValue(ctx, registryCredentialsKey{}, creds)
}

// getRegistryAuth returns a base64-encoded JSON AuthConfig for the given image's
// registry, or an empty string if no credentials are found.
//
// Priority:
//  1. Job-scoped credentials from ctx (see WithRegistryCredentials)
//  2. CHIPERKA_REGISTRY_USER + CHIPERKA_REGISTRY_PASSWORD + CHIPERKA_REGISTRY_HOST env vars
//  3. ~/.docker/config.json (standard docker login credentials)
//
// Job-scoped and env var credentials are only sent when the image's registry matches
// their host, preventing credential leaks to untrusted registries.
//
// Returns empty string on any error — public images pull without auth as before.
func getRegistryAuth(ctx context.Context, image string) string {
	host := registryHost(image)

	// 1. Check job-scoped credentials
	if creds, ok := ctx.Value(registryCredentialsKey{}).([]RegistryCredential); ok {
		for _, c := range creds {
			if c.Host != "" && c.Host == host && c.Username != "" && c.Password != "" {
				return encodeAuth(registry.AuthConfig{
					Username:      c.Username,
					Password:      c.Password,
					ServerAddress: host,
				})
			}
		}
	}

	// 2. Check env vars — requires CHIPERKA_REGISTRY_HOST to scope credentials
	if envHost := os.Getenv("CHIPERKA_REGISTRY_HOST"); envHost != "" && envHost == host {
		if user := os.Getenv("CHIPERKA_REGISTRY_USER"); user != "" {
			if pass := os.Getenv("CHIPERKA_REGISTRY_PASSWORD"); pass != "" {
				return encodeAuth(registry.AuthConfig{
					Username:      user,
					Password:      pass,
					ServerAddress: host,
				})
			}
		}
	}

	// 3. Check docker config
	auth := lookupDockerConfig(host)
	if auth != "" {
		return auth
	}

	return ""
}

// registryHost extracts the registry hostname from a Docker image reference.
// Returns "https://index.docker.io/v1/" for Docker Hub images.
func registryHost(image string) string {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return ""
	}
	return reference.Domain(named)
}

// dockerConfig represents the relevant parts of ~/.docker/config.json.
type dockerConfig struct {
	Auths map[string]dockerConfigAuth `json:"auths"`
}

type dockerConfigAuth struct {
	Auth string `json:"auth"`
}

// lookupDockerConfig reads docker config.json and returns base64-encoded
// AuthConfig for the given registry host, or empty string.
func lookupDockerConfig(host string) string {
	configPath := dockerConfigPath()
	if configPath == "" {
		return ""
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return ""
	}

	var cfg dockerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return ""
	}

	// Try exact match first, then common variants
	candidates := []string{host}
	if host == "docker.io" {
		candidates = append(candidates, "https://index.docker.io/v1/", "index.docker.io")
	}

	for _, candidate := range candidates {
		entry, ok := cfg.Auths[candidate]
		if !ok || entry.Auth == "" {
			continue
		}

		// entry.Auth is base64(user:pass)
		decoded, err := base64.StdEncoding.DecodeString(entry.Auth)
		if err != nil {
			continue
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 {
			continue
		}

		return encodeAuth(registry.AuthConfig{
			Username:      parts[0],
			Password:      parts[1],
			ServerAddress: host,
		})
	}

	return ""
}

// dockerConfigPath returns the path to docker config.json.
// Respects DOCKER_CONFIG env var, falls back to ~/.docker/config.json.
func dockerConfigPath() string {
	if dir := os.Getenv("DOCKER_CONFIG"); dir != "" {
		return filepath.Join(dir, "config.json")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".docker", "config.json")
}

// encodeAuth encodes an AuthConfig to the base64 JSON string expected by the Docker SDK.
func encodeAuth(auth registry.AuthConfig) string {
	data, err := json.Marshal(auth)
	if err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(data)
}

package docker

import (
	"context"
	"io"
	"os"
	"sync"

	imagetypes "github.com/docker/docker/api/types/image"
)

const (
	defaultCurlImage = "curlimages/curl:latest"
)

// CurlImage returns the curl image to use for HTTP requests in Docker networks.
// Override with CHIPERKA_INTERNAL_CURL_IMAGE env variable.
func CurlImage() string {
	if img := os.Getenv("CHIPERKA_INTERNAL_CURL_IMAGE"); img != "" {
		return img
	}
	return defaultCurlImage
}

// PrewarmImages pulls all given images in parallel if they don't exist locally.
// Returns the number of images that were pulled.
func PrewarmImages(ctx context.Context, images []string) int {
	if len(images) == 0 {
		return 0
	}

	// Deduplicate images
	imageSet := make(map[string]bool)
	for _, img := range images {
		if img != "" {
			imageSet[img] = true
		}
	}

	// Add internal images
	imageSet[CurlImage()] = true

	// Check and pull in parallel
	var wg sync.WaitGroup
	var pullCount int
	var mu sync.Mutex

	for image := range imageSet {
		wg.Add(1)
		go func(img string) {
			defer wg.Done()

			// Check if image exists
			_, _, err := defaultDockerClient.ImageInspectWithRaw(ctx, img)
			if err == nil {
				return // Image exists
			}

			// Pull image
			reader, err := defaultDockerClient.ImagePull(ctx, img, imagetypes.PullOptions{
				RegistryAuth: getRegistryAuth(ctx, img),
			})
			if err != nil {
				return
			}
			io.Copy(io.Discard, reader) // drain to complete pull
			reader.Close()

			mu.Lock()
			pullCount++
			mu.Unlock()
		}(image)
	}
	wg.Wait()

	return pullCount
}

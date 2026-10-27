# Chiperka Worker

Executes [Chiperka](https://about.chiperka.com) tests for a [Chiperka Hub](https://github.com/chiperka/hub), or for any server that implements the same worker API.

The worker connects to the hub over plain HTTP(S) and needs no inbound ports. It long-polls for jobs, then runs each test on the local Docker daemon in its own network. A test runs through these steps:

1. start the services,
2. run the setup steps,
3. execute the test,
4. evaluate the assertions,
5. collect logs and artifacts,
6. clean up.

It streams progress and logs back to the hub as it goes and reports the result.

## Run

1. In the hub UI, open **Workers → Add worker** and copy the token (`spt_…`).
2. Start the worker on any machine with Docker:

```bash
docker run -d --name chiperka-worker --restart unless-stopped \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e CHIPERKA_API_URL=https://hub.example.com \
  -e CHIPERKA_WORKER_TOKEN=spt_… \
  finie/chiperka-worker
```

### Without the UI

If the hub has `CHIPERKA_WORKER_JOIN_TOKEN` set, pass that token instead of a worker token. The worker then registers itself and needs no clicking in the UI, so it works in plain `docker compose up`:

```yaml
worker:
  image: finie/chiperka-worker
  environment:
    CHIPERKA_API_URL: http://hub:8080
    CHIPERKA_WORKER_TOKEN: ${CHIPERKA_JOIN_TOKEN}
    CHIPERKA_WORKER_NAME: worker-1   # optional, defaults to the hostname
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
```

On registration, the hub gives the worker its own token, which it uses from then on. If a worker restarts under the same name, it takes over its previous entry. When the hub runs with `CHIPERKA_AUTH=disabled` and has no join token set, the token can be left out entirely.

Binaries for Linux and macOS are attached to every [release](https://github.com/chiperka/worker/releases):

```bash
CHIPERKA_API_URL=https://hub.example.com CHIPERKA_WORKER_TOKEN=spt_… chiperka-worker worker
```

## Configuration

| Flag | Env | Default | Description |
|---|---|---|---|
| `--api` | `CHIPERKA_API_URL` | `http://localhost:8080` | Hub URL |
| `--token` | `CHIPERKA_WORKER_TOKEN` | – | Worker token from the hub, or the hub's join token |
| `--name` | `CHIPERKA_WORKER_NAME` | hostname | Name shown in the hub |
| `--concurrency` | | CPU count | Maximum parallel tests |
| `--capacity` | | CPU count × 2 | Maximum total test weight running at once |
| `--max-containers` | | unlimited | Maximum concurrent containers |
| `--timeout` | | `300` | Per-test timeout in seconds |
| `--drain-timeout` | | `5m` | How long to wait for running tests on shutdown |

Each test's **weight** is the sum of its services' weights, with a default of 1 per service. A test heavier than `--capacity` still runs, but only when the worker is otherwise idle.

### Private images

Services are pulled with the credentials from `~/.docker/config.json`. Inside the container, mount that file, or set these variables:

```bash
-e CHIPERKA_REGISTRY_HOST=registry.example.com \
-e CHIPERKA_REGISTRY_USER=… \
-e CHIPERKA_REGISTRY_PASSWORD=…
```

The env credentials are only sent to `CHIPERKA_REGISTRY_HOST`.

### Notes

- Run one worker per Docker daemon. On startup, a worker removes leftover Chiperka containers and networks.
- On `SIGTERM`, the worker stops taking new jobs and lets running ones finish, waiting up to `--drain-timeout`. The hub can also cancel a running job, for example when a run is stopped.

## Development

```bash
go test ./...
go build -o chiperka-worker . && ./chiperka-worker worker --api http://localhost:8080 --token spt_…
```

Releases are cut by pushing a semver tag. GitHub Actions then builds the binaries and the multi-arch `finie/chiperka-worker` image.

## License

Apache 2.0

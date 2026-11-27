---
title: Installation via Docker
---


## Try Online

If you want to avoid running your own XTDB server locally, you can instantly play with inserting data and querying right now using the [XTDB Play](https://play.xtdb.com/) web-based console.
The interactive [SQL Quickstart](/quickstart/sql-overview.html) uses this console to showcase XTDB's SQL dialect and bitemporal capabilities.

Otherwise, let's get XTDB downloaded and running on your own machine...​

## Docker Install

XTDB supports production usage via the Postgres wire protocol so that you can work with various Postgres-compatible tools, drivers etc.

You can start a 'standalone' (i.e. non-production, non-distributed) XTDB server using the following command:

``` bash
## see https://github.com/xtdb/xtdb/pkgs/container/xtdb/versions for tags
## `latest`: latest tagged release
## `nightly`: built every night from `main` branch
## `edge`: latest nightly plus urgent fixes

docker run -it --pull=always \
  -p 5432:5432 \
  -p 3000:3000 \
  -p 8080:8080 \
  ghcr.io/xtdb/xtdb

## 5432: Postgres wire-compatible server (primary API)
## 3000: Arrow Flight SQL / ADBC server
## 8080: Monitoring/healthz HTTP endpoints
```

This command starts a Postgres wire-compatible endpoint on port 5432, an Arrow Flight SQL endpoint on port 3000, and the healthz/metrics server on port 8080.
By default, the root `xtdb` user accepts connections without a password; set `XTDB_PASSWORD` to require a password for `xtdb`, or mount a configuration file with explicit [authentication rules](/ops/config/authentication).
The container image includes a `HEALTHCHECK` that polls `http://localhost:8080/healthz/alive` after a 15-second start period.

By default your data will only be stored temporarily using a local directory within the Docker container (`/var/lib/xtdb/log` and `/var/lib/xtdb/buffers`).

### (Optional) Mount a custom configuration file

The image's working directory is `/usr/local/lib/xtdb` and its default command is `-f config.yaml`.
Mount your own YAML configuration at `/usr/local/lib/xtdb/config.yaml` to replace the bundled standalone configuration:

``` bash
docker run -it --pull=always \
  -p 5432:5432 \
  -p 3000:3000 \
  -p 8080:8080 \
  -v ./my-config.yaml:/usr/local/lib/xtdb/config.yaml \
  ghcr.io/xtdb/xtdb
```

On the RTDB native image, the entrypoint keeps two fallbacks for volumes and mounts from earlier RTDB images when running the default command:

- If `/usr/local/lib/xtdb/config.yaml` still holds the bundled default and a custom file is mounted at `/etc/rtdb/config.yaml`, the entrypoint serves `/etc/rtdb/config.yaml` and logs a notice to stderr.
- If neither `/var/lib/xtdb/log` nor `/var/lib/xtdb/buffers` exists and data is present under `/var/lib/rtdb/log` or `/var/lib/rtdb/storage`, the entrypoint serves that directory (via `/etc/rtdb/config-var-lib-rtdb.yaml`) and logs a notice to stderr.
  Move the data to `/var/lib/xtdb/log` and `/var/lib/xtdb/buffers` to drop the fallback.

### (Optional) Mount a host directory

You can attach a host volume to preserve your data across container restarts, e.g. by adding `-v /tmp/xtdb-data-dir:/var/lib/xtdb`, however because the XTDB container runs as a non-root user by default (UID 20000), you must ensure the container can write to it:

- For Docker, before running the container:
  
  ``` bash
  sudo chown -R 20000:20000 /tmp/xtdb-data-dir
  ```

- For Podman, ensure the directory is owned by your user and use `--userns=keep-id`, e.g.:

  ``` bash
  podman run -it --pull=always \
    --userns=keep-id \
    -p 5432:5432 \
    -p 8080:8080 \
    -v /tmp/xtdb-data-dir:/var/lib/xtdb \
    ghcr.io/xtdb/xtdb
  ```

## Wait for XTDB to start

After seeing a 'Node started' log message (e.g. `09:00:00 | INFO xtdb.cli | Node started`) you are able to confirm your XTDB server is running using cURL:

``` bash
curl http://localhost:8080/healthz/alive

## Alive.
```

## Connect with `psql`

``` bash
## if you have Postgres installed, psql is already available
psql -h localhost -U xtdb xtdb
```

## Run your first query

```
psql (16.2, server 16)
Type "help" for help.

user=> SELECT 'foo' AS bar;

 bar
-----
 foo
(1 row)
```

Next up:

- if you haven't already run through the Quickstart already, you probably want to start there with inserting data, running your first queries, and learning about XTDB's novel capabilities: [let's INSERT some data](/quickstart/sql-overview)!
- to connect to XTDB from your language/tool of choice, have a look at XTDB's [driver support](/drivers).

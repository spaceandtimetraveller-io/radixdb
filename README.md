# artbenchmark / radixdb

Mmap-backed radix tree with gRPC API. This repo includes [`radixdb`](radixdb/) (library), a [`cmd/server`](cmd/server) gRPC server, and a [`cmd/cli`](cmd/cli) client.

## Prerequisites

- Go 1.22+ (see `go.mod`)

## Run the server

From the repository root:

```bash
go run ./cmd/server -addr :50051 -db /path/to/data.rdx
```

Useful flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `:50051` | gRPC listen address (`host:port` or `:port`) |
| `-db` | `radix.db` | Path to the radix database file (created if missing) |
| `-readonly` | `false` | Open the DB read-only (`Insert` / `Sync` fail) |
| `-metrics-addr` | `:9090` | Prometheus HTTP endpoint; set to empty string to disable |

Examples:

```bash
# Default: gRPC on :50051, DB file ./radix.db, metrics on :9090
go run ./cmd/server

# Custom paths
go run ./cmd/server -addr 127.0.0.1:50051 -db ./mydata.rdx -metrics-addr :9090
```

Stop the server with `Ctrl+C` (graceful shutdown).

## CLI client (`cmd/cli`)

The CLI connects to the gRPC server (insecure). Default address is `127.0.0.1:50051`.

```bash
go run ./cmd/cli -addr 127.0.0.1:50051 <flags>
```

You can combine flags in one invocation (e.g. load CSV, then print stats).

### Load data from CSV

CSV format: **semicolon-separated** columns `id;name;parent_id` (same idea as [`benchs/neigborhood.csv`](benchs/neigborhood.csv)).

```bash
go run ./cmd/cli -addr 127.0.0.1:50051 -file benchs/neigborhood.csv
```

Rows where `parent_id >= id` are skipped (invalid hierarchy). Each valid row is sent as an `Insert` with the given `key` (= name) and ids.

### Insert a single key

```bash
# Explicit id and parent (parent_id must be less than id when both set)
go run ./cmd/cli -key MERKEZ -id 1 -parent_id 0

# Let the server assign the next id (omit -id)
go run ./cmd/cli -key NEWNODE -parent_id 0
```

### Stats (distinct keys and total rows)

```bash
go run ./cmd/cli -stats
```

Prints `distinct_keys` and `total_rows` (including multiple rows per key).

### Search: prefix walk (stream keys)

```bash
go run ./cmd/cli -prefix ABDİ
```

Streams all keys whose UTF-8 byte sequence has the given prefix, printing each key and its rows (`parent_id`, `id`, `full_path`).

### Optional: sync after writes

```bash
go run ./cmd/cli -file benchs/neigborhood.csv -sync
```

Calls `Sync` after bulk or single insert so the backing file is fsync’d.

### Get by exact key (gRPC)

The CLI does not expose `Get`. Use [`grpcurl`](https://github.com/fullstorydev/grpcurl) or another gRPC client, for example:

```bash
grpcurl -plaintext -d '{"key":"MERKEZ"}' 127.0.0.1:50051 radixdb.v1.RadixDB/Get
```

## Regenerating protobuf Go code

If you change [`proto/radixdb/v1/radixdb.proto`](proto/radixdb/v1/radixdb.proto), regenerate stubs from the repo root:

```bash
PATH="$PATH:$(go env GOPATH)/bin" protoc -I . --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/radixdb/v1/radixdb.proto
```

## Typical workflow

1. Start the server: `go run ./cmd/server -db ./data.rdx`
2. Load data: `go run ./cmd/cli -file benchs/neigborhood.csv`
3. Check stats: `go run ./cmd/cli -stats`
4. Prefix search: `go run ./cmd/cli -prefix SOME_PREFIX`
5. Insert more rows: `go run ./cmd/cli -key NEW -parent_id 0` (or with `-id`)

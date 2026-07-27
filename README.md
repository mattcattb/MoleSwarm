# go-torrent

This system contains an interoperable BitTorrent v1 client and HTTP tracker in
progress. The first local deployment uses one controlled, single-file swarm:

```text
tracker
├── official seeder
├── learner-1
└── learner-2
```

## Run the local swarm

From the Break My System repository root:

```bash
docker compose up -d --build --wait \
  torrent-tracker torrent-seeder torrent-learner-1 torrent-learner-2
docker compose ps \
  torrent-tracker torrent-seeder torrent-learner-1 torrent-learner-2
```

Both learner services become healthy only after `/data/lesson-payload-v1.bin`
matches the owned payload's SHA-256. The torrent processes remain alive after
completion and continue seeding until Compose stops them.

Learner downloads stay in their container filesystems. Reset the demonstration
by recreating the two learner containers:

```bash
docker compose up -d --force-recreate torrent-learner-1 torrent-learner-2
```

## Local configuration

The root Compose defaults can be changed without editing `docker-compose.yml`:

```text
TORRENT_ARTIFACT_DIR                ./systems/go-torrent/artifacts/lesson-payload-v1
TORRENT_TRACKER_HOST_PORT           16969
TORRENT_TRACKER_STATUS_HOST_PORT    18080
TORRENT_SEED_STATUS_HOST_PORT       18081
TORRENT_LEARNER_1_STATUS_HOST_PORT  18082
TORRENT_LEARNER_2_STATUS_HOST_PORT  18083
```

For example:

```bash
TORRENT_TRACKER_HOST_PORT=26969 docker compose up -d --build --wait \
  torrent-tracker torrent-seeder torrent-learner-1 torrent-learner-2
```

The tracker command also accepts deployment configuration from environment
variables. Scalar flags override their corresponding environment values:

```text
TRACKER_LISTEN               HTTP announce listen address
TRACKER_STATUS_LISTEN        optional private status listen address
TRACKER_INTERVAL             recommended announce interval, such as 30s
TRACKER_PEER_TTL             peer expiry duration; zero derives 3 × interval
TRACKER_ALLOWED_INFO_HASHES  comma-separated hexadecimal info hashes
TRACKER_TORRENT_DIR          directory of allowed .torrent files
```

Allowed swarm inputs are additive and deduplicated. This permits Railway to
configure the tracker without mounting metainfo files:

```bash
TRACKER_ALLOWED_INFO_HASHES=<hash-a>,<hash-b> tracker serve
```

For local deployments, mount a directory and load every `.torrent` file in it
at startup:

```bash
TRACKER_TORRENT_DIR=/artifacts tracker serve
```

Repeated `--allow-torrent` and `--allow-info-hash` flags can add more swarms.
If no allowlist source is configured, the tracker remains unrestricted. Config
changes are applied on process restart; the tracker does not hot-reload them.

These are host-facing observation ports. Peer port `6881` is intentionally not
published to the host. Inside `bms-runtime`, every peer has its own network
namespace and can listen on `6881`, while the tracker is reachable at
`http://tracker:6969/announce`.

## Environment-specific metainfo

The tracker announce URL is part of the distributed `.torrent` file. It is not
an environment variable substituted when a client starts.

The checked-in `lesson-payload-v1.torrent` uses the Compose URL:

```text
http://tracker:6969/announce
```

For Railway, first assign the tracker a stable public URL, then regenerate the
metainfo from the unchanged payload:

```bash
go run ./cmd/torrent create \
  -announce https://TRACKER-DOMAIN/announce \
  -output /tmp/lesson-payload-v1.railway.torrent \
  ./artifacts/lesson-payload-v1/lesson-payload-v1.bin
```

The payload's piece hashes and info hash remain unchanged when only the
top-level announce URL changes. The raw metainfo SHA-256 changes because the
distributed `.torrent` bytes changed.

Railway environment variables can configure process listener ports and
private service addresses, but an interoperable `.torrent` should contain the
public tracker URL that external clients can reach. Public Railway deployment
also needs an explicit externally advertised peer port and trusted tracker
proxy-address handling; those are separate from this local Compose setup.

## Read-only observation endpoints

The Compose services expose a private HTTP status listener from each Go
process. The public BitTorrent protocol ports remain separate:

```text
http://localhost:18080/v1/snapshot  tracker swarms and announced peers
http://localhost:18081/v1/snapshot  official seeder session
http://localhost:18082/v1/snapshot  learner-1 session
http://localhost:18083/v1/snapshot  learner-2 session
```

These snapshots copy state through the torrent event loop or under the tracker
swarm lock. The HTTP handlers do not read mutable protocol state directly.
They are intended for a trusted BMS API or local development and should not be
published directly to the internet.

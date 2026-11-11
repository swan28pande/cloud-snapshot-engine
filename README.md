# Cloud Snapshot and Recovery Engine

A Java service that takes incremental block-level snapshots of virtual disk volumes, deduplicates blocks with content-addressed hashing, stores them in S3, and reassembles volume images through a gRPC recovery API.

**Stack:** Java 17 · gRPC · AWS S3 (SDK v2) · Docker

## How it works

```
volume ──read in 4 MiB blocks──► SHA-256 each block
                                   ├─ all zeros      → recorded as "", never stored
                                   ├─ hash in store  → reused (deduplicated)
                                   └─ new content    → PUT blocks/ab/cd/<sha256>
                         manifest (ordered block hashes) → PUT manifests/<volume>/<snapshot>.pb
```

- **Incremental and deduplicated.** Blocks are addressed by their hash, so a snapshot uploads only content the store has never seen, whether it came from this volume's parent, another snapshot, or a different volume. Hashes from the parent manifest are trusted without a HEAD request.
- **Consistency.** The manifest is written only after all its blocks are stored, so a snapshot is visible only once it's complete.
- **Recovery.** A restore fetches each block named in the manifest and checks its SHA-256 before writing it. Restoring to an image file leaves zero blocks as sparse holes. Restoring to a block device writes the zeros explicitly.
- **Storage.** Everything lives in an S3 bucket, or any S3-compatible store via `S3_ENDPOINT`. A local filesystem store with the same key layout is available for development.

## gRPC API (`src/main/proto/snapshot.proto`)

| RPC | Description |
|---|---|
| `CreateSnapshot(volume_id, device_path, parent_snapshot_id?)` | Snapshot a device or image on the server. The parent defaults to the latest snapshot. Returns block stats: changed, new, reused, zero, bytes uploaded |
| `ListSnapshots(volume_id)` | Snapshots, oldest first |
| `RestoreVolume(volume_id, snapshot_id, target_path)` | Reassemble onto a device or file on the server |
| `ReadSnapshot(volume_id, snapshot_id)` | Server-streamed `VolumeChunk`s (≤ 1 MiB each, zero runs sent without data) for reassembly by the client |

Server reflection is enabled, so `grpcurl -plaintext localhost:50051 list` works.

## Run

```bash
mvn package                                   # -> target/snapshot-engine.jar

# Local store
STORE=local LOCAL_STORE_DIR=./store ALLOWED_PATHS=$PWD/volumes java -jar target/snapshot-engine.jar

# S3 (credentials and region from the default AWS provider chain)
STORE=s3 S3_BUCKET=my-snapshots S3_PREFIX=prod ALLOWED_PATHS=/dev java -jar target/snapshot-engine.jar

# Docker: engine + MinIO
docker compose up --build
```

Client:

```bash
cli() { java -cp target/snapshot-engine.jar io.github.swan28pande.snapshot.SnapshotCli "$@"; }
cli create vm1 /volumes/disk.img          # full snapshot (only non-zero blocks uploaded)
cli create vm1 /volumes/disk.img          # incremental: only changed content uploaded
cli list vm1
cli restore vm1 <snapshot-id> /volumes/restored.img   # written by the server
cli pull    vm1 <snapshot-id> ./local-copy.img        # streamed to this machine
```

| Env var | Default | |
|---|---|---|
| `PORT` | `50051` | gRPC port |
| `STORE` | `local` | `s3` or `local` |
| `S3_BUCKET`, `S3_PREFIX`, `S3_ENDPOINT` | | S3 settings. Set the endpoint for MinIO/LocalStack |
| `LOCAL_STORE_DIR` | `./snapshot-store` | root directory for the local store |
| `BLOCK_SIZE` | `4194304` | bytes per block. Must match the parent snapshot |
| `UPLOAD_THREADS` | `8` | parallel block uploads |
| `ALLOWED_PATHS` | *(any)* | comma-separated prefixes that `device_path`/`target_path` must be under |

## Limitations

This is a starting skeleton:
- **No authentication or TLS.** Set `ALLOWED_PATHS` and keep the port private.
- **Crash consistency is your job.** Snapshot an unmounted, frozen (`fsfreeze`) or cloned volume. Every block is read and hashed, because there's no changed-block tracking yet.
- **No retention or GC.** Unreferenced blocks stay in the store, and there's no compression or encryption yet.

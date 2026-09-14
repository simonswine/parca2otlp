# parca2otlp

`parca2otlp` accepts Parca ProfileStore Arrow writes and persists each completed
RPC batch as a binary OTLP Profiles `ExportProfilesServiceRequest` protobuf.

The service supports the Parca v1 streaming `Write` protocol, including its
stacktrace handshake, and v2 inline-stack `WriteArrow` payloads. Files are
atomically published as `*.otlp.pb`.

## Run

```sh
go run ./cmd/parca2otlp --storage-path ./data --max-storage-size 256MiB
```

The server listens on `:7070` by default. Configure Parca Agent's ProfileStore
endpoint to point at this server. The maximum inbound gRPC message size is 64
MiB and can be adjusted with `--max-recv-message-size`.

## Retention

Managed profile files are capped at 256 MiB by default. Before writing a new
batch, `parca2otlp` deletes the oldest `*.otlp.pb` files until the new batch
fits. The same cleanup runs at startup. A batch larger than the configured
limit is rejected without deleting existing data. Set `--max-storage-size 0`
to disable retention. The storage directory must be used exclusively by this
service; unrelated files are ignored.

## Debug Artifacts

`parca2otlp` also implements Parca's gRPC `DebuginfoService`, retaining debug
info, executables, and source archives sent by compatible agents. Artifacts are
stored under `<storage-path>/debuginfo` by default and have their own 256 MiB
oldest-first limit. Configure a different location or budget with
`--debuginfo-storage-path` and `--max-debuginfo-storage-size`. Set the latter
to `0` to disable debug-artifact retention.

Uploads use the Parca gRPC strategy and are streamed to disk. Incomplete
uploads become replaceable after 15 minutes; configure this with
`--debuginfo-upload-stale-after`. Debug artifacts are persisted only: this
service does not yet use them to symbolize stored OTLP profiles.

## Files

Each file contains one `ExportProfilesServiceRequest` encoded with standard
protobuf binary encoding. It has one shared OTLP Profiles dictionary and one
resource/scope grouping for the source RPC batch. Parca labels become sample
attributes; Parca-only metadata is preserved under `parca.*` attributes.

OTLP Profiles remains alpha, so this project pins the generated
`v1development` protobuf API rather than claiming a stable on-disk schema.

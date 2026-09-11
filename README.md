# parca2otlp

`parca2otlp` accepts Parca ProfileStore Arrow writes and persists each completed
RPC batch as a binary OTLP Profiles `ExportProfilesServiceRequest` protobuf.

The service supports the Parca v1 streaming `Write` protocol, including its
stacktrace handshake, and v2 inline-stack `WriteArrow` payloads. Files are
append-only and atomically published as `*.otlp.pb`.

## Run

```sh
go run ./cmd/parca2otlp --storage-path ./data
```

The server listens on `:7070` by default. Configure Parca Agent's ProfileStore
endpoint to point at this server. The maximum inbound gRPC message size is 64
MiB and can be adjusted with `--max-recv-message-size`.

## Files

Each file contains one `ExportProfilesServiceRequest` encoded with standard
protobuf binary encoding. It has one shared OTLP Profiles dictionary and one
resource/scope grouping for the source RPC batch. Parca labels become sample
attributes; Parca-only metadata is preserved under `parca.*` attributes.

OTLP Profiles remains alpha, so this project pins the generated
`v1development` protobuf API rather than claiming a stable on-disk schema.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	debuginfogrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/debuginfo/v1alpha1/debuginfov1alpha1grpc"
	profilestoregrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/profilestore/v1alpha1/profilestorev1alpha1grpc"
	collectorprofiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/simonswine/parca2otlp/internal/debuginfo"
	"github.com/simonswine/parca2otlp/internal/profilestore"
	"github.com/simonswine/parca2otlp/internal/storage"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "dump" {
		if err := dump(os.Args[2:], os.Stdout, os.Stdin); err != nil {
			log.Fatal(err)
		}
		return
	}
	serve()
}

func serve() {
	listenAddress := flag.String("listen-address", ":7070", "gRPC listen address")
	storagePath := flag.String("storage-path", "", "directory for OTLP Profiles protobuf files (required)")
	maxStorageSize := flag.String("max-storage-size", "256MiB", "maximum managed OTLP storage; 0 disables retention")
	debuginfoStoragePath := flag.String("debuginfo-storage-path", "", "directory for Parca debug artifacts; defaults to <storage-path>/debuginfo")
	maxDebuginfoStorageSize := flag.String("max-debuginfo-storage-size", "256MiB", "maximum managed debug artifact storage; 0 disables retention")
	debuginfoUploadStaleAfter := flag.Duration("debuginfo-upload-stale-after", 15*time.Minute, "age after which an unfinished debug upload can be replaced")
	maxReceiveSize := flag.Int("max-recv-message-size", 64<<20, "maximum gRPC request size in bytes")
	flag.Parse()

	storageLimit, err := storage.ParseSize(*maxStorageSize)
	if err != nil {
		log.Fatal(err)
	}
	debuginfoLimit, err := storage.ParseSize(*maxDebuginfoStorageSize)
	if err != nil {
		log.Fatal(err)
	}
	store, err := storage.New(*storagePath, storageLimit)
	if err != nil {
		log.Fatal(err)
	}
	if *debuginfoStoragePath == "" {
		*debuginfoStoragePath = filepath.Join(*storagePath, "debuginfo")
	}
	debuginfoStore, err := debuginfo.New(*debuginfoStoragePath, debuginfoLimit, *debuginfoUploadStaleAfter)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen on %s: %v", *listenAddress, err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(*maxReceiveSize))
	profilestoregrpc.RegisterProfileStoreServiceServer(server, profilestore.New(store))
	debuginfogrpc.RegisterDebuginfoServiceServer(server, debuginfo.NewService(debuginfoStore))
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	reflection.Register(server)

	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
	log.Printf("parca2otlp listening on %s, profiles in %s, debug artifacts in %s", *listenAddress, *storagePath, *debuginfoStoragePath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		server.GracefulStop()
	case err := <-errCh:
		if err != nil {
			log.Fatal(fmt.Errorf("serve: %w", err))
		}
	}
}

func dump(args []string, output io.Writer, input io.Reader) error {
	flags := flag.NewFlagSet("dump", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	format := flags.String("format", "text", "output format: text or json")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse dump flags: %w", err)
	}
	paths := flags.Args()
	if len(paths) == 0 {
		return fmt.Errorf("usage: parca2otlp dump [--format=text|json] <file.otlp.pb> [...]; use - for standard input")
	}
	if *format != "text" && *format != "json" {
		return fmt.Errorf("unsupported dump format %q (want text or json)", *format)
	}
	for _, path := range paths {
		var (
			data []byte
			err  error
		)
		if path == "-" {
			data, err = io.ReadAll(input)
		} else {
			data, err = os.ReadFile(path)
		}
		if err != nil {
			return fmt.Errorf("read %q: %w", path, err)
		}
		request := &collectorprofiles.ExportProfilesServiceRequest{}
		if err := proto.Unmarshal(data, request); err != nil {
			return fmt.Errorf("decode %q as OTLP profiles: %w", path, err)
		}
		var rendered []byte
		if *format == "json" {
			rendered, err = protojson.MarshalOptions{Multiline: true, Indent: "  ", UseProtoNames: true}.Marshal(request)
		} else {
			rendered, err = prototext.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(request)
		}
		if err != nil {
			return fmt.Errorf("format %q: %w", path, err)
		}
		if _, err := output.Write(rendered); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
		if _, err := fmt.Fprintln(output); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	return nil
}

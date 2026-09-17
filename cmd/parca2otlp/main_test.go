package main

import (
	"bytes"
	"strings"
	"testing"

	collectorprofiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
	"google.golang.org/protobuf/proto"
)

func TestDumpTextAndJSON(t *testing.T) {
	request := &collectorprofiles.ExportProfilesServiceRequest{
		ResourceProfiles: []*profiles.ResourceProfiles{{}},
	}
	data, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}

	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			var output bytes.Buffer
			if err := dump([]string{"--format=" + format, "-"}, &output, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), "resource_profiles") && !strings.Contains(output.String(), "resourceProfiles") {
				t.Fatalf("dump output missing profiles: %s", output.String())
			}
		})
	}
}

func TestDumpRejectsInvalidInput(t *testing.T) {
	var output bytes.Buffer
	err := dump([]string{"-"}, &output, strings.NewReader("not protobuf"))
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("dump error = %v, want decode error", err)
	}
}

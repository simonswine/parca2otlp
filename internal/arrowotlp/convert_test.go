package arrowotlp

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestConvertV1(t *testing.T) {
	allocator := memory.NewGoAllocator()
	sample := v1Sample(t, allocator)
	defer sample.Release()
	locations := v1LocationsRecord(t, allocator)
	defer locations.Release()

	export, err := ConvertV1(sample, []Record{locations})
	if err != nil {
		t.Fatal(err)
	}
	if len(export.ResourceProfiles) != 1 {
		t.Fatalf("resource profiles = %d, want 1", len(export.ResourceProfiles))
	}
	profile := export.ResourceProfiles[0].ScopeProfiles[0].Profiles[0]
	if got := profile.Samples[0].Values; len(got) != 1 || got[0] != 7 {
		t.Fatalf("sample values = %v, want [7]", got)
	}
	if got := export.Dictionary.StringTable[profile.SampleType.TypeStrindex]; got != "samples" {
		t.Fatalf("sample type = %q, want samples", got)
	}
	if len(export.Dictionary.StackTable) != 2 {
		t.Fatalf("stacks = %d, want zero plus one", len(export.Dictionary.StackTable))
	}
}

func v1Sample(t *testing.T, allocator memory.Allocator) Record {
	t.Helper()
	fields := []arrow.Field{
		{Name: "stacktrace_id", Type: arrow.BinaryTypes.Binary}, {Name: "value", Type: arrow.PrimitiveTypes.Int64},
		{Name: "producer", Type: arrow.BinaryTypes.Binary}, {Name: "sample_type", Type: arrow.BinaryTypes.Binary}, {Name: "sample_unit", Type: arrow.BinaryTypes.Binary},
		{Name: "period_type", Type: arrow.BinaryTypes.Binary}, {Name: "period_unit", Type: arrow.BinaryTypes.Binary}, {Name: "period", Type: arrow.PrimitiveTypes.Int64},
		{Name: "duration", Type: arrow.PrimitiveTypes.Int64}, {Name: "timestamp", Type: arrow.PrimitiveTypes.Int64}, {Name: "labels.job", Type: arrow.BinaryTypes.Binary},
	}
	arrays := []arrow.Array{binaryArray(allocator, "trace"), intArray(allocator, 7), binaryArray(allocator, "agent"), binaryArray(allocator, "samples"), binaryArray(allocator, "count"), binaryArray(allocator, "cpu"), binaryArray(allocator, "nanoseconds"), intArray(allocator, 100), intArray(allocator, 10), intArray(allocator, 1_000), binaryArray(allocator, "test")}
	metadata := arrow.NewMetadata([]string{schemaVersionKey}, []string{"v1"})
	return array.NewRecordBatch(arrow.NewSchema(fields, &metadata), arrays, 1)
}

func v1LocationsRecord(t *testing.T, allocator memory.Allocator) Record {
	t.Helper()
	listType := arrow.ListOf(arrow.StructOf(arrow.Field{Name: "address", Type: arrow.PrimitiveTypes.Uint64}))
	builder := array.NewBuilder(allocator, listType).(*array.ListBuilder)
	structs := builder.ValueBuilder().(*array.StructBuilder)
	addresses := structs.FieldBuilder(0).(*array.Uint64Builder)
	builder.Append(true)
	structs.Append(true)
	addresses.Append(42)
	locations := builder.NewArray()
	builder.Release()
	metadata := arrow.NewMetadata([]string{schemaVersionKey}, []string{"v1"})
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "stacktrace_id", Type: arrow.BinaryTypes.Binary}, {Name: "locations", Type: listType}}, &metadata), []arrow.Array{binaryArray(allocator, "trace"), locations}, 1)
}

func binaryArray(allocator memory.Allocator, value string) arrow.Array {
	builder := array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.AppendString(value)
	return builder.NewArray()
}

func intArray(allocator memory.Allocator, value int64) arrow.Array {
	builder := array.NewInt64Builder(allocator)
	defer builder.Release()
	builder.Append(value)
	return builder.NewArray()
}

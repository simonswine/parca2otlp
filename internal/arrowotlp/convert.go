package arrowotlp

import (
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	collectorprofiles "go.opentelemetry.io/proto/otlp/collector/profiles/v1development"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
)

const schemaVersionKey = "parca_write_schema_version"

type Record = arrow.RecordBatch

type location struct {
	address   uint64
	mapping   mapping
	lines     []line
	frameType string
}

type mapping struct {
	start, limit, offset uint64
	file, buildID        string
}

type line struct {
	line, column int64
	name         string
	systemName   string
	filename     string
	startLine    int64
}

type row struct {
	labels                              map[string]string
	stack                               []location
	value, timestamp, duration, period  int64
	producer, sampleType, sampleUnit    string
	periodType, periodUnit, temporality string
}

func ConvertV1(samples Record, locationRecords []Record) (*collectorprofiles.ExportProfilesServiceRequest, error) {
	if err := requireVersion(samples, "v1"); err != nil {
		return nil, err
	}
	locations, err := v1Locations(locationRecords)
	if err != nil {
		return nil, err
	}
	rows, err := v1Rows(samples, locations)
	if err != nil {
		return nil, err
	}
	return buildExport(rows), nil
}

func ConvertV2(records []Record) (*collectorprofiles.ExportProfilesServiceRequest, error) {
	var rows []row
	for _, record := range records {
		if err := requireVersion(record, "v2"); err != nil {
			return nil, err
		}
		converted, err := v2Rows(record)
		if err != nil {
			return nil, err
		}
		rows = append(rows, converted...)
	}
	return buildExport(rows), nil
}

func StacktraceIDs(record Record, _ memory.Allocator) ([][]byte, error) {
	if err := requireVersion(record, "v1"); err != nil {
		return nil, err
	}
	column, err := column(record, "stacktrace_id")
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var ids [][]byte
	for i := 0; i < column.Len(); i++ {
		value, ok := bytesAt(column, i)
		if !ok {
			return nil, fmt.Errorf("stacktrace_id at row %d is not binary", i)
		}
		if _, ok := seen[string(value)]; !ok {
			seen[string(value)] = struct{}{}
			ids = append(ids, value)
		}
	}
	return ids, nil
}

func StacktraceRequest(ids [][]byte, allocator memory.Allocator) (Record, error) {
	builder := array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	for _, id := range ids {
		builder.Append(id)
	}
	values := builder.NewArray()
	metadata := arrow.NewMetadata([]string{schemaVersionKey}, []string{"v1"})
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "stacktrace_id", Type: arrow.BinaryTypes.Binary}}, &metadata), []arrow.Array{values}, int64(len(ids))), nil
}

func v1Locations(records []Record) (map[string][]location, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("missing locations record")
	}
	result := map[string][]location{}
	for _, record := range records {
		if err := requireVersion(record, "v1"); err != nil {
			return nil, err
		}
		ids, err := column(record, "stacktrace_id")
		if err != nil {
			return nil, err
		}
		locations, err := column(record, "locations")
		if err != nil {
			return nil, err
		}
		list, ok := locations.(*array.List)
		if !ok {
			return nil, fmt.Errorf("locations must be a List, got %T", locations)
		}
		structs, ok := list.ListValues().(*array.Struct)
		if !ok {
			return nil, fmt.Errorf("locations values must be Struct, got %T", list.ListValues())
		}
		for i := 0; i < ids.Len(); i++ {
			id, ok := bytesAt(ids, i)
			if !ok {
				return nil, fmt.Errorf("invalid stacktrace_id at row %d", i)
			}
			start, end := list.ValueOffsets(i)
			stack := make([]location, 0, end-start)
			for j := start; j < end; j++ {
				location, err := decodeLocation(structs, int(j), true)
				if err != nil {
					return nil, fmt.Errorf("decode stacktrace %x: %w", id, err)
				}
				stack = append(stack, location)
			}
			result[string(id)] = stack
		}
	}
	return result, nil
}

func v1Rows(record Record, locations map[string][]location) ([]row, error) {
	required := []string{"stacktrace_id", "value", "producer", "sample_type", "sample_unit", "period_type", "period_unit", "period", "duration", "timestamp"}
	for _, name := range required {
		if _, err := column(record, name); err != nil {
			return nil, err
		}
	}
	result := make([]row, 0, record.NumRows())
	for i := 0; i < int(record.NumRows()); i++ {
		id, ok := bytesAt(mustColumn(record, "stacktrace_id"), i)
		if !ok {
			return nil, fmt.Errorf("invalid stacktrace_id at row %d", i)
		}
		stack, ok := locations[string(id)]
		if !ok {
			return nil, fmt.Errorf("locations missing for stacktrace %x", id)
		}
		row, err := decodeCommonRow(record, i)
		if err != nil {
			return nil, err
		}
		row.stack = stack
		result = append(result, row)
	}
	return result, nil
}

func v2Rows(record Record) ([]row, error) {
	stacktraces, err := column(record, "stacktrace")
	if err != nil {
		return nil, err
	}
	stacks, ok := stacktraces.(*array.ListView)
	if !ok {
		return nil, fmt.Errorf("stacktrace must be a ListView, got %T", stacktraces)
	}
	dictionary, ok := stacks.ListValues().(*array.Dictionary)
	if !ok {
		return nil, fmt.Errorf("stacktrace values must be a Dictionary, got %T", stacks.ListValues())
	}
	locations, ok := dictionary.Dictionary().(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("stacktrace dictionary values must be Struct, got %T", dictionary.Dictionary())
	}
	result := make([]row, 0, record.NumRows())
	for i := 0; i < int(record.NumRows()); i++ {
		row, err := decodeCommonRow(record, i)
		if err != nil {
			return nil, err
		}
		start, end := stacks.ValueOffsets(i)
		row.stack = make([]location, 0, end-start)
		for j := start; j < end; j++ {
			index := dictionary.GetValueIndex(int(j))
			location, err := decodeLocation(locations, index, false)
			if err != nil {
				return nil, fmt.Errorf("decode stacktrace at row %d: %w", i, err)
			}
			row.stack = append(row.stack, location)
		}
		result = append(result, row)
	}
	return result, nil
}

func decodeCommonRow(record Record, index int) (row, error) {
	getString := func(name string) (string, error) {
		value, ok := stringAt(mustColumn(record, name), index)
		if !ok {
			return "", fmt.Errorf("invalid %s at row %d", name, index)
		}
		return value, nil
	}
	getInt := func(name string) (int64, error) {
		value, ok := intAt(mustColumn(record, name), index)
		if !ok {
			return 0, fmt.Errorf("invalid %s at row %d", name, index)
		}
		return value, nil
	}
	for _, name := range []string{"value", "producer", "sample_type", "sample_unit", "period_type", "period_unit", "period", "duration", "timestamp"} {
		if _, err := column(record, name); err != nil {
			return row{}, err
		}
	}
	value, err := getInt("value")
	if err != nil {
		return row{}, err
	}
	timestamp, err := getInt("timestamp")
	if err != nil {
		return row{}, err
	}
	duration, err := getInt("duration")
	if err != nil {
		return row{}, err
	}
	period, err := getInt("period")
	if err != nil {
		return row{}, err
	}
	producer, err := getString("producer")
	if err != nil {
		return row{}, err
	}
	sampleType, err := getString("sample_type")
	if err != nil {
		return row{}, err
	}
	sampleUnit, err := getString("sample_unit")
	if err != nil {
		return row{}, err
	}
	periodType, err := getString("period_type")
	if err != nil {
		return row{}, err
	}
	periodUnit, err := getString("period_unit")
	if err != nil {
		return row{}, err
	}
	labels := map[string]string{}
	for fieldIndex, field := range record.Schema().Fields() {
		if strings.HasPrefix(field.Name, "labels.") {
			if value, ok := stringAt(record.Column(fieldIndex), index); ok {
				labels[strings.TrimPrefix(field.Name, "labels.")] = value
			}
		}
	}
	if labelsArray, err := column(record, "labels"); err == nil {
		if structLabels, ok := labelsArray.(*array.Struct); ok {
			for i, field := range structLabels.DataType().(*arrow.StructType).Fields() {
				if value, ok := stringAt(structLabels.Field(i), index); ok {
					labels[field.Name] = value
				}
			}
		}
	}
	temporality := ""
	if temporalityColumn, err := column(record, "temporality"); err == nil {
		temporality, _ = stringAt(temporalityColumn, index)
	}
	return row{labels: labels, value: value, timestamp: timestamp, duration: duration, period: period, producer: producer, sampleType: sampleType, sampleUnit: sampleUnit, periodType: periodType, periodUnit: periodUnit, temporality: temporality}, nil
}

func buildExport(rows []row) *collectorprofiles.ExportProfilesServiceRequest {
	builder := newDictionary()
	groups := map[string]*profiles.Profile{}
	keys := make([]string, 0)
	for _, row := range rows {
		key := strings.Join([]string{row.producer, row.sampleType, row.sampleUnit, row.periodType, row.periodUnit, fmt.Sprint(row.period), row.temporality}, "\x00")
		profile, ok := groups[key]
		if !ok {
			profile = &profiles.Profile{SampleType: &profiles.ValueType{TypeStrindex: builder.string(row.sampleType), UnitStrindex: builder.string(row.sampleUnit)}, PeriodType: &profiles.ValueType{TypeStrindex: builder.string(row.periodType), UnitStrindex: builder.string(row.periodUnit)}, Period: row.period}
			if row.temporality != "" {
				profile.AttributeIndices = append(profile.AttributeIndices, builder.attribute("parca.temporality", row.temporality))
			}
			groups[key] = profile
			keys = append(keys, key)
		}
		oldEnd := profile.TimeUnixNano + profile.DurationNano
		if profile.TimeUnixNano == 0 || uint64(row.timestamp) < profile.TimeUnixNano {
			profile.TimeUnixNano = uint64(row.timestamp)
		}
		end := uint64(row.timestamp + row.duration)
		if oldEnd > end {
			end = oldEnd
		}
		if end > profile.TimeUnixNano {
			profile.DurationNano = end - profile.TimeUnixNano
		}
		attributes := make([]int32, 0, len(row.labels)+1)
		attributes = append(attributes, builder.attribute("parca.producer", row.producer))
		labelNames := make([]string, 0, len(row.labels))
		for name := range row.labels {
			labelNames = append(labelNames, name)
		}
		sort.Strings(labelNames)
		for _, name := range labelNames {
			attributes = append(attributes, builder.attribute(name, row.labels[name]))
		}
		profile.Samples = append(profile.Samples, &profiles.Sample{StackIndex: builder.stack(row.stack), AttributeIndices: attributes, Values: []int64{row.value}, TimestampsUnixNano: []uint64{uint64(row.timestamp)}})
	}
	sort.Strings(keys)
	scopeProfiles := &profiles.ScopeProfiles{}
	for _, key := range keys {
		scopeProfiles.Profiles = append(scopeProfiles.Profiles, groups[key])
	}
	if len(scopeProfiles.Profiles) == 0 {
		return &collectorprofiles.ExportProfilesServiceRequest{Dictionary: builder.dictionary}
	}
	return &collectorprofiles.ExportProfilesServiceRequest{ResourceProfiles: []*profiles.ResourceProfiles{{ScopeProfiles: []*profiles.ScopeProfiles{scopeProfiles}}}, Dictionary: builder.dictionary}
}

func requireVersion(record Record, expected string) error {
	version, ok := record.Schema().Metadata().GetValue(schemaVersionKey)
	if !ok {
		return fmt.Errorf("missing %q schema metadata", schemaVersionKey)
	}
	if version != expected {
		return fmt.Errorf("unsupported Arrow schema version %q, expected %q", version, expected)
	}
	return nil
}

func column(record Record, name string) (arrow.Array, error) {
	indices := record.Schema().FieldIndices(name)
	if len(indices) != 1 {
		return nil, fmt.Errorf("missing required column %q", name)
	}
	return record.Column(indices[0]), nil
}

func mustColumn(record Record, name string) arrow.Array {
	value, _ := column(record, name)
	return value
}

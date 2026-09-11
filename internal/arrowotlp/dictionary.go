package arrowotlp

import (
	"fmt"
	"strings"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	profiles "go.opentelemetry.io/proto/otlp/profiles/v1development"
)

type dictionary struct {
	dictionary *profiles.ProfilesDictionary
	strings    map[string]int32
	attributes map[string]int32
	mappings   map[string]int32
	functions  map[string]int32
	locations  map[string]int32
	stacks     map[string]int32
}

func newDictionary() *dictionary {
	return &dictionary{
		dictionary: &profiles.ProfilesDictionary{StringTable: []string{""}, MappingTable: []*profiles.Mapping{{}}, LocationTable: []*profiles.Location{{}}, FunctionTable: []*profiles.Function{{}}, AttributeTable: []*profiles.KeyValueAndUnit{{}}, StackTable: []*profiles.Stack{{}}, LinkTable: []*profiles.Link{{}}},
		strings:    map[string]int32{"": 0}, attributes: map[string]int32{}, mappings: map[string]int32{}, functions: map[string]int32{}, locations: map[string]int32{}, stacks: map[string]int32{},
	}
}

func (d *dictionary) string(value string) int32 {
	if index, ok := d.strings[value]; ok {
		return index
	}
	index := int32(len(d.dictionary.StringTable))
	d.strings[value] = index
	d.dictionary.StringTable = append(d.dictionary.StringTable, value)
	return index
}

func (d *dictionary) attribute(key, value string) int32 {
	id := key + "\x00" + value
	if index, ok := d.attributes[id]; ok {
		return index
	}
	index := int32(len(d.dictionary.AttributeTable))
	d.attributes[id] = index
	d.dictionary.AttributeTable = append(d.dictionary.AttributeTable, &profiles.KeyValueAndUnit{KeyStrindex: d.string(key), Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: value}}})
	return index
}

func (d *dictionary) mapping(value mapping) int32 {
	if value == (mapping{}) {
		return 0
	}
	id := fmt.Sprintf("%d\x00%d\x00%d\x00%s\x00%s", value.start, value.limit, value.offset, value.file, value.buildID)
	if index, ok := d.mappings[id]; ok {
		return index
	}
	index := int32(len(d.dictionary.MappingTable))
	d.mappings[id] = index
	attributes := []int32{}
	if value.buildID != "" {
		attributes = append(attributes, d.attribute("parca.mapping.build_id", value.buildID))
	}
	d.dictionary.MappingTable = append(d.dictionary.MappingTable, &profiles.Mapping{MemoryStart: value.start, MemoryLimit: value.limit, FileOffset: value.offset, FilenameStrindex: d.string(value.file), AttributeIndices: attributes})
	return index
}

func (d *dictionary) function(value line) int32 {
	if value.name == "" && value.systemName == "" && value.filename == "" && value.startLine == 0 {
		return 0
	}
	id := strings.Join([]string{value.name, value.systemName, value.filename, fmt.Sprint(value.startLine)}, "\x00")
	if index, ok := d.functions[id]; ok {
		return index
	}
	index := int32(len(d.dictionary.FunctionTable))
	d.functions[id] = index
	d.dictionary.FunctionTable = append(d.dictionary.FunctionTable, &profiles.Function{NameStrindex: d.string(value.name), SystemNameStrindex: d.string(value.systemName), FilenameStrindex: d.string(value.filename), StartLine: value.startLine})
	return index
}

func (d *dictionary) location(value location) int32 {
	lineIDs := make([]int32, 0, len(value.lines))
	keyParts := []string{fmt.Sprint(value.address), fmt.Sprint(d.mapping(value.mapping)), value.frameType}
	for _, line := range value.lines {
		function := d.function(line)
		lineIDs = append(lineIDs, function)
		keyParts = append(keyParts, fmt.Sprintf("%d:%d:%d", function, line.line, line.column))
	}
	id := strings.Join(keyParts, "\x00")
	if index, ok := d.locations[id]; ok {
		return index
	}
	index := int32(len(d.dictionary.LocationTable))
	d.locations[id] = index
	lines := make([]*profiles.Line, 0, len(value.lines))
	for i, line := range value.lines {
		lines = append(lines, &profiles.Line{FunctionIndex: lineIDs[i], Line: line.line, Column: line.column})
	}
	attributes := []int32{}
	if value.frameType != "" {
		attributes = append(attributes, d.attribute("parca.frame_type", value.frameType))
	}
	d.dictionary.LocationTable = append(d.dictionary.LocationTable, &profiles.Location{MappingIndex: d.mapping(value.mapping), Address: value.address, Lines: lines, AttributeIndices: attributes})
	return index
}

func (d *dictionary) stack(values []location) int32 {
	ids := make([]int32, 0, len(values))
	keys := make([]string, 0, len(values))
	for _, value := range values {
		index := d.location(value)
		ids = append(ids, index)
		keys = append(keys, fmt.Sprint(index))
	}
	id := strings.Join(keys, ",")
	if index, ok := d.stacks[id]; ok {
		return index
	}
	index := int32(len(d.dictionary.StackTable))
	d.stacks[id] = index
	d.dictionary.StackTable = append(d.dictionary.StackTable, &profiles.Stack{LocationIndices: ids})
	return index
}

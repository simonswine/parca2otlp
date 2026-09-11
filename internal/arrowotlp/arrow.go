package arrowotlp

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func physicalIndex(values arrow.Array, index int) (arrow.Array, int, bool) {
	if ree, ok := values.(*array.RunEndEncoded); ok {
		runEnds, ok := ree.RunEndsArr().(*array.Int32)
		if !ok {
			return nil, 0, false
		}
		for i := 0; i < runEnds.Len(); i++ {
			if index < int(runEnds.Value(i)) {
				return ree.Values(), i, !ree.IsNull(index)
			}
		}
		return nil, 0, false
	}
	return values, index, !values.IsNull(index)
}

func stringAt(values arrow.Array, index int) (string, bool) {
	values, index, ok := physicalIndex(values, index)
	if !ok {
		return "", false
	}
	switch values := values.(type) {
	case *array.String:
		return values.Value(index), true
	case *array.StringView:
		return values.Value(index), true
	case *array.Binary:
		return string(values.Value(index)), true
	case *array.Dictionary:
		return stringAt(values.Dictionary(), values.GetValueIndex(index))
	default:
		return "", false
	}
}

func bytesAt(values arrow.Array, index int) ([]byte, bool) {
	values, index, ok := physicalIndex(values, index)
	if !ok {
		return nil, false
	}
	switch values := values.(type) {
	case *array.Binary:
		return values.Value(index), true
	case *array.FixedSizeBinary:
		return values.Value(index), true
	case *array.Dictionary:
		return bytesAt(values.Dictionary(), values.GetValueIndex(index))
	default:
		return nil, false
	}
}

func intAt(values arrow.Array, index int) (int64, bool) {
	values, index, ok := physicalIndex(values, index)
	if !ok {
		return 0, false
	}
	switch values := values.(type) {
	case *array.Int64:
		return values.Value(index), true
	case *array.Uint64:
		return int64(values.Value(index)), true
	case *array.Timestamp:
		return int64(values.Value(index)), true
	default:
		return 0, false
	}
}

func decodeLocation(values *array.Struct, index int, v1 bool) (location, error) {
	field := func(name string) (arrow.Array, bool) {
		indices := values.DataType().(*arrow.StructType).FieldIndices(name)
		if len(indices) != 1 {
			return nil, false
		}
		return values.Field(indices[0]), true
	}
	addressValues, ok := field("address")
	if !ok {
		return location{}, fmt.Errorf("location missing address")
	}
	address, ok := intAt(addressValues, index)
	if !ok {
		return location{}, fmt.Errorf("location address is invalid")
	}
	result := location{address: uint64(address)}
	if frame, ok := field("frame_type"); ok {
		result.frameType, _ = stringAt(frame, index)
	}
	if start, ok := field("mapping_start"); ok {
		value, valid := intAt(start, index)
		if valid {
			result.mapping.start = uint64(value)
		}
	}
	if limit, ok := field("mapping_limit"); ok {
		value, valid := intAt(limit, index)
		if valid {
			result.mapping.limit = uint64(value)
		}
	}
	if offset, ok := field("mapping_offset"); ok {
		value, valid := intAt(offset, index)
		if valid {
			result.mapping.offset = uint64(value)
		}
	}
	if file, ok := field("mapping_file"); ok {
		result.mapping.file, _ = stringAt(file, index)
	}
	if buildID, ok := field("mapping_build_id"); ok {
		result.mapping.buildID, _ = stringAt(buildID, index)
	}
	lines, ok := field("lines")
	if !ok || lines.IsNull(index) {
		return result, nil
	}
	if v1 {
		return decodeV1Lines(lines, index, result)
	}
	return decodeV2Lines(lines, index, result)
}

func decodeV1Lines(values arrow.Array, index int, result location) (location, error) {
	list, ok := values.(*array.List)
	if !ok {
		return result, fmt.Errorf("location lines must be List, got %T", values)
	}
	items, ok := list.ListValues().(*array.Struct)
	if !ok {
		return result, fmt.Errorf("location line values must be Struct")
	}
	start, end := list.ValueOffsets(index)
	for i := start; i < end; i++ {
		line, err := decodeLine(items, int(i), false)
		if err != nil {
			return result, err
		}
		result.lines = append(result.lines, line)
	}
	return result, nil
}

func decodeV2Lines(values arrow.Array, index int, result location) (location, error) {
	list, ok := values.(*array.ListView)
	if !ok {
		return result, fmt.Errorf("location lines must be ListView, got %T", values)
	}
	items, ok := list.ListValues().(*array.Struct)
	if !ok {
		return result, fmt.Errorf("location line values must be Struct")
	}
	start, end := list.ValueOffsets(index)
	for i := start; i < end; i++ {
		line, err := decodeLine(items, int(i), true)
		if err != nil {
			return result, err
		}
		result.lines = append(result.lines, line)
	}
	return result, nil
}

func decodeLine(values *array.Struct, index int, v2 bool) (line, error) {
	field := func(name string) (arrow.Array, bool) {
		indices := values.DataType().(*arrow.StructType).FieldIndices(name)
		if len(indices) != 1 {
			return nil, false
		}
		return values.Field(indices[0]), true
	}
	result := line{}
	if number, ok := field("line"); ok {
		result.line, _ = intAt(number, index)
	}
	if column, ok := field("column"); ok {
		result.column, _ = intAt(column, index)
	}
	if !v2 {
		if value, ok := field("function_name"); ok {
			result.name, _ = stringAt(value, index)
		}
		if value, ok := field("function_system_name"); ok {
			result.systemName, _ = stringAt(value, index)
		}
		if value, ok := field("function_filename"); ok {
			result.filename, _ = stringAt(value, index)
		}
		if value, ok := field("function_start_line"); ok {
			result.startLine, _ = intAt(value, index)
		}
		return result, nil
	}
	function, ok := field("function")
	if !ok || function.IsNull(index) {
		return result, nil
	}
	dictionary, ok := function.(*array.Dictionary)
	if !ok {
		return result, fmt.Errorf("line function must be Dictionary, got %T", function)
	}
	structs, ok := dictionary.Dictionary().(*array.Struct)
	if !ok {
		return result, fmt.Errorf("line function dictionary values must be Struct")
	}
	functionIndex := dictionary.GetValueIndex(index)
	functionField := func(name string) (arrow.Array, bool) {
		indices := structs.DataType().(*arrow.StructType).FieldIndices(name)
		if len(indices) != 1 {
			return nil, false
		}
		return structs.Field(indices[0]), true
	}
	if value, ok := functionField("name"); ok {
		result.name, _ = stringAt(value, functionIndex)
	}
	if value, ok := functionField("system_name"); ok {
		result.systemName, _ = stringAt(value, functionIndex)
	}
	if value, ok := functionField("filename"); ok {
		result.filename, _ = stringAt(value, functionIndex)
	}
	if value, ok := functionField("start_line"); ok {
		result.startLine, _ = intAt(value, functionIndex)
	}
	return result, nil
}

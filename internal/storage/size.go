package storage

import (
	"fmt"
	"strconv"
	"strings"
)

func ParseSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "0" {
		return 0, nil
	}
	for _, unit := range []struct {
		suffix string
		size   int64
	}{
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"B", 1},
	} {
		if !strings.HasSuffix(value, unit.suffix) {
			continue
		}
		amount, err := strconv.ParseInt(strings.TrimSuffix(value, unit.suffix), 10, 64)
		if err != nil || amount < 0 || amount > (1<<63-1)/unit.size {
			return 0, fmt.Errorf("invalid storage size %q", value)
		}
		return amount * unit.size, nil
	}
	amount, err := strconv.ParseInt(value, 10, 64)
	if err != nil || amount < 0 {
		return 0, fmt.Errorf("invalid storage size %q", value)
	}
	return amount, nil
}

package domain

import (
	"sort"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

const maxAttributeDepth = 16

func attributeBytes(attrs []*commonpb.KeyValue) ([]byte, error) {
	normalized, err := normalizeAttrs(attrs, 0)
	if err != nil {
		return nil, err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&commonpb.KeyValueList{Values: normalized})
	if err != nil {
		return nil, invalid("invalid attribute encoding")
	}
	return encoded, nil
}

func normalizeAttrs(attrs []*commonpb.KeyValue, depth int) ([]*commonpb.KeyValue, error) {
	if depth > maxAttributeDepth {
		return nil, invalid("metric attribute nesting is too deep")
	}
	normalized := make([]*commonpb.KeyValue, 0, len(attrs))
	seen := make(map[string]struct{}, len(attrs))
	for _, attr := range attrs {
		key := attr.GetKey()
		if key == "" || !utf8.ValidString(key) {
			return nil, invalid("empty or malformed metric attribute name")
		}
		if _, ok := seen[key]; ok {
			return nil, invalid("duplicate metric attribute name")
		}
		seen[key] = struct{}{}
		val, err := normalizeValue(attr.GetValue(), depth)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, &commonpb.KeyValue{Key: key, Value: val})
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].GetKey() < normalized[j].GetKey() })
	return normalized, nil
}

func normalizeValue(val *commonpb.AnyValue, depth int) (*commonpb.AnyValue, error) {
	if depth > maxAttributeDepth {
		return nil, invalid("metric attribute nesting is too deep")
	}
	switch v := val.GetValue().(type) {
	case nil:
		// OTLP explicitly permits an empty AnyValue.
		return &commonpb.AnyValue{}, nil
	case *commonpb.AnyValue_KvlistValue:
		fields, err := normalizeAttrs(v.KvlistValue.GetValues(), depth+1)
		if err != nil {
			return nil, err
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: fields}}}, nil
	case *commonpb.AnyValue_ArrayValue:
		values := make([]*commonpb.AnyValue, 0, len(v.ArrayValue.GetValues()))
		for _, item := range v.ArrayValue.GetValues() {
			normalized, err := normalizeValue(item, depth+1)
			if err != nil {
				return nil, err
			}
			values = append(values, normalized)
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: values}}}, nil
	case *commonpb.AnyValue_StringValueStrindex:
		return nil, invalid("indexed strings require an OTLP profiles dictionary")
	default:
		return val, nil
	}
}

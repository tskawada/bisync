package grpc

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/encoding"
)

func init() {
	// Override the default protobuf codec with JSON so we don't need protoc.
	encoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("jsonCodec marshal: %w", err)
	}
	return b, nil
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("jsonCodec unmarshal: %w", err)
	}
	return nil
}

func (jsonCodec) Name() string { return "proto" }

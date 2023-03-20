package events

import (
	"go-micro.dev/v4/codec/json"
	"go-micro.dev/v4/codec/proto"

	"go-micro.dev/v4/codec"
)

var globalUnpackers []PayloadUnpacker

func init() {
	globalUnpackers = []PayloadUnpacker{
		JsonUnpacker(),
		ProtoUnpacker(),
	}
}

func RegisterPayloadUnpacker(unpackers ...PayloadUnpacker) {
	globalUnpackers = append(globalUnpackers, unpackers...)
}

type Payloader interface {
	Payload() ([]byte, error)
	Encoding() string
}

type PayloadUnpacker interface {
	Unpack(payload []byte, data any) error
	Encoding() string
}

type payloadUnpacker struct {
	marshaler codec.Marshaler
	encoding  string
}

func JsonUnpacker() PayloadUnpacker {
	return &payloadUnpacker{marshaler: json.Marshaler{}, encoding: EncodingJson}
}

func ProtoUnpacker() PayloadUnpacker {
	return &payloadUnpacker{marshaler: proto.Marshaler{}, encoding: EncodingProto}
}

func (u payloadUnpacker) Unpack(payload []byte, data any) error {
	return u.marshaler.Unmarshal(payload, data)
}

func (u payloadUnpacker) Encoding() string {
	return u.encoding
}

type payloader struct {
	data      any
	encoding  string
	marshaler codec.Marshaler
}

func (p payloader) Encoding() string {
	return p.encoding
}

func (p payloader) Payload() ([]byte, error) {
	return p.marshaler.Marshal(p.data)
}

func Json(data any) Payloader {
	return payloader{data: data, marshaler: json.Marshaler{}, encoding: EncodingJson}
}

func Proto(data any) Payloader {
	return payloader{data: data, marshaler: proto.Marshaler{}, encoding: EncodingProto}
}

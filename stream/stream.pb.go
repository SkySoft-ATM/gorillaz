// Code generated by protoc-gen-go. DO NOT EDIT.
// source: stream.proto

package stream

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	_go "github.com/prometheus/client_model/go"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type EventType int32

const (
	EventType_UNKNOWN_EVENT_TYPE EventType = 0
	EventType_UPDATE             EventType = 1
	EventType_INITIAL_STATE      EventType = 2
	EventType_DELETE             EventType = 3
)

var EventType_name = map[int32]string{
	0: "UNKNOWN_EVENT_TYPE",
	1: "UPDATE",
	2: "INITIAL_STATE",
	3: "DELETE",
}

var EventType_value = map[string]int32{
	"UNKNOWN_EVENT_TYPE": 0,
	"UPDATE":             1,
	"INITIAL_STATE":      2,
	"DELETE":             3,
}

func (x EventType) String() string {
	return proto.EnumName(EventType_name, int32(x))
}

func (EventType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{0}
}

type StreamType int32

const (
	StreamType_UNKNOWN_STREAM_TYPE StreamType = 0
	StreamType_STREAM              StreamType = 1
	StreamType_GET_AND_WATCH       StreamType = 2
)

var StreamType_name = map[int32]string{
	0: "UNKNOWN_STREAM_TYPE",
	1: "STREAM",
	2: "GET_AND_WATCH",
}

var StreamType_value = map[string]int32{
	"UNKNOWN_STREAM_TYPE": 0,
	"STREAM":              1,
	"GET_AND_WATCH":       2,
}

func (x StreamType) String() string {
	return proto.EnumName(StreamType_name, int32(x))
}

func (StreamType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{1}
}

type StreamRequest struct {
	Name                     string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	RequesterName            string   `protobuf:"bytes,2,opt,name=requesterName,proto3" json:"requesterName,omitempty"`
	ExpectHello              bool     `protobuf:"varint,3,opt,name=expectHello,proto3" json:"expectHello,omitempty"`
	DisconnectOnBackpressure bool     `protobuf:"varint,4,opt,name=disconnect_on_backpressure,json=disconnectOnBackpressure,proto3" json:"disconnect_on_backpressure,omitempty"`
	XXX_NoUnkeyedLiteral     struct{} `json:"-"`
	XXX_unrecognized         []byte   `json:"-"`
	XXX_sizecache            int32    `json:"-"`
}

func (m *StreamRequest) Reset()         { *m = StreamRequest{} }
func (m *StreamRequest) String() string { return proto.CompactTextString(m) }
func (*StreamRequest) ProtoMessage()    {}
func (*StreamRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{0}
}

func (m *StreamRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StreamRequest.Unmarshal(m, b)
}
func (m *StreamRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StreamRequest.Marshal(b, m, deterministic)
}
func (m *StreamRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamRequest.Merge(m, src)
}
func (m *StreamRequest) XXX_Size() int {
	return xxx_messageInfo_StreamRequest.Size(m)
}
func (m *StreamRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamRequest.DiscardUnknown(m)
}

var xxx_messageInfo_StreamRequest proto.InternalMessageInfo

func (m *StreamRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *StreamRequest) GetRequesterName() string {
	if m != nil {
		return m.RequesterName
	}
	return ""
}

func (m *StreamRequest) GetExpectHello() bool {
	if m != nil {
		return m.ExpectHello
	}
	return false
}

func (m *StreamRequest) GetDisconnectOnBackpressure() bool {
	if m != nil {
		return m.DisconnectOnBackpressure
	}
	return false
}

type GetAndWatchRequest struct {
	Name                     string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	RequesterName            string   `protobuf:"bytes,2,opt,name=requesterName,proto3" json:"requesterName,omitempty"`
	ExpectHello              bool     `protobuf:"varint,3,opt,name=expectHello,proto3" json:"expectHello,omitempty"`
	DisconnectOnBackpressure bool     `protobuf:"varint,4,opt,name=disconnect_on_backpressure,json=disconnectOnBackpressure,proto3" json:"disconnect_on_backpressure,omitempty"`
	XXX_NoUnkeyedLiteral     struct{} `json:"-"`
	XXX_unrecognized         []byte   `json:"-"`
	XXX_sizecache            int32    `json:"-"`
}

func (m *GetAndWatchRequest) Reset()         { *m = GetAndWatchRequest{} }
func (m *GetAndWatchRequest) String() string { return proto.CompactTextString(m) }
func (*GetAndWatchRequest) ProtoMessage()    {}
func (*GetAndWatchRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{1}
}

func (m *GetAndWatchRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetAndWatchRequest.Unmarshal(m, b)
}
func (m *GetAndWatchRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetAndWatchRequest.Marshal(b, m, deterministic)
}
func (m *GetAndWatchRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetAndWatchRequest.Merge(m, src)
}
func (m *GetAndWatchRequest) XXX_Size() int {
	return xxx_messageInfo_GetAndWatchRequest.Size(m)
}
func (m *GetAndWatchRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetAndWatchRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetAndWatchRequest proto.InternalMessageInfo

func (m *GetAndWatchRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *GetAndWatchRequest) GetRequesterName() string {
	if m != nil {
		return m.RequesterName
	}
	return ""
}

func (m *GetAndWatchRequest) GetExpectHello() bool {
	if m != nil {
		return m.ExpectHello
	}
	return false
}

func (m *GetAndWatchRequest) GetDisconnectOnBackpressure() bool {
	if m != nil {
		return m.DisconnectOnBackpressure
	}
	return false
}

type StreamEvent struct {
	Key                  []byte    `protobuf:"bytes,1,opt,name=Key,json=key,proto3" json:"Key,omitempty"`
	Value                []byte    `protobuf:"bytes,2,opt,name=Value,json=value,proto3" json:"Value,omitempty"`
	Metadata             *Metadata `protobuf:"bytes,3,opt,name=metadata,proto3" json:"metadata,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *StreamEvent) Reset()         { *m = StreamEvent{} }
func (m *StreamEvent) String() string { return proto.CompactTextString(m) }
func (*StreamEvent) ProtoMessage()    {}
func (*StreamEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{2}
}

func (m *StreamEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StreamEvent.Unmarshal(m, b)
}
func (m *StreamEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StreamEvent.Marshal(b, m, deterministic)
}
func (m *StreamEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamEvent.Merge(m, src)
}
func (m *StreamEvent) XXX_Size() int {
	return xxx_messageInfo_StreamEvent.Size(m)
}
func (m *StreamEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamEvent.DiscardUnknown(m)
}

var xxx_messageInfo_StreamEvent proto.InternalMessageInfo

func (m *StreamEvent) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *StreamEvent) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *StreamEvent) GetMetadata() *Metadata {
	if m != nil {
		return m.Metadata
	}
	return nil
}

type Metadata struct {
	EventTimestamp        int64             `protobuf:"varint,1,opt,name=EventTimestamp,json=eventTimestamp,proto3" json:"EventTimestamp,omitempty"`
	OriginStreamTimestamp int64             `protobuf:"varint,2,opt,name=OriginStreamTimestamp,json=originStreamTimestamp,proto3" json:"OriginStreamTimestamp,omitempty"`
	StreamTimestamp       int64             `protobuf:"varint,3,opt,name=StreamTimestamp,json=streamTimestamp,proto3" json:"StreamTimestamp,omitempty"`
	KeyValue              map[string]string `protobuf:"bytes,4,rep,name=keyValue,proto3" json:"keyValue,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral  struct{}          `json:"-"`
	XXX_unrecognized      []byte            `json:"-"`
	XXX_sizecache         int32             `json:"-"`
}

func (m *Metadata) Reset()         { *m = Metadata{} }
func (m *Metadata) String() string { return proto.CompactTextString(m) }
func (*Metadata) ProtoMessage()    {}
func (*Metadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{3}
}

func (m *Metadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Metadata.Unmarshal(m, b)
}
func (m *Metadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Metadata.Marshal(b, m, deterministic)
}
func (m *Metadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Metadata.Merge(m, src)
}
func (m *Metadata) XXX_Size() int {
	return xxx_messageInfo_Metadata.Size(m)
}
func (m *Metadata) XXX_DiscardUnknown() {
	xxx_messageInfo_Metadata.DiscardUnknown(m)
}

var xxx_messageInfo_Metadata proto.InternalMessageInfo

func (m *Metadata) GetEventTimestamp() int64 {
	if m != nil {
		return m.EventTimestamp
	}
	return 0
}

func (m *Metadata) GetOriginStreamTimestamp() int64 {
	if m != nil {
		return m.OriginStreamTimestamp
	}
	return 0
}

func (m *Metadata) GetStreamTimestamp() int64 {
	if m != nil {
		return m.StreamTimestamp
	}
	return 0
}

func (m *Metadata) GetKeyValue() map[string]string {
	if m != nil {
		return m.KeyValue
	}
	return nil
}

type GetAndWatchEvent struct {
	Key                  []byte    `protobuf:"bytes,1,opt,name=Key,json=key,proto3" json:"Key,omitempty"`
	Value                []byte    `protobuf:"bytes,2,opt,name=Value,json=value,proto3" json:"Value,omitempty"`
	Metadata             *Metadata `protobuf:"bytes,3,opt,name=metadata,proto3" json:"metadata,omitempty"`
	EventType            EventType `protobuf:"varint,4,opt,name=eventType,proto3,enum=stream.EventType" json:"eventType,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *GetAndWatchEvent) Reset()         { *m = GetAndWatchEvent{} }
func (m *GetAndWatchEvent) String() string { return proto.CompactTextString(m) }
func (*GetAndWatchEvent) ProtoMessage()    {}
func (*GetAndWatchEvent) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{4}
}

func (m *GetAndWatchEvent) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetAndWatchEvent.Unmarshal(m, b)
}
func (m *GetAndWatchEvent) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetAndWatchEvent.Marshal(b, m, deterministic)
}
func (m *GetAndWatchEvent) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetAndWatchEvent.Merge(m, src)
}
func (m *GetAndWatchEvent) XXX_Size() int {
	return xxx_messageInfo_GetAndWatchEvent.Size(m)
}
func (m *GetAndWatchEvent) XXX_DiscardUnknown() {
	xxx_messageInfo_GetAndWatchEvent.DiscardUnknown(m)
}

var xxx_messageInfo_GetAndWatchEvent proto.InternalMessageInfo

func (m *GetAndWatchEvent) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *GetAndWatchEvent) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *GetAndWatchEvent) GetMetadata() *Metadata {
	if m != nil {
		return m.Metadata
	}
	return nil
}

func (m *GetAndWatchEvent) GetEventType() EventType {
	if m != nil {
		return m.EventType
	}
	return EventType_UNKNOWN_EVENT_TYPE
}

type StreamDefinition struct {
	Name                 string     `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	DataType             string     `protobuf:"bytes,2,opt,name=dataType,proto3" json:"dataType,omitempty"`
	StreamType           StreamType `protobuf:"varint,3,opt,name=streamType,proto3,enum=stream.StreamType" json:"streamType,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *StreamDefinition) Reset()         { *m = StreamDefinition{} }
func (m *StreamDefinition) String() string { return proto.CompactTextString(m) }
func (*StreamDefinition) ProtoMessage()    {}
func (*StreamDefinition) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{5}
}

func (m *StreamDefinition) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StreamDefinition.Unmarshal(m, b)
}
func (m *StreamDefinition) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StreamDefinition.Marshal(b, m, deterministic)
}
func (m *StreamDefinition) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamDefinition.Merge(m, src)
}
func (m *StreamDefinition) XXX_Size() int {
	return xxx_messageInfo_StreamDefinition.Size(m)
}
func (m *StreamDefinition) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamDefinition.DiscardUnknown(m)
}

var xxx_messageInfo_StreamDefinition proto.InternalMessageInfo

func (m *StreamDefinition) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *StreamDefinition) GetDataType() string {
	if m != nil {
		return m.DataType
	}
	return ""
}

func (m *StreamDefinition) GetStreamType() StreamType {
	if m != nil {
		return m.StreamType
	}
	return StreamType_UNKNOWN_STREAM_TYPE
}

type Metrics struct {
	Metrics              []*_go.MetricFamily `protobuf:"bytes,1,rep,name=metrics,proto3" json:"metrics,omitempty"`
	XXX_NoUnkeyedLiteral struct{}            `json:"-"`
	XXX_unrecognized     []byte              `json:"-"`
	XXX_sizecache        int32               `json:"-"`
}

func (m *Metrics) Reset()         { *m = Metrics{} }
func (m *Metrics) String() string { return proto.CompactTextString(m) }
func (*Metrics) ProtoMessage()    {}
func (*Metrics) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{6}
}

func (m *Metrics) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Metrics.Unmarshal(m, b)
}
func (m *Metrics) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Metrics.Marshal(b, m, deterministic)
}
func (m *Metrics) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Metrics.Merge(m, src)
}
func (m *Metrics) XXX_Size() int {
	return xxx_messageInfo_Metrics.Size(m)
}
func (m *Metrics) XXX_DiscardUnknown() {
	xxx_messageInfo_Metrics.DiscardUnknown(m)
}

var xxx_messageInfo_Metrics proto.InternalMessageInfo

func (m *Metrics) GetMetrics() []*_go.MetricFamily {
	if m != nil {
		return m.Metrics
	}
	return nil
}

func init() {
	proto.RegisterEnum("stream.EventType", EventType_name, EventType_value)
	proto.RegisterEnum("stream.StreamType", StreamType_name, StreamType_value)
	proto.RegisterType((*StreamRequest)(nil), "stream.StreamRequest")
	proto.RegisterType((*GetAndWatchRequest)(nil), "stream.GetAndWatchRequest")
	proto.RegisterType((*StreamEvent)(nil), "stream.StreamEvent")
	proto.RegisterType((*Metadata)(nil), "stream.Metadata")
	proto.RegisterMapType((map[string]string)(nil), "stream.Metadata.KeyValueEntry")
	proto.RegisterType((*GetAndWatchEvent)(nil), "stream.GetAndWatchEvent")
	proto.RegisterType((*StreamDefinition)(nil), "stream.StreamDefinition")
	proto.RegisterType((*Metrics)(nil), "stream.Metrics")
}

func init() { proto.RegisterFile("stream.proto", fileDescriptor_bb17ef3f514bfe54) }

var fileDescriptor_bb17ef3f514bfe54 = []byte{
	// 616 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xcc, 0x54, 0xcd, 0x6e, 0xd3, 0x40,
	0x10, 0xc6, 0x71, 0x9b, 0x26, 0x93, 0xa6, 0x75, 0xa7, 0x14, 0x22, 0x1f, 0x50, 0x64, 0x21, 0x14,
	0x55, 0x28, 0xa0, 0x80, 0x10, 0x2a, 0xbd, 0x04, 0xb2, 0xb4, 0x55, 0x5b, 0xb7, 0x72, 0xdc, 0x56,
	0x9c, 0x2c, 0xd7, 0x1d, 0xa8, 0x95, 0xd8, 0x0e, 0xf6, 0x26, 0x22, 0x4f, 0xc0, 0x81, 0x27, 0xe0,
	0x11, 0xb8, 0xf0, 0x8c, 0xc8, 0xbb, 0xb6, 0xe3, 0x94, 0x5e, 0x91, 0xb8, 0x79, 0xbf, 0xef, 0xf3,
	0xfc, 0x7c, 0x3b, 0x3b, 0xb0, 0x9e, 0xf0, 0x98, 0xdc, 0xa0, 0x3b, 0x89, 0x23, 0x1e, 0x61, 0x55,
	0x9e, 0xf4, 0x66, 0x40, 0x3c, 0xf6, 0xbd, 0x44, 0xc2, 0xc6, 0x2f, 0x05, 0x9a, 0x43, 0xc1, 0x58,
	0xf4, 0x75, 0x4a, 0x09, 0x47, 0x84, 0x95, 0xd0, 0x0d, 0xa8, 0xa5, 0xb4, 0x95, 0x4e, 0xdd, 0x12,
	0xdf, 0xf8, 0x14, 0x9a, 0xb1, 0xa4, 0x29, 0x36, 0x53, 0xb2, 0x22, 0xc8, 0x65, 0x10, 0xdb, 0xd0,
	0xa0, 0x6f, 0x13, 0xf2, 0xf8, 0x21, 0x8d, 0xc7, 0x51, 0x4b, 0x6d, 0x2b, 0x9d, 0x9a, 0x55, 0x86,
	0x70, 0x1f, 0xf4, 0x1b, 0x3f, 0xf1, 0xa2, 0x30, 0x24, 0x8f, 0x3b, 0x51, 0xe8, 0x5c, 0xbb, 0xde,
	0x68, 0x12, 0x53, 0x92, 0x4c, 0x63, 0x6a, 0xad, 0x88, 0x1f, 0x5a, 0x0b, 0xc5, 0x59, 0xf8, 0xbe,
	0xc4, 0x1b, 0xbf, 0x15, 0xc0, 0x03, 0xe2, 0xfd, 0xf0, 0xe6, 0xca, 0xe5, 0xde, 0xed, 0xff, 0x5f,
	0xb0, 0x07, 0x0d, 0xe9, 0x2d, 0x9b, 0x51, 0xc8, 0x51, 0x03, 0xf5, 0x98, 0xe6, 0xa2, 0xce, 0x75,
	0x4b, 0x1d, 0xd1, 0x1c, 0x1f, 0xc2, 0xea, 0xa5, 0x3b, 0x9e, 0xca, 0xf2, 0xd6, 0xad, 0xd5, 0x59,
	0x7a, 0xc0, 0xe7, 0x50, 0x0b, 0x88, 0xbb, 0x37, 0x2e, 0x77, 0x45, 0x4d, 0x8d, 0x9e, 0xd6, 0xcd,
	0xee, 0xf2, 0x34, 0xc3, 0xad, 0x42, 0x61, 0xfc, 0xa8, 0x40, 0x2d, 0x87, 0xf1, 0x19, 0x6c, 0x88,
	0x5c, 0xb6, 0x1f, 0x50, 0xc2, 0xdd, 0x60, 0x22, 0xb2, 0xa9, 0xd6, 0x06, 0x2d, 0xa1, 0xf8, 0x1a,
	0x76, 0xce, 0x62, 0xff, 0x8b, 0x1f, 0xca, 0xfa, 0x16, 0xf2, 0x8a, 0x90, 0xef, 0x44, 0xf7, 0x91,
	0xd8, 0x81, 0xcd, 0xbb, 0x7a, 0x55, 0xe8, 0x37, 0x93, 0x3b, 0xca, 0x3d, 0xa8, 0x8d, 0x68, 0x2e,
	0x7b, 0x5b, 0x69, 0xab, 0x9d, 0x46, 0xef, 0xc9, 0xdd, 0x16, 0xba, 0xc7, 0x99, 0x80, 0x85, 0x3c,
	0x9e, 0x5b, 0x85, 0x5e, 0x7f, 0x07, 0xcd, 0x25, 0x2a, 0xf5, 0x6d, 0x94, 0xf9, 0x56, 0x2f, 0x7c,
	0x9b, 0x15, 0xbe, 0xd5, 0x33, 0xdf, 0xf6, 0x2a, 0x6f, 0x15, 0xe3, 0xa7, 0x02, 0x5a, 0x69, 0x46,
	0xfe, 0xa1, 0xf1, 0xf8, 0x02, 0xea, 0xd2, 0xd5, 0xf9, 0x44, 0x8e, 0xc2, 0x46, 0x6f, 0x2b, 0x97,
	0xb3, 0x9c, 0xb0, 0x16, 0x1a, 0x63, 0x06, 0x9a, 0xb4, 0x6f, 0x40, 0x9f, 0xfd, 0xd0, 0xe7, 0x7e,
	0x14, 0xde, 0x3b, 0xbc, 0x3a, 0xd4, 0xd2, 0x04, 0x22, 0xae, 0x6c, 0xb0, 0x38, 0x63, 0x0f, 0x20,
	0xf3, 0x3a, 0x65, 0x55, 0x91, 0x15, 0xf3, 0xac, 0xc3, 0x82, 0xb1, 0x4a, 0x2a, 0xe3, 0x00, 0xd6,
	0x4e, 0xe5, 0xa3, 0xc7, 0x7d, 0x58, 0xcb, 0xde, 0x7f, 0x4b, 0x11, 0xd7, 0x62, 0x74, 0xfd, 0x28,
	0x5d, 0x05, 0x01, 0xf1, 0x5b, 0x9a, 0x26, 0x5d, 0x6f, 0xec, 0x53, 0xc8, 0xbb, 0x52, 0xff, 0xd1,
	0x0d, 0xfc, 0xf1, 0xdc, 0xca, 0x7f, 0xd9, 0x35, 0xa1, 0x5e, 0x34, 0x86, 0x8f, 0x00, 0x2f, 0xcc,
	0x63, 0xf3, 0xec, 0xca, 0x74, 0xd8, 0x25, 0x33, 0x6d, 0xc7, 0xfe, 0x74, 0xce, 0xb4, 0x07, 0x08,
	0x50, 0xbd, 0x38, 0x1f, 0xf4, 0x6d, 0xa6, 0x29, 0xb8, 0x05, 0xcd, 0x23, 0xf3, 0xc8, 0x3e, 0xea,
	0x9f, 0x38, 0x43, 0x3b, 0x85, 0x2a, 0x29, 0x3d, 0x60, 0x27, 0xcc, 0x66, 0x9a, 0xba, 0x3b, 0x00,
	0x58, 0x94, 0x8c, 0x8f, 0x61, 0x3b, 0x0f, 0x38, 0xb4, 0x2d, 0xd6, 0x3f, 0x2d, 0x45, 0x94, 0x80,
	0x8c, 0x78, 0xc0, 0x6c, 0xa7, 0x6f, 0x0e, 0x9c, 0xab, 0xbe, 0xfd, 0xe1, 0x50, 0xab, 0xf4, 0xbe,
	0x2b, 0x50, 0x95, 0x61, 0xf0, 0x4d, 0xf1, 0xb5, 0xb3, 0xec, 0x49, 0xb6, 0x2b, 0xf4, 0xed, 0x65,
	0x58, 0x74, 0xf3, 0x52, 0x41, 0x06, 0x8d, 0xd2, 0xd0, 0xa0, 0x9e, 0xab, 0xfe, 0xde, 0x36, 0x7a,
	0xeb, 0x1e, 0x2e, 0x0b, 0x73, 0x5d, 0x15, 0x3b, 0xf5, 0xd5, 0x9f, 0x00, 0x00, 0x00, 0xff, 0xff,
	0x27, 0x22, 0x8d, 0x08, 0x7a, 0x05, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// StreamClient is the client API for Stream service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type StreamClient interface {
	Stream(ctx context.Context, in *StreamRequest, opts ...grpc.CallOption) (Stream_StreamClient, error)
	// Gets the initial states and watches for updates
	GetAndWatch(ctx context.Context, in *GetAndWatchRequest, opts ...grpc.CallOption) (Stream_GetAndWatchClient, error)
}

type streamClient struct {
	cc *grpc.ClientConn
}

func NewStreamClient(cc *grpc.ClientConn) StreamClient {
	return &streamClient{cc}
}

func (c *streamClient) Stream(ctx context.Context, in *StreamRequest, opts ...grpc.CallOption) (Stream_StreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Stream_serviceDesc.Streams[0], "/stream.Stream/Stream", opts...)
	if err != nil {
		return nil, err
	}
	x := &streamStreamClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Stream_StreamClient interface {
	Recv() (*StreamEvent, error)
	grpc.ClientStream
}

type streamStreamClient struct {
	grpc.ClientStream
}

func (x *streamStreamClient) Recv() (*StreamEvent, error) {
	m := new(StreamEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *streamClient) GetAndWatch(ctx context.Context, in *GetAndWatchRequest, opts ...grpc.CallOption) (Stream_GetAndWatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Stream_serviceDesc.Streams[1], "/stream.Stream/GetAndWatch", opts...)
	if err != nil {
		return nil, err
	}
	x := &streamGetAndWatchClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Stream_GetAndWatchClient interface {
	Recv() (*GetAndWatchEvent, error)
	grpc.ClientStream
}

type streamGetAndWatchClient struct {
	grpc.ClientStream
}

func (x *streamGetAndWatchClient) Recv() (*GetAndWatchEvent, error) {
	m := new(GetAndWatchEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// StreamServer is the server API for Stream service.
type StreamServer interface {
	Stream(*StreamRequest, Stream_StreamServer) error
	// Gets the initial states and watches for updates
	GetAndWatch(*GetAndWatchRequest, Stream_GetAndWatchServer) error
}

func RegisterStreamServer(s *grpc.Server, srv StreamServer) {
	s.RegisterService(&_Stream_serviceDesc, srv)
}

func _Stream_Stream_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StreamRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StreamServer).Stream(m, &streamStreamServer{stream})
}

type Stream_StreamServer interface {
	Send(*StreamEvent) error
	grpc.ServerStream
}

type streamStreamServer struct {
	grpc.ServerStream
}

func (x *streamStreamServer) Send(m *StreamEvent) error {
	return x.ServerStream.SendMsg(m)
}

func _Stream_GetAndWatch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(GetAndWatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StreamServer).GetAndWatch(m, &streamGetAndWatchServer{stream})
}

type Stream_GetAndWatchServer interface {
	Send(*GetAndWatchEvent) error
	grpc.ServerStream
}

type streamGetAndWatchServer struct {
	grpc.ServerStream
}

func (x *streamGetAndWatchServer) Send(m *GetAndWatchEvent) error {
	return x.ServerStream.SendMsg(m)
}

var _Stream_serviceDesc = grpc.ServiceDesc{
	ServiceName: "stream.Stream",
	HandlerType: (*StreamServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Stream",
			Handler:       _Stream_Stream_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "GetAndWatch",
			Handler:       _Stream_GetAndWatch_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "stream.proto",
}

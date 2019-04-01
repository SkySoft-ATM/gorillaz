// Code generated by protoc-gen-go. DO NOT EDIT.
// source: stream.proto

package stream

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
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

type StreamRequest struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
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
	return fileDescriptor_bb17ef3f514bfe54, []int{1}
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
	StreamTimestamp      int64             `protobuf:"varint,1,opt,name=StreamTimestamp,json=streamTimestamp,proto3" json:"StreamTimestamp,omitempty"`
	KeyValue             map[string]string `protobuf:"bytes,2,rep,name=keyValue,proto3" json:"keyValue,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *Metadata) Reset()         { *m = Metadata{} }
func (m *Metadata) String() string { return proto.CompactTextString(m) }
func (*Metadata) ProtoMessage()    {}
func (*Metadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_bb17ef3f514bfe54, []int{2}
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

func init() {
	proto.RegisterType((*StreamRequest)(nil), "StreamRequest")
	proto.RegisterType((*StreamEvent)(nil), "StreamEvent")
	proto.RegisterType((*Metadata)(nil), "Metadata")
	proto.RegisterMapType((map[string]string)(nil), "Metadata.KeyValueEntry")
}

func init() { proto.RegisterFile("stream.proto", fileDescriptor_bb17ef3f514bfe54) }

var fileDescriptor_bb17ef3f514bfe54 = []byte{
	// 246 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x54, 0x90, 0x41, 0x4b, 0xc3, 0x40,
	0x10, 0x85, 0xd9, 0xae, 0x2d, 0xc9, 0x24, 0xb5, 0x32, 0x08, 0x86, 0x9e, 0x4a, 0x44, 0xd8, 0xd3,
	0x22, 0xe9, 0x45, 0xf4, 0xdc, 0x53, 0xf1, 0xb2, 0x8a, 0x27, 0x2f, 0x2b, 0xce, 0x41, 0xe2, 0xa6,
	0x35, 0x99, 0x06, 0xf2, 0x87, 0xfc, 0x9d, 0xd2, 0x4d, 0xb2, 0x98, 0xdb, 0xcc, 0x9b, 0xc7, 0xbe,
	0xfd, 0x1e, 0xa4, 0x0d, 0xd7, 0x64, 0x9d, 0x3e, 0xd6, 0x07, 0x3e, 0xe4, 0xb7, 0xb0, 0x7c, 0xf1,
	0xbb, 0xa1, 0x9f, 0x13, 0x35, 0x8c, 0x08, 0x17, 0x95, 0x75, 0x94, 0x89, 0x8d, 0x50, 0xb1, 0xf1,
	0x73, 0xfe, 0x0e, 0x49, 0x6f, 0xda, 0xb5, 0x54, 0x31, 0x5e, 0x81, 0xdc, 0x53, 0xe7, 0x1d, 0xa9,
	0x91, 0x25, 0x75, 0x78, 0x0d, 0xf3, 0x37, 0xfb, 0x7d, 0xa2, 0x6c, 0xe6, 0xb5, 0x79, 0x7b, 0x5e,
	0xf0, 0x0e, 0x22, 0x47, 0x6c, 0x3f, 0x2d, 0xdb, 0x4c, 0x6e, 0x84, 0x4a, 0x8a, 0x58, 0x3f, 0x0f,
	0x82, 0x09, 0xa7, 0xfc, 0x57, 0x40, 0x34, 0xca, 0xa8, 0x60, 0xd5, 0x47, 0xbd, 0x7e, 0x39, 0x6a,
	0xd8, 0xba, 0xa3, 0xcf, 0x91, 0x66, 0xd5, 0x4c, 0x65, 0xdc, 0x42, 0x54, 0x52, 0x37, 0xc6, 0x4a,
	0x95, 0x14, 0x37, 0xe1, 0x75, 0xbd, 0x1f, 0x2e, 0xbb, 0x8a, 0xeb, 0xce, 0x04, 0xe3, 0xfa, 0x09,
	0x96, 0x93, 0xd3, 0x99, 0xa5, 0x1c, 0x58, 0xe2, 0xc0, 0xd2, 0x06, 0x96, 0x78, 0x60, 0x79, 0x9c,
	0x3d, 0x88, 0xa2, 0x80, 0x45, 0xff, 0x37, 0x54, 0x61, 0xba, 0xd4, 0x93, 0xfa, 0xd6, 0xa9, 0xfe,
	0xd7, 0xd4, 0xbd, 0xf8, 0x58, 0xf8, 0x9a, 0xb7, 0x7f, 0x01, 0x00, 0x00, 0xff, 0xff, 0xfa, 0xb9,
	0x42, 0x84, 0x76, 0x01, 0x00, 0x00,
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
}

type streamClient struct {
	cc *grpc.ClientConn
}

func NewStreamClient(cc *grpc.ClientConn) StreamClient {
	return &streamClient{cc}
}

func (c *streamClient) Stream(ctx context.Context, in *StreamRequest, opts ...grpc.CallOption) (Stream_StreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Stream_serviceDesc.Streams[0], "/Stream/Stream", opts...)
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

// StreamServer is the server API for Stream service.
type StreamServer interface {
	Stream(*StreamRequest, Stream_StreamServer) error
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

var _Stream_serviceDesc = grpc.ServiceDesc{
	ServiceName: "Stream",
	HandlerType: (*StreamServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Stream",
			Handler:       _Stream_Stream_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "stream.proto",
}

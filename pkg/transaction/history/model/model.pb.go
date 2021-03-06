// Code generated by protoc-gen-go. DO NOT EDIT.
// source: model.proto

package model

import (
	fmt "fmt"
	_ "github.com/envoyproxy/protoc-gen-validate/validate"
	proto "github.com/golang/protobuf/proto"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
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

type KinVersion int32

const (
	KinVersion_UNKNOWN  KinVersion = 0
	KinVersion_RESERVED KinVersion = 1
	KinVersion_KIN2     KinVersion = 2
	KinVersion_KIN3     KinVersion = 3
	KinVersion_KIN4     KinVersion = 4
)

var KinVersion_name = map[int32]string{
	0: "UNKNOWN",
	1: "RESERVED",
	2: "KIN2",
	3: "KIN3",
	4: "KIN4",
}

var KinVersion_value = map[string]int32{
	"UNKNOWN":  0,
	"RESERVED": 1,
	"KIN2":     2,
	"KIN3":     3,
	"KIN4":     4,
}

func (x KinVersion) String() string {
	return proto.EnumName(KinVersion_name, int32(x))
}

func (KinVersion) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_4c16552f9fdb66d8, []int{0}
}

// Entry is a generalized history entry that contains a blockchain
// transaction, as well as the KinVersion the transaction is for.
//
// This allows for the generalization of a history store, which allows
// for a continuous view of history across blockchains.
type Entry struct {
	Version KinVersion `protobuf:"varint,1,opt,name=version,proto3,enum=KinVersion" json:"version,omitempty"`
	// Types that are valid to be assigned to Kind:
	//	*Entry_Stellar
	//	*Entry_Solana
	Kind                 isEntry_Kind `protobuf_oneof:"kind"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *Entry) Reset()         { *m = Entry{} }
func (m *Entry) String() string { return proto.CompactTextString(m) }
func (*Entry) ProtoMessage()    {}
func (*Entry) Descriptor() ([]byte, []int) {
	return fileDescriptor_4c16552f9fdb66d8, []int{0}
}

func (m *Entry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Entry.Unmarshal(m, b)
}
func (m *Entry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Entry.Marshal(b, m, deterministic)
}
func (m *Entry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Entry.Merge(m, src)
}
func (m *Entry) XXX_Size() int {
	return xxx_messageInfo_Entry.Size(m)
}
func (m *Entry) XXX_DiscardUnknown() {
	xxx_messageInfo_Entry.DiscardUnknown(m)
}

var xxx_messageInfo_Entry proto.InternalMessageInfo

func (m *Entry) GetVersion() KinVersion {
	if m != nil {
		return m.Version
	}
	return KinVersion_UNKNOWN
}

type isEntry_Kind interface {
	isEntry_Kind()
}

type Entry_Stellar struct {
	Stellar *StellarEntry `protobuf:"bytes,2,opt,name=stellar,proto3,oneof"`
}

type Entry_Solana struct {
	Solana *SolanaEntry `protobuf:"bytes,3,opt,name=solana,proto3,oneof"`
}

func (*Entry_Stellar) isEntry_Kind() {}

func (*Entry_Solana) isEntry_Kind() {}

func (m *Entry) GetKind() isEntry_Kind {
	if m != nil {
		return m.Kind
	}
	return nil
}

func (m *Entry) GetStellar() *StellarEntry {
	if x, ok := m.GetKind().(*Entry_Stellar); ok {
		return x.Stellar
	}
	return nil
}

func (m *Entry) GetSolana() *SolanaEntry {
	if x, ok := m.GetKind().(*Entry_Solana); ok {
		return x.Solana
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Entry) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Entry_Stellar)(nil),
		(*Entry_Solana)(nil),
	}
}

type StellarEntry struct {
	Ledger               uint64               `protobuf:"varint,1,opt,name=ledger,proto3" json:"ledger,omitempty"`
	PagingToken          uint64               `protobuf:"varint,2,opt,name=paging_token,json=pagingToken,proto3" json:"paging_token,omitempty"`
	LedgerCloseTime      *timestamp.Timestamp `protobuf:"bytes,3,opt,name=ledger_close_time,json=ledgerCloseTime,proto3" json:"ledger_close_time,omitempty"`
	NetworkPassphrase    string               `protobuf:"bytes,4,opt,name=network_passphrase,json=networkPassphrase,proto3" json:"network_passphrase,omitempty"`
	EnvelopeXdr          []byte               `protobuf:"bytes,5,opt,name=envelope_xdr,json=envelopeXdr,proto3" json:"envelope_xdr,omitempty"`
	ResultXdr            []byte               `protobuf:"bytes,6,opt,name=result_xdr,json=resultXdr,proto3" json:"result_xdr,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *StellarEntry) Reset()         { *m = StellarEntry{} }
func (m *StellarEntry) String() string { return proto.CompactTextString(m) }
func (*StellarEntry) ProtoMessage()    {}
func (*StellarEntry) Descriptor() ([]byte, []int) {
	return fileDescriptor_4c16552f9fdb66d8, []int{1}
}

func (m *StellarEntry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_StellarEntry.Unmarshal(m, b)
}
func (m *StellarEntry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_StellarEntry.Marshal(b, m, deterministic)
}
func (m *StellarEntry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StellarEntry.Merge(m, src)
}
func (m *StellarEntry) XXX_Size() int {
	return xxx_messageInfo_StellarEntry.Size(m)
}
func (m *StellarEntry) XXX_DiscardUnknown() {
	xxx_messageInfo_StellarEntry.DiscardUnknown(m)
}

var xxx_messageInfo_StellarEntry proto.InternalMessageInfo

func (m *StellarEntry) GetLedger() uint64 {
	if m != nil {
		return m.Ledger
	}
	return 0
}

func (m *StellarEntry) GetPagingToken() uint64 {
	if m != nil {
		return m.PagingToken
	}
	return 0
}

func (m *StellarEntry) GetLedgerCloseTime() *timestamp.Timestamp {
	if m != nil {
		return m.LedgerCloseTime
	}
	return nil
}

func (m *StellarEntry) GetNetworkPassphrase() string {
	if m != nil {
		return m.NetworkPassphrase
	}
	return ""
}

func (m *StellarEntry) GetEnvelopeXdr() []byte {
	if m != nil {
		return m.EnvelopeXdr
	}
	return nil
}

func (m *StellarEntry) GetResultXdr() []byte {
	if m != nil {
		return m.ResultXdr
	}
	return nil
}

type SolanaEntry struct {
	Slot      uint64 `protobuf:"varint,1,opt,name=slot,proto3" json:"slot,omitempty"`
	Confirmed bool   `protobuf:"varint,2,opt,name=confirmed,proto3" json:"confirmed,omitempty"`
	// Maximum size taken from: https://github.com/solana-labs/solana/blob/39b3ac6a8d29e14faa1de73d8b46d390ad41797b/sdk/src/packet.rs#L9-L13
	Transaction []byte `protobuf:"bytes,3,opt,name=transaction,proto3" json:"transaction,omitempty"`
	// Optional value represented by 'TransactionError' type returned
	// from the Solana RPCs.
	TransactionError []byte `protobuf:"bytes,4,opt,name=transaction_error,json=transactionError,proto3" json:"transaction_error,omitempty"`
	// Optional estimated block time of the transaction.
	//
	// This isn't mandatory since we may not always have information
	// on the timestamp. Notably:
	//
	//   1. If the entry hasn't been confirmed yet, getBlockTime() won't work.
	//   2. If the block is older than the RPC node's timestamp history.
	//
	// In case (1), we expect that the block time will be updated once it is confirmed.
	// In case (2), it either means an ingestor has fallen extremely behind, or the RPC
	// node has had some issue loosing history. In the latter case, we'd want to try and
	// repair the timestamps from another node.
	//
	// Since we do _not_ want to prevent the ingestion of transactions (by requiring block_time),
	// we make it optional.
	BlockTime            *timestamp.Timestamp `protobuf:"bytes,5,opt,name=block_time,json=blockTime,proto3" json:"block_time,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *SolanaEntry) Reset()         { *m = SolanaEntry{} }
func (m *SolanaEntry) String() string { return proto.CompactTextString(m) }
func (*SolanaEntry) ProtoMessage()    {}
func (*SolanaEntry) Descriptor() ([]byte, []int) {
	return fileDescriptor_4c16552f9fdb66d8, []int{2}
}

func (m *SolanaEntry) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SolanaEntry.Unmarshal(m, b)
}
func (m *SolanaEntry) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SolanaEntry.Marshal(b, m, deterministic)
}
func (m *SolanaEntry) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SolanaEntry.Merge(m, src)
}
func (m *SolanaEntry) XXX_Size() int {
	return xxx_messageInfo_SolanaEntry.Size(m)
}
func (m *SolanaEntry) XXX_DiscardUnknown() {
	xxx_messageInfo_SolanaEntry.DiscardUnknown(m)
}

var xxx_messageInfo_SolanaEntry proto.InternalMessageInfo

func (m *SolanaEntry) GetSlot() uint64 {
	if m != nil {
		return m.Slot
	}
	return 0
}

func (m *SolanaEntry) GetConfirmed() bool {
	if m != nil {
		return m.Confirmed
	}
	return false
}

func (m *SolanaEntry) GetTransaction() []byte {
	if m != nil {
		return m.Transaction
	}
	return nil
}

func (m *SolanaEntry) GetTransactionError() []byte {
	if m != nil {
		return m.TransactionError
	}
	return nil
}

func (m *SolanaEntry) GetBlockTime() *timestamp.Timestamp {
	if m != nil {
		return m.BlockTime
	}
	return nil
}

func init() {
	proto.RegisterEnum("KinVersion", KinVersion_name, KinVersion_value)
	proto.RegisterType((*Entry)(nil), "Entry")
	proto.RegisterType((*StellarEntry)(nil), "StellarEntry")
	proto.RegisterType((*SolanaEntry)(nil), "SolanaEntry")
}

func init() { proto.RegisterFile("model.proto", fileDescriptor_4c16552f9fdb66d8) }

var fileDescriptor_4c16552f9fdb66d8 = []byte{
	// 507 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x92, 0xcf, 0x8e, 0xd3, 0x30,
	0x10, 0xc6, 0xeb, 0x36, 0x6d, 0xd3, 0x49, 0x80, 0xd4, 0x07, 0x88, 0x56, 0x48, 0x94, 0x4a, 0xa0,
	0x2e, 0x82, 0x54, 0xea, 0x22, 0xb8, 0x67, 0x89, 0x04, 0xaa, 0x54, 0x2a, 0x77, 0x59, 0xf6, 0x16,
	0xa5, 0x8d, 0xb7, 0x44, 0x75, 0xed, 0xc8, 0xf6, 0x96, 0x3f, 0xa7, 0xbd, 0xf2, 0x30, 0x5c, 0x78,
	0x12, 0xde, 0x83, 0x37, 0xe8, 0x09, 0xc5, 0x49, 0x76, 0x8b, 0xb8, 0x70, 0x9b, 0x6f, 0xbe, 0xdf,
	0xd8, 0xce, 0x37, 0x01, 0x67, 0x2b, 0x52, 0xca, 0x82, 0x5c, 0x0a, 0x2d, 0x8e, 0x1e, 0xec, 0x12,
	0x96, 0xa5, 0x89, 0xa6, 0xe3, 0xba, 0xa8, 0x8c, 0x47, 0x6b, 0x21, 0xd6, 0x8c, 0x8e, 0x8d, 0x5a,
	0x5e, 0x5d, 0x8e, 0x75, 0xb6, 0xa5, 0x4a, 0x27, 0xdb, 0xbc, 0x04, 0x86, 0xdf, 0x11, 0xb4, 0x23,
	0xae, 0xe5, 0x57, 0xfc, 0x04, 0xba, 0x3b, 0x2a, 0x55, 0x26, 0xb8, 0x8f, 0x06, 0x68, 0x74, 0x77,
	0xe2, 0x04, 0xd3, 0x8c, 0x9f, 0x97, 0x2d, 0x52, 0x7b, 0xf8, 0x18, 0xba, 0x4a, 0x53, 0xc6, 0x12,
	0xe9, 0x37, 0x07, 0x68, 0xe4, 0x4c, 0xee, 0x04, 0x8b, 0x52, 0x9b, 0x63, 0xde, 0x36, 0x48, 0xed,
	0xe3, 0xa7, 0xd0, 0x51, 0x82, 0x25, 0x3c, 0xf1, 0x5b, 0x86, 0x74, 0x83, 0x85, 0x91, 0x35, 0x58,
	0xb9, 0x61, 0x07, 0xac, 0x4d, 0xc6, 0xd3, 0xe1, 0x8f, 0x26, 0xb8, 0x87, 0x67, 0xe1, 0xfb, 0xd0,
	0x61, 0x34, 0x5d, 0x53, 0x69, 0x5e, 0x64, 0x91, 0x4a, 0xe1, 0xc7, 0xe0, 0xe6, 0xc9, 0x3a, 0xe3,
	0xeb, 0x58, 0x8b, 0x0d, 0xe5, 0xe6, 0x21, 0x16, 0x71, 0xca, 0xde, 0x59, 0xd1, 0xc2, 0x73, 0xe8,
	0x97, 0x70, 0xbc, 0x62, 0x42, 0xd1, 0xb8, 0xf8, 0xee, 0xea, 0x19, 0x47, 0x41, 0x19, 0x4a, 0x50,
	0x87, 0x12, 0x9c, 0xd5, 0xa1, 0x84, 0xf6, 0x3e, 0x6c, 0xff, 0x44, 0x4d, 0x1b, 0x91, 0x7b, 0xe5,
	0xf8, 0x69, 0x31, 0x5d, 0xf8, 0xf8, 0x15, 0x60, 0x4e, 0xf5, 0x67, 0x21, 0x37, 0x71, 0x9e, 0x28,
	0x95, 0x7f, 0x92, 0x89, 0xa2, 0xbe, 0x35, 0x40, 0xa3, 0x5e, 0xd8, 0xdd, 0x87, 0x96, 0x6c, 0x7a,
	0x88, 0xf4, 0x2b, 0x64, 0x7e, 0x43, 0xe0, 0x17, 0xe0, 0x52, 0xbe, 0xa3, 0x4c, 0xe4, 0x34, 0xfe,
	0x92, 0x4a, 0xbf, 0x3d, 0x40, 0x23, 0x37, 0x84, 0x7d, 0xd8, 0xfd, 0xd6, 0xf6, 0x90, 0x7f, 0x3d,
	0x27, 0x4e, 0xed, 0x5f, 0xa4, 0x12, 0x1f, 0x03, 0x48, 0xaa, 0xae, 0x98, 0x36, 0x70, 0xe7, 0x1f,
	0xb8, 0x57, 0xba, 0x17, 0xa9, 0x1c, 0xfe, 0x46, 0xe0, 0x1c, 0x24, 0x8a, 0x31, 0x58, 0x8a, 0x09,
	0x5d, 0x85, 0x65, 0x6a, 0xfc, 0x10, 0x7a, 0x2b, 0xc1, 0x2f, 0x33, 0xb9, 0xa5, 0xa9, 0xc9, 0xc9,
	0x26, 0xb7, 0x0d, 0xfc, 0x1c, 0x1c, 0x2d, 0x13, 0xae, 0x92, 0x95, 0x2e, 0xf6, 0xde, 0xfa, 0xfb,
	0xb6, 0x5f, 0x3d, 0x72, 0x68, 0xe3, 0xd7, 0xd0, 0x3f, 0x90, 0x31, 0x95, 0x52, 0x48, 0x13, 0xc0,
	0xcd, 0x4c, 0xc3, 0xbf, 0xb6, 0x89, 0x77, 0x00, 0x45, 0x05, 0x83, 0x4f, 0x01, 0x96, 0x4c, 0xac,
	0x36, 0xe5, 0x16, 0xda, 0xff, 0xbd, 0x85, 0x06, 0xe9, 0x99, 0xb9, 0xc2, 0x79, 0x16, 0x01, 0xdc,
	0xfe, 0x8f, 0xd8, 0x81, 0xee, 0x87, 0xd9, 0x74, 0xf6, 0xfe, 0xe3, 0xcc, 0x6b, 0x60, 0x17, 0x6c,
	0x12, 0x2d, 0x22, 0x72, 0x1e, 0xbd, 0xf1, 0x10, 0xb6, 0xc1, 0x9a, 0xbe, 0x9b, 0x4d, 0xbc, 0x66,
	0x55, 0x9d, 0x78, 0xad, 0xaa, 0x7a, 0xe9, 0x59, 0xcb, 0x8e, 0xb9, 0xef, 0xe4, 0x4f, 0x00, 0x00,
	0x00, 0xff, 0xff, 0x65, 0x63, 0x95, 0x7d, 0x40, 0x03, 0x00, 0x00,
}

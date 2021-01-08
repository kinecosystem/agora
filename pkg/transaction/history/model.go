package history

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/mr-tron/base58"
)

type StateChange struct {
	LastKey []byte

	Creations        []*Creation
	Payments         []*Payment
	OwnershipChanges []*OwnershipChange
}

type Creation struct {
	BlockTime    time.Time
	TxID         solana.Signature
	Offset       int
	Successful   bool
	Account      ed25519.PublicKey
	AccountOwner ed25519.PublicKey
	MemoText     *string
	Memo         []byte
	AppIndex     int
	Subsidizer   ed25519.PublicKey
}

// Save implements bigquery.ValueSaver
func (c *Creation) Save() (row map[string]bigquery.Value, insertID string, err error) {
	buf := make([]byte, len(c.TxID)+len(c.Account))
	copy(buf, c.TxID[:])
	copy(buf[len(c.TxID):], c.Account)
	insertID = base64.StdEncoding.EncodeToString(buf)

	// Reference: https://github.com/kinecosystem/history-collector/blob/9ed2f79ff79184d78830d1844a4706e4a29d838c/python/main.py#L153q
	// Real Reference: https://github.com/kinecosystem/py-kin-base/blob/master/kin_base/stellarxdr/StellarXDR_const.py#L363
	var txStatus string
	if c.Successful {
		txStatus = "txSUCCESS"
	} else {
		txStatus = "txFAILED"
	}

	row = map[string]bigquery.Value{
		"date":            c.BlockTime.Format("2006-01-02"),
		"time":            c.BlockTime.Format("2006-01-02 15:04:05 UTC"),
		"tx_id":           base58.Encode(c.TxID[:]),
		"tx_status":       txStatus,
		"account":         base58.Encode(c.Account),
		"account_owner":   base58.Encode(c.AccountOwner),
		"initial_balance": "0",
		"initial_quarks":  0,
	}

	if c.MemoText != nil {
		row["memo_text"] = *c.MemoText
	}
	if len(c.Memo) > 0 {
		row["memo_binary"] = c.Memo
	}
	if c.AppIndex > 0 {
		row["app_index"] = c.AppIndex
	}
	if len(c.Subsidizer) > 0 {
		row["subsidizer"] = base58.Encode(c.Subsidizer)
	}

	return row, insertID, err
}

type Payment struct {
	BlockTime   time.Time
	TxID        solana.Signature
	Offset      int
	Successful  bool
	Source      ed25519.PublicKey
	SourceOwner ed25519.PublicKey
	Dest        ed25519.PublicKey
	DestOwner   ed25519.PublicKey
	Quarks      uint64
	MemoText    *string
	Memo        []byte
	AppIndex    int
	Subsidizer  ed25519.PublicKey
}

// Save implements bigquery.ValueSaver
func (p *Payment) Save() (row map[string]bigquery.Value, insertID string, err error) {
	buf := make([]byte, len(p.TxID)+4)
	copy(buf, p.TxID[:])
	binary.BigEndian.PutUint32(buf, uint32(p.Offset))

	insertID = base64.StdEncoding.EncodeToString(buf)

	// Reference: https://github.com/kinecosystem/history-collector/blob/8ed2f79ff79184d78830d1844a4706e4a29d838c/python/main.py#L153q
	// Real Reference: https://github.com/kinecosystem/py-kin-base/blob/master/kin_base/stellarxdr/StellarXDR_const.py#L363
	var txStatus string
	if p.Successful {
		txStatus = "txSUCCESS"
	} else {
		txStatus = "txFAILED"
	}

	row = map[string]bigquery.Value{
		"date":               p.BlockTime.Format("2006-01-02"),
		"time":               p.BlockTime.Format("2006-01-02 15:04:05 UTC"),
		"tx_id":              base58.Encode(p.TxID[:]),
		"tx_status":          txStatus,
		"instruction_offset": p.Offset,
		"source":             base58.Encode(p.Source),
		"source_owner":       base58.Encode(p.SourceOwner),
		"destination":        base58.Encode(p.Dest),
		"destination_owner":  base58.Encode(p.DestOwner),
		"amount":             kin.FromQuarks(int64(p.Quarks)),
		"quarks":             strconv.FormatUint(p.Quarks, 10),
	}

	if p.MemoText != nil {
		row["memo_text"] = *p.MemoText
	}
	if len(p.Memo) > 0 {
		row["memo_binary"] = p.Memo
	}
	if p.AppIndex > 0 {
		row["app_index"] = p.AppIndex
	}
	if len(p.Subsidizer) > 0 {
		row["subsidizer"] = base58.Encode(p.Subsidizer)
	}

	return row, insertID, err
}

type MemoData struct {
	Offset int

	Text     *string
	Data     []byte
	AppIndex int
}

type OwnershipChange struct {
	Address ed25519.PublicKey
	Owner   ed25519.PublicKey
}

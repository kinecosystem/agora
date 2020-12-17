package kre

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/mr-tron/base58"

	"github.com/kinecosystem/agora/client"
)

type creation struct {
	blockTime    time.Time
	txID         solana.Signature
	offset       int
	successful   bool
	account      ed25519.PublicKey
	accountOwner ed25519.PublicKey
	memoText     *string
	memo         []byte
	appIndex     int
	subsidizer   ed25519.PublicKey
}

// Save implements bigquery.ValueSaver
func (c *creation) Save() (row map[string]bigquery.Value, insertID string, err error) {
	buf := make([]byte, len(c.txID)+len(c.account))
	copy(buf, c.txID[:])
	copy(buf[len(c.txID):], c.account)
	insertID = base64.StdEncoding.EncodeToString(buf)

	// Reference: https://github.com/kinecosystem/history-collector/blob/9ed2f79ff79184d78830d1844a4706e4a29d838c/python/main.py#L153q
	// Real Reference: https://github.com/kinecosystem/py-kin-base/blob/master/kin_base/stellarxdr/StellarXDR_const.py#L363
	var txStatus string
	if c.successful {
		txStatus = "txSUCCESS"
	} else {
		txStatus = "txFAILED"
	}

	row = map[string]bigquery.Value{
		"date":            c.blockTime.Format("2006-01-02"),
		"time":            c.blockTime.Format("2006-01-02 15:04:05 UTC"),
		"tx_id":           base58.Encode(c.txID[:]),
		"tx_status":       txStatus,
		"account":         base58.Encode(c.account),
		"account_owner":   base58.Encode(c.accountOwner),
		"initial_balance": "0",
		"initial_quarks":  0,
	}

	if c.memoText != nil {
		row["memo_text"] = *c.memoText
	}
	if len(c.memo) > 0 {
		row["memo_binary"] = c.memo
	}
	if c.appIndex > 0 {
		row["app_index"] = c.appIndex
	}
	if len(c.subsidizer) > 0 {
		row["subsidizer"] = base58.Encode(c.subsidizer)
	}

	return row, insertID, err
}

type payment struct {
	blockTime   time.Time
	txID        solana.Signature
	offset      int
	successful  bool
	source      ed25519.PublicKey
	sourceOwner ed25519.PublicKey
	dest        ed25519.PublicKey
	destOwner   ed25519.PublicKey
	quarks      uint64
	memoText    *string
	memo        []byte
	appIndex    int
	subsidizer  ed25519.PublicKey
}

// Save implements bigquery.ValueSaver
func (p *payment) Save() (row map[string]bigquery.Value, insertID string, err error) {
	buf := make([]byte, len(p.txID)+4)
	copy(buf, p.txID[:])
	binary.BigEndian.PutUint32(buf, uint32(p.offset))

	insertID = base64.StdEncoding.EncodeToString(buf)

	// Reference: https://github.com/kinecosystem/history-collector/blob/8ed2f79ff79184d78830d1844a4706e4a29d838c/python/main.py#L153q
	// Real Reference: https://github.com/kinecosystem/py-kin-base/blob/master/kin_base/stellarxdr/StellarXDR_const.py#L363
	var txStatus string
	if p.successful {
		txStatus = "txSUCCESS"
	} else {
		txStatus = "txFAILED"
	}

	row = map[string]bigquery.Value{
		"date":               p.blockTime.Format("2006-01-02"),
		"time":               p.blockTime.Format("2006-01-02 15:04:05 UTC"),
		"tx_id":              base58.Encode(p.txID[:]),
		"tx_status":          txStatus,
		"instruction_offset": p.offset,
		"source":             base58.Encode(p.source),
		"source_owner":       base58.Encode(p.sourceOwner),
		"destination":        base58.Encode(p.dest),
		"destination_owner":  base58.Encode(p.destOwner),
		"amount":             client.QuarksToKin(int64(p.quarks)),
		"quarks":             p.quarks,
	}

	if p.memoText != nil {
		row["memo_text"] = *p.memoText
	}
	if len(p.memo) > 0 {
		row["memo_binary"] = p.memo
	}
	if p.appIndex > 0 {
		row["app_index"] = p.appIndex
	}
	if len(p.subsidizer) > 0 {
		row["subsidizer"] = base58.Encode(p.subsidizer)
	}

	return row, insertID, err
}

type memoData struct {
	offset int

	text     *string
	data     []byte
	appIndex int
}

func (l *Loader) getMemos(txn solana.Transaction) (memos []memoData) {
	for i := range txn.Message.Instructions {
		decompiled, err := memo.DecompileMemo(txn.Message, i)
		if err != nil {
			continue
		}
		if len(decompiled.Data) == 0 {
			continue
		}

		m := memoData{
			data:   decompiled.Data,
			offset: i,
		}
		if strings.HasPrefix(string(m.data), "1-") {
			str := string(m.data)
			m.text = &str
		}

		if raw, err := base64.StdEncoding.DecodeString(string(m.data)); err == nil {
			var km kin.Memo
			copy(km[:], raw)

			if kin.IsValidMemoStrict(km) {
				m.appIndex = int(km.AppIndex())
			}
		}

		memos = append(memos, m)
	}

	return memos
}

type ownershipChange struct {
	account  ed25519.PublicKey
	newOwner ed25519.PublicKey
}

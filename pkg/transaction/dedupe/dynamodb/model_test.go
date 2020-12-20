package dynamodb

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-api/genproto/transaction/v4"
	"github.com/stretchr/testify/assert"

	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
)

func TestModel_NoResp(t *testing.T) {
	di := &dedupe.Info{
		Signature:      []byte("hello"),
		SubmissionTime: time.Now(),
	}

	item, err := getItem([]byte("hello"), di, time.Second)
	assert.NoError(t, err)

	assert.Contains(t, item, "id")
	assert.Contains(t, item, "sig")
	assert.Contains(t, item, "stime")
	assert.Contains(t, item, "ttl")

	actual, err := getInfo(item)
	assert.NoError(t, err)
	assert.EqualValues(t, actual.Signature, di.Signature)
	assert.EqualValues(t, actual.SubmissionTime.Unix(), di.SubmissionTime.Unix())
}

func TestModel_Resp(t *testing.T) {
	di := &dedupe.Info{
		Signature:      []byte("hello"),
		SubmissionTime: time.Now(),
		Response: &transaction.SubmitTransactionResponse{
			Result: transaction.SubmitTransactionResponse_FAILED,
		},
	}

	item, err := getItem([]byte("hello"), di, time.Second)
	assert.NoError(t, err)

	assert.Contains(t, item, "id")
	assert.Contains(t, item, "sig")
	assert.Contains(t, item, "stime")
	assert.Contains(t, item, "ttl")

	actual, err := getInfo(item)
	assert.NoError(t, err)
	assert.EqualValues(t, actual.Signature, di.Signature)
	assert.EqualValues(t, actual.SubmissionTime.Unix(), di.SubmissionTime.Unix())
	assert.True(t, proto.Equal(di.Response, actual.Response))
}

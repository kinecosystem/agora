package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/kinecosystem/agora/pkg/transaction/history/kre"
)

type submitter struct {
	table *bigquery.Table
}

func New(bq *bigquery.Client, table string) kre.Submitter {
	return &submitter{
		table: bq.Dataset("solana").Table(table),
	}
}

func (s *submitter) Submit(ctx context.Context, src interface{}) (err error) {
	return s.table.Inserter().Put(ctx, src)
}

package txsql

import "context"

type Transactor interface {
	Do(ctx context.Context, fn func(context.Context) error, opts ...TxOption) error
}

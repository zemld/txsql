package txsql

type IsolationLevel string

type TxOptions struct {
	Isolation IsolationLevel
}

type TxOption func(*TxOptions)

const (
	ReadUncommitted IsolationLevel = "read uncommitted"
	ReadCommitted   IsolationLevel = "read committed"
	RepeatableRead  IsolationLevel = "repeatable read"
	Serializable    IsolationLevel = "serializable"
)

func WithIsolation(level IsolationLevel) TxOption {
	return func(options *TxOptions) { options.Isolation = level }
}

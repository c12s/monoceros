package monoceros

type Config struct {
	NodeID   int64 `env:"NODE_ID"`
	TAggSec  int64 `env:"T_AGG_SEC"`
	JoinWait int64 `env:"JOIN_WAIT"`
}

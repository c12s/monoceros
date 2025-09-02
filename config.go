package monoceros

import (
	"time"

	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/plumtree"
)

type TreeOverlayConfig struct {
	Membership hyparview.Config
	Plumtree   plumtree.Config
}

type AggregationConfig struct {
	TAggSec        int64 `env:"T_AGG_SEC"`
	TAggMaxSec     int64 `env:"T_AGG_MAX_SEC"`
	ScoreGossipSec int64 `env:"SCORE_GOSSIP_SEC"`
}

type Config struct {
	NodeID         string `env:"NODE_ID"`
	Region         string `env:"NODE_REGION"`
	HTTPServerAddr string `env:"HTTP_SERVER_ADDR"`
	GNListenAddr   string `env:"GN_LISTEN_ADDR"`
	RNListenAddr   string `env:"RN_LISTEN_ADDR"`
	RRNListenAddr  string `env:"RRN_LISTEN_ADDR"`
	GNContactID    string `env:"GN_CONTACT_NODE_ID"`
	GNContactAddr  string `env:"GN_CONTACT_NODE_ADDR"`
	RNContactID    string `env:"RN_CONTACT_NODE_ID"`
	RNContactAddr  string `env:"RN_CONTACT_NODE_ADDR"`
	Aggregation    AggregationConfig
	LogPath        string `env:"LOG_PATH"`
	WaitFilePath   string `env:"WAIT_FILE_PATH"`
}

func (c Config) ScoreGossipInterval() time.Duration {
	return time.Duration(c.Aggregation.ScoreGossipSec) * time.Second
}

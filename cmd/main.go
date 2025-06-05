package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/c12s/monoceros"
	"github.com/c12s/plumtree"
	"github.com/caarlos0/env"
	"github.com/natefinch/lumberjack"
)

func main() {
	rnHvCfg := hyparview.Config{
		NodeID:             os.Getenv("NODE_ID"),
		ListenAddress:      os.Getenv("RN_LISTEN_ADDR"),
		ContactNodeID:      os.Getenv("RN_CONTACT_NODE_ID"),
		ContactNodeAddress: os.Getenv("RN_CONTACT_NODE_ADDR"),
	}
	// err := env.Parse(&hvCfg)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	err := env.Parse(&rnHvCfg.HyParViewConfig)
	if err != nil {
		log.Fatal(err)
	}
	gnHvCfh := hyparview.Config{
		NodeID:             os.Getenv("NODE_ID"),
		ListenAddress:      os.Getenv("GN_LISTEN_ADDR"),
		ContactNodeID:      os.Getenv("GN_CONTACT_NODE_ID"),
		ContactNodeAddress: os.Getenv("GN_CONTACT_NODE_ADDR"),
	}
	gnHvCfh.HyParViewConfig = rnHvCfg.HyParViewConfig

	rrnHvCfh := hyparview.Config{
		NodeID:        os.Getenv("NODE_ID"),
		ListenAddress: os.Getenv("RRN_LISTEN_ADDR"),
	}
	rrnHvCfh.HyParViewConfig = rnHvCfg.HyParViewConfig
	log.Println(rnHvCfg)
	log.Println(gnHvCfh)
	log.Println(rrnHvCfh)

	plumtreeCfg := plumtree.Config{}
	err = env.Parse(&plumtreeCfg)
	if err != nil {
		log.Fatal(err)
	}
	// log.Println(plumtreeCfg)

	monocerosCfg := monoceros.Config{}
	err = env.Parse(&monocerosCfg)
	if err != nil {
		log.Fatal(err)
	}
	// log.Println(monocerosCfg)

	// global network setup
	gnSelf := data.Node{
		ID:            gnHvCfh.NodeID,
		ListenAddress: gnHvCfh.ListenAddress,
	}
	gnHvLogger := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/gn_hv_%s.log", monocerosCfg.NodeID),
	}, monocerosCfg.NodeID, log.LstdFlags|log.Lshortfile)

	gnConnManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(gnSelf.ListenAddress))
	gnHv, err := hyparview.NewHyParView(gnHvCfh.HyParViewConfig, gnSelf, gnConnManager, gnHvLogger)
	if err != nil {
		log.Fatal(err)
	}
	err = gnHv.Join(gnHvCfh.ContactNodeID, gnHvCfh.ContactNodeAddress)
	if err != nil {
		log.Fatal(err)
	}
	gn := monoceros.NewGossipNode(gnHv, gnHvLogger)
	// time.Sleep(time.Duration(monocerosCfg.JoinWait) * time.Second)

	// regional network setup
	self := data.Node{
		ID:            rnHvCfg.NodeID,
		ListenAddress: rnHvCfg.ListenAddress,
	}
	rnHvLogger := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/rn_hv_%s.log", monocerosCfg.NodeID),
	}, monocerosCfg.NodeID, log.LstdFlags|log.Lshortfile)

	connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
	hv, err := hyparview.NewHyParView(rnHvCfg.HyParViewConfig, self, connManager, rnHvLogger)
	if err != nil {
		log.Fatal(err)
	}
	err = hv.Join(rnHvCfg.ContactNodeID, rnHvCfg.ContactNodeAddress)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Duration(monocerosCfg.JoinWait) * time.Second)

	rnPtLogger := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/rn_pt_%s.log", monocerosCfg.NodeID),
	}, monocerosCfg.NodeID, log.LstdFlags|log.Lshortfile)
	tree := plumtree.NewPlumtree(plumtreeCfg, hv, rnPtLogger)

	// regional roots network
	selfRRN := data.Node{
		ID:            rrnHvCfh.NodeID,
		ListenAddress: rrnHvCfh.ListenAddress,
	}
	rrnHvLogger := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/rrn_hv_%s.log", monocerosCfg.NodeID),
	}, monocerosCfg.NodeID, log.LstdFlags|log.Lshortfile)
	connManagerRRN := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(selfRRN.ListenAddress))
	hvRRN, err := hyparview.NewHyParView(rrnHvCfh.HyParViewConfig, selfRRN, connManagerRRN, rrnHvLogger)
	if err != nil {
		log.Fatal(err)
	}
	rrnPtLogger := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/rrn_pt_%s.log", monocerosCfg.NodeID),
	}, monocerosCfg.NodeID, log.LstdFlags|log.Lshortfile)
	treeRRN := plumtree.NewPlumtree(plumtreeCfg, hvRRN, rrnPtLogger)
	// monoceros setup
	mcLogger := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/mc_%s.log", monocerosCfg.NodeID),
	}, monocerosCfg.NodeID, log.LstdFlags|log.Lshortfile)
	mc := monoceros.NewMonoceros(tree, treeRRN, gn, monocerosCfg, mcLogger)
	mc.Start()

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Waiting for quit signal...")
	<-quit
	fmt.Println("Quit signal received. Shutting down.")
}

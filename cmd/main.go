package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	hvCfg := hyparview.Config{}
	err := env.Parse(&hvCfg)
	if err != nil {
		log.Fatal(err)
	}
	err = env.Parse(&hvCfg.HyParViewConfig)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(hvCfg)

	plumtreeCfg := plumtree.Config{}
	err = env.Parse(&plumtreeCfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(plumtreeCfg)

	monocerosCfg := monoceros.Config{}
	err = env.Parse(&monocerosCfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(monocerosCfg)

	self := data.Node{
		ID:            hvCfg.NodeID,
		ListenAddress: hvCfg.ListenAddress,
	}
	logger := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/%d.log", monocerosCfg.NodeID),
	}, strconv.Itoa(int(monocerosCfg.NodeID)), log.LstdFlags|log.Lshortfile)

	connManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(self.ListenAddress))
	hyparview, err := hyparview.NewHyParView(hvCfg.HyParViewConfig, self, connManager, logger)
	if err != nil {
		log.Fatal(err)
	}
	err = hyparview.Join(hvCfg.ContactNodeID, hvCfg.ContactNodeAddress)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Duration(monocerosCfg.JoinWait) * time.Second)
	l1 := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/pt_%d.log", monocerosCfg.NodeID),
	}, strconv.Itoa(int(monocerosCfg.NodeID)), log.LstdFlags|log.Lshortfile)
	tree := plumtree.NewPlumtree(plumtreeCfg, hyparview, l1)
	l2 := log.New(&lumberjack.Logger{
		Filename: fmt.Sprintf("log/mc_%d.log", monocerosCfg.NodeID),
	}, strconv.Itoa(int(monocerosCfg.NodeID)), log.LstdFlags|log.Lshortfile)
	mc := monoceros.NewMonoceros(tree, monocerosCfg, l2)
	mc.Start()

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Waiting for quit signal...")
	<-quit
	fmt.Println("Quit signal received. Shutting down.")
}

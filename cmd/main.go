package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/c12s/monoceros"
	"github.com/c12s/plumtree"
	"github.com/caarlos0/env"
)

func main() {
	// CONFIG

	hvConfig := hyparview.Config{}
	err := env.Parse(&hvConfig)
	if err != nil {
		log.Fatal(err)
	}

	ptConfig := plumtree.Config{}
	err = env.Parse(&ptConfig)
	if err != nil {
		log.Fatal(err)
	}

	aggConfig := monoceros.AggregationConfig{}
	err = env.Parse(&aggConfig)
	if err != nil {
		log.Fatal(err)
	}

	mcConfig := monoceros.Config{
		Aggregation: aggConfig,
	}
	err = env.Parse(&mcConfig)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(mcConfig)

	// loggers

	gnHvLogFile, err := os.Create(fmt.Sprintf("log/gn_hv_%s.log", mcConfig.NodeID))
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer gnHvLogFile.Close()
	gnHvLogger := log.New(gnHvLogFile, "", log.LstdFlags|log.Lshortfile)

	rnHvLogFile, err := os.Create(fmt.Sprintf("log/rn_hv_%s.log", mcConfig.NodeID))
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer rnHvLogFile.Close()
	rnHvLogger := log.New(rnHvLogFile, "", log.LstdFlags|log.Lshortfile)

	rrnHvLogFile, err := os.Create(fmt.Sprintf("log/rrn_hv_%s.log", mcConfig.NodeID))
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer rrnHvLogFile.Close()
	rrnHvLogger := log.New(rrnHvLogFile, "", log.LstdFlags|log.Lshortfile)

	rnPtLogFile, err := os.Create(fmt.Sprintf("log/rn_pt_%s.log", mcConfig.NodeID))
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer rnPtLogFile.Close()
	rnPtLogger := log.New(rnPtLogFile, "", log.LstdFlags|log.Lshortfile)

	rrnPtLogFile, err := os.Create(fmt.Sprintf("log/rrn_pt_%s.log", mcConfig.NodeID))
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer rrnPtLogFile.Close()
	rrnPtLogger := log.New(rrnPtLogFile, "", log.LstdFlags|log.Lshortfile)

	mcLogFile, err := os.Create(fmt.Sprintf("log/mc_%s.log", mcConfig.NodeID))
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer mcLogFile.Close()
	mcLogger := log.New(mcLogFile, "", log.LstdFlags|log.Lshortfile)

	// INIT

	// global network setup

	gnSelf := data.Node{
		ID:            mcConfig.NodeID,
		ListenAddress: mcConfig.GNListenAddr,
	}
	gnConnManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(gnSelf.ListenAddress))
	gnHv, err := hyparview.NewHyParView(hvConfig, gnSelf, gnConnManager, gnHvLogger)
	if err != nil {
		log.Fatal(err)
	}
	gn := monoceros.NewGossipNode(gnHv)

	// regional network setup

	// hyparview
	rnSelf := data.Node{
		ID:            mcConfig.NodeID,
		ListenAddress: mcConfig.RNListenAddr,
	}
	rnConnManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(rnSelf.ListenAddress))
	rnHv, err := hyparview.NewHyParView(hvConfig, rnSelf, rnConnManager, rnHvLogger)
	if err != nil {
		log.Fatal(err)
	}
	// plumtree
	rnTree := plumtree.NewPlumtree(ptConfig, rnHv, rnPtLogger)

	// regional roots network

	// hyparview
	rrnSelf := data.Node{
		ID:            mcConfig.NodeID,
		ListenAddress: mcConfig.RRNListenAddr,
	}
	rrnConnManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(rrnSelf.ListenAddress))
	rrnHv, err := hyparview.NewHyParView(hvConfig, rrnSelf, rrnConnManager, rrnHvLogger)
	if err != nil {
		log.Fatal(err)
	}
	rrnTree := plumtree.NewPlumtree(ptConfig, rrnHv, rrnPtLogger)

	// monoceros setup
	mc := monoceros.NewMonoceros(rnTree, rrnTree, gn, mcConfig, mcLogger)
	mc.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", mc.MetricsHandler)
	server := http.Server{
		Addr:    mcConfig.HTTPServerAddr,
		Handler: mux,
	}
	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Waiting for quit signal...")
	<-quit
	fmt.Println("Quit signal received. Shutting down.")

	if err := server.Close(); err != nil {
		log.Fatalf("HTTP close error: %v", err)
	}
}

package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
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

	// log.Println("waiting for the start signal, path", mcConfig.WaitFilePath)
	// if err := waitForFile(mcConfig.WaitFilePath, 1000*time.Second); err != nil {
	// 	log.Println("Error:", err)
	// 	return
	// }

	// loggers

	// gnHvLogFile, err := os.Create(fmt.Sprintf("%s/gn_hv_%s.log", mcConfig.LogPath, mcConfig.NodeID))
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }
	// defer gnHvLogFile.Close()
	// gnHvLogger := log.New(gnHvLogFile, "", log.LstdFlags|log.Lshortfile)
	gnHvLogger := log.New(os.Stdout, "GLOBAL HV", log.LstdFlags|log.Lshortfile)

	// rnHvLogFile, err := os.Create(fmt.Sprintf("%s/rn_hv_%s.log", mcConfig.LogPath, mcConfig.NodeID))
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }
	// defer rnHvLogFile.Close()
	// rnHvLogger := log.New(rnHvLogFile, "", log.LstdFlags|log.Lshortfile)
	rnHvLogger := log.New(os.Stdout, "REGION HV", log.LstdFlags|log.Lshortfile)

	// rrnHvLogFile, err := os.Create(fmt.Sprintf("%s/rrn_hv_%s.log", mcConfig.LogPath, mcConfig.NodeID))
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }
	// defer rrnHvLogFile.Close()
	// rrnHvLogger := log.New(rrnHvLogFile, "", log.LstdFlags|log.Lshortfile)
	rrnHvLogger := log.New(os.Stdout, "RRN HV", log.LstdFlags|log.Lshortfile)

	// rnPtLogFile, err := os.Create(fmt.Sprintf("%s/rn_pt_%s.log", mcConfig.LogPath, mcConfig.NodeID))
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }
	// defer rnPtLogFile.Close()
	// rnPtLogger := log.New(rnPtLogFile, "", log.LstdFlags|log.Lshortfile)
	rnPtLogger := log.New(os.Stdout, "REGION PT", log.LstdFlags|log.Lshortfile)

	// rrnPtLogFile, err := os.Create(fmt.Sprintf("%s/rrn_pt_%s.log", mcConfig.LogPath, mcConfig.NodeID))
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }
	// defer rrnPtLogFile.Close()
	// rrnPtLogger := log.New(rrnPtLogFile, "", log.LstdFlags|log.Lshortfile)
	rrnPtLogger := log.New(os.Stdout, "RRN PT", log.LstdFlags|log.Lshortfile)

	// mcLogFile, err := os.Create(fmt.Sprintf("%s/mc_%s.log", mcConfig.LogPath, mcConfig.NodeID))
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }
	// defer mcLogFile.Close()
	// mcLogger := log.New(mcLogFile, "", log.LstdFlags|log.Lshortfile)
	mcLogger := log.New(os.Stdout, "MONOCEROS", log.LstdFlags|log.Lshortfile)

	// INIT

	// global network setup

	gnSelf := data.Node{
		ID:            mcConfig.NodeID,
		ListenAddress: mcConfig.GNListenAddr,
	}
	gnConnManager := transport.NewConnManager(false, transport.NewTCPConn, transport.AcceptTcpConnsFn(gnSelf.ListenAddress, false))
	// gnConnManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(fmt.Sprintf("0.0.0.0:%s", strings.Split(gnSelf.ListenAddress, ":")[1])))
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
	rnConnManager := transport.NewConnManager(true, transport.NewTCPConn, transport.AcceptTcpConnsFn(rnSelf.ListenAddress, true))
	// rnConnManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(fmt.Sprintf("0.0.0.0:%s", strings.Split(rnSelf.ListenAddress, ":")[1])))
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
	rrnConnManager := transport.NewConnManager(true, transport.NewTCPConn, transport.AcceptTcpConnsFn(rrnSelf.ListenAddress, true))
	// rrnConnManager := transport.NewConnManager(transport.NewTCPConn, transport.AcceptTcpConnsFn(fmt.Sprintf("0.0.0.0:%s", strings.Split(rrnSelf.ListenAddress, ":")[1])))
	rrnHvConfig := hvConfig
	rrnHvConfig.Fanout = 1000
	rrnHv, err := hyparview.NewHyParView(rrnHvConfig, rrnSelf, rrnConnManager, rrnHvLogger)
	if err != nil {
		log.Fatal(err)
	}
	rrnTree := plumtree.NewPlumtree(ptConfig, rrnHv, rrnPtLogger)

	// monoceros setup
	mc := monoceros.NewMonoceros(rnTree, rrnTree, gn, mcConfig, mcLogger)
	mc.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", mc.MetricsHandler)
	mux.HandleFunc("GET /state", mc.StateHandler)
	mux.HandleFunc("POST /rules", mc.AddRulesHandler)
	mux.HandleFunc("DELETE /rules/{id}", mc.RemoveRulesHandler)
	server := http.Server{
		Addr: mcConfig.HTTPServerAddr,
		// Addr:    fmt.Sprintf("0.0.0.0:%s", strings.Split(mcConfig.HTTPServerAddr, ":")[1]),
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

func waitForFile(path string, timeout time.Duration) error {
	start := time.Now()
	for {
		if _, err := os.Stat(path); err == nil {
			// file exists
			return nil
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout waiting for %s", path)
		}
		time.Sleep(100 * time.Millisecond) // backoff interval
	}
}

package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"omnistore/raft"
	"omnistore/server"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <node_id_0_1_2>")
		return
	}

	me, _ := strconv.Atoi(os.Args[1])
	ports := []string{":8000", ":8001", ":8002"}
	
	// Create cluster peer list (localhost for testing)
	peers := []string{"localhost:8000", "localhost:8001", "localhost:8002"}

	// 1. Initialize WAL
	persister := raft.MakePersister(fmt.Sprintf("wal_node_%d.dat", me))

	// 2. Start the Server and Raft instance
	omniServer := server.MakeServer(peers, me, persister)

	// 3. Register with Go's net/rpc package
	rpc.Register(omniServer)
	rpc.HandleHTTP()

	// 4. Listen and Serve
	listener, err := net.Listen("tcp", ports[me])
	if err != nil {
		fmt.Printf("Listen error: %v\n", err)
		return
	}

	fmt.Printf("OmniStore Node %d booting up on port %s...\n", me, ports[me])
	http.Serve(listener, nil)
}
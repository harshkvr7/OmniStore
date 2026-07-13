package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"kvstore/server"
	"net/rpc"
	"sync"
	"time"
)

const ChunkSize = 1024 * 1024 // 1MB

func chunkData(data []byte) (map[string][]byte, []string) {
	chunks := make(map[string][]byte)
	var manifest []string
	for i := 0; i < len(data); i += ChunkSize {
		end := i + ChunkSize
		if end > len(data) {
			end = len(data)
		}
		hash := sha256.Sum256(data[i:end])
		hashStr := hex.EncodeToString(hash[:])
		chunks[hashStr] = data[i:end]
		manifest = append(manifest, hashStr)
	}
	return chunks, manifest
}

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:8000")
	if err != nil {
		fmt.Println("Connection error:", err)
		return
	}
	defer client.Close()

	// 1. GENERATE AND PUT
	fmt.Println("Generating 5MB object...")
	largeData := make([]byte, 5*1024*1024)
	chunks, manifest := chunkData(largeData)

	fmt.Printf("Uploading %d chunks concurrently...\n", len(manifest))
	var wg sync.WaitGroup
	for hash, data := range chunks {
		wg.Add(1)
		go func(h string, d []byte) {
			defer wg.Done()
			args := &server.PutArgs{OpType: "PutChunk", Key: h, Value: d}
			reply := &server.PutReply{}
			client.Call("OmniServer.Put", args, reply)
		}(hash, data)
	}
	wg.Wait()

	manifestArgs := &server.PutArgs{OpType: "PutManifest", Key: "file.bin", Manifest: manifest}
	manifestReply := &server.PutReply{}
	if err := client.Call("OmniServer.Put", manifestArgs, manifestReply); err == nil && manifestReply.Success {
		fmt.Println("Success: Object replicated.")
	}

	time.Sleep(1 * time.Second)

	// 2. UPDATE
	fmt.Println("\nUpdating object...")
	newManifest := append(manifest, "simulated_new_chunk_hash")
	updateArgs := &server.UpdateArgs{Key: "file.bin", Manifest: newManifest}
	updateReply := &server.UpdateReply{}
	if err := client.Call("OmniServer.Update", updateArgs, updateReply); err == nil && updateReply.Success {
		fmt.Println("Success: Object updated.")
	}

	time.Sleep(1 * time.Second)

	// 3. DELETE
	// fmt.Println("\nDeleting object...")
	// deleteArgs := &server.DeleteArgs{Key: "file.bin"}
	// deleteReply := &server.DeleteReply{}
	// if err := client.Call("OmniServer.Delete", deleteArgs, deleteReply); err == nil && deleteReply.Success {
	// 	fmt.Println("Success: Object deleted and chunks garbage collected.")
	// }
}

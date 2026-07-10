package store

import (
	"crypto/sha256"
	"encoding/hex"
)

const ChunkSize = 1024 * 1024 // 1MB blocks

func ChunkSplitter(data []byte) (map[string][]byte, []string) {
	chunks := make(map[string][]byte)
	var manifest []string

	for i := 0; i < len(data); i += ChunkSize {
		end := i + ChunkSize
		if end > len(data) {
			end = len(data)
		}

		chunkBytes := data[i:end]
		hash := sha256.Sum256(chunkBytes)
		hashStr := hex.EncodeToString(hash[:])

		chunks[hashStr] = chunkBytes
		manifest = append(manifest, hashStr)
	}

	return chunks, manifest
}
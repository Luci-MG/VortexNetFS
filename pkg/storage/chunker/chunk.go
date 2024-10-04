package chunker

import (
	"bytes"
	"fmt"
)

// chunk size of the packets
const ChunkSize = 5 * 1024 * 1024 //5mb

// ChunkFile splits the file into chunks.
func ChunkFile(data []byte, fileId string) (map[string][]byte, int, error) {
	chunks := make(map[string][]byte)
	totalSize := len(data)
	chunkCounter := uint16(1)
	for i := 0; i < totalSize; i += ChunkSize {
		end := i + ChunkSize
		if end > totalSize {
			end = totalSize
		}
		chunkId := fmt.Sprintf("%s::%04x", fileId, chunkCounter)
		chunks[chunkId] = append([]byte{}, data[i:end]...)
		chunkCounter++
	}
	return chunks, len(chunks), nil
}

// GenerateChunkIDs generates chunk ids by fileSize.
func GenerateChunkIDs(fileId string, fileSize int64) []string {
	chunkCount := int((fileSize + ChunkSize - 1) / ChunkSize)
	chunkIds := make([]string, chunkCount)

	for i := 0; i < chunkCount; i++ {
		chunkIds[i] = fmt.Sprintf("%s::%04x", fileId, i+1)
	}

	return chunkIds
}

// MergeChunks merges chunks.
func MergeChunks(chunks [][]byte) ([]byte, error) {
	var buffer bytes.Buffer
	for _, chunk := range chunks {
		_, err := buffer.Write(chunk)
		if err != nil {
			return nil, fmt.Errorf("error merging chunks: %v", err)
		}
	}
	return buffer.Bytes(), nil
}

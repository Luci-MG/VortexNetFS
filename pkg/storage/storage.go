package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// StorgaeEngine Interfaces
type StorageEngine interface {
	SaveChunk(chunkId string, data []byte) error
	RetrieveChunk(chunkId string) ([]byte, error)
}

// LocalStorageEngine storage engine where we store files and retrieve.
type LocalStorageEngine struct {
	storageDir string
}

// NewLocalStorageEngine created new storage engine.
func NewLocalStorageEngine(storageDir string) *LocalStorageEngine {
	return &LocalStorageEngine{
		storageDir: storageDir,
	}
}

// SaveChunk saves chunk.
func (s *LocalStorageEngine) SaveChunk(chunkId string, data []byte) error {
	chunkPath := filepath.Join(s.storageDir, chunkId)

	// ensure dir exists
	if err := os.MkdirAll(filepath.Dir(chunkPath), 0755); err != nil {
		return err
	}

	// create and write the chunk
	file, err := os.Create(chunkPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// write the chunk data into the file
	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// RetrieveChunk retrieves chunk.
func (s *LocalStorageEngine) RetrieveChunk(chunkId string) ([]byte, bool, error) {
	chunkPath := filepath.Join(s.storageDir, chunkId)

	// chunk not found
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		return nil, false, fmt.Errorf("chunk %s not found", chunkId)
	}

	//open the file
	file, err := os.Open(chunkPath)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	// read the content
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, false, fmt.Errorf("failed to retrieve chunk %s: %v", chunkId, err)
	}

	fmt.Printf("Chunk %s retrieved successfully\n", chunkId)
	return data, true, nil
}

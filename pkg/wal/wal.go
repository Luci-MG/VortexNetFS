package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// LogEntry structure
type LogEntry struct {
	Timestamp     time.Time `json:"timestamp"`
	NodeId        uint64    `json:"node_id"`
	OperationType string    `json:"operation_type"`
	Details       string    `json:"details"`
}

// WAL - Write Ahead Log structure
type WAL struct {
	logFile        *os.File
	currentSize    int64
	maxSize        int64
	filePath       string
	logFileCounter int
}

// NewWAL Creates a new WAL
func NewWAL(walFilePath string, maxSize int64) (*WAL, error) {
	walFilePath = walFilePath + "/wal"
	dir := filepath.Dir(walFilePath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	file, err := os.OpenFile(fmt.Sprintf("%s_%d.log", walFilePath, 0), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	fileInfo, _ := file.Stat()
	return &WAL{
		logFile:     file,
		currentSize: fileInfo.Size(),
		maxSize:     maxSize,
		filePath:    walFilePath,
	}, nil
}

// WriteLogEntry writes entry to WAL.
func (w *WAL) WriteLogEntry(nodeId uint64, operationType string, details string) error {
	entry := LogEntry{
		Timestamp:     time.Now(),
		NodeId:        nodeId,
		OperationType: operationType,
		Details:       details,
	}

	entryData, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = w.logFile.Write(append(entryData, '\n'))
	if err != nil {
		return err
	}

	w.currentSize += int64(len(entryData))
	if w.currentSize > w.maxSize {
		return w.rotateLog()
	}
	return nil
}

// rotateLog When a log file exceeds size creates New WAL File.
func (w *WAL) rotateLog() error {
	err := w.logFile.Close()
	if err != nil {
		return err
	}

	w.logFileCounter++
	newLogFileName := fmt.Sprintf("%s_%d.log", w.filePath+"/wal", w.logFileCounter)
	w.logFile, err = os.OpenFile(newLogFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.currentSize = 0
	return nil
}

// Close closes the WAL Writes.
func (w *WAL) Close() error {
	return w.logFile.Close()
}

// RecoverWAL recovers info from past WAL.
func (w *WAL) RecoverWAL(recoverFunc func(LogEntry) error) error {
	for i := 0; i <= w.logFileCounter; i++ {
		file, err := os.Open(fmt.Sprintf("%s_%d.log", w.filePath, i))
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var entry LogEntry
			if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
				return err
			}
			if err := recoverFunc(entry); err != nil {
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	fmt.Println("WAL recovery completed successfully")
	return nil
}

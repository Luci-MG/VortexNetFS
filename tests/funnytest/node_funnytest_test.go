package funnytest_test

import (
	"VNFS/internal/node"
	"VNFS/pkg/communication"
	"VNFS/pkg/communication/client"
	"VNFS/pkg/storage/chunker"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNodeSetupAndFileTransfer(t *testing.T) {

	// clear the past test data
	if err := os.RemoveAll("wal/"); err != nil {
		fmt.Printf("error removing %s: %v", "wal/", err)
	}

	if err := os.RemoveAll("extracted/"); err != nil {
		fmt.Printf("error removing %s: %v", "extracted/", err)
	}

	if err := os.RemoveAll("storage/"); err != nil {
		fmt.Printf("error removing %s: %v", "storage/", err)
	}

	time.Sleep(3 * time.Second)

	// node 1 config
	configFile := "configs/nodeconfig1.json"
	NodeConfig := loadConfig(configFile)
	node1, err := node.NewNode(NodeConfig)
	if err != nil {
		fmt.Printf("Error creating Node 1: %v\n", err)
		os.Exit(1)
	}

	// starting node 1
	go func() {
		err := node1.Start()
		if err != nil {
			fmt.Printf("Error starting Node 1: %v\n", err)
		}
	}()

	time.Sleep(5 * time.Second)

	// node 2 config
	configFile2 := "configs/nodeconfig2.json"
	NodeConfig2 := loadConfig(configFile2)
	node2, err := node.NewNode(NodeConfig2)
	if err != nil {
		fmt.Printf("Error creating Node 2: %v\n", err)
		os.Exit(1)
	}

	// starting node 2
	go func() {
		err := node2.Start()
		if err != nil {
			fmt.Printf("Error starting Node 2: %v\n", err)
		}
	}()

	// allow nodes to start and communicate not require just added for testing
	time.Sleep(5 * time.Second)
	fmt.Println("Starting small tests!!!")

	// file we use to do test
	filePath := "Moodeng.jpg"
	fileData, err := readFile(filePath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	// creating a new client
	testclient, _ := client.NewTCPClient(NodeConfig.Address, NodeConfig.NodeId)
	// creating a mock filemetadata
	filemetdata := *node1.DHT.NewFileMetaData("Moodeng", ".jpg", int64(len(fileData)))
	// filemetadatapayload
	filemetapaylod, _ := json.Marshal(filemetdata)
	// creating message
	msg := &communication.Message{
		Type:    uint8(communication.MessageTypeFileMetaData),
		NodeId:  NodeConfig.NodeId,
		Payload: filemetapaylod,
	}
	testclient.SendMesasge(*msg)
	// receive the message
	receivedFileId, err := testclient.ReceiveMessage()
	// check if any error
	if err != nil {
		fmt.Println("error while receving the fileid: ", err)
	}
	fmt.Println("received FileId: ", string(receivedFileId.Payload))
	testclient.Close()
	// wait for some time
	time.Sleep(3 * time.Second)

	// for key, value := range node1.DHT.FileMetaDataDHT {
	// 	fmt.Println("key: ", key, ", value: ", value)
	// }

	// for key, value := range node2.DHT.FileMetaDataDHT {
	// 	fmt.Println("key: ", key, ", value: ", value)
	// }

	// make chunks and send based on fileid
	testclient, _ = client.NewTCPClient(NodeConfig.Address, NodeConfig.NodeId)
	chunks, _, _ := chunker.ChunkFile(fileData, string(receivedFileId.Payload))
	fmt.Println("Sending file chunks! ", len(chunks))
	for chunkId, chunkData := range chunks {
		chunkMsg := &communication.Message{
			Type:    uint8(communication.MessageTypeSaveChunk),
			NodeId:  NodeConfig.NodeId,
			Payload: append([]byte(chunkId), chunkData...),
		}
		err := testclient.SendMesasge(*chunkMsg)
		if err != nil {
			fmt.Println("error while chunk sending ", err)
		}
	}
	fmt.Println("Sent file chunks created!")
	testclient.Close()

	// sent and closed client connection and closed
	time.Sleep(3 * time.Second)

	// test if the filemetadeta have been saved
	testclient, _ = client.NewTCPClient(NodeConfig.Address, NodeConfig.NodeId)
	requestFilemsg := &communication.Message{
		Type:    uint8(communication.MessageTypeRetrieveFileMetaData),
		NodeId:  NodeConfig.NodeId,
		Payload: []byte(string(receivedFileId.Payload)),
	}
	testclient.SendMesasge(*requestFilemsg)
	receivedFileMetaData, _ := testclient.ReceiveMessage()
	receivedFileMetaDataDes := node1.DHT.FileMetaDataFromBytes(receivedFileMetaData.Payload)
	fmt.Println("recieved file metadata: ", receivedFileMetaDataDes)

	time.Sleep(3 * time.Second)
	// another message to test the chunk
	requestFilemsg = &communication.Message{
		Type:    uint8(communication.MessageTypeRetrieveChunk),
		NodeId:  NodeConfig.NodeId,
		Payload: []byte(receivedFileMetaDataDes.Chunks[0]),
	}
	testclient.SendMesasge(*requestFilemsg)
	// wait for some time
	time.Sleep(1 * time.Second)
	// receive the message
	receivedChunk, _ := testclient.ReceiveMessage()
	fmt.Println("recieved chunk: ", len(receivedChunk.Payload))
	testclient.Close()

	// test get the file
	testclient, _ = client.NewTCPClient(NodeConfig.Address, NodeConfig.NodeId)
	requestFilefullmsg := &communication.Message{
		Type:    uint8(communication.MessageTypeRetrieveFile),
		NodeId:  NodeConfig.NodeId,
		Payload: []byte(receivedFileId.Payload),
	}
	testclient.SendMesasge(*requestFilefullmsg)
	err = saveFileByType(testclient.Conn, receivedFileMetaDataDes.FileName,
		receivedFileMetaDataDes.FileType, uint(receivedFileMetaDataDes.FileSize))
	if err != nil {
		fmt.Println("error while savinf the file", err)
	}

	// testing to get the data from another node like node 2
	// first lets test if we can get the chunk
	testclient, _ = client.NewTCPClient(NodeConfig2.Address, NodeConfig2.NodeId)
	requestFilemsg = &communication.Message{
		Type:    uint8(communication.MessageTypeRetrieveChunk),
		NodeId:  NodeConfig2.NodeId,
		Payload: []byte(receivedFileMetaDataDes.Chunks[0]),
	}
	testclient.SendMesasge(*requestFilemsg)
	// wait for some time
	time.Sleep(1 * time.Second)
	// receive the message
	receivedChunk, err = testclient.ReceiveMessage()
	fmt.Println("recieved chunk from node 2: ", len(receivedChunk.Payload))
	testclient.Close()
	if err != nil {
		fmt.Println("error while getting chunk from other node", err)
		os.Exit(1)
	}

	// testing to get the file from the other node
	testclient, _ = client.NewTCPClient(NodeConfig2.Address, NodeConfig2.NodeId)
	requestFilefullmsg = &communication.Message{
		Type:    uint8(communication.MessageTypeRetrieveFile),
		NodeId:  NodeConfig.NodeId,
		Payload: []byte(receivedFileId.Payload),
	}
	testclient.SendMesasge(*requestFilefullmsg)
	err = saveFileByType(testclient.Conn, receivedFileMetaDataDes.FileName+"_node2",
		receivedFileMetaDataDes.FileType, uint(receivedFileMetaDataDes.FileSize))
	if err != nil {
		fmt.Println("error while saving the file from node 2", err)
		os.Exit(1)
	}

	fmt.Println("end testing !!!")

	for key, value := range node2.DHT.FileMetaDataDHT {
		fmt.Println("key: ", key, ", value: ", value)
	}

	node1.Shutdown()
	node2.Shutdown()
}

// function to load config
func loadConfig(configFile string) *node.NodeConfig {
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Printf("Failed to open config file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	nodeConfig := &node.NodeConfig{}
	err = decoder.Decode(nodeConfig)
	if err != nil {
		fmt.Printf("Error decoding config file: %v\n", err)
		os.Exit(1)
	}
	return nodeConfig
}

// function to read file
func readFile(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}

	// read the file into a byte slice
	fileData := make([]byte, fileInfo.Size())
	_, err = file.Read(fileData)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	return fileData, nil
}

// save the extracted bytes
func saveFileByType(conn net.Conn, fileName string, fileextenrstion string, sizefile uint) error {
	// Create or open the file based on the file type
	excPath := "./extracted/" + fileName + fileextenrstion

	// ensure dir exists
	if err := os.MkdirAll(filepath.Dir(excPath), 0755); err != nil {
		return err
	}

	file, err := os.Create(excPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	// buffer for reading data from the connection
	buf := make([]byte, sizefile)

	fmt.Printf("Receiving file: %s\n", fileName)

	for {
		// read from our TCP connection
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("file transfer completed.")
				break
			}
			return fmt.Errorf("error reading from connection: %v", err)
		}

		if n > 0 {
			// write to the file
			_, writeErr := file.Write(buf[:n])
			if writeErr != nil {
				return fmt.Errorf("error writing to file: %v", writeErr)
			}
		}
	}

	fmt.Printf("successfully saved file: %s\n", fileName)
	return nil
}

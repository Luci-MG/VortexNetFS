package communication

const (
	MessageTypeJoin uint = iota
	MessageTypeLeave
	MessageTypeHeartBeat
	MessageTypeAck
	MessageTypeSaveChunk
	MessageTypeRetrieveChunk
	MessageTypeDHTSync
	MessageTypeFileMetaData
	MessageTypeRetrieveFileMetaData
	MessageTypeRetrieveFile
)

// Message structure for the communication
type Message struct {
	Type    uint8  `json:"type"`    // type of the message
	NodeId  uint64 `json:"nodeId"`  // node the message is sent from
	Payload []byte `json:"payload"` // payload of the message
}

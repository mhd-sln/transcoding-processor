package processor

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

const partition = 0

type submitter struct {
	topic      string
	connection *kafka.Conn
}

type Sender interface {
	send(string) error
}

// transcode
type Entry struct {
	Id        string    `json:"id,omitempty"`
	Path      string    `json:"path,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

func NewSubmitter(topic string) *submitter {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	s := &submitter{
		connection: conn,
	}
	return s
}

func NewMessage(message string) Entry {
	return Entry{
		Id:        uuid.New().String(),
		Path:      message,
		Timestamp: time.Now(),
	}
}

func (s *submitter) Send(msg string) (Entry, error) {
	m := NewMessage(msg)
	bs, err := json.Marshal(m)
	if err != nil {
		return Entry{}, err
	}

	_, err = s.connection.WriteMessages(kafka.Message{Value: bs})
	return m, err
}

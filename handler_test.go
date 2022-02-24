package root

import (
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	kafka "github.com/segmentio/kafka-go"
)

type fakeMinioClient struct {
	removeObjectCallCount int
	fPutObjectCallCount   int
}

type fakeReader struct{}

func (fakeReader) ReadMessage(c context.Context) (kafka.Message, error) {
	return kafka.Message{Value: []byte(`{"Id":"testId", "Path":"testValue", "Timestamp":"2006-01-02T15:04:05+07:00"}`)}, nil
}
func (fMClient *fakeMinioClient) FPutObject(ctx context.Context, bucketName string, objectName string, filePath string, opts minio.PutObjectOptions) (info minio.UploadInfo, err error) {
	fMClient.fPutObjectCallCount++
	return info, err
}
func (fMClient *fakeMinioClient) RemoveObject(ctx context.Context, bucketName string, objectName string, opts minio.RemoveObjectOptions) error {
	fMClient.removeObjectCallCount++
	return nil
}

func Test_handlerSendFailure(t *testing.T) {
	fmc := &fakeMinioClient{}
	h := handler{
		//connection:  &fakeKafkaConn{},
		minioClient: fmc,
	}
	h.Send("test message")
	if fmc.removeObjectCallCount != 1 {
		t.Fatal("Unexpected remove object call count")
	}

}

func Test_handlerSendSuccess(t *testing.T) {
	fmc := &fakeMinioClient{}
	h := handler{
		//connection:  &fakeKafkaConn{succeeds: true},
		minioClient: fmc,
	}
	h.Send("test message")
	if fmc.removeObjectCallCount != 0 {
		t.Fatal("Unexpected remove object call count")
	}
}

func (*fakeReader) Close() error { return nil }
func Test_handlerReceive(t *testing.T) {
	r := &fakeReader{}
	h := handler{
		reader: r,
	}
	en, err := h.Receive()
	if err != nil {
		t.Fatalf("Unexpected receive error %v", err)
	}
	// "testId", "Value":"testValue", "Timestamp":"12:23:34"
	ti, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	if en.Id != "testId" && en.Path != "testValue" && !en.Timestamp.Equal(ti) {
		t.Errorf("Unexpected entry %v", en)
	}
}

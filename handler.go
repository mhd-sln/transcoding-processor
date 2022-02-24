package root

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"

	kafka "github.com/segmentio/kafka-go"
)

const partition = 0

type MinioClientInterface interface {
	FPutObject(context.Context, string, string, string, minio.PutObjectOptions) (minio.UploadInfo, error)
	RemoveObject(context.Context, string, string, minio.RemoveObjectOptions) error
	FGetObject(context.Context, string, string, string, minio.GetObjectOptions) error
	MakeBucket(ctx context.Context, bucketName string, opts minio.MakeBucketOptions) error
	BucketExists(ctx context.Context, bucketName string) (found bool, err error)
	StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
}

type KafkaReaderInterface interface {
	io.Closer
	ReadMessage(context.Context) (kafka.Message, error)
}

type handler struct {
	topic       string
	reader      KafkaReaderInterface
	minioClient MinioClientInterface
	writer      *kafka.Writer
}

type Sender interface {
	send(string) error
}

// transcode
type Entry struct {
	Id         string    `json:"id,omitempty"`
	Path       string    `json:"path,omitempty"`
	Objectname string    `json:"objectname,omitempty"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
}

func ReadHandler(topic string, groupid string, mAddr string, mAKey string, mPw string, kAddr string) *handler {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kAddr},
		Topic:   topic,
		//Partition: 0,
		// todo : look into improving two groupid
		GroupID:         groupid,
		MinBytes:        1,    // 1B
		MaxBytes:        10e6, // 10MB
		ReadLagInterval: 500 * time.Millisecond,
		QueueCapacity:   1,
		MaxWait:         1 * time.Second,
	})
	endpoint := mAddr
	accessKeyID := mAKey
	secretAccessKey := mPw
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln(err)
	}
	s := &handler{
		topic:       topic,
		reader:      r,
		minioClient: minioClient,
	}
	return s
}

func WriteHandler(topic string, mAddr string, mAKey string, mPw string, kAddr string) *handler {

	w := &kafka.Writer{
		Addr:      kafka.TCP(kAddr),
		Topic:     topic,
		BatchSize: 10,
	}
	endpoint := mAddr
	accessKeyID := mAKey
	secretAccessKey := mPw
	// useSSL := true

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln(err)
	}

	s := &handler{
		topic:       topic,
		writer:      w,
		minioClient: minioClient,
	}

	/*go func() {
		for {
			log.Printf("%#v \n", s.reader.(*kafka.Reader).Stats())
			time.Sleep(5 * time.Second)
		}
	}()*/
	return s
}

func NewMessage(message string) Entry {
	return Entry{
		Id:        uuid.New().String(),
		Path:      message,
		Timestamp: time.Now(),
	}
}

func (s *handler) Close() {
	s.reader.Close()
}

func getFfmpegCmd(path string) (*exec.Cmd, string) {
	// TODO: store the file with random name in the tmp folder and remove it once uploaded to minio with some relevant name.
	op := path[:len(path)-len(filepath.Ext(path))] + ".jpg"
	c := exec.Command("ffmpeg", "-nostdin", "-ss", "3", "-i", path, "-frames:v", "1", op)
	return c, op
}

func (s *handler) Process(e Entry, w_num int) (string, error) {
	// folder/file.mp4
	bucketName := "submissions"

	// Send to minio
	// Upload the zip file
	objectName := e.Objectname
	t := os.TempDir()
	wt, err := os.MkdirTemp(t, fmt.Sprint(w_num))

	fmt.Println("tmp dir", t, w_num)
	fmt.Println("worker tmp dir", wt, w_num)

	filePath := filepath.Join(wt, filepath.Base(e.Path))

	ctx := context.Background()
	// TODO: Minio GET is a bottleneck. Cache it.
	start := time.Now()
	s.minioClient.FGetObject(ctx, bucketName, objectName, filePath, minio.GetObjectOptions{})
	minioGetDuration.Observe(float64(time.Since(start)) / float64(time.Second))

	c, fn := getFfmpegCmd(filePath)

	start = time.Now()
	log.Printf("before the combined output, worker num %d", w_num)
	b, err := c.CombinedOutput()

	if err != nil {
		fmt.Printf("metadata is %v, error is %v \n", string(b), err)
		return fn, fmt.Errorf("error: %w %d", err, w_num)
	}

	ffmpegDuration.Observe(float64(time.Since(start)) / float64(time.Second))
	log.Printf("after the ffmpeg process %v, worker num %d, error %v", time.Since(start), w_num, err)

	err = os.Remove(filePath)
	if err != nil {
		fmt.Printf("file could not be removed %v \n", err)
		return fn, fmt.Errorf("error: %w %d", err, w_num)
	}
	return fn, nil
}

func (s *handler) Send(msg string) (Entry, error) {
	bucketName := "submissions"

	// Send to minio
	// Upload the zip file
	filePath := msg
	contentType := "application/video"

	// open the file in the path. hash the content. check minio for a file with the same hash. if it exists then do not put in the minio
	f, err := os.Open(filePath)
	if err != nil {
		return Entry{}, err
	}
	defer f.Close()

	w := sha256.New()
	_, err = io.Copy(w, f)
	if err != nil {
		return Entry{}, err
	}
	h := w.Sum(nil)
	oName := hex.EncodeToString(h)

	m := NewMessage(msg)
	m.Objectname = oName
	jsonMsg, err := json.Marshal(m)
	if err != nil {
		return Entry{}, err
	}

	ctx := context.Background()
	weUploaded := false

	_, err = s.minioClient.StatObject(ctx, bucketName, oName, minio.GetObjectOptions{})
	if err != nil && err.Error() == "The specified key does not exist." {
		//fmt.Println("It means that the file does not exist ", oInfo, err)
		n, err := s.minioClient.FPutObject(ctx, bucketName, oName, filePath, minio.PutObjectOptions{ContentType: contentType})
		if err != nil {
			log.Fatalln(err)
		}
		weUploaded = true
		log.Printf("Successfully uploaded %s of size %d\n", oName, n.Size)
	}
	if err != nil {
		log.Fatalln(err)
		return Entry{}, err
	}
	err = s.writer.WriteMessages(ctx, kafka.Message{Value: jsonMsg})
	if err != nil && weUploaded {
		log.Printf("Writing to kafka failed. Removing the object %s from the object store", oName)
		err2 := s.minioClient.RemoveObject(ctx, bucketName, oName, minio.RemoveObjectOptions{})
		if err2 != nil {
			return Entry{}, fmt.Errorf("%s: Failed to remove the object with an error %v : %v", oName, err2, err)
		}
	}

	return m, err
}

var (
	AppDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "application_duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	readMsgDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "read_message_duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	ffmpegDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "ffmpeg_duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	minioGetDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "minio_get_duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
)

func init() {
	prometheus.MustRegister(readMsgDuration)
	prometheus.MustRegister(ffmpegDuration)
	prometheus.MustRegister(AppDuration)
	prometheus.MustRegister(minioGetDuration)
}

func (s *handler) Receive() (*Entry, error) {
	start := time.Now()
	m, err := s.reader.ReadMessage(context.Background())
	readMsgDuration.Observe(float64(time.Since(start)) / float64(time.Second))

	log.Printf("after the read message %v", time.Since(start))

	if err != nil {
		return nil, fmt.Errorf("Error reading message: %w", err)
	}
	v := &Entry{}
	err = json.Unmarshal(m.Value, v)
	if err != nil {
		//fmt.Printf("err is %v \n", err)
		return nil, fmt.Errorf("Error in unmarshalling %q: %w", m.Value, err)
	}
	return v, nil
	// fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

	//_, err := s.connection.Read(b)

}

func (s *handler) UploadResolve(fn string) error {
	bucketName := "output"

	// Send to minio
	objectName := filepath.Base(fn)
	filePath := fn
	contentType := "application/jpeg"
	found, err := s.minioClient.BucketExists(context.Background(), bucketName)
	if err != nil {
		return fmt.Errorf("Error checking for the bucket existence %v : %w", bucketName, err)
	}
	if !found {
		err := s.minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("Error creating the bucket %v : %w", bucketName, err)
		}
	}

	_, err = s.minioClient.FPutObject(context.Background(), bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})

	if err != nil {
		return fmt.Errorf("Error uploading the output file %v : %w", fn, err)
	}
	return nil
}

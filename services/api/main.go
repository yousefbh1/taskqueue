package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	gen "example.com/taskqueue/api/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Redis & RabbitMQ globals
var (
	rdb *redis.Client
	amq *amqp.Connection
	ch  *amqp.Channel
	ctx = context.Background()
)

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func atoi32(s string) int32 {
	if s == "" {
		return 0
	}
	n, _ := strconv.Atoi(s)
	return int32(n)
}

// init Redis
func initRedis() {
	addr := getenv("REDIS_ADDR", "redis:6379")
	rdb = redis.NewClient(&redis.Options{Addr: addr})
	must(rdb.Ping(ctx).Err())
}

// init RabbitMQ
func initRabbit() {
	url := getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
	var (
		conn *amqp.Connection
		err  error
	)
	for i := 0; i < 20; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Printf("API waiting for rabbitmq... (%v)", err)
		time.Sleep(2 * time.Second)
	}
	must(err)
	amq = conn

	chx, err := amq.Channel()
	must(err)
	ch = chx

	must(ch.ExchangeDeclare("jobs.direct", "direct", true, false, false, false, nil))
}

// gRPC server
type server struct {
	gen.UnimplementedJobApiServer
}

func (s *server) SubmitJob(ctx context.Context, in *gen.SubmitJobReq) (*gen.SubmitJobResp, error) {
	t := in.GetType()
	if t == "" {
		t = "unknown"
	}

	// Validation
	if in.GetType() == "" {
		return nil, status.Error(codes.InvalidArgument, "type required")
	}
	if pr := in.GetPriority(); pr < 0 || pr > 10 {
		return nil, status.Error(codes.InvalidArgument, "priority must be 0..10")
	}
	if len(in.GetPayload()) > 1_000_000 {
		return nil, status.Error(codes.InvalidArgument, "payload too large")
	}

	// Idempotency check
	if key := in.GetIdempotencyKey(); key != "" {
		if jid, err := rdb.Get(ctx, "idempo:"+key).Result(); err == nil && jid != "" {
			return &gen.SubmitJobResp{JobId: jid}, nil
		}
	}

	// New job ID
	jobID := "job_" + uuid.NewString()
	jobKey := "job:" + jobID
	meta := map[string]interface{}{
		"status":       "queued",
		"type":         in.GetType(),
		"priority":     in.GetPriority(),
		"attempts":     0,
		"submitted_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := rdb.HSet(ctx, jobKey, meta).Err(); err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis write failed: %v", err)
	}
	if key := in.GetIdempotencyKey(); key != "" {
		_ = rdb.Set(ctx, "idempo:"+key, jobID, 24*time.Hour).Err()
	}

	// Build message
	body, _ := json.Marshal(map[string]interface{}{
		"job_id":   jobID,
		"type":     in.GetType(),
		"payload":  json.RawMessage(in.GetPayload()),
		"priority": in.GetPriority(),
	})
	pub := amqp.Publishing{
		ContentType: "application/json",
		MessageId:   jobID,
		Priority:    uint8(in.GetPriority()),
		Headers: amqp.Table{
			"attempt":      int32(0),
			"submitted_at": time.Now().UTC().Format(time.RFC3339Nano),
			"type":         in.GetType(),
		},
		Body: body,
	}

	// Publish with timeout
	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ch.PublishWithContext(pubCtx, "jobs.direct", "jobs.submit", false, false, pub); err != nil {
		return nil, status.Errorf(codes.Unavailable, "publish failed: %v", err)
	}

	return &gen.SubmitJobResp{JobId: jobID}, nil
}

func (s *server) GetJob(ctx context.Context, in *gen.GetJobReq) (*gen.GetJobResp, error) {
	m, err := rdb.HGetAll(ctx, "job:"+in.JobId).Result()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis error: %v", err)
	}
	if len(m) == 0 {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	return &gen.GetJobResp{
		Status:     m["status"],
		ResultJson: m["result_json"],
		Error:      m["error"],
		Attempts:   atoi32(m["attempts"]),
	}, nil
}

func main() {
	initRedis()
	initRabbit()

	// Start metrics server
	go func() {
		log.Println("API metrics on :2112/metrics")
		log.Fatal(http.ListenAndServe(":2112", nil))
	}()

	// gRPC server
	lis, err := net.Listen("tcp", ":8080")
	must(err)
	grpcServer := grpc.NewServer()
	gen.RegisterJobApiServer(grpcServer, &server{})
	reflection.Register(grpcServer)

	log.Println("API gRPC listening on :8080")
	must(grpcServer.Serve(lis))
}

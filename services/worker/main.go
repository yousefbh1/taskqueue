package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	JobID    string          `json:"job_id"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority"`
}

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

func declareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare("jobs.direct", "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare("jobs.dlx", "direct", true, false, false, false, nil); err != nil {
		return err
	}

	args := amqp.Table{"x-max-priority": int32(10), "x-dead-letter-exchange": "jobs.dlx"}
	if _, err := ch.QueueDeclare("jobs.q", true, false, false, false, args); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare("jobs.dlq", true, false, false, false, nil); err != nil {
		return err
	}

	if err := ch.QueueBind("jobs.q", "jobs.submit", "jobs.direct", false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind("jobs.dlq", "jobs.dlq", "jobs.dlx", false, nil); err != nil {
		return err
	}
	// === retry queue
	// each retry queue dead-letters back to jobs.direct with rk=jobs.submit
	if _, err := ch.QueueDeclare("jobs.retry.1", true, false, false, false, amqp.Table{
		"x-message-ttl":             int32(5000), // 5s
		"x-dead-letter-exchange":    "jobs.direct",
		"x-dead-letter-routing-key": "jobs.submit",
	}); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare("jobs.retry.2", true, false, false, false, amqp.Table{
		"x-message-ttl":             int32(30000), // 30s
		"x-dead-letter-exchange":    "jobs.direct",
		"x-dead-letter-routing-key": "jobs.submit",
	}); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare("jobs.retry.3", true, false, false, false, amqp.Table{
		"x-message-ttl":             int32(120000),
		"x-dead-letter-exchange":    "jobs.direct",
		"x-dead-letter-routing-key": "jobs.submit",
	}); err != nil {
		return err
	}
	return nil
}

var (
	rdb  *redis.Client
	rctx = context.Background()
)

func main() {
	// Redis
	rdb = redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "redis:6379")})
	must(rdb.Ping(rctx).Err())

	// RabbitMQ
	url := getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
	var conn *amqp.Connection
	var err error
	for i := 0; i < 20; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Printf("waiting for rabbitmq... (%v)", err)
		time.Sleep(2 * time.Second)
	}
	must(err)
	defer conn.Close()

	ch, err := conn.Channel()
	must(err)
	defer ch.Close()

	must(declareTopology(ch))
	must(ch.Qos(1, 0, false))

	msgs, err := ch.Consume("jobs.q", "", false, false, false, false, nil)
	must(err)
	log.Println("Worker consuming from jobs.q ...")

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutdown: closing AMQP channel...")
		_ = ch.Cancel("", false)
		cancel()
	}()

	for msg := range msgs {
		// Parse incoming message
		var j Job
		if err := json.Unmarshal(msg.Body, &j); err != nil {
			log.Printf("bad message (json): %v body=%s", err, string(msg.Body))
			_ = msg.Ack(false)
			continue
		}
		// prefer AMQP MessageId if present
		if msg.MessageId != "" {
			j.JobID = msg.MessageId
		}
		if j.JobID == "" {
			log.Printf("message missing job_id; acking. body=%s", string(msg.Body))
			_ = msg.Ack(false)
			continue
		}

		//  running
		start := time.Now().UTC().Format(time.RFC3339Nano)
		if err := rdb.HSet(rctx, "job:"+j.JobID,
			"status", "running",
			"started_at", start,
		).Err(); err != nil {
			log.Printf("redis set running err job=%s: %v", j.JobID, err)
		}

		err = process(j)

		if err == nil {
			// success
			done := time.Now().UTC().Format(time.RFC3339Nano)
			res := map[string]any{"ok": true, "processed_at": done, "type": j.Type}
			b, _ := json.Marshal(res)
			if e := rdb.HSet(rctx, "job:"+j.JobID,
				"status", "succeeded",
				"completed_at", done,
				"result_json", string(b),
			).Err(); e != nil {
				log.Printf("redis set succeeded err job=%s: %v", j.JobID, e)
			}
			_ = msg.Ack(false)
			continue
		}

		// failure
		attempt := int32(0)
		if v, ok := msg.Headers["attempt"]; ok {
			if n, ok := v.(int32); ok {
				attempt = n
			}
			if n, ok := v.(int64); ok {
				attempt = int32(n)
			}
			if n, ok := v.(int); ok {
				attempt = int32(n)
			}
		}

		nextAttempt := attempt + 1
		_ = rdb.HIncrBy(rctx, "job:"+j.JobID, "attempts", 1).Err()

		if nextAttempt <= 3 {
			// choose retry queue
			var retryQ string
			switch nextAttempt {
			case 1:
				retryQ = "jobs.retry.1"
			case 2:
				retryQ = "jobs.retry.2"
			default:
				retryQ = "jobs.retry.3"
			}

			// re-publish to retry queue with updated attempt header
			headers := msg.Headers
			if headers == nil {
				headers = amqp.Table{}
			}
			headers["attempt"] = nextAttempt

			pub := amqp.Publishing{
				ContentType: msg.ContentType,
				MessageId:   msg.MessageId, // keep same job_id
				Timestamp:   time.Now(),
				Priority:    msg.Priority, // preserve priority
				Headers:     headers,
				Body:        msg.Body, // original body
			}

			// publish DIRECTLY to queue (empty exchange)
			if err := ch.Publish("", retryQ, false, false, pub); err != nil {
				log.Printf("retry publish failed job=%s: %v", j.JobID, err)
				failTime := time.Now().UTC().Format(time.RFC3339Nano)
				_ = rdb.HSet(rctx, "job:"+j.JobID, "status", "failed", "error", "retry publish failed: "+err.Error(), "completed_at", failTime).Err()
				_ = msg.Ack(false)
				continue
			}

			_ = rdb.HSet(rctx, "job:"+j.JobID, "status", "queued").Err()
			log.Printf("job requeued (attempt %d) via %s job=%s", nextAttempt, retryQ, j.JobID)
			_ = msg.Ack(false)
			continue
		}

		failTime := time.Now().UTC().Format(time.RFC3339Nano)
		_ = rdb.HSet(rctx, "job:"+j.JobID,
			"status", "failed",
			"error", err.Error(),
			"completed_at", failTime,
		).Err()

		headers := msg.Headers
		if headers == nil {
			headers = amqp.Table{}
		}
		headers["final_error"] = err.Error()

		pub := amqp.Publishing{
			ContentType: msg.ContentType,
			MessageId:   msg.MessageId,
			Timestamp:   time.Now(),
			Priority:    msg.Priority,
			Headers:     headers,
			Body:        msg.Body,
		}
		if err := ch.Publish("jobs.dlx", "jobs.dlq", false, false, pub); err != nil {
			log.Printf("DLQ publish failed job=%s: %v", j.JobID, err)
		}
		_ = msg.Ack(false)

	}

	log.Println("shutdown: draining done; closing conn")
	_ = ch.Close()
	_ = conn.Close()
}

func process(j Job) error {
	var p map[string]any
	_ = json.Unmarshal(j.Payload, &p)

	if j.Type == "fail" {
		return errors.New("forced failure (type=fail)")
	}

	if v, ok := p["sleep_ms"]; ok {
		switch ms := v.(type) {
		case float64:
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	} else {
		time.Sleep(1 * time.Second)
	}
	return nil
}

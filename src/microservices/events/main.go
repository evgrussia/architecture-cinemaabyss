package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type MovieEvent struct {
	MovieID int     `json:"movie_id"`
	Title   string  `json:"title"`
	Action  string  `json:"action"`
	UserID  int     `json:"user_id"`
	Rating  float64 `json:"rating"`
	Genres  []string `json:"genres"`
	Desc    string  `json:"description"`
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type"`
}

func main() {
	port := envOrDefault("PORT", "8082")
	brokers := splitCSV(envOrDefault("KAFKA_BROKERS", "localhost:9092"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startConsumers(ctx, brokers)

	movieWriter := newWriter(brokers, movieTopic)
	userWriter := newWriter(brokers, userTopic)
	paymentWriter := newWriter(brokers, paymentTopic)
	defer func() {
		_ = movieWriter.Close()
		_ = userWriter.Close()
		_ = paymentWriter.Close()
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
	})

	mux.HandleFunc("/api/events/movie", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload MovieEvent
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		evt := Event{
			ID:        "movie-" + time.Now().UTC().Format("20060102T150405.000000000Z07:00"),
			Type:      "movie",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload:   payload,
		}

		partition, offset, err := publishEvent(ctx, movieWriter, evt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeEventResponse(w, evt, partition, offset)
	})

	mux.HandleFunc("/api/events/user", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload UserEvent
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		evt := Event{
			ID:        "user-" + time.Now().UTC().Format("20060102T150405.000000000Z07:00"),
			Type:      "user",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload:   payload,
		}

		partition, offset, err := publishEvent(ctx, userWriter, evt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeEventResponse(w, evt, partition, offset)
	})

	mux.HandleFunc("/api/events/payment", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload PaymentEvent
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		evt := Event{
			ID:        "payment-" + time.Now().UTC().Format("20060102T150405.000000000Z07:00"),
			Type:      "payment",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload:   payload,
		}

		partition, offset, err := publishEvent(ctx, paymentWriter, evt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeEventResponse(w, evt, partition, offset)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("Starting events service on port %s (brokers=%v)", port, brokers)
	log.Fatal(server.ListenAndServe())
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func publishEvent(ctx context.Context, w *kafka.Writer, evt Event) (int, int64, error) {
	value, err := json.Marshal(evt)
	if err != nil {
		return 0, 0, err
	}

	// kafka-go Writer does not provide delivery metadata (partition/offset) here.
	// For MVP and Postman checks, we return 0/0 and rely on consumers/logs + Kafka UI for verification.
	if err := w.WriteMessages(ctx, kafka.Message{Value: value}); err != nil {
		return 0, 0, err
	}
	return 0, 0, nil
}

func writeEventResponse(w http.ResponseWriter, evt Event, partition int, offset int64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     evt,
	})
}

func startConsumers(ctx context.Context, brokers []string) {
	startConsumer(ctx, brokers, movieTopic)
	startConsumer(ctx, brokers, userTopic)
	startConsumer(ctx, brokers, paymentTopic)
}

func startConsumer(ctx context.Context, brokers []string, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "events-service",
	})

	go func() {
		defer func() { _ = r.Close() }()
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("consumer error (topic=%s): %v", topic, err)
				continue
			}
			log.Printf("event consumed topic=%s partition=%d offset=%d value=%s", topic, msg.Partition, msg.Offset, string(msg.Value))
		}
	}()
}

func envOrDefault(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

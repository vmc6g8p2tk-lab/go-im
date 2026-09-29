package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
)

type sample struct {
	At                 time.Time          `json:"at"`
	Seconds            float64            `json:"seconds"`
	MessageBacklog     int64              `json:"message_backlog"`
	StatusBacklog      int64              `json:"status_backlog"`
	PGConnections      int64              `json:"pg_connections"`
	PGActive           int64              `json:"pg_active"`
	PGIdleTransaction  int64              `json:"pg_idle_transaction"`
	PGWaiting          int64              `json:"pg_waiting"`
	PGXactCommit       int64              `json:"pg_xact_commit"`
	PGXactRollback     int64              `json:"pg_xact_rollback"`
	PGBlocksHit        int64              `json:"pg_blocks_hit"`
	PGBlocksRead       int64              `json:"pg_blocks_read"`
	PGMaxActiveSeconds float64            `json:"pg_max_active_seconds"`
	App1               map[string]float64 `json:"app1,omitempty"`
	App2               map[string]float64 `json:"app2,omitempty"`
	KafkaEnd           int64              `json:"kafka_end_offset"`
	KafkaLagApp1       int64              `json:"kafka_lag_app1"`
	KafkaLagApp2       int64              `json:"kafka_lag_app2"`
	Errors             []string           `json:"errors,omitempty"`
}

func main() {
	duration := flag.Duration("duration", 30*time.Second, "observation duration")
	out := flag.String("out", "", "JSONL output")
	flag.Parse()
	if *out == "" || os.Getenv("DATABASE_URL") == "" {
		fmt.Fprintln(os.Stderr, "-out and DATABASE_URL are required")
		os.Exit(2)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	file, err := os.Create(*out)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	output := bufio.NewWriter(file)
	defer output.Flush()
	client := &http.Client{Timeout: 750 * time.Millisecond}
	broker := &kafka.Client{Addr: kafka.TCP("127.0.0.1:29092"), Timeout: 1500 * time.Millisecond}
	started := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		s := sample{At: time.Now(), Seconds: time.Since(started).Seconds()}
		if err := readDB(pool, &s); err != nil {
			s.Errors = append(s.Errors, "db: "+err.Error())
		}
		if s.App1, err = readMetrics(client, "http://127.0.0.1:8080/metrics"); err != nil {
			s.Errors = append(s.Errors, "app1: "+err.Error())
		}
		if s.App2, err = readMetrics(client, "http://127.0.0.1:8081/metrics"); err != nil {
			s.Errors = append(s.Errors, "app2: "+err.Error())
		}
		if err := readKafka(broker, &s); err != nil {
			s.Errors = append(s.Errors, "kafka: "+err.Error())
		}
		data, _ := json.Marshal(s)
		_, _ = output.Write(append(data, '\n'))
		_ = output.Flush()
		if time.Since(started) >= *duration {
			break
		}
		<-ticker.C
	}
}

func readDB(pool *pgxpool.Pool, s *sample) error {
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	return pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM message_outbox WHERE published_at IS NULL),
		(SELECT count(*) FROM delivery_status_outbox WHERE published_at IS NULL),
		(SELECT count(*) FROM pg_stat_activity WHERE datname='im'),
		(SELECT count(*) FROM pg_stat_activity WHERE datname='im' AND state='active' AND pid<>pg_backend_pid()),
		(SELECT count(*) FROM pg_stat_activity WHERE datname='im' AND state='idle in transaction'),
		(SELECT count(*) FROM pg_stat_activity WHERE datname='im' AND wait_event_type IS NOT NULL AND state='active' AND pid<>pg_backend_pid()),
		(SELECT xact_commit FROM pg_stat_database WHERE datname='im'),
		(SELECT xact_rollback FROM pg_stat_database WHERE datname='im'),
		(SELECT blks_hit FROM pg_stat_database WHERE datname='im'),
		(SELECT blks_read FROM pg_stat_database WHERE datname='im'),
		COALESCE((SELECT max(extract(epoch from now()-query_start)) FROM pg_stat_activity
			WHERE datname='im' AND state='active' AND pid<>pg_backend_pid()),0)`).
		Scan(&s.MessageBacklog, &s.StatusBacklog, &s.PGConnections, &s.PGActive,
			&s.PGIdleTransaction, &s.PGWaiting, &s.PGXactCommit, &s.PGXactRollback,
			&s.PGBlocksHit, &s.PGBlocksRead, &s.PGMaxActiveSeconds)
}

func readMetrics(client *http.Client, url string) (map[string]float64, error) {
	response, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	values := make(map[string]float64)
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "im_db_operation_seconds_sum{") &&
			!strings.HasPrefix(line, "im_db_operation_seconds_count{") &&
			!strings.HasPrefix(line, "im_kafka_publish_total{") &&
			!strings.HasPrefix(line, "im_kafka_consume_total{") &&
			!strings.HasPrefix(line, "im_message_processing_seconds_sum{") &&
			!strings.HasPrefix(line, "im_message_processing_seconds_count{") &&
			!strings.HasPrefix(line, "process_cpu_seconds_total ") &&
			!strings.HasPrefix(line, "process_resident_memory_bytes ") &&
			!strings.HasPrefix(line, "go_goroutines ") &&
			!strings.HasPrefix(line, "im_websocket_connections ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 2 {
			if value, err := strconv.ParseFloat(fields[1], 64); err == nil {
				values[fields[0]] = value
			}
		}
	}
	return values, nil
}

func readKafka(client *kafka.Client, s *sample) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	end := make(map[int]int64)
	for partition := 0; partition < 8; partition++ {
		conn, err := kafka.DialLeader(ctx, "tcp", "127.0.0.1:29092", "im.message.created.v1", partition)
		if err != nil {
			return err
		}
		_ = conn.SetDeadline(time.Now().Add(350 * time.Millisecond))
		last, err := conn.ReadLastOffset()
		_ = conn.Close()
		if err != nil {
			return err
		}
		end[partition] = last
		s.KafkaEnd += last
	}
	for _, group := range []struct {
		name string
		lag  *int64
	}{{"im-delivery-app1", &s.KafkaLagApp1}, {"im-delivery-app2", &s.KafkaLagApp2}} {
		committed, err := client.ConsumerOffsets(ctx, kafka.TopicAndGroup{Topic: "im.message.created.v1", GroupId: group.name})
		if err != nil {
			return err
		}
		for partition, last := range end {
			offset := committed[partition]
			if offset >= 0 && last > offset {
				*group.lag += last - offset
			}
		}
	}
	return nil
}

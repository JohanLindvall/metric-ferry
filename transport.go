package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

var errRequestTooLarge = errors.New("request too large")

type destination struct {
	protocol, endpoint string
	headers            http.Header
	client             *http.Client
	config             config
	stats              *statistics
	log                *log.Logger
}

func httpClient(c config) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: c.timeout, KeepAlive: 30 * time.Second}).DialContext
	t.MaxIdleConns = c.workers * 2
	t.MaxIdleConnsPerHost = c.workers
	t.MaxConnsPerHost = c.workers * 2
	t.ResponseHeaderTimeout = c.timeout
	return &http.Client{
		Transport: t, Timeout: c.timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (d *destination) send(ctx context.Context, body []byte) error {
	if d.config.dryRun {
		return nil
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header = d.headers.Clone()
		req.Header.Set("User-Agent", "promimp/"+version)
		if d.protocol == "prometheus" {
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("Content-Encoding", "snappy")
			req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		} else {
			req.Header.Set("Content-Type", "application/x-protobuf")
			if d.config.otlpCompression == "gzip" {
				req.Header.Set("Content-Encoding", "gzip")
			}
		}
		resp, sendErr := d.client.Do(req)
		var retryAfter string
		retry := false
		if sendErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			retry, err = true, sendErr
		} else {
			response, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
			resp.Body.Close()
			retryAfter = resp.Header.Get("Retry-After")
			if resp.StatusCode == http.StatusRequestEntityTooLarge {
				return errRequestTooLarge
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if readErr != nil {
					// Delivery may have succeeded. Retrying cumulative samples
					// is safe for usual receivers, but never exactly-once.
					err, retry = readErr, true
				} else if len(response) > 1<<20 {
					return errors.New("response exceeds 1 MiB limit")
				} else if d.protocol == "otlp" {
					return d.checkOTLPResponse(response)
				} else {
					return nil
				}
			} else {
				err = fmt.Errorf("%s HTTP %d: %s", d.protocol, resp.StatusCode, strings.TrimSpace(string(response[:min(len(response), 2048)])))
				if d.protocol == "prometheus" {
					retry = resp.StatusCode == 429 || resp.StatusCode >= 500
				} else {
					switch resp.StatusCode {
					case 429, 502, 503, 504:
						retry = true
					}
				}
			}
		}
		if !retry || attempt >= d.config.retries {
			return fmt.Errorf("%s export failed after %d attempt(s): %w", d.protocol, attempt+1, err)
		}
		wait := retryDelay(attempt, retryAfter, time.Now())
		d.stats.retries.Add(1)
		d.log.Printf("%s retry %d/%d in %s: %v", d.protocol, attempt+1, d.config.retries, wait.Round(time.Millisecond), err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (d *destination) checkOTLPResponse(body []byte) error {
	response := new(collectorpb.ExportMetricsServiceResponse)
	if err := proto.Unmarshal(body, response); err != nil {
		return fmt.Errorf("decode OTLP success response: %w", err)
	}
	if partial := response.PartialSuccess; partial != nil {
		if partial.RejectedDataPoints != 0 {
			return fmt.Errorf("OTLP partial success: receiver rejected %d data points: %s (not retrying an already partially accepted request)",
				partial.RejectedDataPoints, partial.ErrorMessage)
		}
		if partial.ErrorMessage != "" {
			d.log.Printf("OTLP receiver warning: %s", partial.ErrorMessage)
		}
	}
	return nil
}

func retryDelay(attempt int, retryAfter string, now time.Time) time.Duration {
	base := min(30*time.Second, (500*time.Millisecond)*time.Duration(1<<min(attempt, 6)))
	delay := base + time.Duration(rand.Int64N(int64(base/2)+1))
	if seconds, err := strconv.ParseInt(retryAfter, 10, 64); err == nil && seconds >= 0 {
		if seconds > int64((time.Duration(1<<63-1))/time.Second) {
			return time.Duration(1<<63 - 1)
		}
		delay = max(delay, time.Duration(seconds)*time.Second)
	} else if date, err := http.ParseTime(retryAfter); err == nil {
		delay = max(delay, date.Sub(now))
	}
	return delay
}

type exporter struct {
	destination *destination
	encoder     encoder
}

func (e *exporter) sendBatch(ctx context.Context, points []point) error {
	if len(points) == 0 {
		return nil
	}
	body, rawSize, err := e.encoder.encode(points)
	if err != nil {
		return err
	}
	if rawSize > e.destination.config.batchBytes {
		err = errRequestTooLarge
	} else {
		err = e.destination.send(ctx, body)
	}
	if errors.Is(err, errRequestTooLarge) {
		if len(points) == 1 {
			return fmt.Errorf("%s: one sample cannot fit the receiver/request byte limit", e.destination.protocol)
		}
		mid := len(points) / 2
		if err := e.sendBatch(ctx, points[:mid]); err != nil {
			return err
		}
		return e.sendBatch(ctx, points[mid:])
	}
	if err != nil {
		return err
	}
	e.destination.stats.requests.Add(1)
	e.destination.stats.wireBytes.Add(int64(len(body)))
	if e.destination.protocol == "prometheus" {
		e.destination.stats.prometheus.Add(int64(len(points)))
	} else {
		e.destination.stats.otlp.Add(int64(len(points)))
	}
	return nil
}

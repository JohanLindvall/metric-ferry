package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

type statistics struct {
	samples, duplicates, series, decompressed, requests, wireBytes, retries atomic.Int64
	prometheus, otlp, exemplars                                             atomic.Int64
	minTimestamp, maxTimestamp                                              int64 // Reader-owned; read only after join.
}

func (s *statistics) report(logger *log.Logger, started time.Time, final bool) {
	elapsed := time.Since(started)
	prefix := "progress"
	if final {
		prefix = "finished"
	}
	logger.Printf("%s samples=%d series=%d duplicates=%d prometheus=%d otlp=%d requests=%d wire=%.1fMiB read=%.1fMiB rate=%.0f samples/s elapsed=%s retries=%d",
		prefix, s.samples.Load(), s.series.Load(), s.duplicates.Load(), s.prometheus.Load(), s.otlp.Load(),
		s.requests.Load(), float64(s.wireBytes.Load())/(1<<20), float64(s.decompressed.Load())/(1<<20),
		float64(s.samples.Load())/elapsed.Seconds(), elapsed.Round(time.Millisecond), s.retries.Load())
}

func run(ctx context.Context, c config, logger *log.Logger) (*statistics, error) {
	stats := &statistics{minTimestamp: math.MaxInt64, maxTimestamp: math.MinInt64}
	prom, otlp := c.destinations()
	var mapper *attributeMapper
	var err error
	if otlp {
		mapper, err = newMapper(c)
		if err != nil {
			return stats, err
		}
	}
	ph, err := headers(c.promHeaders)
	if err != nil {
		return stats, err
	}
	oh, err := headers(c.otlpHeaders)
	if err != nil {
		return stats, err
	}
	shift := int64(c.shift)
	if c.rebase != "" && c.rebaseAnchor == "end" {
		logger.Printf("scanning timestamps to rebase the latest sample; this requires two decompression passes")
		latest, err := latestTimestamp(ctx, c)
		if err != nil {
			return stats, err
		}
		target, err := targetNow(c.rebase)
		if err != nil {
			return stats, err
		}
		shift, err = subtractTimestamp(target, latest)
		if err != nil {
			return stats, err
		}
		logger.Printf("rebase latest %s -> %s (shift %s)", formatTimestamp(latest), formatTimestamp(target), time.Duration(shift))
	}
	client := httpClient(c)
	defer client.CloseIdleConnections()
	var destinations []*destination
	if prom {
		destinations = append(destinations, &destination{
			protocol: "prometheus", endpoint: c.prometheusURL, headers: ph, client: client, config: c, stats: stats, log: logger,
		})
	}
	if otlp {
		destinations = append(destinations, &destination{
			protocol: "otlp", endpoint: c.otlpURL, headers: oh, client: client, config: c, stats: stats, log: logger,
		})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	progressDone := make(chan struct{})
	if c.progress > 0 {
		go func() {
			defer close(progressDone)
			ticker := time.NewTicker(c.progress)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					stats.report(logger, started, false)
				}
			}
		}()
	} else {
		close(progressDone)
	}
	g, groupCtx := errgroup.WithContext(ctx)
	queues := make([]chan []point, c.workers)
	for i := range queues {
		queues[i] = make(chan []point, 2)
		queue := queues[i]
		g.Go(func() error { return exportWorker(groupCtx, c, queue, destinations) })
	}
	g.Go(func() error {
		defer func() {
			for _, queue := range queues {
				close(queue)
			}
		}()
		return readSamples(groupCtx, c, shift, mapper, queues, stats, logger)
	})
	err = g.Wait()
	cancel()
	<-progressDone
	stats.report(logger, started, true)
	if stats.samples.Load() > 0 {
		logger.Printf("output timestamps %s to %s", formatTimestamp(stats.minTimestamp), formatTimestamp(stats.maxTimestamp))
	}
	if stats.exemplars.Load() > 0 {
		logger.Printf("explicitly discarded %d exemplars", stats.exemplars.Load())
	}
	return stats, err
}

func readSamples(ctx context.Context, c config, shift int64, mapper *attributeMapper, queues []chan []point, stats *statistics, logger *log.Logger) error {
	in, err := openInput(c)
	if err != nil {
		return err
	}
	defer in.Close()
	p := newParser(c, mapper)
	buffers := make([][]point, len(queues))
	chunkSize := min(256, c.batchSize)
	dispatch := func(i int) error {
		if len(buffers[i]) == 0 {
			return nil
		}
		select {
		case queues[i] <- buffers[i]:
			buffers[i] = make([]point, 0, chunkSize)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s := scanner(in.decoder, c.maxLineBytes)
	scale, _ := timestampScale(c.timestampUnit)
	prom, otlp := c.destinations()
	var lineNumber int64
	eof := false
	for s.Scan() {
		lineNumber++
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.decompressed.Add(int64(len(s.Bytes()) + 1))
		line := trimSpace(s.Bytes())
		if len(line) == 0 {
			continue
		}
		fail := func(err error) error { return fmt.Errorf("line %d: %w", lineNumber, err) }
		if string(line) == "# EOF" {
			eof = true
			continue
		}
		if eof {
			return fail(errors.New("content after OpenMetrics # EOF marker"))
		}
		if line[0] == '#' {
			if err := p.comment(line); err != nil {
				return fail(err)
			}
			continue
		}
		prefix, value, timestamp, dropped, err := splitSample(line, c.dropExemplars)
		if err != nil {
			return fail(err)
		}
		v, err := parseValue(value)
		if err != nil {
			return fail(err)
		}
		ns, err := parseTimestamp(timestamp, scale)
		if err != nil {
			return fail(err)
		}
		if c.rebase != "" && c.rebaseAnchor == "first" && stats.samples.Load() == 0 {
			target, err := targetNow(c.rebase)
			if err != nil {
				return fail(err)
			}
			shift, err = subtractTimestamp(target, ns)
			if err != nil {
				return fail(err)
			}
			logger.Printf("rebase first %s -> %s (shift %s)", formatTimestamp(ns), formatTimestamp(target), time.Duration(shift))
		}
		ns, err = addTimestamp(ns, shift)
		if err != nil {
			return fail(err)
		}
		if otlp && ns < 0 {
			return fail(errors.New("OTLP requires timestamps at/after the Unix epoch; use -rebase or -shift"))
		}
		def, err := p.get(prefix)
		if err != nil {
			return fail(err)
		}
		if otlp && def.counter && v < 0 {
			return fail(fmt.Errorf("counter %s has a negative value; disable -infer-counters if its name is misleading", def.name))
		}
		if def.seen && ns < def.lastNS {
			return fail(fmt.Errorf("out-of-order sample for %s (%s < %s); input must be ordered per series", def.name, formatTimestamp(ns), formatTimestamp(def.lastNS)))
		}
		bits := math.Float64bits(v)
		ms := unixMillis(ns)
		if def.seen && (ns == def.lastNS || prom && ms == def.lastMS) && bits != def.lastValue {
			return fail(fmt.Errorf("conflicting samples at the same output timestamp for %s (Prometheus has millisecond precision)", def.name))
		}
		stats.samples.Add(1)
		stats.series.Store(int64(len(p.canonical)))
		stats.minTimestamp = min(stats.minTimestamp, ns)
		stats.maxTimestamp = max(stats.maxTimestamp, ns)
		if dropped {
			stats.exemplars.Add(1)
		}
		if def.seen && ns == def.lastNS {
			stats.duplicates.Add(1)
		} else {
			def.lastNS, def.lastMS, def.lastValue, def.seen = ns, ms, bits, true
			i := int(def.shard % uint64(len(queues)))
			buffers[i] = append(buffers[i], point{series: def, timestamp: ns, value: v})
			if len(buffers[i]) >= chunkSize {
				if err := dispatch(i); err != nil {
					return err
				}
			}
		}
		if c.limit > 0 && stats.samples.Load() == c.limit {
			break
		}
	}
	if err := s.Err(); err != nil {
		return fmt.Errorf("read zstd near line %d (line limit %d bytes): %w", lineNumber, c.maxLineBytes, err)
	}
	if stats.samples.Load() == 0 {
		return errors.New("input has no samples")
	}
	if otlp && len(p.scalarTypes) > 0 {
		logger.Printf("%d histogram/summary families preserved as scalar component series; no native OTLP histogram/summary reconstruction", len(p.scalarTypes))
	}
	for i := range queues {
		if err := dispatch(i); err != nil {
			return err
		}
	}
	return nil
}

func exportWorker(ctx context.Context, c config, queue <-chan []point, destinations []*destination) error {
	exporters := make([]*exporter, len(destinations))
	for i, d := range destinations {
		exporters[i] = &exporter{destination: d, encoder: encoder{protocol: d.protocol, compression: c.otlpCompression}}
	}
	batch := make([]point, 0, c.batchSize)
	estimatedBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if len(exporters) == 1 {
			if err := exporters[0].sendBatch(ctx, batch); err != nil {
				return err
			}
		} else {
			g, sendCtx := errgroup.WithContext(ctx)
			for _, e := range exporters {
				g.Go(func() error { return e.sendBatch(sendCtx, batch) })
			}
			if err := g.Wait(); err != nil {
				return err
			}
		}
		clear(batch)
		batch = batch[:0]
		estimatedBytes = 0
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-queue:
			if !ok {
				return flush()
			}
			for _, point := range chunk {
				batch = append(batch, point)
				estimatedBytes += point.series.estimatedBytes
				if len(batch) >= c.batchSize || estimatedBytes >= c.batchBytes {
					if err := flush(); err != nil {
						return err
					}
				}
			}
		}
	}
}

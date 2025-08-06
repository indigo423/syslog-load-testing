package main

import (
	"context"
	"flag"
	"fmt"
	"log/syslog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Stats struct {
	sent    int64
	errors  int64
	started time.Time
}

func (s *Stats) incrementSent() {
	atomic.AddInt64(&s.sent, 1)
}

func (s *Stats) incrementErrors() {
	atomic.AddInt64(&s.errors, 1)
}

func (s *Stats) getSent() int64 {
	return atomic.LoadInt64(&s.sent)
}

func (s *Stats) getErrors() int64 {
	return atomic.LoadInt64(&s.errors)
}

func main() {
	// Command line flags
	var (
		protocol    = flag.String("protocol", "udp", "Network type (tcp, udp, or empty for local)")
		target      = flag.String("target", "localhost:514", "Syslog server target (e.g., localhost:514)")
		priority    = flag.String("priority", "info", "Log priority (emerg, alert, crit, err, warning, notice, info, debug)")
		tag         = flag.String("tag", "load_test", "Syslog tag/program name")
		message     = flag.String("message", "Load test message", "Base message to send")
		rate        = flag.Int("rate", 10, "Messages per second to send")
		count       = flag.Int64("count", 100, "Total number of messages to send (0 for unlimited)")
		duration    = flag.Duration("duration", 0, "Test duration (0 for unlimited, e.g., 30s, 5m)")
		connections = flag.Int("connections", 1, "Number of concurrent connections")
		unique      = flag.Bool("unique", false, "Add unique ID and timestamp to each message")
		stats       = flag.Bool("stats", true, "Show real-time statistics")
	)
	flag.Parse()

	// Parse priority
	syslogPriority := parsePriority(*priority)
	if syslogPriority == -1 {
		fmt.Fprintf(os.Stderr, "Error: invalid priority %s\n", *priority)
		os.Exit(1)
	}

	// Validate parameters
	if *rate <= 0 {
		fmt.Fprintf(os.Stderr, "Error: rate must be positive\n")
		os.Exit(1)
	}
	if *connections <= 0 {
		fmt.Fprintf(os.Stderr, "Error: connections must be positive\n")
		os.Exit(1)
	}

	fmt.Printf("Starting syslog load test:\n")
	fmt.Printf("  Target: %s://%s\n", *protocol, *target)
	fmt.Printf("  Rate: %d msg/s\n", *rate)
	fmt.Printf("  Connections: %d\n", *connections)
	if *count > 0 {
		fmt.Printf("  Total messages: %d\n", *count)
	} else {
		fmt.Printf("  Total messages: unlimited\n")
	}
	if *duration > 0 {
		fmt.Printf("  Duration: %v\n", *duration)
	}
	fmt.Printf("  Priority: %s\n", *priority)
	fmt.Printf("  Tag: %s\n", *tag)
	fmt.Printf("  Unique messages: %t\n", *unique)
	fmt.Println()

	// Setup context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle duration limit
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	// Handle interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, stopping...")
		cancel()
	}()

	// Initialize stats
	testStats := &Stats{started: time.Now()}

	// Start statistics reporter
	if *stats {
		go statsReporter(ctx, testStats)
	}

	// Calculate rate per connection
	ratePerConnection := *rate / *connections
	if ratePerConnection == 0 {
		ratePerConnection = 1
	}

	// Start load test workers
	var wg sync.WaitGroup
	for i := 0; i < *connections; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, *protocol, *target, syslogPriority, *tag, *message,
				ratePerConnection, *count, *unique, testStats)
		}(i)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Final statistics
	elapsed := time.Since(testStats.started)
	sent := testStats.getSent()
	errors := testStats.getErrors()

	fmt.Printf("\n=== Load Test Complete ===\n")
	fmt.Printf("Duration: %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Messages sent: %d\n", sent)
	fmt.Printf("Errors: %d\n", errors)
	fmt.Printf("Success rate: %.2f%%\n", float64(sent)/float64(sent+errors)*100)
	fmt.Printf("Average rate: %.2f msg/s\n", float64(sent)/elapsed.Seconds())
}

func runWorker(ctx context.Context, workerID int, protocol, address string, priority syslog.Priority,
	tag, baseMessage string, rate int, maxCount int64, unique bool, stats *Stats) {

	// Create rate limiter
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	var messageCount int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check if we've reached the message limit
			if maxCount > 0 && atomic.LoadInt64(&stats.sent) >= maxCount {
				return
			}

			// Create connection for each message (or reuse based on protocol)
			writer, err := createSyslogWriter(protocol, address, priority, tag)
			if err != nil {
				stats.incrementErrors()
				continue
			}

			// Prepare message
			message := baseMessage
			if unique {
				messageCount++
				message = fmt.Sprintf("%s [worker:%d msg:%d ts:%d]",
					baseMessage, workerID, messageCount, time.Now().Unix())
			}

			// Send message
			err = sendSyslogMessage(writer, priority, message)
			writer.Close()

			if err != nil {
				stats.incrementErrors()
			} else {
				stats.incrementSent()
			}
		}
	}
}

func createSyslogWriter(protocol, address string, priority syslog.Priority, tag string) (*syslog.Writer, error) {
	if protocol == "" || address == "" {
		return syslog.New(priority, tag)
	}
	return syslog.Dial(protocol, address, priority, tag)
}

func sendSyslogMessage(writer *syslog.Writer, priority syslog.Priority, message string) error {
	switch priority {
	case syslog.LOG_EMERG:
		return writer.Emerg(message)
	case syslog.LOG_ALERT:
		return writer.Alert(message)
	case syslog.LOG_CRIT:
		return writer.Crit(message)
	case syslog.LOG_ERR:
		return writer.Err(message)
	case syslog.LOG_WARNING:
		return writer.Warning(message)
	case syslog.LOG_NOTICE:
		return writer.Notice(message)
	case syslog.LOG_INFO:
		return writer.Info(message)
	case syslog.LOG_DEBUG:
		return writer.Debug(message)
	default:
		return writer.Info(message)
	}
}

func parsePriority(priority string) syslog.Priority {
	switch priority {
	case "emerg":
		return syslog.LOG_EMERG
	case "alert":
		return syslog.LOG_ALERT
	case "crit":
		return syslog.LOG_CRIT
	case "err":
		return syslog.LOG_ERR
	case "warning":
		return syslog.LOG_WARNING
	case "notice":
		return syslog.LOG_NOTICE
	case "info":
		return syslog.LOG_INFO
	case "debug":
		return syslog.LOG_DEBUG
	default:
		return -1
	}
}

func statsReporter(ctx context.Context, stats *Stats) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastSent int64
	var lastTime time.Time = stats.started

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			sent := stats.getSent()
			errors := stats.getErrors()

			// Calculate current rate
			currentRate := float64(sent-lastSent) / now.Sub(lastTime).Seconds()

			// Calculate overall rate
			overallRate := float64(sent) / now.Sub(stats.started).Seconds()

			fmt.Printf("\r[%s] Sent: %d | Errors: %d | Current: %.1f/s | Average: %.1f/s",
				now.Sub(stats.started).Truncate(time.Second),
				sent, errors, currentRate, overallRate)

			lastSent = sent
			lastTime = now
		}
	}
}

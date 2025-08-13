package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/spf13/cobra"
)

// Constants for byte sizes, API limits, and retry logic to improve readability.
const (
	KB                = 1024
	MB                = 1024 * 1024
	s3DeleteBatchSize = 1000
	maxRetries        = 5
	initialBackoff    = 100 * time.Millisecond
)

// Global flags and the shared S3 client instance.
var (
	endpointURL           string
	accessKey             string
	secretKey             string
	region                string
	bucket                string
	objectKey             string
	numRequests           int
	concurrency           int
	fileSizeStr           string
	fileSize              int64
	retentionMode         string
	retainUntil           string
	jsonOutput            bool
	debug                 bool
	useVirtualHostedStyle bool
	force                 bool // For the delete-all command
	deleteBatchSize       int  // For delete-objects benchmark

	// s3Client is a single, shared S3 client instance created once and reused
	// across all commands to ensure efficient use of network sockets.
	s3Client *s3.Client
)

// operationResult holds the outcome of a single benchmark operation.
type operationResult struct {
	latency          time.Duration
	bytesTransferred int64
}

// BenchmarkResult defines the structure for JSON output
type BenchmarkResult struct {
	TotalRequests      int             `json:"totalRequests"`
	SuccessfulRequests int             `json:"successfulRequests"`
	FailedRequests     int             `json:"failedRequests"`
	TotalTimeSeconds   float64         `json:"totalTimeSeconds"`
	RequestsPerSecond  float64         `json:"requestsPerSecond"`
	Latency            LatencyStats    `json:"latency"`
	Bandwidth          *BandwidthStats `json:"bandwidth,omitempty"`
}

// LatencyStats defines the structure for latency statistics in JSON output
type LatencyStats struct {
	Average string `json:"average"`
	Min     string `json:"min"`
	Max     string `json:"max"`
	P50     string `json:"p50"`
	P90     string `json:"p90"`
	P95     string `json:"p95"`
	P99     string `json:"p99"`
}

// BandwidthStats defines the structure for bandwidth statistics in JSON output
type BandwidthStats struct {
	Average string `json:"averageMBs"`
	Min     string `json:"minMBs"`
	Max     string `json:"maxMBs"`
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "icbench",
	Short: "A simple S3 benchmarking tool",
	Long: `icbench is a CLI tool to benchmark basic S3 operations like
GET, HEAD, and PUT against an S3-compatible object store.`,
	// PersistentPreRunE runs before any subcommand. It's used here to initialize
	// the shared S3 client, ensuring it's created only once. This is crucial
	// for efficient TCP connection (socket) reuse.
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if !debug {
			log.SetOutput(os.Stderr) // Default logger goes to stderr
		}
		var err error
		s3Client, err = newS3Client(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to create S3 client: %w", err)
		}
		return nil
	},
}

// prepareCmd represents the command to create objects for tests.
var prepareCmd = &cobra.Command{
	Use:   "prepare",
	Short: "Creates a set of objects in a bucket for testing",
	Long: `Creates a specified number of objects in a bucket. This is the recommended
first step before running 'get-object', 'head-object', or 'put-object-retention' benchmarks.`,
	Run: func(cmd *cobra.Command, args []string) {
		runPrepare(cmd.Context())
	},
}

// getObjectCmd represents the get-object command
var getObjectCmd = &cobra.Command{
	Use:   "get-object",
	Short: "Benchmark S3 GetObject performance on existing objects",
	Long:  "Benchmarks GetObject performance. It is recommended to run 'icbench prepare' first to create the objects.",
	Run: func(cmd *cobra.Command, args []string) {
		runBenchmark(cmd.Context(), benchmarkGetObject, "get-object")
	},
}

// headObjectCmd represents the head-object command
var headObjectCmd = &cobra.Command{
	Use:   "head-object",
	Short: "Benchmark S3 HeadObject performance on existing objects",
	Long:  "Benchmarks HeadObject performance. It is recommended to run 'icbench prepare' first to create the objects.",
	Run: func(cmd *cobra.Command, args []string) {
		runBenchmark(cmd.Context(), benchmarkHeadObject, "head-object")
	},
}

// putObjectCmd represents the put-object command
var putObjectCmd = &cobra.Command{
	Use:   "put-object",
	Short: "Benchmark S3 PutObject performance (creates new objects)",
	Run: func(cmd *cobra.Command, args []string) {
		runBenchmark(cmd.Context(), benchmarkPutObject, "put-object")
	},
}

// deleteObjectCmd represents the delete-object command
var deleteObjectCmd = &cobra.Command{
	Use:   "delete-object",
	Short: "Benchmark S3 DeleteObject performance on existing objects",
	Long:  "Benchmarks DeleteObject performance. It is recommended to run 'icbench prepare' first to create the objects.",
	Run: func(cmd *cobra.Command, args []string) {
		runBenchmark(cmd.Context(), benchmarkDeleteObject, "delete-object")
	},
}

// deleteObjectsCmd represents the delete-objects (batch) command
var deleteObjectsCmd = &cobra.Command{
	Use:   "delete-objects",
	Short: "Benchmark S3 DeleteObjects performance using batch deletes",
	Long:  "Benchmarks batch DeleteObjects performance over a dataset using the <prefix>-<i> key pattern created by 'icbench prepare'.",
	Run: func(cmd *cobra.Command, args []string) {
		runDeleteObjectsBenchmark(cmd.Context())
	},
}

// putObjectRetentionCmd represents the put-object-retention command
var putObjectRetentionCmd = &cobra.Command{
	Use:   "put-object-retention",
	Short: "Benchmark S3 PutObjectRetention performance on existing objects",
	Long:  "Benchmarks PutObjectRetention performance. It is recommended to run 'icbench prepare' first to create the objects.",
	Run: func(cmd *cobra.Command, args []string) {
		runBenchmark(cmd.Context(), benchmarkPutObjectRetention, "put-object-retention")
	},
}

// getObjectRetentionCmd represents the get-object-retention command
var getObjectRetentionCmd = &cobra.Command{
	Use:   "get-object-retention",
	Short: "Benchmark S3 GetObjectRetention performance on existing objects",
	Long:  "Benchmarks GetObjectRetention performance. It is recommended to run 'icbench prepare' first to create the objects.",
	Run: func(cmd *cobra.Command, args []string) {
		runBenchmark(cmd.Context(), benchmarkGetObjectRetention, "get-object-retention")
	},
}

// cleanupCmd represents the cleanup command
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Deletes objects with a specific prefix from a bucket",
	Long: `Deletes all objects in the specified bucket that match the key prefix
used during the benchmarks. This is useful for cleaning up the environment
after running tests.`,
	Run: func(cmd *cobra.Command, args []string) {
		runCleanup(cmd.Context(), objectKey)
	},
}

// deleteAllCmd represents the delete-all command
var deleteAllCmd = &cobra.Command{
	Use:   "delete-all",
	Short: "Deletes ALL objects in a given bucket",
	Long: `Deletes every object within the specified bucket.
This is a highly destructive operation and cannot be undone.
To prevent accidental data loss, you MUST use the --force flag to proceed.`,
	Run: func(cmd *cobra.Command, args []string) {
		if !force {
			log.Println("ERROR: This is a destructive operation that will delete all objects in the bucket.")
			log.Println("To proceed, you must explicitly add the --force flag.")
			os.Exit(1)
		}
		runCleanup(cmd.Context(), "") // An empty prefix matches all objects
	},
}

// parseSize converts a human-readable size string (e.g., "10k", "2m") to bytes.
func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.ToLower(strings.TrimSpace(sizeStr))
	var multiplier int64 = 1
	var numPart string

	if strings.HasSuffix(sizeStr, "k") {
		multiplier = KB
		numPart = strings.TrimSuffix(sizeStr, "k")
	} else if strings.HasSuffix(sizeStr, "m") {
		multiplier = MB
		numPart = strings.TrimSuffix(sizeStr, "m")
	} else {
		numPart = sizeStr
	}

	size, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	// Add edge case handling for negative sizes.
	if size < 0 {
		return 0, fmt.Errorf("size cannot be negative: %d", size)
	}

	return size * multiplier, nil
}

// newS3Client creates and returns a new S3 client, configured with global flags.
func newS3Client(ctx context.Context) (*s3.Client, error) {
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if endpointURL != "" {
			if debug {
				log.Printf("DEBUG: Using custom endpoint URL: %s", endpointURL)
			}
			return aws.Endpoint{
				URL:           endpointURL,
				SigningRegion: region,
				PartitionID:   "aws",
				Source:        aws.EndpointSourceCustom,
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(resolver),
	}

	// If access key and secret key are provided as flags, use them.
	// Otherwise, allow the SDK to fall back to its default credential chain
	// (env vars, shared config, etc.).
	if accessKey != "" && secretKey != "" {
		if debug {
			log.Println("DEBUG: Using static credentials provided by flags.")
		}
		creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		loadOptions = append(loadOptions, config.WithCredentialsProvider(creds))
	} else if debug {
		log.Println("DEBUG: No static credentials provided, using default AWS credential chain (e.g., environment variables).")
	}

	if debug {
		log.Println("DEBUG: Debug mode enabled. Activating AWS request logging.")
		// Log requests without the body for cleaner debug output.
		// Errors will still be logged in full by our custom logic.
		loadOptions = append(loadOptions, config.WithClientLogMode(aws.LogRequest))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	// Create the S3 client with options
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Default to path-style addressing unless virtual-hosted is explicitly requested.
		// This is often required for non-AWS S3-compatible systems.
		if !useVirtualHostedStyle {
			o.UsePathStyle = true
			if debug {
				log.Println("DEBUG: Using path-style addressing (default).")
			}
		} else if debug {
			log.Println("DEBUG: Using virtual-hosted (DNS) style addressing.")
		}
	})

	return client, nil
}

// isRetryableError checks if the error from S3 is a throttling/overload error
// or another transient error that warrants a retry.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific S3 API error codes that indicate throttling.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "SlowDown", "Throttling", "ThrottlingException":
			return true
		}
	}

	// Check for errors from the SDK's internal retryer and rate limiter.
	// These indicate that the SDK has already tried to handle the issue
	// but has given up, so we can perform our own retry.
	errMsg := err.Error()
	if strings.Contains(errMsg, "retry quota exceeded") || strings.Contains(errMsg, "failed to get rate limit token") {
		return true
	}

	// Check for generic transient network errors.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		// url.Error covers a wide range of network issues like DNS problems,
		// connection refused, etc., which are often temporary.
		return true
	}

	return false
}

// executeWithRetry wraps a function call with an exponential backoff retry mechanism.
func executeWithRetry(ctx context.Context, fn func() (operationResult, error)) (operationResult, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		res, err := fn()
		if err == nil {
			return res, nil // Success
		}

		lastErr = err

		if isRetryableError(err) {
			backoff := initialBackoff * time.Duration(math.Pow(2, float64(attempt)))
			// Add jitter: a random duration up to half of the backoff time.
			jitter := time.Duration(mrand.Int63n(int64(backoff / 2)))
			waitTime := backoff + jitter

			if debug {
				log.Printf("DEBUG: Retryable error detected (attempt %d/%d): %v. Waiting for %v before retrying.", attempt+1, maxRetries, err, waitTime)
			}

			// Wait for the backoff period, but check for context cancellation.
			select {
			case <-time.After(waitTime):
				continue // Continue to the next attempt
			case <-ctx.Done():
				return operationResult{}, ctx.Err() // Context was cancelled
			}
		} else {
			return operationResult{}, err // Not a retryable error, fail fast
		}
	}
	return operationResult{}, fmt.Errorf("request failed after %d attempts, last error: %w", maxRetries, lastErr)
}

// benchmarkFunc defines the function signature for a benchmark test.
type benchmarkFunc func(ctx context.Context, client *s3.Client, key string) (operationResult, error)

// runPrepare handles the creation of test objects.
func runPrepare(ctx context.Context) {
	if bucket == "" || objectKey == "" {
		log.Fatal("Bucket and key prefix must be provided for prepare")
	}

	prepSize, err := parseSize(fileSizeStr)
	if err != nil {
		log.Fatalf("Error parsing size: %v", err)
	}

	if !jsonOutput {
		fmt.Printf("Preparing by creating %d objects with size %s in bucket '%s'...\n", numRequests, fileSizeStr, bucket)
	}

	dummyData := make([]byte, prepSize)
	rand.Read(dummyData)

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	createdCount := 0
	mu := &sync.Mutex{}

	for i := 0; i < numRequests; i++ {
		select {
		case <-ctx.Done():
			log.Println("Cancellation detected during preparation. Halting.")
			return
		default:
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(objIndex int) {
			defer func() { <-sem }()
			defer wg.Done()
			key := fmt.Sprintf("%s-%d", objectKey, objIndex)
			_, err := executeWithRetry(ctx, func() (operationResult, error) {
				if debug {
					log.Printf("DEBUG: Preparing object %s", key)
				}
				// Use the shared s3Client instance.
				_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   bytes.NewReader(dummyData),
				})
				return operationResult{}, err
			})
			if err != nil {
				log.Printf("ERROR: Failed to create preparation object %s: %v", key, err)
			} else {
				mu.Lock()
				createdCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if !jsonOutput {
		fmt.Printf("Preparation complete. Successfully created %d objects.\n", createdCount)
	} else {
		result := map[string]interface{}{
			"objectsCreated":  createdCount,
			"objectsTargeted": numRequests,
		}
		jsonData, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(jsonData))
	}
}

// runBenchmark orchestrates the benchmark execution.
func runBenchmark(ctx context.Context, bf benchmarkFunc, benchmarkName string) {
	if bucket == "" {
		log.Fatal("Bucket must be provided")
	}

	// Parse file size for relevant benchmarks.
	if benchmarkName == "put-object" || benchmarkName == "get-object" {
		var err error
		fileSize, err = parseSize(fileSizeStr)
		if err != nil {
			log.Fatalf("Error parsing size: %v", err)
		}
		if debug {
			log.Printf("DEBUG: Parsed file size for %s to %d bytes", benchmarkName, fileSize)
		}
	}

	var wg sync.WaitGroup
	results := make(chan operationResult, numRequests)
	errors := make(chan error, numRequests)
	sem := make(chan struct{}, concurrency) // Semaphore to control concurrency

	if !jsonOutput {
		fmt.Printf("Starting benchmark with %d requests and concurrency %d...\n", numRequests, concurrency)
		if benchmarkName != "put-object" {
			fmt.Println("Note: This benchmark assumes objects were created with 'icbench prepare'.")
		}
		fmt.Printf("Each request will use a unique object key (e.g., %s-0, %s-1, ...)\n", objectKey, objectKey)
	}
	startTime := time.Now()
	requestsDispatched := 0

	for i := 0; i < numRequests; i++ {
		// Check for cancellation before starting a new request.
		if ctx.Err() != nil {
			if !jsonOutput {
				log.Printf("Cancellation detected, stopping benchmark after dispatching %d requests.", i)
			}
			break // Exit the loop
		}
		requestsDispatched++

		currentObjectKey := fmt.Sprintf("%s-%d", objectKey, i)

		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore
		go func(keyForRequest string) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore
			// Use the shared s3Client instance.
			res, err := executeWithRetry(ctx, func() (operationResult, error) {
				return bf(ctx, s3Client, keyForRequest)
			})
			if err != nil {
				errors <- err
			} else {
				results <- res
			}
		}(currentObjectKey)
	}

	wg.Wait()
	close(results)
	close(errors)

	totalTime := time.Since(startTime)
	var totalLatency time.Duration
	var latencies []time.Duration
	var bandwidths []float64 // in MB/s
	var errorCount int

	for res := range results {
		totalLatency += res.latency
		latencies = append(latencies, res.latency)
		if res.bytesTransferred > 0 && res.latency.Seconds() > 0 {
			// Calculate bandwidth in MB/s
			bandwidths = append(bandwidths, (float64(res.bytesTransferred)/res.latency.Seconds())/(MB))
		}
	}
	successCount := len(latencies)

	for err := range errors {
		// Always log errors in debug mode, otherwise only for non-json output
		if !jsonOutput || debug {
			log.Printf("ERROR: Request failed: %v", err)
		}
		errorCount++
	}

	// --- Calculate all metrics before printing ---
	var rps float64
	var avgLatency, minLatency, maxLatency, p50, p90, p95, p99 time.Duration
	var avgBandwidth, minBandwidth, maxBandwidth float64

	if successCount > 0 {
		rps = float64(successCount) / totalTime.Seconds()
		avgLatency = totalLatency / time.Duration(successCount)

		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		minLatency = latencies[0]
		maxLatency = latencies[successCount-1]
		p50 = latencies[int(float64(successCount)*0.5)]
		p90 = latencies[int(float64(successCount)*0.9)]
		p95 = latencies[int(float64(successCount)*0.95)]
		p99 = latencies[int(float64(successCount)*0.99)]

		if len(bandwidths) > 0 {
			sort.Float64s(bandwidths)
			minBandwidth = bandwidths[0]
			maxBandwidth = bandwidths[len(bandwidths)-1]
			var totalBandwidth float64
			for _, b := range bandwidths {
				totalBandwidth += b
			}
			avgBandwidth = totalBandwidth / float64(len(bandwidths))
		}
	}

	// --- Format and Print Results ---
	if jsonOutput {
		latencyStats := LatencyStats{}
		var bandwidthStats *BandwidthStats

		if successCount > 0 {
			latencyStats = LatencyStats{
				Average: avgLatency.String(),
				Min:     minLatency.String(),
				Max:     maxLatency.String(),
				P50:     p50.String(),
				P90:     p90.String(),
				P95:     p95.String(),
				P99:     p99.String(),
			}
			if len(bandwidths) > 0 {
				bandwidthStats = &BandwidthStats{
					Average: fmt.Sprintf("%.2f", avgBandwidth),
					Min:     fmt.Sprintf("%.2f", minBandwidth),
					Max:     fmt.Sprintf("%.2f", maxBandwidth),
				}
			}
		}

		result := BenchmarkResult{
			TotalRequests:      requestsDispatched,
			SuccessfulRequests: successCount,
			FailedRequests:     errorCount,
			TotalTimeSeconds:   totalTime.Seconds(),
			RequestsPerSecond:  rps,
			Latency:            latencyStats,
			Bandwidth:          bandwidthStats,
		}

		jsonData, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Fatalf("Failed to generate JSON output: %v", err)
		}
		fmt.Println(string(jsonData))
	} else {
		// Print human-readable output
		fmt.Println("\n--- Benchmark Results ---")
		fmt.Printf("Total Requests:      %d\n", requestsDispatched)
		fmt.Printf("Successful Requests: %d\n", successCount)
		fmt.Printf("Failed Requests:     %d\n", errorCount)
		fmt.Printf("Total Time:          %v\n", totalTime)

		if successCount > 0 {
			fmt.Printf("Requests Per Second: %.2f\n", rps)
			fmt.Println("\n--- Latency Distribution ---")
			fmt.Printf("Average: %v\n", avgLatency)
			fmt.Printf("Min:     %v\n", minLatency)
			fmt.Printf("Max:     %v\n", maxLatency)
			fmt.Printf("p50:     %v\n", p50)
			fmt.Printf("p90:     %v\n", p90)
			fmt.Printf("p95:     %v\n", p95)
			fmt.Printf("p99:     %v\n", p99)

			if len(bandwidths) > 0 {
				fmt.Println("\n--- Bandwidth Distribution (MB/s) ---")
				fmt.Printf("Average: %.2f\n", avgBandwidth)
				fmt.Printf("Min:     %.2f\n", minBandwidth)
				fmt.Printf("Max:     %.2f\n", maxBandwidth)
			}
		}
		fmt.Println("-------------------------")
	}
}

// runCleanup handles the deletion of benchmark objects.
// An empty prefix will match all objects in the bucket.
func runCleanup(ctx context.Context, prefix string) {
	if bucket == "" {
		log.Fatal("Bucket must be provided for cleanup")
	}
	if prefix == "" { // This is the delete-all case
		if !jsonOutput {
			log.Printf("WARNING: Deleting ALL objects from bucket '%s'.", bucket)
		}
	} else {
		if !jsonOutput {
			log.Printf("Starting cleanup for objects in bucket '%s' with prefix '%s'...\n", bucket, prefix)
		}
	}

	// List all objects with the given prefix
	var objectsToDelete []types.ObjectIdentifier
	// Use the shared s3Client instance.
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		// Check for cancellation.
		if ctx.Err() != nil {
			log.Println("Cleanup cancelled during object listing.")
			return
		}
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("Failed to list objects for cleanup: %v", err)
		}
		if debug {
			log.Printf("DEBUG: Found %d objects in page to consider for deletion.", len(page.Contents))
		}
		for _, obj := range page.Contents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{Key: obj.Key})
		}
	}

	if len(objectsToDelete) == 0 {
		if !jsonOutput {
			fmt.Println("No objects found with the specified prefix. Nothing to clean up.")
		} else {
			fmt.Println(`{"deletedCount": 0, "status": "No objects found"}`)
		}
		return
	}

	// Delete objects in batches of 1000 (the S3 API limit)
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency) // Use concurrency for deletion as well
	deletedCount := 0
	mu := &sync.Mutex{}

	for i := 0; i < len(objectsToDelete); i += s3DeleteBatchSize {
		if ctx.Err() != nil {
			log.Println("Cleanup cancelled during deletion.")
			break
		}
		end := i + s3DeleteBatchSize
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}
		batch := objectsToDelete[i:end]

		wg.Add(1)
		sem <- struct{}{}
		go func(b []types.ObjectIdentifier) {
			defer func() { <-sem }()
			defer wg.Done()
			if debug {
				log.Printf("DEBUG: Deleting batch of %d objects", len(b))
			}
			// Use the shared s3Client instance.
			output, err := s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &types.Delete{Objects: b},
			})
			if err != nil {
				log.Printf("ERROR: Failed to delete a batch of objects: %v", err)
				return
			}
			mu.Lock()
			deletedCount += len(output.Deleted)
			mu.Unlock()
			for _, e := range output.Errors {
				log.Printf("ERROR: Could not delete object %s: %s", *e.Key, *e.Message)
			}
		}(batch)
	}
	wg.Wait()

	if jsonOutput {
		result := map[string]interface{}{
			"deletedCount": deletedCount,
			"totalFound":   len(objectsToDelete),
		}
		jsonData, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(jsonData))
	} else {
		fmt.Printf("Cleanup complete. Deleted %d out of %d objects found.\n", deletedCount, len(objectsToDelete))
	}
}

// benchmarkGetObject performs the GetObject benchmark.
func benchmarkGetObject(ctx context.Context, client *s3.Client, key string) (operationResult, error) {
	start := time.Now()
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(o *s3.Options) {
		// This option disables a specific log message that can be noisy during
		// high-throughput GetObject benchmarks, improving debuggability.
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	latency := time.Since(start)

	if err != nil {
		return operationResult{}, err
	}

	// Safely dereference the ContentLength pointer.
	var contentLength int64
	if output.ContentLength != nil {
		contentLength = *output.ContentLength
	}
	return operationResult{latency: latency, bytesTransferred: contentLength}, nil
}

// benchmarkHeadObject performs the HeadObject benchmark.
func benchmarkHeadObject(ctx context.Context, client *s3.Client, key string) (operationResult, error) {
	start := time.Now()
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	latency := time.Since(start)

	if err != nil {
		return operationResult{}, err
	}
	return operationResult{latency: latency, bytesTransferred: 0}, nil
}

// benchmarkPutObject performs the PutObject benchmark.
func benchmarkPutObject(ctx context.Context, client *s3.Client, key string) (operationResult, error) {
	// Create random data for the object body
	body := make([]byte, fileSize)
	if _, err := rand.Read(body); err != nil {
		return operationResult{}, fmt.Errorf("failed to generate random data: %w", err)
	}

	// For this benchmark, we use the unique key passed as a parameter.
	start := time.Now()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	latency := time.Since(start)

	if err != nil {
		return operationResult{}, err
	}
	return operationResult{latency: latency, bytesTransferred: fileSize}, nil
}

// benchmarkPutObjectRetention performs the PutObjectRetention benchmark.
func benchmarkPutObjectRetention(ctx context.Context, client *s3.Client, key string) (operationResult, error) {
	var retention types.ObjectLockRetention
	if retentionMode != "" {
		rm := types.ObjectLockRetentionMode(retentionMode)
		retention.Mode = rm
	}

	if retainUntil != "" {
		t, err := time.Parse(time.RFC3339, retainUntil)
		if err != nil {
			return operationResult{}, fmt.Errorf("invalid retain-until-date format: %w", err)
		}
		retention.RetainUntilDate = &t
	}

	start := time.Now()
	_, err := client.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		Retention: &retention,
	})
	latency := time.Since(start)

	if err != nil {
		return operationResult{}, err
	}
	return operationResult{latency: latency, bytesTransferred: 0}, nil
}

// benchmarkGetObjectRetention performs the GetObjectRetention benchmark.
func benchmarkGetObjectRetention(ctx context.Context, client *s3.Client, key string) (operationResult, error) {
	start := time.Now()
	_, err := client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	latency := time.Since(start)
	if err != nil {
		return operationResult{}, err
	}
	return operationResult{latency: latency, bytesTransferred: 0}, nil
}

// benchmarkDeleteObject performs the DeleteObject benchmark.
func benchmarkDeleteObject(ctx context.Context, client *s3.Client, key string) (operationResult, error) {
	start := time.Now()
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	latency := time.Since(start)
	if err != nil {
		return operationResult{}, err
	}
	return operationResult{latency: latency, bytesTransferred: 0}, nil
}

// runDeleteObjectsBenchmark benchmarks DeleteObjects in batches over numRequests keys.
func runDeleteObjectsBenchmark(ctx context.Context) {
	if bucket == "" || objectKey == "" {
		log.Fatal("Bucket and key prefix must be provided for delete-objects")
	}
	batch := deleteBatchSize
	if batch <= 0 || batch > s3DeleteBatchSize {
		batch = s3DeleteBatchSize
	}
	// Build the list of keys to delete based on numRequests and prefix
	objects := make([]types.ObjectIdentifier, 0, numRequests)
	for i := 0; i < numRequests; i++ {
		k := fmt.Sprintf("%s-%d", objectKey, i)
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(k)})
	}

	if !jsonOutput {
		fmt.Printf("Starting delete-objects benchmark with %d objects, batch size %d, concurrency %d...\n", len(objects), batch, concurrency)
	}

	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	results := make(chan operationResult, (len(objects)+batch-1)/batch)
	errs := make(chan error, (len(objects)+batch-1)/batch)

	for i := 0; i < len(objects); i += batch {
		if ctx.Err() != nil {
			break
		}
		end := i + batch
		if end > len(objects) {
			end = len(objects)
		}
		b := objects[i:end]
		wg.Add(1)
		sem <- struct{}{}
		go func(objs []types.ObjectIdentifier) {
			defer wg.Done()
			defer func() { <-sem }()
			res, err := executeWithRetry(ctx, func() (operationResult, error) {
				st := time.Now()
				out, err := s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(bucket),
					Delete: &types.Delete{Objects: objs},
				})
				lat := time.Since(st)
				if err != nil {
					return operationResult{}, err
				}
				// Count only successfully deleted in bytesTransferred as zero; use latency only
				if len(out.Errors) > 0 && (!jsonOutput || debug) {
					for _, e := range out.Errors {
						log.Printf("ERROR: Could not delete %s: %s", aws.ToString(e.Key), aws.ToString(e.Message))
					}
				}
				return operationResult{latency: lat, bytesTransferred: 0}, nil
			})
			if err != nil {
				errs <- err
			} else {
				results <- res
			}
		}(b)
	}
	wg.Wait()
	close(results)
	close(errs)

	// aggregate
	var lats []time.Duration
	var errCount int
	var totalLatency time.Duration
	for r := range results {
		lats = append(lats, r.latency)
		totalLatency += r.latency
	}
	for range errs {
		errCount++
	}
	success := len(lats)
	totalTime := time.Since(start)
	var rps float64
	var avg, min, max, p50, p90, p95, p99 time.Duration
	if success > 0 {
		rps = float64(success) / totalTime.Seconds()
		avg = totalLatency / time.Duration(success)
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		min = lats[0]
		max = lats[success-1]
		p50 = lats[int(float64(success)*0.5)]
		p90 = lats[int(float64(success)*0.9)]
		p95 = lats[int(float64(success)*0.95)]
		p99 = lats[int(float64(success)*0.99)]
	}

	if jsonOutput {
		lat := LatencyStats{}
		if success > 0 {
			lat = LatencyStats{Average: avg.String(), Min: min.String(), Max: max.String(), P50: p50.String(), P90: p90.String(), P95: p95.String(), P99: p99.String()}
		}
		res := BenchmarkResult{
			TotalRequests:      (len(objects) + batch - 1) / batch,
			SuccessfulRequests: success,
			FailedRequests:     errCount,
			TotalTimeSeconds:   totalTime.Seconds(),
			RequestsPerSecond:  rps,
			Latency:            lat,
		}
		b, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Println("\n--- Benchmark Results (delete-objects) ---")
		fmt.Printf("Total Batches:       %d\n", (len(objects)+batch-1)/batch)
		fmt.Printf("Successful Batches:  %d\n", success)
		fmt.Printf("Failed Batches:      %d\n", errCount)
		fmt.Printf("Total Time:          %v\n", totalTime)
		if success > 0 {
			fmt.Printf("Batches Per Second:  %.2f\n", rps)
			fmt.Println("\n--- Latency Distribution ---")
			fmt.Printf("Average: %v\n", avg)
			fmt.Printf("Min:     %v\n", min)
			fmt.Printf("Max:     %v\n", max)
			fmt.Printf("p50:     %v\n", p50)
			fmt.Printf("p90:     %v\n", p90)
			fmt.Printf("p95:     %v\n", p95)
			fmt.Printf("p99:     %v\n", p99)
			fmt.Println("-------------------------")
		}
	}
}

func init() {
	// Seed the math/rand package for jitter calculation.
	mrand.Seed(time.Now().UnixNano())

	// Add global flags to rootCmd
	rootCmd.PersistentFlags().StringVar(&endpointURL, "endpoint-url", "", "S3 endpoint URL (e.g., http://localhost:9000)")
	rootCmd.PersistentFlags().StringVar(&accessKey, "access-key", "", "S3 access key (or use AWS_ACCESS_KEY_ID env var)")
	rootCmd.PersistentFlags().StringVar(&secretKey, "secret-key", "", "S3 secret key (or use AWS_SECRET_ACCESS_KEY env var)")
	rootCmd.PersistentFlags().StringVarP(&region, "region", "r", "us-east-1", "AWS region")
	rootCmd.PersistentFlags().StringVarP(&bucket, "bucket", "b", "", "S3 bucket name")
	rootCmd.PersistentFlags().StringVarP(&objectKey, "key", "k", "icbench-object", "S3 object key base name (prefix)")
	rootCmd.PersistentFlags().IntVarP(&numRequests, "requests", "n", 100, "Number of requests to perform (and objects to create/cleanup)")
	rootCmd.PersistentFlags().IntVarP(&concurrency, "concurrency", "c", 10, "Number of concurrent requests")
	rootCmd.PersistentFlags().BoolVar(&jsonOutput, "json", false, "Output results in JSON format")
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging for detailed API calls")
	rootCmd.PersistentFlags().BoolVar(&useVirtualHostedStyle, "use-virtual-hosted-style", false, "Enable virtual-hosted (DNS) style addressing (e.g., BUCKET.s3.amazonaws.com/KEY). Path-style is default.")

	// Add flags for tests that use object size
	prepareCmd.Flags().StringVarP(&fileSizeStr, "size", "s", "1k", "Size of each object to create for the test set (e.g., 1024, 4k, 2m)")
	putObjectCmd.Flags().StringVarP(&fileSizeStr, "size", "s", "1k", "Size of each new object to upload during the benchmark (e.g., 1024, 4k, 2m)")
	getObjectCmd.Flags().StringVarP(&fileSizeStr, "size", "s", "1k", "Expected size of objects to download for bandwidth calculation (e.g., 1024, 4k, 2m)")

	// Add flags for put-object-retention
	putObjectRetentionCmd.Flags().StringVar(&retentionMode, "mode", "COMPLIANCE", "Retention mode (COMPLIANCE or GOVERNANCE)")
	putObjectRetentionCmd.Flags().StringVar(&retainUntil, "retain-until-date", "", "Retain until date in RFC3339 format (e.g., 2024-12-31T15:04:05Z)")

	// Add flags for delete-all
	deleteAllCmd.Flags().BoolVar(&force, "force", false, "Required to confirm the deletion of all objects in the bucket.")

	// Add subcommands to rootCmd
	rootCmd.AddCommand(prepareCmd)
	rootCmd.AddCommand(getObjectCmd)
	rootCmd.AddCommand(headObjectCmd)
	rootCmd.AddCommand(putObjectCmd)
	rootCmd.AddCommand(putObjectRetentionCmd)
	rootCmd.AddCommand(getObjectRetentionCmd)
	rootCmd.AddCommand(cleanupCmd)
	rootCmd.AddCommand(deleteAllCmd)
	rootCmd.AddCommand(deleteObjectCmd)
	rootCmd.AddCommand(deleteObjectsCmd)

	// flags for delete-objects
	deleteObjectsCmd.Flags().IntVarP(&deleteBatchSize, "batch-size", "B", 1000, "Batch size for DeleteObjects (max 1000)")
}

func main() {
	// Create a context that is cancelled on an interrupt signal (Ctrl+C).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		// The error is already logged by Cobra or our functions.
		// We exit with a non-zero status code.
		os.Exit(1)
	}
}

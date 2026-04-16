package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"

	scheduler_handler "github.com/PaddlePaddle/FastDeploy/router/internal/scheduler/handler"
	"github.com/PaddlePaddle/FastDeploy/router/pkg/logger"
	"github.com/gin-gonic/gin"
)

// Precompile regex to avoid repeated compilation
var (
	runningRequestsRegex      = regexp.MustCompile(`fastdeploy:num_requests_running\s+([0-9.]+)`)
	waitingRequestsRegex      = regexp.MustCompile(`fastdeploy:num_requests_waiting\s+([0-9.]+)`)
	availableGpuBlockNumRegex = regexp.MustCompile(`available_gpu_block_num\s+([0-9.]+)`)
)

// parseMetricsResponse parses metrics response string and extracts key metrics
func parseMetricsResponse(response string) (running, waiting, availableGpuBlockNum int, err error) {
	runningCnt := -1.0
	waitingCnt := -1.0
	gpuBlockNum := -1.0

	// Find fastdeploy:num_requests_running field using precompiled regex
	if matches := runningRequestsRegex.FindStringSubmatch(response); len(matches) >= 2 {
		if score, parseErr := strconv.ParseFloat(matches[1], 64); parseErr == nil {
			runningCnt = score
		}
	}

	// Find fastdeploy:num_requests_waiting field using precompiled regex
	if matches := waitingRequestsRegex.FindStringSubmatch(response); len(matches) >= 2 {
		if score, parseErr := strconv.ParseFloat(matches[1], 64); parseErr == nil {
			waitingCnt = score
		}
	}

	// Parse available_gpu_block_num field
	if matches := availableGpuBlockNumRegex.FindStringSubmatch(response); len(matches) >= 2 {
		if score, parseErr := strconv.ParseFloat(matches[1], 64); parseErr == nil {
			gpuBlockNum = score
		}
	}

	if runningCnt < 0 || waitingCnt < 0 || gpuBlockNum < 0 {
		return 0, 0, 0, errors.New("failed to parse metrics response")
	}

	return int(runningCnt), int(waitingCnt), int(gpuBlockNum), nil
}

// redrictCounter gets or creates a counter for the given URL and returns current count
func redrictCounter(ctx context.Context, rawURL string) int {
	counter := scheduler_handler.GetOrCreateCounter(ctx, rawURL)
	return int(counter.Get())
}

// fetchMetricsByURL retrieves metrics from a worker's /metrics endpoint
func fetchMetricsByURL(ctx context.Context, rawURL string) (*WorkerMetrics, error) {
	workerInfo := getWorkerInfo(ctx, rawURL)
	if workerInfo == nil {
		return nil, errors.New("worker info not found for URL")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	u.Host = net.JoinHostPort(host, workerInfo.MetricsPort)
	metricsUrl := fmt.Sprintf("%s/metrics", u.String())

	client := &http.Client{Timeout: defaultCheckTimeout}
	resp, err := client.Get(metricsUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if len(body) == 0 {
		return nil, errors.New("metrics response is empty")
	}

	running, waiting, gpuBlockNum, err := parseMetricsResponse(string(body))
	if err != nil {
		return nil, err
	}

	return &WorkerMetrics{
		NumRequestsRunning:   running,
		NumRequestsWaiting:   waiting,
		AvailableGpuBlockNum: gpuBlockNum,
	}, nil
}

// GetMetrics retrieves running metrics of the worker for the specified URL (in-memory counting)
func (m *Manager) GetMetrics(ctx context.Context, rawURL string) (int, int, int) {
	runningCnt := redrictCounter(ctx, rawURL)
	return runningCnt, 0, 0
}

// GetRemoteMetrics retrieves metrics from the cached workerMetrics map, fetches from worker if not cached
func (m *Manager) GetRemoteMetrics(ctx context.Context, rawURL string) (int, int, int) {
	m.mu.RLock()
	metrics := m.workerMetrics[rawURL]
	m.mu.RUnlock()

	if metrics == nil {
		// Fetch from worker and update cache
		fetchedMetrics, err := fetchMetricsByURL(ctx, rawURL)
		if err != nil {
			logger.Warn(ctx, "GetRemoteMetrics failed for %s, falling back to local counter: %v", rawURL, err)
			runningNewCnt := redrictCounter(ctx, rawURL)
			return runningNewCnt, 0, 0
		}
		m.mu.Lock()
		m.workerMetrics[rawURL] = fetchedMetrics
		m.mu.Unlock()
		return fetchedMetrics.NumRequestsRunning, fetchedMetrics.NumRequestsWaiting, fetchedMetrics.AvailableGpuBlockNum
	}
	return metrics.NumRequestsRunning, metrics.NumRequestsWaiting, metrics.AvailableGpuBlockNum
}

// MonitorWorkerMetrics periodically updates metrics for all worker instances
func MonitorWorkerMetrics(ctx context.Context, intervalSecs float64) {
	ticker := time.NewTicker(time.Duration(intervalSecs * float64(time.Second)))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateAllWorkerMetrics(ctx)
		}
	}
}

// updateAllWorkerMetrics fetches and updates metrics for all registered workers
func updateAllWorkerMetrics(ctx context.Context) {
	if DefaultManager == nil {
		return
	}

	allServers := GetAllMapServers(ctx)
	if len(allServers) == 0 {
		return
	}

	type metricsResult struct {
		url     string
		metrics *WorkerMetrics
		err     error
	}

	resultCh := make(chan metricsResult, len(allServers))
	var wg sync.WaitGroup

	for _, workerInfo := range allServers {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			metrics, err := fetchMetricsByURL(ctx, url)
			resultCh <- metricsResult{
				url:     url,
				metrics: metrics,
				err:     err,
			}
		}(workerInfo.Url)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect all results first WITHOUT holding the lock, so that goroutines
	// which need RLock (e.g. getWorkerInfo inside fetchMetricsByURL) are not
	// blocked by a pending writer lock.
	var results []metricsResult
	for res := range resultCh {
		results = append(results, res)
	}

	// Now that all goroutines have finished, acquire the write lock and update.
	DefaultManager.mu.Lock()
	defer DefaultManager.mu.Unlock()

	for _, res := range results {
		if res.err != nil {
			logger.Debug(ctx, "Failed to fetch metrics for %s: %v", res.url, res.err)
			continue
		}
		DefaultManager.workerMetrics[res.url] = res.metrics
	}
}

// GetWorkerMetrics returns the cached metrics for a specific worker
func GetWorkerMetrics(ctx context.Context, rawURL string) *WorkerMetrics {
	if DefaultManager == nil {
		return nil
	}
	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()
	return DefaultManager.workerMetrics[rawURL]
}

// GetAllWorkerMetrics returns a copy of all cached worker metrics
func GetAllWorkerMetrics(ctx context.Context) map[string]*WorkerMetrics {
	if DefaultManager == nil {
		return nil
	}
	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()

	result := make(map[string]*WorkerMetrics, len(DefaultManager.workerMetrics))
	for url, metrics := range DefaultManager.workerMetrics {
		result[url] = &WorkerMetrics{
			NumRequestsRunning:   metrics.NumRequestsRunning,
			NumRequestsWaiting:   metrics.NumRequestsWaiting,
			AvailableGpuBlockNum: metrics.AvailableGpuBlockNum,
		}
	}
	return result
}

// WorkerMetricsHandler is the HTTP handler for /worker_metrics endpoint
func WorkerMetricsHandler(c *gin.Context) {
	allMetrics := GetAllWorkerMetrics(c.Request.Context())
	c.JSON(http.StatusOK, gin.H{
		"code":                  200,
		"metrics":               allMetrics,
		"is_queueing":           IsQueueing(),
		"pending_request_count": GetPendingRequestCount(),
	})
}

// MonitorQueueStatus periodically checks and updates the global queueing state
func MonitorQueueStatus(ctx context.Context, intervalSecs float64, threshold int) {
	ticker := time.NewTicker(time.Duration(intervalSecs * float64(time.Second)))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateQueueStatus(ctx, threshold)
		}
	}
}

// updateQueueStatus calculates total queue count and updates queueing state
func updateQueueStatus(ctx context.Context, threshold int) {
	if DefaultManager == nil {
		return
	}

	// Get pending request count (requests waiting to select a worker)
	pendingCount := GetPendingRequestCount()

	// Get total waiting requests from all workers
	var workerWaitingCount int64
	DefaultManager.mu.RLock()
	for _, metrics := range DefaultManager.workerMetrics {
		if metrics != nil {
			workerWaitingCount += int64(metrics.NumRequestsWaiting)
		}
	}
	DefaultManager.mu.RUnlock()

	// Total queue count = pending requests + worker-side waiting requests
	totalQueueCount := pendingCount + workerWaitingCount

	// Update queueing state based on threshold
	isQueueing := totalQueueCount > int64(threshold)
	SetQueueingState(isQueueing)

	logger.Debug(ctx, "Queue status: pending=%d, workerWaiting=%d, total=%d, threshold=%d, isQueueing=%v",
		pendingCount, workerWaitingCount, totalQueueCount, threshold, isQueueing)
}

package manager

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/PaddlePaddle/FastDeploy/router/internal/config"
	scheduler_handler "github.com/PaddlePaddle/FastDeploy/router/internal/scheduler/handler"
	"github.com/PaddlePaddle/FastDeploy/router/pkg/logger"
)

type Manager struct {
	mixedWorkerMap      map[string]*WorkerInfo
	prefillWorkerMap    map[string]*WorkerInfo
	decodeWorkerMap     map[string]*WorkerInfo
	splitwise           bool
	inferReady          bool
	workerMetrics       map[string]*WorkerMetrics
	isQueueing          bool  // whether the system is in queueing state
	pendingRequestCount int64 // number of requests waiting to select a worker
	mu                  sync.RWMutex
}

type WorkerInfo struct {
	Url                   string   `json:"url"`
	WorkerType            string   `json:"worker_type"`
	ConnectorPort         string   `json:"connector_port"`
	EngineWorkerQueuePort string   `json:"engine_worker_queue_port"`
	TransferProtocol      []string `json:"transfer_protocol"`
	RdmaPorts             []string `json:"rdma_ports"`
	DeviceIDs             []string `json:"device_ids"`
	MetricsPort           string   `json:"metrics_port"`
	TpSize                int      `json:"tp_size"`
}

// WorkerMetrics stores real-time metrics for a worker instance
type WorkerMetrics struct {
	NumRequestsRunning   int `json:"num_requests_running"`
	NumRequestsWaiting   int `json:"num_requests_waiting"`
	AvailableGpuBlockNum int `json:"available_gpu_block_num"`
}

var DefaultManager *Manager
var defaultCheckTimeout time.Duration
var healthEndpoint string
var failureThreshold int
var successThreshold int

var selectWorkerMu sync.Mutex

// Manager module initialization
func Init(cfg *config.Config) {
	manager := &Manager{
		mixedWorkerMap:      make(map[string]*WorkerInfo),
		prefillWorkerMap:    make(map[string]*WorkerInfo),
		decodeWorkerMap:     make(map[string]*WorkerInfo),
		splitwise:           cfg.Server.Splitwise,
		inferReady:          false,
		workerMetrics:       make(map[string]*WorkerMetrics),
		isQueueing:          false,
		pendingRequestCount: 0,
	}
	DefaultManager = manager
	// Define a default timeout duration
	defaultCheckTimeout = time.Duration(cfg.Manager.HealthCheckTimeoutSecs * float64(time.Second))
	healthEndpoint = cfg.Manager.HealthCheckEndpoint
	failureThreshold = cfg.Manager.HealthFailureThreshold
	successThreshold = cfg.Manager.HealthSuccessThreshold
}

func WorkerMapToList(ctx context.Context, workerType string) []string {
	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()

	var workerMap map[string]*WorkerInfo
	switch workerType {
	case "mixed":
		workerMap = DefaultManager.mixedWorkerMap
	case "prefill":
		workerMap = DefaultManager.prefillWorkerMap
	case "decode":
		workerMap = DefaultManager.decodeWorkerMap
	default:
		return []string{}
	}

	if workerMap == nil {
		return []string{}
	}

	// Get all keys and sort them
	keys := make([]string, 0, len(workerMap))
	for key := range workerMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build worker list
	workerURLs := make([]string, 0, len(keys))
	for _, key := range keys {
		if workerInfo, exists := workerMap[key]; exists {
			workerURLs = append(workerURLs, workerInfo.Url)
		}
	}
	return workerURLs
}

func (m *Manager) GetHealthyURLs(ctx context.Context) []string {
	if m == nil {
		return []string{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	totalSeversLength := len(m.prefillWorkerMap) + len(m.decodeWorkerMap) + len(m.mixedWorkerMap)
	allServerURLs := make([]string, 0, totalSeversLength)

	for id := range m.prefillWorkerMap {
		allServerURLs = append(allServerURLs, id)
	}
	for id := range m.decodeWorkerMap {
		allServerURLs = append(allServerURLs, id)
	}
	for id := range m.mixedWorkerMap {
		allServerURLs = append(allServerURLs, id)
	}
	return allServerURLs
}

func SelectWorker(ctx context.Context, message string) (string, error) {
	selectWorkerMu.Lock()
	defer selectWorkerMu.Unlock()

	workers := WorkerMapToList(ctx, "mixed")
	selectedWorkerURL, err := scheduler_handler.SelectWorker(ctx, workers, message, "mixed")
	if err != nil {
		logger.Error(ctx, "Failed to select mixed worker: %v", err)
		return "", err
	}
	return selectedWorkerURL, nil
}

func SelectWorkerPair(ctx context.Context, message string) (string, string, error) {
	selectWorkerMu.Lock()
	defer selectWorkerMu.Unlock()

	prefillWorkers := WorkerMapToList(ctx, "prefill")
	decodeWorkers := WorkerMapToList(ctx, "decode")
	if len(prefillWorkers) == 0 || len(decodeWorkers) == 0 {
		return "", "", nil
	}
	logger.Info(ctx, "before SelectWorker prefill. ts_ms=%s", time.Now().Format("2006-01-02 15:04:05.000"))
	selectedPrefillWorkerURL, err := scheduler_handler.SelectWorker(ctx, prefillWorkers, message, "prefill")
	if err != nil {
		logger.Error(ctx, "Failed to select prefill worker: %v", err)
		return "", "", err
	}
	logger.Info(ctx, "before SelectWorker decode, after prefill. ts_ms=%s", time.Now().Format("2006-01-02 15:04:05.000"))
	selectedDecodeWorkerURL, err := scheduler_handler.SelectWorker(ctx, decodeWorkers, message, "decode")
	if err != nil {
		// Prefill counter was already incremented but decode failed;
		// release prefill counters here since CommonCompletions defer is not yet registered.
		scheduler_handler.Release(ctx, selectedPrefillWorkerURL)
		scheduler_handler.ReleasePrefillTokens(ctx, selectedPrefillWorkerURL, message)
		logger.Info(ctx, "[SelectWorkerPair] decode selection failed, releasing prefill counter url=%s", selectedPrefillWorkerURL)
		return "", "", err
	}
	logger.Info(ctx, "after SelectWorker decode, before return. ts_ms=%s", time.Now().Format("2006-01-02 15:04:05.000"))
	return selectedPrefillWorkerURL, selectedDecodeWorkerURL, nil
}

// IncrementPendingRequest increments the pending request counter
func IncrementPendingRequest() {
	if DefaultManager == nil {
		return
	}
	DefaultManager.mu.Lock()
	DefaultManager.pendingRequestCount++
	DefaultManager.mu.Unlock()
}

// DecrementPendingRequest decrements the pending request counter
func DecrementPendingRequest() {
	if DefaultManager == nil {
		return
	}
	DefaultManager.mu.Lock()
	if DefaultManager.pendingRequestCount > 0 {
		DefaultManager.pendingRequestCount--
	}
	DefaultManager.mu.Unlock()
}

// GetPendingRequestCount returns the current pending request count
func GetPendingRequestCount() int64 {
	if DefaultManager == nil {
		return 0
	}
	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()
	return DefaultManager.pendingRequestCount
}

// SetQueueingState sets the queueing state
func SetQueueingState(isQueueing bool) {
	if DefaultManager == nil {
		return
	}
	DefaultManager.mu.Lock()
	DefaultManager.isQueueing = isQueueing
	DefaultManager.mu.Unlock()
}

// IsQueueing returns the current queueing state
func IsQueueing() bool {
	if DefaultManager == nil {
		return false
	}
	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()
	return DefaultManager.isQueueing
}

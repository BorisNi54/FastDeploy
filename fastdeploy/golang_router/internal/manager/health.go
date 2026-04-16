package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	scheduler_handler "github.com/PaddlePaddle/FastDeploy/router/internal/scheduler/handler"
	"github.com/PaddlePaddle/FastDeploy/router/pkg/logger"
	"github.com/gin-gonic/gin"
)

type healthCheckResult struct {
	url       string
	isHealthy bool
}

type healthMonitorResult struct {
	id        string
	worker    *WorkerInfo // node information
	isHealthy bool        // check error (nil means healthy)
}

func CheckServiceHealth(ctx context.Context, baseURL string, timeout ...time.Duration) bool {
	// Handle empty baseURL
	if baseURL == "" {
		logger.Error(ctx, "empty baseURL provided")
		return false
	}

	healthPath := healthEndpoint
	url := baseURL + healthPath
	timeoutToUse := defaultCheckTimeout // Default timeout

	// Override default value if caller provides valid timeout parameter
	if len(timeout) > 0 && timeout[0] > 0 {
		timeoutToUse = timeout[0]
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error(ctx, "failed to create request: %v", err)
		return false
	}

	// Send request
	client := &http.Client{Timeout: timeoutToUse}
	resp, err := client.Do(req)

	if err != nil {
		logger.Error(ctx, "failed to send request to %s with error: %v", url, err)
		return false
	}
	defer resp.Body.Close()

	// Read response body
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		logger.Error(ctx, "failed to read response body: %v", err)
		return false
	}

	// Check response status code
	if resp.StatusCode == http.StatusOK {
		return true
	}
	return false
}

func CheckWorkerHealth(ctx context.Context, baseURL string) bool {
	allServers := GetAllMapServers(ctx)
	_, exists := allServers[baseURL]
	if exists {
		for i := 0; i < failureThreshold; i++ {
			checkOk := CheckServiceHealth(ctx, baseURL)
			if checkOk {
				return true
			}
		}
		return false
	}
	for i := 0; i < successThreshold; i++ {
		checkOk := CheckServiceHealth(ctx, baseURL)
		if !checkOk {
			return false
		}
	}
	return true
}

// Get all URLs of Prefill and Decode servers
func GetAllServerURLs(ctx context.Context) []string {
	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()

	totalSeversLength := len(DefaultManager.prefillWorkerMap) + len(DefaultManager.decodeWorkerMap)
	allServerURLs := make([]string, 0, totalSeversLength)

	for _, server := range DefaultManager.prefillWorkerMap {
		allServerURLs = append(allServerURLs, server.Url)
	}
	for _, server := range DefaultManager.decodeWorkerMap {
		allServerURLs = append(allServerURLs, server.Url)
	}
	return allServerURLs
}

func HealthGenerate(c *gin.Context) {
	// The buffer size of this channel equals the total number of tasks, avoids goroutine blocking
	results := make(chan healthCheckResult, len(DefaultManager.prefillWorkerMap)+len(DefaultManager.decodeWorkerMap))
	// Use WaitGroup to wait for all goroutines to complete sending results
	var wg sync.WaitGroup

	allServerURLs := GetAllServerURLs(c.Request.Context())

	for _, s := range allServerURLs {
		wg.Add(1)
		go func(serverURL string) {
			defer wg.Done()
			baseURL := serverURL
			isHealthy := CheckWorkerHealth(c.Request.Context(), baseURL)
			results <- healthCheckResult{
				url:       serverURL,
				isHealthy: isHealthy,
			}
		}(s)
	}

	// Start a goroutine to close the result channel after all check tasks complete
	// Used to notify the range loop can end
	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		// Process each result
		if !res.isHealthy {
			logger.Warn(c.Request.Context(), "Server %s is not healthy", res.url)
		} else {
			logger.Info(c.Request.Context(), "Server %s is healthy", res.url)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"msg":  "Health check complete",
	})
}

func RemoveServers(ctx context.Context, prefillToRemove []string, decodeToRemove []string, mixedToRemove []string) {
	DefaultManager.mu.Lock()
	defer DefaultManager.mu.Unlock()

	for _, id := range prefillToRemove {
		if worker, exists := DefaultManager.prefillWorkerMap[id]; exists {
			delete(DefaultManager.prefillWorkerMap, id)
			logger.Error(ctx, "Removed unhealthy prefill instance: %s", worker.Url)
		}
	}
	for _, id := range decodeToRemove {
		if worker, exists := DefaultManager.decodeWorkerMap[id]; exists {
			delete(DefaultManager.decodeWorkerMap, id)
			logger.Error(ctx, "Removed unhealthy decode instance: %s", worker.Url)
		}
	}
	for _, id := range mixedToRemove {
		if worker, exists := DefaultManager.mixedWorkerMap[id]; exists {
			delete(DefaultManager.mixedWorkerMap, id)
			logger.Error(ctx, "Removed unhealthy mixed instance: %s", worker.Url)
		}
	}
}

func ReadServers(ctx context.Context) (prefillInstances, decodeInstances, mixedInstances []string) {
	if DefaultManager == nil {
		logger.Debug(ctx, "Healthy instances: prefill=[], decode=[], mixed=[] (DefaultManager is nil)")
		return []string{}, []string{}, []string{}
	}

	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()

	// Pre-allocate sufficient capacity to avoid multiple expansions
	prefillInstances = make([]string, 0, len(DefaultManager.prefillWorkerMap))
	decodeInstances = make([]string, 0, len(DefaultManager.decodeWorkerMap))
	mixedInstances = make([]string, 0, len(DefaultManager.mixedWorkerMap))

	// Copy data to avoid holding lock for long time
	for _, w := range DefaultManager.prefillWorkerMap {
		prefillInstances = append(prefillInstances, w.Url)
	}
	for _, w := range DefaultManager.decodeWorkerMap {
		decodeInstances = append(decodeInstances, w.Url)
	}
	for _, w := range DefaultManager.mixedWorkerMap {
		mixedInstances = append(mixedInstances, w.Url)
	}
	logger.Debug(ctx,
		"Healthy instances: prefill=%v, decode=%v, mixed=%v",
		prefillInstances,
		decodeInstances,
		mixedInstances,
	)
	return prefillInstances, decodeInstances, mixedInstances
}

func MonitorInstanceHealthCore(ctx context.Context) {
	if DefaultManager == nil {
		return
	}
	// Concurrently check health status of all nodes (fix concurrent security issues)
	allServers := GetAllMapServers(ctx)
	length := len(allServers)

	resultCh := make(chan healthMonitorResult, length)
	var wg sync.WaitGroup

	for id, server := range allServers {
		wg.Add(1)
		go func(id string, server *WorkerInfo) {
			defer wg.Done()
			// Execute health check logic
			baseURL := server.Url
			isHealthy := CheckWorkerHealth(ctx, baseURL)
			resultCh <- healthMonitorResult{
				id:        id,
				worker:    server,
				isHealthy: isHealthy,
			}
		}(id, server)
	}

	// Wait for all checks to complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var prefillToRemove, decodeToRemove, mixedToRemove []string

	for res := range resultCh {
		if !res.isHealthy {
			// logger.Warn("Server %s meets error: %v", res.worker.url, res.err)
			switch res.worker.WorkerType {
			case "prefill":
				prefillToRemove = append(prefillToRemove, res.id)
			case "decode":
				decodeToRemove = append(decodeToRemove, res.id)
			case "mixed":
				mixedToRemove = append(mixedToRemove, res.id)
			}
			go scheduler_handler.CleanupUnhealthyCounter(ctx, res.id)
		}
	}

	// Remove unhealthy instances
	RemoveServers(ctx, prefillToRemove, decodeToRemove, mixedToRemove)

	ReadServers(ctx)
}

func MonitorInstanceHealth(ctx context.Context, intervalSecs float64) {
	ticker := time.NewTicker(time.Duration(intervalSecs * float64(time.Second)))
	defer ticker.Stop()

	// Infinite loop: continuously execute health checks
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			go MonitorInstanceHealthCore(ctx)
		}
	}
}

// CheckInferenceReady checks inference availability for all instances/PD pairs
// In centralized mode: checks each mixed instance
// In PD splitwise mode: checks all prefill-decode pairs
// intervalSecs controls the wait time between check iterations
func CheckInferenceReady(ctx context.Context, intervalSecs float64) {
	if DefaultManager == nil {
		logger.Error(ctx, "CheckInferenceReady: DefaultManager is nil")
		return
	}

	isSplitwise := GetSplitwise(ctx)
	logger.Info(ctx, "CheckInferenceReady started, splitwise=%v, interval=%.2fs", isSplitwise, intervalSecs)

	// Track ready status across iterations
	readyMixedInstances := make(map[string]bool)
	readyPDPairs := make(map[pdPair]bool)

	ticker := time.NewTicker(time.Duration(intervalSecs * float64(time.Second)))
	defer ticker.Stop()

	for {
		var allReady bool
		if isSplitwise {
			allReady = checkPDPairsInference(ctx, readyPDPairs)
		} else {
			allReady = checkMixedInstancesInference(ctx, readyMixedInstances)
		}

		if allReady {
			setInferReady(true)
			logger.Info(ctx, "CheckInferenceReady: all instances/pairs ready, setting inferReady=true")
			return
		}

		// Wait for next check interval or context cancellation
		select {
		case <-ctx.Done():
			logger.Info(ctx, "CheckInferenceReady: context cancelled")
			return
		case <-ticker.C:
		}
	}
}

// checkMixedInstancesInference checks inference for all mixed instances
func checkMixedInstancesInference(ctx context.Context, readyInstances map[string]bool) bool {
	mixedInstances := WorkerMapToList(ctx, "mixed")
	if len(mixedInstances) == 0 {
		logger.Debug(ctx, "checkMixedInstancesInference: no mixed instances available")
		return false
	}

	for _, url := range mixedInstances {
		if readyInstances[url] {
			continue
		}
		if checkSingleInstanceInference(ctx, url) {
			readyInstances[url] = true
			logger.Info(ctx, "checkMixedInstancesInference: instance %s is ready", url)
		} else {
			logger.Debug(ctx, "checkMixedInstancesInference: instance %s not ready yet", url)
		}
	}

	// Check if all instances are ready
	for _, url := range mixedInstances {
		if !readyInstances[url] {
			return false
		}
	}
	return len(mixedInstances) > 0
}

// pdPair represents a prefill-decode instance pair
type pdPair struct {
	prefill string
	decode  string
}

// checkPDPairsInference checks inference for all PD pairs
func checkPDPairsInference(ctx context.Context, readyPairs map[pdPair]bool) bool {
	prefillInstances := WorkerMapToList(ctx, "prefill")
	decodeInstances := WorkerMapToList(ctx, "decode")

	if len(prefillInstances) == 0 || len(decodeInstances) == 0 {
		logger.Debug(ctx, "checkPDPairsInference: no prefill or decode instances available")
		return false
	}

	// Check all PD combinations
	for _, prefillURL := range prefillInstances {
		for _, decodeURL := range decodeInstances {
			pair := pdPair{prefill: prefillURL, decode: decodeURL}
			if readyPairs[pair] {
				continue
			}
			if checkPDPairInference(ctx, prefillURL, decodeURL) {
				readyPairs[pair] = true
				logger.Info(ctx, "checkPDPairsInference: PD pair (%s, %s) is ready", prefillURL, decodeURL)
			} else {
				logger.Debug(ctx, "checkPDPairsInference: PD pair (%s, %s) not ready yet", prefillURL, decodeURL)
			}
		}
	}

	// Check if all pairs are ready
	totalPairs := len(prefillInstances) * len(decodeInstances)
	return len(readyPairs) == totalPairs && totalPairs > 0
}

// checkSingleInstanceInference sends a simple inference request to a single instance
func checkSingleInstanceInference(ctx context.Context, instanceURL string) bool {
	return sendInferenceRequest(ctx, instanceURL)
}

// checkPDPairInference sends inference request to a PD pair
func checkPDPairInference(ctx context.Context, prefillURL, decodeURL string) bool {
	// Build disaggregate_info for PD pair
	disagg, err := BuildDisaggregateInfo(ctx, prefillURL, decodeURL)
	if err != nil {
		logger.Error(ctx, "checkPDPairInference: failed to build disaggregate_info: %v", err)
		return false
	}

	return sendPDInferenceRequest(ctx, prefillURL, decodeURL, disagg)
}

// sendInferenceRequest sends a simple inference request to check if the instance can complete inference
func sendInferenceRequest(ctx context.Context, instanceURL string) bool {
	reqBody := map[string]any{
		"model": "default",
		"messages": []map[string]string{
			{"role": "user", "content": "hi"},
		},
		"max_tokens": 1,
		"stream":     false,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		logger.Error(ctx, "sendInferenceRequest: failed to marshal request: %v", err)
		return false
	}

	endpoint := instanceURL + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		logger.Error(ctx, "sendInferenceRequest: failed to create request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error(ctx, "sendInferenceRequest: request to %s failed: %v", instanceURL, err)
		return false
	}
	defer resp.Body.Close()

	// Read and discard response body
	_, _ = io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK {
		return true
	}
	logger.Debug(ctx, "sendInferenceRequest: instance %s returned status %d", instanceURL, resp.StatusCode)
	return false
}

// sendPDInferenceRequest sends inference request to PD pair
func sendPDInferenceRequest(ctx context.Context, prefillURL, decodeURL string, disagg map[string]any) bool {
	reqBody := map[string]any{
		"model": "default",
		"messages": []map[string]string{
			{"role": "user", "content": "hi"},
		},
		"max_tokens":        1,
		"stream":            false,
		"disaggregate_info": disagg,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		logger.Error(ctx, "sendPDInferenceRequest: failed to marshal request: %v", err)
		return false
	}

	// Send requests to both prefill and decode concurrently
	type respResult struct {
		url     string
		success bool
		err     error
	}

	prefillCh := make(chan respResult, 1)
	decodeCh := make(chan respResult, 1)

	client := &http.Client{Timeout: 60 * time.Second}

	// Send to prefill
	go func() {
		endpoint := prefillURL + "/v1/chat/completions"
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			prefillCh <- respResult{url: prefillURL, success: false, err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			prefillCh <- respResult{url: prefillURL, success: false, err: err}
			return
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		prefillCh <- respResult{url: prefillURL, success: resp.StatusCode == http.StatusOK, err: nil}
	}()

	// Send to decode
	go func() {
		endpoint := decodeURL + "/v1/chat/completions"
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			decodeCh <- respResult{url: decodeURL, success: false, err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			decodeCh <- respResult{url: decodeURL, success: false, err: err}
			return
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		decodeCh <- respResult{url: decodeURL, success: resp.StatusCode == http.StatusOK, err: nil}
	}()

	prefillRes := <-prefillCh
	decodeRes := <-decodeCh

	if prefillRes.err != nil {
		logger.Error(ctx, "sendPDInferenceRequest: prefill request to %s failed: %v", prefillURL, prefillRes.err)
	}
	if decodeRes.err != nil {
		logger.Error(ctx, "sendPDInferenceRequest: decode request to %s failed: %v", decodeURL, decodeRes.err)
	}

	// Both must succeed for the PD pair to be considered ready
	return prefillRes.success && decodeRes.success
}

// setInferReady sets the inferReady status
func setInferReady(ready bool) {
	if DefaultManager == nil {
		return
	}
	DefaultManager.mu.Lock()
	defer DefaultManager.mu.Unlock()
	DefaultManager.inferReady = ready
}

// GetInferReady returns the current inferReady status
func GetInferReady(ctx context.Context) bool {
	if DefaultManager == nil {
		return false
	}
	DefaultManager.mu.RLock()
	defer DefaultManager.mu.RUnlock()
	return DefaultManager.inferReady
}

// InferReady is the HTTP handler for /ready endpoint
func InferReady(c *gin.Context) {
	ready := GetInferReady(c.Request.Context())
	c.JSON(http.StatusOK, gin.H{
		"code":        200,
		"infer_ready": ready,
	})
}

// Health is the HTTP handler for /health endpoint
// Used for external service availability probing
func Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"code":   200,
		"status": "ok",
	})
}

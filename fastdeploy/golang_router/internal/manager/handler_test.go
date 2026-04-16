package manager

import (
	"context"
	"testing"

	"github.com/PaddlePaddle/FastDeploy/router/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Splitwise: true,
		},
		Manager: config.ManagerConfig{
			HealthCheckTimeoutSecs: 5.0,
			HealthCheckEndpoint:    "/health",
			HealthFailureThreshold: 3,
			HealthSuccessThreshold: 2,
		},
	}

	Init(cfg)

	assert.NotNil(t, DefaultManager)
	assert.True(t, DefaultManager.splitwise)
	assert.Equal(t, "/health", healthEndpoint)
	assert.Equal(t, 3, failureThreshold)
	assert.Equal(t, 2, successThreshold)
}

func TestWorkerMapToList(t *testing.T) {
	// Setup test data
	Init(&config.Config{})
	DefaultManager.prefillWorkerMap = map[string]*WorkerInfo{
		"http://worker1": {Url: "http://worker1"},
		"http://worker2": {Url: "http://worker2"},
	}
	DefaultManager.decodeWorkerMap = map[string]*WorkerInfo{
		"http://worker3": {Url: "http://worker3"},
	}
	DefaultManager.mixedWorkerMap = map[string]*WorkerInfo{
		"http://worker4": {Url: "http://worker4"},
	}

	t.Run("prefill workers", func(t *testing.T) {
		workers := WorkerMapToList(context.Background(), "prefill")
		assert.Len(t, workers, 2)
		assert.Contains(t, workers, "http://worker1")
		assert.Contains(t, workers, "http://worker2")
	})

	t.Run("decode workers", func(t *testing.T) {
		workers := WorkerMapToList(context.Background(), "decode")
		assert.Len(t, workers, 1)
		assert.Contains(t, workers, "http://worker3")
	})

	t.Run("mixed workers", func(t *testing.T) {
		workers := WorkerMapToList(context.Background(), "mixed")
		assert.Len(t, workers, 1)
		assert.Contains(t, workers, "http://worker4")
	})

	t.Run("invalid worker type", func(t *testing.T) {
		workers := WorkerMapToList(context.Background(), "invalid")
		assert.Len(t, workers, 0)
	})
}

func TestManager_GetHealthyURLs(t *testing.T) {
	// Setup test data
	Init(&config.Config{})
	DefaultManager.prefillWorkerMap = map[string]*WorkerInfo{
		"worker1": {Url: "http://worker1"},
	}
	DefaultManager.decodeWorkerMap = map[string]*WorkerInfo{
		"worker2": {Url: "http://worker2"},
	}
	DefaultManager.mixedWorkerMap = map[string]*WorkerInfo{
		"worker3": {Url: "http://worker3"},
	}

	urls := DefaultManager.GetHealthyURLs(context.Background())
	assert.Len(t, urls, 3)
	assert.Contains(t, urls, "worker1")
	assert.Contains(t, urls, "worker2")
	assert.Contains(t, urls, "worker3")
}

func TestSelectWorker(t *testing.T) {
	// Setup test data
	Init(&config.Config{})
	DefaultManager.mixedWorkerMap = map[string]*WorkerInfo{
		"http://worker1": {Url: "http://worker1"},
	}

	// This will fail because SelectWorker depends on scheduler
	// which we don't want to mock in this unit test
	t.Skip("Integration test requiring scheduler setup")
}

func TestSelectWorkerPair(t *testing.T) {
	// Setup test data
	Init(&config.Config{})
	DefaultManager.prefillWorkerMap = map[string]*WorkerInfo{
		"http://worker1": {Url: "http://worker1"},
	}
	DefaultManager.decodeWorkerMap = map[string]*WorkerInfo{
		"http://worker2": {Url: "http://worker2"},
	}

	// This will fail because SelectWorkerPair depends on scheduler
	// which we don't want to mock in this unit test
	t.Skip("Integration test requiring scheduler setup")
}

func TestPendingRequestCounter(t *testing.T) {
	// Setup test data
	Init(&config.Config{})

	t.Run("initial count is zero", func(t *testing.T) {
		count := GetPendingRequestCount()
		assert.Equal(t, int64(0), count)
	})

	t.Run("increment increases count", func(t *testing.T) {
		Init(&config.Config{}) // Reset
		IncrementPendingRequest()
		assert.Equal(t, int64(1), GetPendingRequestCount())

		IncrementPendingRequest()
		assert.Equal(t, int64(2), GetPendingRequestCount())
	})

	t.Run("decrement decreases count", func(t *testing.T) {
		Init(&config.Config{}) // Reset
		IncrementPendingRequest()
		IncrementPendingRequest()
		assert.Equal(t, int64(2), GetPendingRequestCount())

		DecrementPendingRequest()
		assert.Equal(t, int64(1), GetPendingRequestCount())

		DecrementPendingRequest()
		assert.Equal(t, int64(0), GetPendingRequestCount())
	})

	t.Run("decrement does not go below zero", func(t *testing.T) {
		Init(&config.Config{}) // Reset
		assert.Equal(t, int64(0), GetPendingRequestCount())

		DecrementPendingRequest()
		assert.Equal(t, int64(0), GetPendingRequestCount())

		DecrementPendingRequest()
		assert.Equal(t, int64(0), GetPendingRequestCount())
	})

	t.Run("handles nil DefaultManager", func(t *testing.T) {
		oldManager := DefaultManager
		DefaultManager = nil

		// Should not panic
		IncrementPendingRequest()
		DecrementPendingRequest()
		count := GetPendingRequestCount()
		assert.Equal(t, int64(0), count)

		DefaultManager = oldManager
	})
}

func TestQueueingState(t *testing.T) {
	// Setup test data
	Init(&config.Config{})

	t.Run("initial state is false", func(t *testing.T) {
		assert.False(t, IsQueueing())
	})

	t.Run("set queueing state to true", func(t *testing.T) {
		SetQueueingState(true)
		assert.True(t, IsQueueing())
	})

	t.Run("set queueing state to false", func(t *testing.T) {
		SetQueueingState(true)
		assert.True(t, IsQueueing())

		SetQueueingState(false)
		assert.False(t, IsQueueing())
	})

	t.Run("handles nil DefaultManager", func(t *testing.T) {
		oldManager := DefaultManager
		DefaultManager = nil

		// Should not panic
		SetQueueingState(true)
		isQueueing := IsQueueing()
		assert.False(t, isQueueing)

		DefaultManager = oldManager
	})
}

func TestManagerInitNewFields(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Splitwise: false,
		},
		Manager: config.ManagerConfig{
			HealthCheckTimeoutSecs: 5.0,
			HealthCheckEndpoint:    "/health",
			HealthFailureThreshold: 3,
			HealthSuccessThreshold: 2,
		},
	}

	Init(cfg)

	assert.NotNil(t, DefaultManager)
	assert.False(t, DefaultManager.inferReady)
	assert.NotNil(t, DefaultManager.workerMetrics)
	assert.False(t, DefaultManager.isQueueing)
	assert.Equal(t, int64(0), DefaultManager.pendingRequestCount)
}

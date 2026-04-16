package handler

import (
	"context"
	"testing"

	"github.com/PaddlePaddle/FastDeploy/router/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestProcessTokensSelectWorker(t *testing.T) {
	ctx := context.Background()

	// Setup test data
	workers := []string{"worker1", "worker2", "worker3"}

	// Initialize scheduler and token counters
	Init(&config.Config{
		Scheduler: config.SchedulerConfig{
			Policy:               "process_tokens",
			EvictionDurationMins: 30,
		},
	}, nil)

	t.Run("select worker with least tokens", func(t *testing.T) {
		// Set up token counts
		tc1 := GetOrCreateTokenCounter(ctx, "worker1")
		tc1.Add(100)
		tc2 := GetOrCreateTokenCounter(ctx, "worker2")
		tc2.Add(50) // Should be selected
		tc3 := GetOrCreateTokenCounter(ctx, "worker3")
		tc3.Add(200)

		selected, err := ProcessTokensSelectWorker(ctx, workers, "test message")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected)
	})

	t.Run("empty workers list", func(t *testing.T) {
		selected, err := ProcessTokensSelectWorker(ctx, []string{}, "test")
		assert.NoError(t, err)
		assert.Equal(t, "", selected)
	})
}

func TestRequestNumSelectWorker(t *testing.T) {
	ctx := context.Background()
	workers := []string{"worker1", "worker2", "worker3"}

	Init(&config.Config{
		Scheduler: config.SchedulerConfig{
			Policy:               "request_num",
			EvictionDurationMins: 30,
		},
	}, nil)

	t.Run("select worker with least requests", func(t *testing.T) {
		// Set up request counts
		c1 := GetOrCreateCounter(ctx, "worker1")
		c1.Inc()
		c1.Inc()                                 // count = 2
		c2 := GetOrCreateCounter(ctx, "worker2") // count = 0 (should be selected)
		c3 := GetOrCreateCounter(ctx, "worker3")
		c3.Inc() // count = 1

		// Verify counts (use variables to avoid "declared and not used" error)
		assert.Equal(t, uint64(2), c1.Get())
		assert.Equal(t, uint64(0), c2.Get())
		assert.Equal(t, uint64(1), c3.Get())

		selected, err := RequestNumSelectWorker(ctx, workers, "test")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected)
	})

	t.Run("empty workers list", func(t *testing.T) {
		selected, err := RequestNumSelectWorker(ctx, []string{}, "test")
		assert.NoError(t, err)
		assert.Equal(t, "", selected)
	})
}

// mockManagerAPIWithMetrics implements ManagerAPI with customizable metrics
type mockManagerAPIWithMetrics struct {
	metricsMap map[string]struct {
		running  int
		waiting  int
		gpuBlock int
	}
}

func (m *mockManagerAPIWithMetrics) GetHealthyURLs(ctx context.Context) []string {
	urls := make([]string, 0, len(m.metricsMap))
	for url := range m.metricsMap {
		urls = append(urls, url)
	}
	return urls
}

func (m *mockManagerAPIWithMetrics) GetMetrics(ctx context.Context, url string) (int, int, int) {
	if metrics, ok := m.metricsMap[url]; ok {
		return metrics.running, metrics.waiting, metrics.gpuBlock
	}
	return 0, 0, 0
}

func (m *mockManagerAPIWithMetrics) GetRemoteMetrics(ctx context.Context, url string) (int, int, int) {
	if metrics, ok := m.metricsMap[url]; ok {
		return metrics.running, metrics.waiting, metrics.gpuBlock
	}
	return 0, 0, 0
}

func TestFDRemoteMetricsScoreSelectWorker(t *testing.T) {
	ctx := context.Background()

	mockAPI := &mockManagerAPIWithMetrics{
		metricsMap: map[string]struct {
			running  int
			waiting  int
			gpuBlock int
		}{
			"worker1": {running: 10, waiting: 5, gpuBlock: 100},
			"worker2": {running: 2, waiting: 1, gpuBlock: 200}, // lowest score
			"worker3": {running: 15, waiting: 10, gpuBlock: 50},
		},
	}

	Init(&config.Config{
		Scheduler: config.SchedulerConfig{
			Policy:               "fd_remote_metrics_score",
			WaitingWeight:        1.0,
			EvictionDurationMins: 30,
		},
	}, mockAPI)

	t.Run("select worker with lowest score", func(t *testing.T) {
		workers := []string{"worker1", "worker2", "worker3"}
		selected, err := FDRemoteMetricsScoreSelectWorker(ctx, workers, "test message")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected)
	})

	t.Run("empty workers list", func(t *testing.T) {
		selected, err := FDRemoteMetricsScoreSelectWorker(ctx, []string{}, "test")
		assert.NoError(t, err)
		assert.Equal(t, "", selected)
	})
}

func TestFDRemoteMetricsMaxGpuBlockSelectWorker(t *testing.T) {
	ctx := context.Background()

	t.Run("select worker with max gpu blocks when not queuing", func(t *testing.T) {
		mockAPI := &mockManagerAPIWithMetrics{
			metricsMap: map[string]struct {
				running  int
				waiting  int
				gpuBlock int
			}{
				"worker1": {running: 5, waiting: 0, gpuBlock: 100},
				"worker2": {running: 3, waiting: 0, gpuBlock: 200}, // max gpu blocks with waiting == 0
				"worker3": {running: 8, waiting: 2, gpuBlock: 300}, // has more gpu blocks but is queuing
			},
		}

		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:               "fd_remote_metrics_max_gpu_block",
				EvictionDurationMins: 30,
			},
		}, mockAPI)

		workers := []string{"worker1", "worker2", "worker3"}
		selected, err := FDRemoteMetricsMaxGpuBlockSelectWorker(ctx, workers, "test message")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected)
	})

	t.Run("returns error when all workers are queuing", func(t *testing.T) {
		mockAPI := &mockManagerAPIWithMetrics{
			metricsMap: map[string]struct {
				running  int
				waiting  int
				gpuBlock int
			}{
				"worker1": {running: 5, waiting: 1, gpuBlock: 100},
				"worker2": {running: 3, waiting: 2, gpuBlock: 200},
			},
		}

		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:               "fd_remote_metrics_max_gpu_block",
				EvictionDurationMins: 30,
			},
		}, mockAPI)

		workers := []string{"worker1", "worker2"}
		selected, err := FDRemoteMetricsMaxGpuBlockSelectWorker(ctx, workers, "test")
		assert.Error(t, err)
		assert.Equal(t, ErrAllWorkersAtCapacity, err)
		assert.Equal(t, "", selected)
	})

	t.Run("returns error when no workers have available gpu blocks", func(t *testing.T) {
		mockAPI := &mockManagerAPIWithMetrics{
			metricsMap: map[string]struct {
				running  int
				waiting  int
				gpuBlock int
			}{
				"worker1": {running: 5, waiting: 0, gpuBlock: 0},
				"worker2": {running: 3, waiting: 0, gpuBlock: 0},
			},
		}

		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:               "fd_remote_metrics_max_gpu_block",
				EvictionDurationMins: 30,
			},
		}, mockAPI)

		workers := []string{"worker1", "worker2"}
		selected, err := FDRemoteMetricsMaxGpuBlockSelectWorker(ctx, workers, "test")
		assert.Error(t, err)
		assert.Equal(t, ErrAllWorkersAtCapacity, err)
		assert.Equal(t, "", selected)
	})

	t.Run("empty workers list returns error", func(t *testing.T) {
		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:               "fd_remote_metrics_max_gpu_block",
				EvictionDurationMins: 30,
			},
		}, &mockManagerAPIWithMetrics{})

		selected, err := FDRemoteMetricsMaxGpuBlockSelectWorker(ctx, []string{}, "test")
		assert.NoError(t, err) // empty list returns nil error per implementation
		assert.Equal(t, "", selected)
	})
}

package manager

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/PaddlePaddle/FastDeploy/router/internal/config"
	"github.com/PaddlePaddle/FastDeploy/router/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func init() {
	// Initialize logger for all tests
	logger.Init("info", "stdout")
}

func TestCheckServiceHealth(t *testing.T) {
	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	t.Run("healthy service", func(t *testing.T) {
		healthy := CheckServiceHealth(context.Background(), ts.URL)
		assert.True(t, healthy)
	})

	t.Run("unhealthy service", func(t *testing.T) {
		unhealthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer unhealthyServer.Close()

		healthy := CheckServiceHealth(context.Background(), unhealthyServer.URL)
		assert.False(t, healthy)
	})

	t.Run("empty baseURL", func(t *testing.T) {
		healthy := CheckServiceHealth(context.Background(), "")
		assert.False(t, healthy)
	})

	t.Run("timeout", func(t *testing.T) {
		slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer slowServer.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		healthy := CheckServiceHealth(ctx, slowServer.URL)
		assert.False(t, healthy)
	})
}

func TestCheckWorkerHealth(t *testing.T) {
	// Setup test data
	Init(&config.Config{
		Manager: config.ManagerConfig{
			HealthFailureThreshold: 1,
			HealthSuccessThreshold: 1,
		},
	})

	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	t.Run("healthy worker", func(t *testing.T) {
		healthy := CheckWorkerHealth(context.Background(), ts.URL)
		assert.True(t, healthy)
	})

	t.Run("unhealthy worker", func(t *testing.T) {
		unhealthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer unhealthyServer.Close()

		healthy := CheckWorkerHealth(context.Background(), unhealthyServer.URL)
		assert.False(t, healthy)
	})
}

func TestHealthGenerate(t *testing.T) {
	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Setup test data
	Init(&config.Config{})
	DefaultManager.prefillWorkerMap = map[string]*WorkerInfo{
		"worker1": {Url: ts.URL},
	}
	DefaultManager.decodeWorkerMap = map[string]*WorkerInfo{
		"worker2": {Url: ts.URL},
	}

	// Test Gin handler
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	// Set up a valid HTTP request for the context
	c.Request = httptest.NewRequest("GET", "/health", nil)
	HealthGenerate(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Health check complete")
}

func TestMonitorInstanceHealthCore(t *testing.T) {
	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Setup test data
	Init(&config.Config{})
	DefaultManager.prefillWorkerMap = map[string]*WorkerInfo{
		"worker1": {Url: ts.URL, WorkerType: "prefill"},
	}
	DefaultManager.decodeWorkerMap = map[string]*WorkerInfo{
		"worker2": {Url: ts.URL, WorkerType: "decode"},
	}

	MonitorInstanceHealthCore(context.Background())

	// Verify workers still exist (since they're healthy)
	_, exists := DefaultManager.prefillWorkerMap["worker1"]
	assert.True(t, exists)
	_, exists = DefaultManager.decodeWorkerMap["worker2"]
	assert.True(t, exists)
}

func TestReadServers(t *testing.T) {
	// Setup test data
	Init(&config.Config{})
	DefaultManager.prefillWorkerMap = map[string]*WorkerInfo{
		"worker1": {Url: "http://worker1"},
	}
	DefaultManager.decodeWorkerMap = map[string]*WorkerInfo{
		"worker2": {Url: "http://worker2"},
	}

	prefill, decode, mixed := ReadServers(context.Background())
	assert.Equal(t, []string{"http://worker1"}, prefill)
	assert.Equal(t, []string{"http://worker2"}, decode)
	assert.Equal(t, []string{}, mixed)
}

func TestGetInferReady(t *testing.T) {
	t.Run("initial state is false", func(t *testing.T) {
		Init(&config.Config{})
		ready := GetInferReady(context.Background())
		assert.False(t, ready)
	})

	t.Run("returns true after setting", func(t *testing.T) {
		Init(&config.Config{})
		setInferReady(true)
		ready := GetInferReady(context.Background())
		assert.True(t, ready)
	})

	t.Run("handles nil DefaultManager", func(t *testing.T) {
		oldManager := DefaultManager
		DefaultManager = nil

		ready := GetInferReady(context.Background())
		assert.False(t, ready)

		DefaultManager = oldManager
	})
}

func TestSetInferReady(t *testing.T) {
	t.Run("sets to true", func(t *testing.T) {
		Init(&config.Config{})
		setInferReady(true)
		assert.True(t, DefaultManager.inferReady)
	})

	t.Run("sets to false", func(t *testing.T) {
		Init(&config.Config{})
		setInferReady(true)
		setInferReady(false)
		assert.False(t, DefaultManager.inferReady)
	})

	t.Run("handles nil DefaultManager", func(t *testing.T) {
		oldManager := DefaultManager
		DefaultManager = nil

		// Should not panic
		setInferReady(true)

		DefaultManager = oldManager
	})
}

func TestInferReadyHandler(t *testing.T) {
	t.Run("returns infer_ready false", func(t *testing.T) {
		Init(&config.Config{})
		setInferReady(false)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/ready", nil)

		InferReady(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), `"infer_ready":false`)
	})

	t.Run("returns infer_ready true", func(t *testing.T) {
		Init(&config.Config{})
		setInferReady(true)

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/ready", nil)

		InferReady(c)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), `"infer_ready":true`)
	})
}

func TestHealthHandler(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)

	Health(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"status":"ok"`)
}

func TestSendInferenceRequest(t *testing.T) {
	t.Run("successful request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/v1/chat/completions", r.URL.Path)
			assert.Equal(t, "POST", r.Method)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"choices": [{"text": "response"}]}`))
		}))
		defer server.Close()

		result := sendInferenceRequest(context.Background(), server.URL)
		assert.True(t, result)
	})

	t.Run("failed request - non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		result := sendInferenceRequest(context.Background(), server.URL)
		assert.False(t, result)
	})

	t.Run("failed request - connection error", func(t *testing.T) {
		result := sendInferenceRequest(context.Background(), "http://invalid-host:9999")
		assert.False(t, result)
	})
}

func TestCheckMixedInstancesInference(t *testing.T) {
	t.Run("no instances available", func(t *testing.T) {
		Init(&config.Config{})
		readyInstances := make(map[string]bool)

		result := checkMixedInstancesInference(context.Background(), readyInstances)
		assert.False(t, result)
	})

	t.Run("all instances ready", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"choices": [{"text": "response"}]}`))
		}))
		defer server.Close()

		Init(&config.Config{})
		DefaultManager.mixedWorkerMap = map[string]*WorkerInfo{
			server.URL: {Url: server.URL},
		}

		readyInstances := make(map[string]bool)
		result := checkMixedInstancesInference(context.Background(), readyInstances)
		assert.True(t, result)
		assert.True(t, readyInstances[server.URL])
	})

	t.Run("some instances not ready", func(t *testing.T) {
		healthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer healthyServer.Close()

		unhealthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer unhealthyServer.Close()

		Init(&config.Config{})
		DefaultManager.mixedWorkerMap = map[string]*WorkerInfo{
			healthyServer.URL:   {Url: healthyServer.URL},
			unhealthyServer.URL: {Url: unhealthyServer.URL},
		}

		readyInstances := make(map[string]bool)
		result := checkMixedInstancesInference(context.Background(), readyInstances)
		assert.False(t, result)
	})
}

func TestCheckPDPairsInference(t *testing.T) {
	t.Run("no prefill instances", func(t *testing.T) {
		Init(&config.Config{})
		DefaultManager.decodeWorkerMap = map[string]*WorkerInfo{
			"http://decode1": {Url: "http://decode1"},
		}

		readyPairs := make(map[pdPair]bool)
		result := checkPDPairsInference(context.Background(), readyPairs)
		assert.False(t, result)
	})

	t.Run("no decode instances", func(t *testing.T) {
		Init(&config.Config{})
		DefaultManager.prefillWorkerMap = map[string]*WorkerInfo{
			"http://prefill1": {Url: "http://prefill1"},
		}

		readyPairs := make(map[pdPair]bool)
		result := checkPDPairsInference(context.Background(), readyPairs)
		assert.False(t, result)
	})
}

package handler

import (
	"context"
	"math"
)

func FDRemoteMetricsScoreSelectWorker(ctx context.Context, workers []string, message string) (string, error) {
	if len(workers) == 0 {
		return "", nil
	}

	var (
		selectedURL string  = ""
		minScore    float64 = math.MaxFloat64
	)

	for _, w := range workers {
		runningCnt, waitingCnt, _ := DefaultScheduler.managerAPI.GetRemoteMetrics(ctx, w)
		score := computeScore(ctx, runningCnt, waitingCnt)
		if score < minScore {
			minScore = score
			selectedURL = w
		}
	}
	return selectedURL, nil
}

// FDRemoteMetricsMaxGpuBlockSelectWorker selects the worker with maximum available GPU blocks
// among workers that are not queuing (waitingCnt == 0) and have available GPU blocks (gpuBlockNum > 0)
func FDRemoteMetricsMaxGpuBlockSelectWorker(ctx context.Context, workers []string, message string) (string, error) {
	if len(workers) == 0 {
		return "", nil
	}

	var (
		selectedURL    string = ""
		maxGpuBlockNum int    = 0
	)

	for _, w := range workers {
		_, waitingCnt, gpuBlockNum := DefaultScheduler.managerAPI.GetRemoteMetrics(ctx, w)
		if waitingCnt == 0 && gpuBlockNum > 0 && gpuBlockNum > maxGpuBlockNum {
			maxGpuBlockNum = gpuBlockNum
			selectedURL = w
		}
	}

	if selectedURL == "" {
		return "", ErrAllWorkersAtCapacity
	}
	return selectedURL, nil
}

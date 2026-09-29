package main

import (
	"math"
	"testing"
)

func TestSummarizeNearestRank(t *testing.T) {
	samples := make([]float64, 100)
	for i := range samples {
		samples[i] = float64(100 - i)
	}
	mean, p50, p95, p99 := summarize(samples)
	if mean != 50.5 || p50 != 50 || p95 != 95 || p99 != 99 {
		t.Fatalf("summary mean=%v p50=%v p95=%v p99=%v", mean, p50, p95, p99)
	}
	mean, p50, p95, p99 = summarize(nil)
	if mean != 0 || p50 != 0 || p95 != 0 || p99 != 0 {
		t.Fatal("empty sample must report zeros")
	}
	if math.IsNaN(mean) {
		t.Fatal("empty mean is NaN")
	}
}

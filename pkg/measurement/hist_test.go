package measurement

import (
	"math/rand"
	"testing"
	"time"
)

func TestHist(t *testing.T) {
	h := NewHistogram(1*time.Millisecond, 20*time.Minute, 1)
	for i := 0; i < 10000; i++ {
		n := rand.Intn(15020)
		h.Measure(time.Millisecond * time.Duration(n))
	}
	h.Measure(time.Minute * 9)
	h.Measure(time.Minute * 8)
	t.Logf("%+v", h.Summary())
}

func TestHistogram_UnfrozenElapsedKeepsAdvancing(t *testing.T) {
	h := NewHistogram(1*time.Millisecond, 20*time.Minute, 1)
	h.Measure(time.Millisecond)

	first := h.GetInfo().Elapsed
	time.Sleep(5 * time.Millisecond)
	second := h.GetInfo().Elapsed

	if second <= first {
		t.Fatalf("unfrozen Elapsed did not advance: first=%v second=%v", first, second)
	}
}

func TestHistogram_FreezeFixesElapsed(t *testing.T) {
	h := NewHistogram(1*time.Millisecond, 20*time.Minute, 1)
	h.Measure(time.Millisecond)

	time.Sleep(5 * time.Millisecond)
	h.Freeze(time.Now())

	first := h.GetInfo()
	time.Sleep(5 * time.Millisecond)
	second := h.GetInfo()

	if first.Elapsed != second.Elapsed {
		t.Fatalf("frozen Elapsed drifted: first=%v second=%v", first.Elapsed, second.Elapsed)
	}
	if first.Ops != second.Ops {
		t.Fatalf("frozen Ops drifted: first=%v second=%v", first.Ops, second.Ops)
	}
}

func TestHistogram_FreezeTwicePanics(t *testing.T) {
	h := NewHistogram(1*time.Millisecond, 20*time.Minute, 1)
	h.Freeze(time.Now())

	defer func() {
		if recover() == nil {
			t.Fatal("expected second Freeze to panic")
		}
	}()
	h.Freeze(time.Now())
}

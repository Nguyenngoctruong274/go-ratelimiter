package limiter

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLimitValidate(t *testing.T) {
	tests := []struct {
		name  string
		limit Limit
		want  string // "" means valid
	}{
		{"per second", PerSecond(10, 20), ""},
		{"per minute", PerMinute(600, 1), ""},
		{"one microsecond period", Limit{Rate: 1, Period: time.Microsecond, Burst: 1}, ""},
		{"largest burst for an hour", Limit{Rate: 1, Period: time.Hour, Burst: (MaxScaled - 1) / 3_600_000_000}, ""},
		{"zero rate", Limit{Rate: 0, Period: time.Second, Burst: 1}, "rate must be positive"},
		{"negative burst", Limit{Rate: 1, Period: time.Second, Burst: -1}, "burst must be positive"},
		{"zero period", Limit{Rate: 1, Burst: 1}, "period"},
		{"sub-microsecond period", Limit{Rate: 1, Period: 1500 * time.Nanosecond, Burst: 1}, "whole number of microseconds"},
		{"burst too large", Limit{Rate: 1, Period: time.Hour, Burst: (MaxScaled-1)/3_600_000_000 + 1}, "too large"},
		{"rate pushes past 2^53", Limit{Rate: MaxScaled, Period: time.Microsecond, Burst: 1}, "too large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.limit.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidLimit) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want ErrInvalidLimit containing %q", err, tt.want)
			}
		})
	}
}

func TestLimitString(t *testing.T) {
	if got := PerSecond(5, 10).String(); got != "5/1s burst 10" {
		t.Fatalf("String() = %q", got)
	}
}

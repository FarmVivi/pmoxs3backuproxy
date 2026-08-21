package main

import (
	"testing"
	"time"
)

func TestWithinGracePeriod(t *testing.T) {
	now := time.Date(2026, 8, 21, 6, 10, 0, 0, time.UTC)
	grace := 24 * time.Hour

	cases := []struct {
		name         string
		lastModified time.Time
		grace        time.Duration
		want         bool
	}{
		{"chunk of a backup running right now", now.Add(-2 * time.Minute), grace, true},
		{"chunk written during tonight's backup", now.Add(-4 * time.Hour), grace, true},
		{"chunk just outside the grace period", now.Add(-25 * time.Hour), grace, false},
		{"chunk from last week", now.Add(-7 * 24 * time.Hour), grace, false},
		{"grace disabled keeps nothing", now.Add(-time.Minute), 0, false},
		{"exactly at the cutoff is collectable", now.Add(-grace), grace, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := withinGracePeriod(tc.lastModified, now, tc.grace); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

package s3backuplog

import (
	"bytes"
	"strings"
	"testing"
)

func TestLogLevelsAndDebugGate(t *testing.T) {
	var out bytes.Buffer
	restore := SetOutput(&out)
	defer restore()
	debugEnabled.Store(false)

	DebugPrint("hidden value=%d", 1)
	InfoPrint("started session=%s", "abc")
	WarnPrint("slow duration=%s", "31s")
	ErrorPrint("failed err=%s", "boom")
	text := out.String()
	if strings.Contains(text, "hidden") {
		t.Fatal("debug log emitted while debug was disabled")
	}
	for _, want := range []string{
		"[INFO] started session=abc",
		"[WARNING] slow duration=31s",
		"[ERROR] failed err=boom",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("log %q missing from %q", want, text)
		}
	}
	if strings.Contains(text, "\x1b[") {
		t.Fatal("journal log contains ANSI color escapes")
	}

	out.Reset()
	EnableDebug()
	DebugPrint("visible key=%s", "value")
	if !strings.Contains(out.String(), "[DEBUG] visible key=value") {
		t.Fatalf("debug log missing: %q", out.String())
	}
	debugEnabled.Store(false)
}

package sink_test

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(b)
}

func backups(t *testing.T, path string) []string {
	t.Helper()
	matches, err := filepath.Glob(path + ".*")
	if err != nil {
		t.Fatalf("globbing backups: %v", err)
	}
	return matches
}

func TestRotatingWriterRotatesAtSize(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{MaxSize: 10})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	for _, chunk := range []string{"12345", "67890", "abcde", "fghij"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q) error = %v", chunk, err)
		}
	}

	if got := readFile(t, path); got != "abcdefghij" {
		t.Errorf("active file = %q, want the current generation", got)
	}
	found := backups(t, path)
	if len(found) != 1 {
		t.Fatalf("got %d backups, want 1", len(found))
	}
	if got := readFile(t, found[0]); got != "1234567890" {
		t.Errorf("first backup = %q, want the rotated generation", got)
	}
}

func TestRotatingWriterKeepsSingleOversizedWriteWhole(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{MaxSize: 4})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := w.Write([]byte("a-very-long-record")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := readFile(t, path); got != "a-very-long-record" {
		t.Errorf("active file = %q, want the oversized write intact", got)
	}
	if got := len(backups(t, path)); got != 0 {
		t.Errorf("got %d backups after the first write, want 0", got)
	}
}

func TestRotatingWriterRotatesOnAge(t *testing.T) {
	t.Parallel()

	now := time.Now()
	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		MaxAge: time.Hour,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := w.Write([]byte("before")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	now = now.Add(2 * time.Hour)
	if err := w.RotateIfNeeded(0); err != nil {
		t.Fatalf("RotateIfNeeded() error = %v", err)
	}
	if got := readFile(t, path); got != "" {
		t.Errorf("active file = %q after time rotation, want empty", got)
	}
	if got := len(backups(t, path)); got != 1 {
		t.Errorf("got %d backups, want 1", got)
	}

	if _, err := w.Write([]byte("after")); err != nil {
		t.Fatalf("Write() after rotation error = %v", err)
	}
	if got := readFile(t, path); got != "after" {
		t.Errorf("active file = %q, want the post-rotation write", got)
	}
}

func TestRotatingWriterCompresses(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		MaxSize:  5,
		Compress: true,
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := w.Write([]byte("second")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	var compressed string
	for _, backup := range backups(t, path) {
		if strings.HasSuffix(backup, ".gz") {
			compressed = backup
		}
	}
	if compressed == "" {
		t.Fatalf("no .gz backup found among %v", backups(t, path))
	}

	f, err := os.Open(compressed)
	if err != nil {
		t.Fatalf("open compressed backup: %v", err)
	}
	defer func() { _ = f.Close() }()

	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer func() { _ = zr.Close() }()

	content, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("reading gzip: %v", err)
	}
	if got := string(content); got != "first" {
		t.Errorf("compressed backup = %q, want %q", got, "first")
	}
}

func TestRotatingWriterKeepsMaxBackups(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "stream.ndjson")
	w, err := sink.NewRotatingWriter(path, sink.RotatorOptions{
		MaxSize:    1,
		MaxBackups: 2,
		Now: func() time.Time {
			// Distinct timestamps keep filenames unique even when rotation
			// happens faster than the clock's resolution.
			return time.Now().Add(time.Duration(len(backupsFor(path))) * time.Second)
		},
	})
	if err != nil {
		t.Fatalf("NewRotatingWriter() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	for _, chunk := range []string{"a", "b", "c", "d", "e"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q) error = %v", chunk, err)
		}
	}

	if got := len(backups(t, path)); got != 2 {
		t.Fatalf("got %d backups, want 2", got)
	}
}

func backupsFor(path string) []string {
	matches, _ := filepath.Glob(path + ".*")
	return matches
}

func TestRotatingWriterRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewRotatingWriter("", sink.RotatorOptions{}); err == nil {
		t.Error("empty path accepted")
	}
	if _, err := sink.NewRotatingWriter(filepath.Join(t.TempDir(), "x"), sink.RotatorOptions{MaxSize: -1}); err == nil {
		t.Error("negative MaxSize accepted")
	}
	if _, err := sink.NewRotatingWriter(filepath.Join(t.TempDir(), "x"), sink.RotatorOptions{MaxBackups: -1}); err == nil {
		t.Error("negative MaxBackups accepted")
	}
}

func TestFileRotatorWritesNDJSONAcrossRotations(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.ndjson")
	rotator, err := sink.NewFileRotator(path, func(w io.Writer) stream.Sink {
		return sink.NewNDJSON(w, false)
	}, sink.RotatorOptions{MaxSize: 1})
	if err != nil {
		t.Fatalf("NewFileRotator() error = %v", err)
	}

	for i := range 3 {
		rec := stream.Record{
			ID:       "0000000001-0001",
			Sequence: 1,
			Kind:     stream.KindEvent,
			Event:    &stream.Event{Type: "contract", ValueXDR: "AAAAAwAAAAc="},
		}
		if err := rotator.Emit(rec); err != nil {
			t.Fatalf("Emit(%d) error = %v", i, err)
		}
	}
	if err := rotator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := rotator.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want idempotence", err)
	}

	files := append([]string{path}, backups(t, path)...)
	lines := 0
	for _, file := range files {
		content := readFile(t, file)
		lines += len(strings.Split(strings.TrimSpace(content), "\n"))
		if strings.Contains(content, "\n\n") {
			t.Errorf("%s contains a blank line", file)
		}
	}
	if lines != 3 {
		t.Errorf("found %d records across %d files, want 3", lines, len(files))
	}
}

func TestFileRotatorRequiresBuilder(t *testing.T) {
	t.Parallel()

	if _, err := sink.NewFileRotator(filepath.Join(t.TempDir(), "x"), nil, sink.RotatorOptions{}); err == nil {
		t.Error("nil builder accepted")
	}
}

func TestFileRotatorAppendsCompressedRecords(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.ndjson")
	rotator, err := sink.NewFileRotator(path, func(w io.Writer) stream.Sink {
		return sink.NewNDJSON(w, false)
	}, sink.RotatorOptions{MaxSize: 10, Compress: true})
	if err != nil {
		t.Fatalf("NewFileRotator() error = %v", err)
	}
	defer func() { _ = rotator.Close() }()

	for range 3 {
		if err := rotator.Emit(stream.Record{
			ID:       "0000000001-0001",
			Sequence: 1,
			Kind:     stream.KindEvent,
			Event:    &stream.Event{Type: "contract", ValueXDR: "AAAAAwAAAAc="},
		}); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}
	if got := len(backups(t, path)); got == 0 {
		t.Fatal("expected at least one compressed backup")
	}
}

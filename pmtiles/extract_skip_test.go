package pmtiles

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipTestSource serves a clustered copy of test_fixture_1 over HTTP with an
// ETag, and counts the requests that read tile data.
type skipTestSource struct {
	url          string
	tileRequests atomic.Int64
}

func newSkipTestSource(t *testing.T) *skipTestSource {
	path := makeFixtureCopy(t, "test_fixture_1", "skip_source")
	require.NoError(t, Cluster(log.New(io.Discard, "", 0), path, true))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	header, err := DeserializeHeader(data[0:HeaderV3LenBytes])
	require.NoError(t, err)

	s := &skipTestSource{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if start, ok := rangeStart(r.Header.Get("Range")); ok && start >= header.TileDataOffset {
			s.tileRequests.Add(1)
		}
		w.Header().Set("ETag", `"v1"`)
		http.ServeContent(w, r, "source.pmtiles", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(server.Close)
	s.url = server.URL + "/source.pmtiles"
	return s
}

func rangeStart(h string) (uint64, bool) {
	h, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, false
	}
	start, _, _ := strings.Cut(h, "-")
	n, err := strconv.ParseUint(start, 10, 64)
	return n, err == nil
}

// extractForTest runs Extract of the whole source into output and returns the
// JSON progress lines.
func (s *skipTestSource) extract(t *testing.T, output string, dryRun bool) ([]string, error) {
	var buf safeBuffer
	err := Extract(context.Background(), log.New(io.Discard, "", 0), "", s.url, -1, -1, "", "", output, 2, 0.05, 0, dryRun, &buf)
	return strings.Split(strings.TrimSpace(buf.String()), "\n"), err
}

func lastLine(lines []string) string {
	return lines[len(lines)-1]
}

func TestExtractSkipsCompleteOutput(t *testing.T) {
	src := newSkipTestSource(t)
	output := t.TempDir() + "/out.pmtiles"

	// first run downloads the tile data
	lines, err := src.extract(t, output, false)
	require.NoError(t, err)
	assert.Equal(t, int64(1), src.tileRequests.Load())
	assert.NotContains(t, lastLine(lines), "already_complete")
	assert.NoFileExists(t, extractJournalPath(output))
	assert.NotContains(t, lines[0], "archive_bytes", "archive size is not known during the plan")
	assert.Contains(t, lastLine(lines), `"archive_bytes":`)
	before, err := os.ReadFile(output)
	require.NoError(t, err)

	// same command again: nothing is downloaded and the output stays the same
	lines, err = src.extract(t, output, false)
	require.NoError(t, err)
	assert.Equal(t, int64(1), src.tileRequests.Load(), "no tile data request for a complete output")
	after, err := os.ReadFile(output)
	require.NoError(t, err)
	assert.Equal(t, before, after)

	last := lastLine(lines)
	assert.Contains(t, last, `"phase":"done"`)
	assert.Contains(t, last, `"exit_code":0`)
	assert.Contains(t, last, `"already_complete":true`)
	assert.Contains(t, last, `"archive_bytes":`+strconv.Itoa(len(before)))
	assert.Contains(t, last, `"requests_total":0`)
	var total, done, resumed string
	for _, field := range strings.Split(strings.Trim(last, "{}"), ",") {
		name, value, _ := strings.Cut(field, ":")
		switch name {
		case `"bytes_total"`:
			total = value
		case `"bytes_done"`:
			done = value
		case `"resumed_bytes"`:
			resumed = value
		}
	}
	assert.NotEqual(t, "0", total)
	assert.Equal(t, total, done)
	assert.Equal(t, total, resumed)
}

func TestExtractDoesNotSkipIncompleteOutput(t *testing.T) {
	cases := []struct {
		name   string
		change func(t *testing.T, output string, data []byte)
	}{
		{"header not written yet", func(t *testing.T, output string, data []byte) {
			copy(data[0:HeaderV3LenBytes], make([]byte, HeaderV3LenBytes))
			require.NoError(t, os.WriteFile(output, data, 0644))
		}},
		{"header differs", func(t *testing.T, output string, data []byte) {
			data[HeaderV3LenBytes-1] ^= 0xff
			require.NoError(t, os.WriteFile(output, data, 0644))
		}},
		{"size differs", func(t *testing.T, output string, data []byte) {
			require.NoError(t, os.WriteFile(output, append(data, 0), 0644))
		}},
		{"journal of another extract", func(t *testing.T, output string, data []byte) {
			require.NoError(t, os.WriteFile(extractJournalPath(output), []byte(`{"version":1,"key":"other"}`), 0644))
		}},
		{"unreadable journal", func(t *testing.T, output string, data []byte) {
			require.NoError(t, os.WriteFile(extractJournalPath(output), []byte(`not json`), 0644))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := newSkipTestSource(t)
			output := t.TempDir() + "/out.pmtiles"
			_, err := src.extract(t, output, false)
			require.NoError(t, err)
			complete, err := os.ReadFile(output)
			require.NoError(t, err)

			c.change(t, output, append([]byte(nil), complete...))
			lines, err := src.extract(t, output, false)
			require.NoError(t, err)
			assert.Equal(t, int64(2), src.tileRequests.Load(), "the tile data is downloaded again")
			assert.NotContains(t, lastLine(lines), "already_complete")
			got, err := os.ReadFile(output)
			require.NoError(t, err)
			assert.Equal(t, complete, got)
			assert.NoFileExists(t, extractJournalPath(output))
		})
	}
}

func TestExtractDryRunWithCompleteOutput(t *testing.T) {
	src := newSkipTestSource(t)
	output := t.TempDir() + "/out.pmtiles"
	_, err := src.extract(t, output, false)
	require.NoError(t, err)
	before, err := os.ReadFile(output)
	require.NoError(t, err)

	lines, err := src.extract(t, output, true)
	require.NoError(t, err)
	assert.Contains(t, lines[0], `"transfer_bytes":`)
	for _, line := range lines {
		assert.NotContains(t, line, "already_complete")
	}
	after, err := os.ReadFile(output)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

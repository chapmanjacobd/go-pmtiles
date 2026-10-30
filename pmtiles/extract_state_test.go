package pmtiles

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var noHooks = downloadHooks{
	transferred: func(uint64) {},
	written:     func(uint64, uint64) {},
	retried:     func(error) {},
}

var fastRetry = retryPolicy{tries: 5, wait: 0}

func TestIntervalSetAddAndMissing(t *testing.T) {
	var s intervalSet
	s = s.add(10, 5)
	s = s.add(30, 5)
	s = s.add(15, 5) // adjacent to the first
	assert.Equal(t, intervalSet{{10, 10}, {30, 5}}, s)
	assert.Equal(t, []dstInterval{{0, 10}, {20, 10}, {35, 5}}, s.missing(0, 40))
	assert.Empty(t, s.missing(12, 3))
	s = s.add(5, 40)
	assert.Equal(t, intervalSet{{5, 40}}, s)
	assert.Equal(t, uint64(40), s.total())
}

func TestIntervalSetRandom(t *testing.T) {
	rnd := rand.New(rand.NewSource(3))
	for iter := 0; iter < 200; iter++ {
		covered := make([]bool, 500)
		var s intervalSet
		for i := 0; i < rnd.Intn(30); i++ {
			off, n := uint64(rnd.Intn(480)), uint64(rnd.Intn(20))
			s = s.add(off, n)
			for k := off; k < off+n; k++ {
				covered[k] = true
			}
		}
		for i := 1; i < len(s); i++ {
			assert.Greater(t, s[i].Offset, s[i-1].Offset+s[i-1].Length)
		}
		off, n := uint64(rnd.Intn(250)), uint64(rnd.Intn(250))
		got := make([]bool, 500)
		for _, gap := range s.missing(off, n) {
			for k := gap.Offset; k < gap.Offset+gap.Length; k++ {
				got[k] = true
			}
		}
		for k := uint64(0); k < 500; k++ {
			assert.Equal(t, k >= off && k < off+n && !covered[k], got[k], "byte %d", k)
		}
	}
}

func TestMissingOverfetchRangesNothingDone(t *testing.T) {
	or := overfetchRange{srcDstRange{1000, 7, 120}, []copyDiscard{{50, 10}, {60, 0}}}
	assert.Equal(t, []overfetchRange{or}, missingOverfetchRanges(or, nil))
	assert.Empty(t, missingOverfetchRanges(or, intervalSet{{0, 1000}}))
}

func TestMissingOverfetchRangesHoles(t *testing.T) {
	// wanted dst [7,57) and [57,117); completed [20,30) and [57,60)
	or := overfetchRange{srcDstRange{1000, 7, 120}, []copyDiscard{{50, 10}, {60, 0}}}
	done := intervalSet{{20, 10}, {57, 3}}
	assert.Equal(t, []overfetchRange{
		{srcDstRange{1000, 7, 13}, []copyDiscard{{13, 0}}},
		{srcDstRange{1023, 30, 27}, []copyDiscard{{27, 0}}},
		{srcDstRange{1063, 60, 57}, []copyDiscard{{57, 0}}},
	}, missingOverfetchRanges(or, done))
}

func TestMissingOverfetchRangesRandom(t *testing.T) {
	rnd := rand.New(rand.NewSource(4))
	for iter := 0; iter < 200; iter++ {
		var ranges []srcDstRange
		srcPos, dstPos := uint64(0), uint64(0)
		for i := 0; i < 1+rnd.Intn(30); i++ {
			srcPos += uint64(rnd.Intn(300))
			length := uint64(1 + rnd.Intn(200))
			ranges = append(ranges, srcDstRange{srcPos, dstPos, length})
			srcPos += length
			dstPos += length
		}
		src := make([]byte, srcPos)
		rnd.Read(src)
		merged, _ := mergeRanges(ranges, rnd.Float32())
		want := make([]byte, dstPos)
		for e := merged.Front(); e != nil; e = e.Next() {
			applyOverfetchRange(src, want, e.Value.(overfetchRange))
		}

		// a previous run wrote some random intervals
		var done intervalSet
		got := make([]byte, dstPos)
		for i := 0; i < rnd.Intn(10); i++ {
			off := uint64(rnd.Intn(int(dstPos)))
			n := min(uint64(rnd.Intn(100)), dstPos-off)
			done = done.add(off, n)
			copy(got[off:off+n], want[off:off+n])
		}

		requested := make([]bool, dstPos)
		for e := merged.Front(); e != nil; e = e.Next() {
			for _, piece := range missingOverfetchRanges(e.Value.(overfetchRange), done) {
				assert.NotZero(t, piece.CopyDiscards[0].Wanted)
				assert.Zero(t, piece.CopyDiscards[len(piece.CopyDiscards)-1].Discard)
				applyOverfetchRange(src, got, piece)
				d := piece.Rng.DstOffset
				for _, cd := range piece.CopyDiscards {
					for k := d; k < d+cd.Wanted; k++ {
						assert.False(t, requested[k], "byte %d requested twice", k)
						requested[k] = true
					}
					d += cd.Wanted
				}
			}
		}
		assert.Equal(t, want, got)
		for k := range requested {
			assert.Equal(t, len(done.missing(uint64(k), 1)) == 1, requested[k], "byte %d", k)
		}
	}
}

func TestJournalSaveLoadResumes(t *testing.T) {
	dir := t.TempDir()
	out, err := os.Create(filepath.Join(dir, "out.pmtiles"))
	assert.NoError(t, err)
	defer out.Close()

	state := extractJournal{
		Version: extractJournalVersion, Bucket: "https://example.com", Key: "a.pmtiles", ETag: `"abc"`,
		SourceHeaderHash: "h", PlanHash: "p", OutputSize: 1000, TileDataLength: 900,
	}
	path := extractJournalPath(out.Name())
	j := newJournalWriter(path, out, state)
	j.record(100, 50)
	j.record(150, 10)
	j.record(500, 1)
	assert.NoError(t, j.save())
	_, err = os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(err))

	loaded, err := loadExtractJournal(path)
	assert.NoError(t, err)
	assert.Equal(t, []dstInterval{{100, 60}, {500, 1}}, loaded.Completed)
	assert.True(t, loaded.resumes(&state))

	for _, change := range []func(*extractJournal){
		func(s *extractJournal) { s.ETag = `"def"` },
		func(s *extractJournal) { s.PlanHash = "q" },
		func(s *extractJournal) { s.SourceHeaderHash = "x" },
		func(s *extractJournal) { s.OutputSize = 1001 },
		func(s *extractJournal) { s.Key = "b.pmtiles" },
	} {
		other := state
		change(&other)
		assert.False(t, loaded.resumes(&other))
	}

	loaded.Completed = append(loaded.Completed, dstInterval{899, 2})
	assert.False(t, loaded.resumes(&state))

	assert.NoError(t, removeExtractJournal(path))
	missing, err := loadExtractJournal(path)
	assert.NoError(t, err)
	assert.Nil(t, missing)
}

func TestRetryRequest(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := retryRequest(ctx, fastRetry, func(error) {}, func() (bool, error) {
		calls++
		if calls < 3 {
			return false, io.ErrUnexpectedEOF
		}
		return false, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, calls)

	calls = 0
	err = retryRequest(ctx, fastRetry, func(error) {}, func() (bool, error) {
		calls++
		return false, io.ErrUnexpectedEOF
	})
	assert.ErrorIs(t, err, ErrNetworkGaveUp)
	assert.Equal(t, 5, calls)

	// progress resets the count of consecutive failures
	calls = 0
	err = retryRequest(ctx, fastRetry, func(error) {}, func() (bool, error) {
		calls++
		if calls == 20 {
			return true, nil
		}
		return true, io.ErrUnexpectedEOF
	})
	assert.NoError(t, err)

	for _, final := range []error{&RefreshRequiredError{412}, ErrRangeNotHonored, &HTTPStatusError{404}, &finalError{os.ErrPermission}} {
		calls = 0
		err = retryRequest(ctx, fastRetry, func(error) {}, func() (bool, error) {
			calls++
			return false, final
		})
		assert.Equal(t, final, err)
		assert.Equal(t, 1, calls)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	err = retryRequest(cancelled, fastRetry, func(error) {}, func() (bool, error) { return false, io.ErrUnexpectedEOF })
	assert.ErrorIs(t, err, context.Canceled)
}

// flakyReader fails after a random number of bytes.
type flakyReader struct {
	r    io.Reader
	left int
}

func (f *flakyReader) Read(p []byte) (int, error) {
	if f.left <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > f.left {
		p = p[:f.left]
	}
	n, err := f.r.Read(p)
	f.left -= n
	return n, err
}

func TestDownloadActiveRangeReconnects(t *testing.T) {
	rnd := rand.New(rand.NewSource(5))
	for iter := 0; iter < 50; iter++ {
		var ranges []srcDstRange
		srcPos, dstPos := uint64(0), uint64(0)
		for i := 0; i < 1+rnd.Intn(20); i++ {
			srcPos += uint64(rnd.Intn(300))
			length := uint64(1 + rnd.Intn(500))
			ranges = append(ranges, srcDstRange{srcPos, dstPos, length})
			srcPos += length
			dstPos += length
		}
		src := make([]byte, srcPos)
		rnd.Read(src)
		merged, _ := mergeRanges(ranges, rnd.Float32())
		want := make([]byte, dstPos)
		for e := merged.Front(); e != nil; e = e.Next() {
			applyOverfetchRange(src, want, e.Value.(overfetchRange))
		}

		got := sliceWriterAt(make([]byte, dstPos))
		scheduler := newRangeScheduler(merged, 8)
		var mu sync.Mutex
		var written intervalSet
		retries := 0
		hooks := downloadHooks{
			transferred: func(uint64) {},
			written: func(off uint64, n uint64) {
				mu.Lock()
				written = written.add(off, n)
				mu.Unlock()
			},
			retried: func(error) {
				mu.Lock()
				retries++
				mu.Unlock()
			},
		}
		var wg sync.WaitGroup
		for w := 0; w < 1+rnd.Intn(6); w++ {
			wg.Add(1)
			seed := rnd.Int63()
			go func(workBack bool) {
				defer wg.Done()
				local := rand.New(rand.NewSource(seed))
				buf := make([]byte, 16)
				open := func(_ context.Context, off uint64, n uint64) (io.ReadCloser, error) {
					return io.NopCloser(&flakyReader{bytes.NewReader(src[off : off+n]), 1 + local.Intn(60)}), nil
				}
				for {
					a := scheduler.next(workBack)
					if a == nil {
						return
					}
					assert.NoError(t, downloadActiveRange(context.Background(), scheduler, a, open, got, 0, buf, hooks, fastRetry))
				}
			}(w == 0)
		}
		wg.Wait()
		assert.Equal(t, want, []byte(got))
		assert.Equal(t, intervalSet{{0, dstPos}}, written)
		assert.Positive(t, retries)
	}
}

func TestDownloadActiveRangeGivesUp(t *testing.T) {
	queue, _ := mergeRanges([]srcDstRange{{0, 0, 100}}, 0)
	scheduler := newRangeScheduler(queue, 8)
	a := scheduler.next(false)
	open := func(context.Context, uint64, uint64) (io.ReadCloser, error) {
		return io.NopCloser(&flakyReader{bytes.NewReader(make([]byte, 100)), 0}), nil
	}
	err := downloadActiveRange(context.Background(), scheduler, a, open, sliceWriterAt(make([]byte, 100)), 0, make([]byte, 16), noHooks, fastRetry)
	assert.ErrorIs(t, err, ErrNetworkGaveUp)
}

func TestHTTPBucketChecksRangesAndETag(t *testing.T) {
	content := make([]byte, 1000)
	rand.New(rand.NewSource(6)).Read(content)
	ignoreRange := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ignoreRange {
			w.Write(content)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		http.ServeContent(w, r, "a.pmtiles", time.Time{}, bytes.NewReader(content))
	}))
	defer server.Close()
	bucket := HTTPBucket{server.URL, strictRangeClient{newDownloadClient(2)}}

	r, etag, _, err := bucket.NewRangeReaderEtag(context.Background(), "a.pmtiles", 100, 50, "")
	assert.NoError(t, err)
	data, _ := io.ReadAll(r)
	assert.Equal(t, content[100:150], data)
	assert.Equal(t, `"v1"`, etag)

	_, _, _, err = bucket.NewRangeReaderEtag(context.Background(), "a.pmtiles", 100, 50, `"v0"`)
	var refresh *RefreshRequiredError
	assert.True(t, errors.As(err, &refresh))
	assert.Equal(t, 412, refresh.StatusCode)

	ignoreRange = true
	_, _, _, err = bucket.NewRangeReaderEtag(context.Background(), "a.pmtiles", 100, 50, "")
	assert.ErrorIs(t, err, ErrRangeNotHonored)
	// a 200 for a range that is the whole file is fine
	_, _, _, err = bucket.NewRangeReaderEtag(context.Background(), "a.pmtiles", 0, 1000, "")
	assert.NoError(t, err)
}

func TestCheckContentRange(t *testing.T) {
	resp := func(status int, contentRange string) *http.Response {
		r := &http.Response{StatusCode: status, Header: http.Header{}, ContentLength: -1}
		if contentRange != "" {
			r.Header.Set("Content-Range", contentRange)
		}
		return r
	}
	assert.NoError(t, checkContentRange(resp(206, "bytes 10-19/100"), 10, 10))
	assert.ErrorIs(t, checkContentRange(resp(206, "bytes 10-18/100"), 10, 10), ErrRangeNotHonored)
	assert.ErrorIs(t, checkContentRange(resp(206, "bytes 0-9/100"), 10, 10), ErrRangeNotHonored)
	assert.ErrorIs(t, checkContentRange(resp(206, ""), 10, 10), ErrRangeNotHonored)
	assert.ErrorIs(t, checkContentRange(resp(200, ""), 10, 10), ErrRangeNotHonored)
}

func TestExtractProgressLines(t *testing.T) {
	var buf safeBuffer
	p := newExtractProgress(&buf)
	p.startDownload(1000, 100, func() int64 { return 7 })
	p.addTransferred(400)
	p.addRetry()
	p.finish("done")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Contains(t, lines[0], `"phase":"plan"`)
	last := lines[len(lines)-1]
	assert.Contains(t, last, `"phase":"done"`)
	assert.Contains(t, last, `"bytes_done":500`)
	assert.Contains(t, last, `"resumed_bytes":100`)
	assert.Contains(t, last, `"retries":1`)
	assert.Contains(t, last, `"requests_total":7`)

	var nilProgress *extractProgress
	nilProgress.setPhase("download")
	nilProgress.finish("done")
}

func TestExtractOutcomeLine(t *testing.T) {
	cases := []struct {
		err     error
		phase   string
		codeStr string
	}{
		{nil, "done", "0"},
		{context.Canceled, "paused", "3"},
		{ErrNetworkGaveUp, "error", "2"},
		{errors.New("boom"), "error", "1"},
	}
	for _, c := range cases {
		var buf safeBuffer
		p := newExtractProgress(&buf)
		p.finishOutcome(c.err)
		out := strings.TrimSpace(buf.String())
		last := out[strings.LastIndex(out, "\n")+1:]
		assert.Contains(t, last, `"phase":"`+c.phase+`"`)
		assert.Contains(t, last, `"exit_code":`+c.codeStr)
	}
}

type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

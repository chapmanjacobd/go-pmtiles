package pmtiles

// Download states for extract: a resume journal, per-request retries and
// machine-readable progress.
//
// The resume journal (identity check, completed destination intervals,
// atomic periodic save, automatic resume, removal on success) follows the
// design of Jacob Chapman's "extract: add resumability"
// (github.com/chapmanjacobd/go-pmtiles, branch resume).

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	extractJournalVersion = 1
	// How often the journal is saved while downloading: a crash re-downloads
	// at most this much work, and each save is a small write plus an fsync
	// (kept infrequent for SD cards and other flash storage).
	extractJournalSaveInterval = 15 * time.Second
)

// ErrNetworkGaveUp is returned when a request still fails after all retries.
// The extract can be resumed by running it again.
var ErrNetworkGaveUp = errors.New("network gave up")

// retryPolicy mirrors aria2's --max-tries and --retry-wait.
type retryPolicy struct {
	tries int
	wait  time.Duration
}

var defaultRetryPolicy = retryPolicy{tries: 5, wait: 5 * time.Second}

// retryableExtractError reports whether a failed request is worth trying again.
// A changed source, a server that ignores Range and client errors are final.
func retryableExtractError(err error) bool {
	var refresh *RefreshRequiredError
	var final *finalError
	if errors.As(err, &refresh) || errors.As(err, &final) || errors.Is(err, ErrRangeNotHonored) {
		return false
	}
	// Local filesystem errors (missing input, permission, no space) are not
	// transient: fail fast with the real cause instead of retrying them as a
	// network blip (otherwise a full disk wastes the retry budget and reports
	// "network gave up").
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return false
	}
	var status *HTTPStatusError
	if errors.As(err, &status) {
		return status.StatusCode >= 500 || status.StatusCode == 408 || status.StatusCode == 429
	}
	return true
}

// retryRequest calls attempt until it succeeds, fails with a final error, or
// fails policy.tries times in a row. attempt reports whether it made progress,
// which resets the count of consecutive failures.
func retryRequest(ctx context.Context, policy retryPolicy, onRetry func(error), attempt func() (bool, error)) error {
	failures := 0
	for {
		progressed, err := attempt()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryableExtractError(err) {
			return err
		}
		if progressed {
			failures = 0
		}
		failures++
		if failures >= policy.tries {
			return fmt.Errorf("%w after %d tries: %v", ErrNetworkGaveUp, failures, err)
		}
		onRetry(err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(policy.wait):
		}
	}
}

// dstInterval is the range [Offset, Offset+Length) of tile data in the output.
type dstInterval struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// intervalSet is a sorted list of disjoint, non-adjacent intervals.
type intervalSet []dstInterval

func normalizeIntervals(in []dstInterval) intervalSet {
	sorted := append([]dstInterval(nil), in...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Offset < sorted[j].Offset })
	var s intervalSet
	for _, iv := range sorted {
		s = s.add(iv.Offset, iv.Length)
	}
	return s
}

func (s intervalSet) add(offset uint64, length uint64) intervalSet {
	if length == 0 {
		return s
	}
	start, end := offset, offset+length
	i := sort.Search(len(s), func(i int) bool { return s[i].Offset+s[i].Length >= start })
	j := i
	for j < len(s) && s[j].Offset <= end {
		start = min(start, s[j].Offset)
		end = max(end, s[j].Offset+s[j].Length)
		j++
	}
	merged := dstInterval{start, end - start}
	if i == j {
		s = append(s, dstInterval{})
		copy(s[i+1:], s[i:])
		s[i] = merged
		return s
	}
	s[i] = merged
	return append(s[:i+1], s[j:]...)
}

// missing returns the parts of [offset, offset+length) not covered by s.
func (s intervalSet) missing(offset uint64, length uint64) []dstInterval {
	var gaps []dstInterval
	end := offset + length
	cur := offset
	i := sort.Search(len(s), func(i int) bool { return s[i].Offset+s[i].Length > offset })
	for ; i < len(s) && s[i].Offset < end; i++ {
		if s[i].Offset > cur {
			gaps = append(gaps, dstInterval{cur, s[i].Offset - cur})
		}
		cur = max(cur, s[i].Offset+s[i].Length)
	}
	if cur < end {
		gaps = append(gaps, dstInterval{cur, end - cur})
	}
	return gaps
}

func (s intervalSet) total() uint64 {
	t := uint64(0)
	for _, iv := range s {
		t += iv.Length
	}
	return t
}

// missingOverfetchRanges returns the requests that fetch the wanted bytes of or
// not yet in done. Missing bytes separated only by discarded bytes stay in one
// request; completed bytes split the request. The completed part need not be a
// prefix: work stealing can leave holes anywhere.
func missingOverfetchRanges(or overfetchRange, done intervalSet) []overfetchRange {
	var result []overfetchRange
	srcRel := uint64(0)
	dstPos := or.Rng.DstOffset
	open := false
	var windowStart, windowEnd uint64

	emit := func() {
		if open {
			if piece, ok := sliceOverfetchRange(or, windowStart, windowEnd); ok {
				result = append(result, piece)
			}
			open = false
		}
	}

	for _, cd := range or.CopyDiscards {
		pos := dstPos
		for _, gap := range done.missing(dstPos, cd.Wanted) {
			if gap.Offset > pos {
				emit()
			}
			gapStart := srcRel + (gap.Offset - dstPos)
			if !open {
				windowStart = gapStart
				open = true
			}
			windowEnd = gapStart + gap.Length
			pos = gap.Offset + gap.Length
		}
		if pos < dstPos+cd.Wanted {
			emit()
		}
		srcRel += cd.Wanted + cd.Discard
		dstPos += cd.Wanted
	}
	emit()
	return result
}

// extractJournal is the content of <output>.resume.
type extractJournal struct {
	Version          int           `json:"version"`
	Bucket           string        `json:"bucket"`
	Key              string        `json:"key"`
	ETag             string        `json:"etag,omitempty"`
	SourceHeaderHash string        `json:"source_header_hash"`
	PlanHash         string        `json:"plan_hash"`
	OutputSize       uint64        `json:"output_size"`
	TileDataLength   uint64        `json:"tile_data_length"`
	Completed        []dstInterval `json:"completed"`
}

func extractJournalPath(output string) string {
	return output + ".resume"
}

func sha256Hex(chunks ...[]byte) string {
	h := sha256.New()
	var length [8]byte
	for _, chunk := range chunks {
		binary.LittleEndian.PutUint64(length[:], uint64(len(chunk)))
		h.Write(length[:])
		h.Write(chunk)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// extractPlanHash changes when the source, region, zoom levels or tile layout change.
func extractPlanHash(newHeader []byte, root []byte, leaves []byte, tileParts []srcDstRange) string {
	parts := make([]byte, 0, 24*len(tileParts))
	for _, r := range tileParts {
		parts = binary.LittleEndian.AppendUint64(parts, r.SrcOffset)
		parts = binary.LittleEndian.AppendUint64(parts, r.DstOffset)
		parts = binary.LittleEndian.AppendUint64(parts, r.Length)
	}
	return sha256Hex(newHeader, root, leaves, parts)
}

func loadExtractJournal(path string) (*extractJournal, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var j extractJournal
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	j.Completed = normalizeIntervals(j.Completed)
	return &j, nil
}

// resumes reports whether j was written by an extract identical to expected.
func (j *extractJournal) resumes(expected *extractJournal) bool {
	if j.Version != expected.Version || j.Bucket != expected.Bucket || j.Key != expected.Key ||
		j.ETag != expected.ETag || j.SourceHeaderHash != expected.SourceHeaderHash ||
		j.PlanHash != expected.PlanHash || j.OutputSize != expected.OutputSize ||
		j.TileDataLength != expected.TileDataLength {
		return false
	}
	for _, iv := range j.Completed {
		if iv.Offset+iv.Length > j.TileDataLength {
			return false
		}
	}
	return true
}

func removeExtractJournal(path string) error {
	os.Remove(path + ".tmp")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// journalWriter records the tile bytes written to out and saves them durably:
// out is synced before the journal claims any byte, and the journal is
// replaced atomically.
type journalWriter struct {
	path string
	out  *os.File

	mu        sync.Mutex
	state     extractJournal
	completed intervalSet

	saveMu sync.Mutex
}

func newJournalWriter(path string, out *os.File, state extractJournal) *journalWriter {
	return &journalWriter{path: path, out: out, state: state, completed: normalizeIntervals(state.Completed)}
}

func (j *journalWriter) record(offset uint64, length uint64) {
	j.mu.Lock()
	j.completed = j.completed.add(offset, length)
	j.mu.Unlock()
}

func (j *journalWriter) completedSet() intervalSet {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append(intervalSet(nil), j.completed...)
}

func (j *journalWriter) save() error {
	j.saveMu.Lock()
	defer j.saveMu.Unlock()

	// Every interval in this snapshot was written before the sync below.
	j.mu.Lock()
	state := j.state
	state.Completed = append([]dstInterval{}, j.completed...)
	j.mu.Unlock()

	if err := j.out.Sync(); err != nil {
		return err
	}
	data, err := json.Marshal(&state)
	if err != nil {
		return err
	}
	tmp := j.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(j.path)); err == nil {
		dir.Sync()
		dir.Close()
	}
	return nil
}

// extractProgress writes one JSON line per second describing the extract.
type extractProgress struct {
	w     io.Writer
	start time.Time

	mu       sync.Mutex
	phase    string
	exitCode *int
	samples  []progressSample

	bytesTotal    atomic.Uint64
	resumedBytes  atomic.Uint64
	transferred   atomic.Uint64
	requestsDone  atomic.Int64
	requestsTotal func() int64
	retries       atomic.Int64

	stop chan struct{}
	done chan struct{}
}

type progressSample struct {
	at    time.Time
	bytes uint64
}

type progressLine struct {
	Phase         string   `json:"phase"`
	BytesDone     uint64   `json:"bytes_done"`
	BytesTotal    uint64   `json:"bytes_total"`
	ResumedBytes  uint64   `json:"resumed_bytes"`
	SpeedBps      uint64   `json:"speed_bps"`
	EtaS          *float64 `json:"eta_s"`
	RequestsDone  int64    `json:"requests_done"`
	RequestsTotal int64    `json:"requests_total"`
	Retries       int64    `json:"retries"`
	// ExitCode is set only on the terminal line (see finishOutcome): it names the
	// process exit code so a consumer that reads stdout alone knows the outcome.
	ExitCode *int `json:"exit_code,omitempty"`
}

const progressSpeedWindow = 5 * time.Second

// newExtractProgress starts reporting to w; it returns nil when w is nil, and
// every method of a nil *extractProgress does nothing.
func newExtractProgress(w io.Writer) *extractProgress {
	if w == nil {
		return nil
	}
	p := &extractProgress{w: w, start: time.Now(), phase: "plan", stop: make(chan struct{}), done: make(chan struct{})}
	p.emit()
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				p.emit()
			}
		}
	}()
	return p
}

func (p *extractProgress) setPhase(phase string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.phase = phase
	p.mu.Unlock()
	p.emit()
}

// startDownload enters the download phase with the full transfer size, the
// part of it a previous run already did, and a source for the request count.
func (p *extractProgress) startDownload(bytesTotal uint64, resumedBytes uint64, requestsTotal func() int64) {
	if p == nil {
		return
	}
	p.bytesTotal.Store(bytesTotal)
	p.resumedBytes.Store(resumedBytes)
	p.mu.Lock()
	p.requestsTotal = requestsTotal
	p.mu.Unlock()
	p.setPhase("download")
}

func (p *extractProgress) addTransferred(n uint64) {
	if p != nil {
		p.transferred.Add(n)
	}
}

func (p *extractProgress) addRetry() {
	if p != nil {
		p.retries.Add(1)
	}
}

func (p *extractProgress) addRequestDone() {
	if p != nil {
		p.requestsDone.Add(1)
	}
}

func (p *extractProgress) emit() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	transferred := p.transferred.Load()
	p.samples = append(p.samples, progressSample{now, transferred})
	for len(p.samples) > 1 && now.Sub(p.samples[0].at) > progressSpeedWindow {
		p.samples = p.samples[1:]
	}
	line := progressLine{
		Phase:        p.phase,
		BytesTotal:   p.bytesTotal.Load(),
		ResumedBytes: p.resumedBytes.Load(),
		RequestsDone: p.requestsDone.Load(),
		Retries:      p.retries.Load(),
	}
	line.BytesDone = min(line.ResumedBytes+transferred, line.BytesTotal)
	if p.requestsTotal != nil {
		line.RequestsTotal = p.requestsTotal()
	}
	if first := p.samples[0]; now.Sub(first.at) > 0 {
		line.SpeedBps = uint64(float64(transferred-first.bytes) / now.Sub(first.at).Seconds())
	}
	if line.SpeedBps > 0 {
		eta := float64(line.BytesTotal-line.BytesDone) / float64(line.SpeedBps)
		line.EtaS = &eta
	} else if line.BytesTotal > 0 && line.BytesDone == line.BytesTotal {
		eta := 0.0
		line.EtaS = &eta
	}
	line.ExitCode = p.exitCode
	data, _ := json.Marshal(&line)
	p.w.Write(append(data, '\n'))
}

// finish stops the ticker and writes a last line, in the given phase if not empty.
func (p *extractProgress) finish(phase string) {
	if p == nil {
		return
	}
	close(p.stop)
	<-p.done
	if phase != "" {
		p.setPhase(phase)
	} else {
		p.emit()
	}
}

// ExtractExitCode maps an Extract error to the process exit code. It is the one
// source for that mapping: main() and the terminal progress line both use it.
// 0 done, 1 other error, 2 network gave up (resumable), 3 paused (resumable),
// 4 source changed (not resumable).
func ExtractExitCode(err error) int {
	var refresh *RefreshRequiredError
	switch {
	case err == nil:
		return 0
	case errors.Is(err, context.Canceled):
		return 3
	case errors.As(err, &refresh):
		return 4
	case errors.Is(err, ErrNetworkGaveUp):
		return 2
	default:
		return 1
	}
}

// extractOutcomePhase names the outcome for the terminal JSON progress line, so
// a consumer that reads stdout alone can tell done from paused or failed without
// the process exit code.
func extractOutcomePhase(err error) string {
	switch ExtractExitCode(err) {
	case 0:
		return "done"
	case 3:
		return "paused"
	case 4:
		return "source_changed"
	default:
		return "error"
	}
}

// finishOutcome stops the ticker and writes one final line whose phase names the
// outcome and whose exit_code equals the process exit code.
func (p *extractProgress) finishOutcome(err error) {
	if p == nil {
		return
	}
	code := ExtractExitCode(err)
	p.mu.Lock()
	p.exitCode = &code
	p.mu.Unlock()
	p.finish(extractOutcomePhase(err))
}

// finalError marks an error that must not be retried, such as a failed write
// to the output file.
type finalError struct {
	err error
}

func (e *finalError) Error() string { return e.err.Error() }
func (e *finalError) Unwrap() error { return e.err }

// fetchRange reads [offset, offset+length) of key into memory, retrying
// failed attempts. It also returns the ETag of the response.
func fetchRange(ctx context.Context, bucket Bucket, key string, offset uint64, length uint64, etag string, policy retryPolicy, onRetry func(error)) ([]byte, string, error) {
	var data []byte
	var gotETag string
	err := retryRequest(ctx, policy, onRetry, func() (bool, error) {
		r, e, _, err := bucket.NewRangeReaderEtag(ctx, key, int64(offset), int64(length), etag)
		if err != nil {
			return false, err
		}
		defer r.Close()
		data, err = io.ReadAll(r)
		if err == nil && uint64(len(data)) != length {
			err = io.ErrUnexpectedEOF
		}
		gotETag = e
		return false, err
	})
	return data, gotETag, err
}

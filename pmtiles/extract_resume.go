package pmtiles

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	extractResumeVersion       = 1
	extractResumeSaveInterval  = 5 * time.Second
	extractResumeCopyChunkSize = 1 << 20
)

// extractResumeRange is a half-open range [Offset, Offset+Length) of tile data
// in the output archive that has been completely written.
type extractResumeRange struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

type extractResumeState struct {
	Version          int                  `json:"version"`
	Bucket           string               `json:"bucket"`
	Key              string               `json:"key"`
	SourceETag       string               `json:"source_etag,omitempty"`
	SourceHeaderHash string               `json:"source_header_hash"`
	PlanHash         string               `json:"plan_hash"`
	OutputSize       uint64               `json:"output_size"`
	TileDataLength   uint64               `json:"tile_data_length"`
	Completed        []extractResumeRange `json:"completed,omitempty"`
}

func extractResumePath(output string) string {
	return output + ".resume"
}

func extractSourceHeaderHash(headerBytes []byte) string {
	sum := sha256.Sum256(headerBytes)
	return hex.EncodeToString(sum[:])
}

func removeExtractResumeFile(path string) error {
	_ = os.Remove(path + ".tmp")
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func loadExtractResume(path string) (*extractResumeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var state extractResumeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	state.Completed = normalizeExtractResumeRanges(state.Completed)
	return &state, nil
}

func (s *extractResumeState) matches(bucket string, key string, etag string, sourceHeaderHash string, planHash string, outputSize uint64, tileDataLength uint64) bool {
	if s.Version != extractResumeVersion ||
		s.Bucket != bucket ||
		s.Key != key ||
		s.SourceHeaderHash != sourceHeaderHash ||
		s.PlanHash != planHash ||
		s.OutputSize != outputSize ||
		s.TileDataLength != tileDataLength {
		return false
	}

	// An empty stored ETag means the source did not provide one. In that case
	// the header and plan hashes above are the best identity check available.
	if s.SourceETag != "" && s.SourceETag != etag {
		return false
	}

	for _, r := range s.Completed {
		if r.Length > tileDataLength || r.Offset > tileDataLength-r.Length {
			return false
		}
	}
	return true
}

// Changing the source, region, zoom levels, or tile ordering changes this hash.
func extractPlanHash(headerBytes []byte, rootBytes []byte, leavesBytes []byte, tileParts []srcDstRange) string {
	h := sha256.New()

	var lengthBytes [8]byte
	writeChunk := func(chunk []byte) {
		binary.LittleEndian.PutUint64(lengthBytes[:], uint64(len(chunk)))
		h.Write(lengthBytes[:])
		h.Write(chunk)
	}

	writeChunk(headerBytes)
	writeChunk(rootBytes)
	writeChunk(leavesBytes)

	var rangeBytes [24]byte
	for _, r := range tileParts {
		binary.LittleEndian.PutUint64(rangeBytes[0:8], r.SrcOffset)
		binary.LittleEndian.PutUint64(rangeBytes[8:16], r.DstOffset)
		binary.LittleEndian.PutUint64(rangeBytes[16:24], r.Length)
		h.Write(rangeBytes[:])
	}

	return hex.EncodeToString(h.Sum(nil))
}

func normalizeExtractResumeRanges(ranges []extractResumeRange) []extractResumeRange {
	result := make([]extractResumeRange, 0, len(ranges))
	for _, r := range ranges {
		if r.Length > 0 {
			result = append(result, r)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Offset < result[j].Offset
	})

	merged := result[:0]
	for _, r := range result {
		if len(merged) == 0 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		if r.Offset <= last.Offset+last.Length {
			if end := r.Offset + r.Length; end > last.Offset+last.Length {
				last.Length = end - last.Offset
			}
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

func mergeExtractResumeRange(ranges []extractResumeRange, add extractResumeRange) []extractResumeRange {
	if add.Length == 0 {
		return ranges
	}

	end := add.Offset + add.Length
	start := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].Offset+ranges[i].Length >= add.Offset
	})

	// Merge every range that touches [add.Offset, end).
	stop := start
	for stop < len(ranges) && ranges[stop].Offset <= end {
		if ranges[stop].Offset < add.Offset {
			add.Offset = ranges[stop].Offset
		}
		if rangeEnd := ranges[stop].Offset + ranges[stop].Length; rangeEnd > end {
			end = rangeEnd
		}
		stop++
	}
	add.Length = end - add.Offset

	if start == stop {
		ranges = append(ranges, extractResumeRange{})
		copy(ranges[start+1:], ranges[start:])
		ranges[start] = add
		return ranges
	}

	ranges[start] = add
	return append(ranges[:start+1], ranges[stop:]...)
}

func completedExtractResumeBytes(ranges []extractResumeRange) uint64 {
	var total uint64
	for _, r := range ranges {
		total += r.Length
	}
	return total
}

func overfetchRangeMissing(or overfetchRange, completedPrefix func(uint64, uint64) uint64) []overfetchRange {
	var destinationLength uint64
	for _, cd := range or.CopyDiscards {
		destinationLength += cd.Wanted
	}
	if destinationLength == 0 {
		return nil
	}

	skip := completedPrefix(or.Rng.DstOffset, destinationLength)
	if skip >= destinationLength {
		return nil
	}
	return []overfetchRange{trimOverfetchRangePrefix(or, skip)}
}

func trimOverfetchRangePrefix(or overfetchRange, skip uint64) overfetchRange {
	result := overfetchRange{}
	started := false

	srcOffset := or.Rng.SrcOffset
	dstOffset := or.Rng.DstOffset

	for _, cd := range or.CopyDiscards {
		if !started {
			if skip >= cd.Wanted {
				skip -= cd.Wanted
				srcOffset += cd.Wanted + cd.Discard
				dstOffset += cd.Wanted
				continue
			}

			wanted := cd.Wanted - skip
			result.Rng = srcDstRange{
				SrcOffset: srcOffset + skip,
				DstOffset: dstOffset + skip,
				Length:    wanted + cd.Discard,
			}
			result.CopyDiscards = append(result.CopyDiscards, copyDiscard{
				Wanted:  wanted,
				Discard: cd.Discard,
			})
			started = true
			skip = 0
		} else {
			result.Rng.Length += cd.Wanted + cd.Discard
			result.CopyDiscards = append(result.CopyDiscards, copyDiscard{
				Wanted:  cd.Wanted,
				Discard: cd.Discard,
			})
		}

		srcOffset += cd.Wanted + cd.Discard
		dstOffset += cd.Wanted
	}

	if !started {
		return overfetchRange{}
	}

	last := len(result.CopyDiscards) - 1
	result.Rng.Length -= result.CopyDiscards[last].Discard
	result.CopyDiscards[last].Discard = 0
	return result
}

type extractResumeProgress struct {
	path string

	mu       sync.Mutex
	state    extractResumeState
	lastSave time.Time

	saveMu sync.Mutex
}

func newExtractResumeProgress(path string, state extractResumeState) *extractResumeProgress {
	return &extractResumeProgress{
		path:     path,
		state:    state,
		lastSave: time.Now(),
	}
}

func (p *extractResumeProgress) add(offset uint64, length uint64) error {
	p.mu.Lock()
	p.state.Completed = mergeExtractResumeRange(p.state.Completed, extractResumeRange{
		Offset: offset,
		Length: length,
	})
	due := time.Since(p.lastSave) >= extractResumeSaveInterval
	p.mu.Unlock()

	if due {
		return p.save()
	}
	return nil
}

func (p *extractResumeProgress) completedPrefix(offset uint64, length uint64) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	end := offset + length
	i := sort.Search(len(p.state.Completed), func(i int) bool {
		return p.state.Completed[i].Offset+p.state.Completed[i].Length > offset
	})

	current := offset
	for i < len(p.state.Completed) && current < end {
		r := p.state.Completed[i]
		if r.Offset > current {
			break
		}
		rangeEnd := r.Offset + r.Length
		if rangeEnd <= current {
			i++
			continue
		}
		if rangeEnd > end {
			rangeEnd = end
		}
		current = rangeEnd
		i++
	}
	return current - offset
}

func (p *extractResumeProgress) save() error {
	p.saveMu.Lock()
	defer p.saveMu.Unlock()

	p.mu.Lock()
	state := p.state
	state.Completed = append([]extractResumeRange(nil), p.state.Completed...)
	p.mu.Unlock()

	data, err := json.Marshal(&state)
	if err != nil {
		return err
	}

	tmpPath := p.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, p.path); err != nil {
		return err
	}

	p.mu.Lock()
	p.lastSave = time.Now()
	p.mu.Unlock()
	return nil
}

func (p *extractResumeProgress) remove() error {
	return removeExtractResumeFile(p.path)
}

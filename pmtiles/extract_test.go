package pmtiles

import (
	"bytes"
	"context"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestRelevantEntries(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 1})

	bitmap := roaring64.New()
	bitmap.Add(0)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 1)
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesRunLength(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 5})

	bitmap := roaring64.New()
	bitmap.Add(1)
	bitmap.Add(2)
	bitmap.Add(4)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 2)
	assert.Equal(t, tiles[0].RunLength, uint32(2))
	assert.Equal(t, tiles[1].RunLength, uint32(1))
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesLeaf(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(1)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 0)
	assert.Equal(t, len(leaves), 1)
}

func TestRelevantEntriesNotLeaf(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})
	entries = append(entries, EntryV3{2, 0, 0, 1})
	entries = append(entries, EntryV3{4, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(3)

	tiles, leaves := RelevantEntries(bitmap, 4, entries)

	assert.Equal(t, len(tiles), 0)
	assert.Equal(t, len(leaves), 0)
}

func TestRelevantEntriesMaxZoom(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 0, 0, 0})

	bitmap := roaring64.New()
	bitmap.Add(6)
	_, leaves := RelevantEntries(bitmap, 1, entries)
	assert.Equal(t, len(leaves), 0)

	_, leaves = RelevantEntries(bitmap, 2, entries)
	assert.Equal(t, len(leaves), 1)
}

func TestReencodeEntries(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 1})
	entries = append(entries, EntryV3{1, 500, 20, 2})

	reencoded, result, datalen, addressed, contents := reencodeEntries(entries)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(10))
	assert.Equal(t, result[1].SrcOffset, uint64(500))
	assert.Equal(t, result[1].Length, uint64(20))

	assert.Equal(t, 2, len(reencoded))
	assert.Equal(t, reencoded[0].Offset, uint64(0))
	assert.Equal(t, reencoded[1].Offset, uint64(10))

	assert.Equal(t, uint64(30), datalen)
	assert.Equal(t, uint64(3), addressed)
	assert.Equal(t, uint64(2), contents)
}

func TestReencodeEntriesDuplicate(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 1})
	entries = append(entries, EntryV3{1, 500, 20, 1})
	entries = append(entries, EntryV3{2, 400, 10, 1})

	reencoded, result, datalen, addressed, contents := reencodeEntries(entries)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(10))
	assert.Equal(t, result[1].SrcOffset, uint64(500))
	assert.Equal(t, result[1].Length, uint64(20))

	assert.Equal(t, len(reencoded), 3)
	assert.Equal(t, reencoded[0].Offset, uint64(0))
	assert.Equal(t, reencoded[1].Offset, uint64(10))
	assert.Equal(t, reencoded[2].Offset, uint64(0))

	assert.Equal(t, uint64(30), datalen)
	assert.Equal(t, uint64(3), addressed)
	assert.Equal(t, uint64(2), contents)
}

func TestReencodeContiguous(t *testing.T) {
	entries := make([]EntryV3, 0)
	entries = append(entries, EntryV3{0, 400, 10, 0})
	entries = append(entries, EntryV3{1, 410, 20, 0})

	_, result, _, _, _ := reencodeEntries(entries)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].SrcOffset, uint64(400))
	assert.Equal(t, result[0].Length, uint64(30))
}

func TestMergeRanges(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{0, 0, 50})
	ranges = append(ranges, srcDstRange{60, 60, 60})

	result, totalTransferBytes := mergeRanges(ranges, 0.1)

	assert.Equal(t, 1, result.Len())
	assert.Equal(t, uint64(120), totalTransferBytes)
	front := result.Front().Value.(overfetchRange)
	assert.Equal(t, srcDstRange{0, 0, 120}, front.Rng)
	assert.Equal(t, 2, len(front.CopyDiscards))
	assert.Equal(t, copyDiscard{50, 10}, front.CopyDiscards[0])
	assert.Equal(t, copyDiscard{60, 0}, front.CopyDiscards[1])
}

func TestMergeRangesMultiple(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{0, 0, 50})
	ranges = append(ranges, srcDstRange{60, 60, 10})
	ranges = append(ranges, srcDstRange{80, 80, 10})

	result, totalTransferBytes := mergeRanges(ranges, 0.3)
	front := result.Front().Value.(overfetchRange)
	assert.Equal(t, uint64(90), totalTransferBytes)
	assert.Equal(t, 1, result.Len())
	assert.Equal(t, srcDstRange{0, 0, 90}, front.Rng)
	assert.Equal(t, 3, len(front.CopyDiscards))
}

func TestMergeRangesNonSrcOrdered(t *testing.T) {
	ranges := make([]srcDstRange, 0)
	ranges = append(ranges, srcDstRange{20, 0, 50})
	ranges = append(ranges, srcDstRange{0, 60, 50})

	result, _ := mergeRanges(ranges, 0.1)
	assert.Equal(t, 2, result.Len())
}

// applyOverfetchRange simulates downloadPart: it reads the requested source
// bytes and writes the wanted ones to dst, returning the bytes requested.
func applyOverfetchRange(src []byte, dst []byte, or overfetchRange) uint64 {
	srcPos := or.Rng.SrcOffset
	dstPos := or.Rng.DstOffset
	for _, cd := range or.CopyDiscards {
		copy(dst[dstPos:dstPos+cd.Wanted], src[srcPos:srcPos+cd.Wanted])
		srcPos += cd.Wanted + cd.Discard
		dstPos += cd.Wanted
	}
	if srcPos != or.Rng.SrcOffset+or.Rng.Length {
		panic("CopyDiscards do not cover the range")
	}
	return or.Rng.Length
}

func TestSplitOverfetchRangeSmall(t *testing.T) {
	or := overfetchRange{srcDstRange{0, 0, 120}, []copyDiscard{{50, 10}, {60, 0}}}
	assert.Equal(t, []overfetchRange{or}, splitOverfetchRange(or, 0))
	assert.Equal(t, []overfetchRange{or}, splitOverfetchRange(or, 120))
}

func TestSplitOverfetchRangeSkipsBoundaryDiscards(t *testing.T) {
	or := overfetchRange{srcDstRange{1000, 7, 120}, []copyDiscard{{50, 10}, {60, 0}}}
	pieces := splitOverfetchRange(or, 50)
	assert.Equal(t, []overfetchRange{
		{srcDstRange{1000, 7, 50}, []copyDiscard{{50, 0}}},
		{srcDstRange{1060, 57, 50}, []copyDiscard{{50, 0}}},
		{srcDstRange{1110, 107, 10}, []copyDiscard{{10, 0}}},
	}, pieces)
}

func TestSplitOverfetchRangesSameOutput(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		var ranges []srcDstRange
		srcPos, dstPos := uint64(0), uint64(0)
		for i := 0; i < 1+rnd.Intn(40); i++ {
			srcPos += uint64(rnd.Intn(300))
			length := uint64(1 + rnd.Intn(200))
			ranges = append(ranges, srcDstRange{srcPos, dstPos, length})
			srcPos += length
			dstPos += length
		}
		src := make([]byte, srcPos)
		rnd.Read(src)
		merged, mergedBytes := mergeRanges(ranges, rnd.Float32())

		want := make([]byte, dstPos)
		for e := merged.Front(); e != nil; e = e.Next() {
			applyOverfetchRange(src, want, e.Value.(overfetchRange))
		}

		maxLen := uint64(1 + rnd.Intn(150))
		split, splitBytes := splitOverfetchRanges(merged, maxLen)
		got := make([]byte, dstPos)
		requested := uint64(0)
		for e := split.Front(); e != nil; e = e.Next() {
			or := e.Value.(overfetchRange)
			assert.LessOrEqual(t, or.Rng.Length, maxLen)
			assert.NotZero(t, or.CopyDiscards[0].Wanted)
			assert.Zero(t, or.CopyDiscards[len(or.CopyDiscards)-1].Discard)
			requested += applyOverfetchRange(src, got, or)
		}
		assert.Equal(t, want, got)
		assert.Equal(t, requested, splitBytes)
		assert.LessOrEqual(t, splitBytes, mergedBytes)
	}
}

func TestSliceOverfetchRange(t *testing.T) {
	or := overfetchRange{srcDstRange{1000, 7, 120}, []copyDiscard{{50, 10}, {60, 0}}}
	piece, ok := sliceOverfetchRange(or, 40, 70)
	assert.True(t, ok)
	assert.Equal(t, overfetchRange{srcDstRange{1040, 47, 30}, []copyDiscard{{10, 10}, {10, 0}}}, piece)
	_, ok = sliceOverfetchRange(or, 52, 58)
	assert.False(t, ok)
}

type sliceWriterAt []byte

func (w sliceWriterAt) WriteAt(p []byte, off int64) (int, error) {
	return copy(w[off:], p), nil
}

// slowReader makes some requests slow so that idle workers steal from them.
type slowReader struct {
	r     io.Reader
	delay time.Duration
}

func (s slowReader) Read(p []byte) (int, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.r.Read(p)
}

func TestRangeSchedulerStealingSameOutput(t *testing.T) {
	rnd := rand.New(rand.NewSource(2))
	stolen := 0
	for iter := 0; iter < 30; iter++ {
		var ranges []srcDstRange
		srcPos, dstPos := uint64(0), uint64(0)
		for i := 0; i < 1+rnd.Intn(30); i++ {
			srcPos += uint64(rnd.Intn(300))
			length := uint64(1 + rnd.Intn(500))
			ranges = append(ranges, srcDstRange{srcPos, dstPos, length})
			srcPos += length
			dstPos += length
		}
		src := make([]byte, srcPos)
		rnd.Read(src)
		merged, _ := mergeRanges(ranges, rnd.Float32())
		queued := merged.Len()

		want := make([]byte, dstPos)
		for e := merged.Front(); e != nil; e = e.Next() {
			applyOverfetchRange(src, want, e.Value.(overfetchRange))
		}

		got := sliceWriterAt(make([]byte, dstPos+5))
		scheduler := newRangeScheduler(merged, uint64(2+rnd.Intn(20)))
		requests := 0
		var mu sync.Mutex
		var wg sync.WaitGroup
		for w := 0; w < 1+rnd.Intn(8); w++ {
			wg.Add(1)
			go func(workBack bool, delay time.Duration) {
				defer wg.Done()
				buf := make([]byte, 4)
				for {
					a := scheduler.next(workBack)
					if a == nil {
						return
					}
					mu.Lock()
					requests++
					mu.Unlock()
					open := func(_ context.Context, off uint64, n uint64) (io.ReadCloser, error) {
						return io.NopCloser(slowReader{bytes.NewReader(src[off : off+n]), delay}), nil
					}
					assert.NoError(t, downloadActiveRange(context.Background(), scheduler, a, open, got, 5, buf, noHooks, defaultRetryPolicy))
				}
			}(w == 0, time.Duration(rnd.Intn(2))*time.Microsecond)
		}
		wg.Wait()
		assert.Equal(t, want, []byte(got[5:]))
		assert.Empty(t, scheduler.active)
		stolen += requests - queued
	}
	assert.Positive(t, stolen)
}

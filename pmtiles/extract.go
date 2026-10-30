package pmtiles

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dustin/go-humanize"
	"github.com/paulmach/orb"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type srcDstRange struct {
	SrcOffset uint64
	DstOffset uint64
	Length    uint64
}

// RelevantEntries finds the intersection of a bitmap and a directory
// return sorted slice of entries, and slice of all leaf entries
// any runlengths > 1 will be "trimmed" to the relevance bitmap
func RelevantEntries(bitmap *roaring64.Bitmap, maxzoom uint8, dir []EntryV3) ([]EntryV3, []EntryV3) {
	lastTile := ZxyToID(maxzoom+1, 0, 0)
	leaves := make([]EntryV3, 0)
	tiles := make([]EntryV3, 0)
	for idx, entry := range dir {
		if entry.RunLength == 0 {
			tmp := roaring64.New()

			// if this is the last thing in the directory, it needs to be bounded
			if idx == len(dir)-1 {
				tmp.AddRange(entry.TileID, lastTile)
			} else {
				tmp.AddRange(entry.TileID, dir[idx+1].TileID)
			}

			if bitmap.Intersects(tmp) {
				leaves = append(leaves, entry)
			}
		} else if entry.RunLength == 1 {
			if bitmap.Contains(entry.TileID) {
				tiles = append(tiles, entry)
			}
		} else {
			// runlength > 1
			currentID := entry.TileID
			currentRunLength := uint32(0)
			for y := entry.TileID; y < entry.TileID+uint64(entry.RunLength); y++ {
				if bitmap.Contains(y) {
					if currentRunLength == 0 {
						currentRunLength = 1
						currentID = y
					} else {
						currentRunLength++
					}
				} else {
					if currentRunLength > 0 {
						tiles = append(tiles, EntryV3{currentID, entry.Offset, entry.Length, currentRunLength})
					}
					currentRunLength = 0
				}
			}
			if currentRunLength > 0 {
				tiles = append(tiles, EntryV3{currentID, entry.Offset, entry.Length, currentRunLength})
			}
		}
	}
	return tiles, leaves
}

// Given a tile entries for a Source archive, sorted in TileID order,
// return:
// * Re-encoded tile-entries, with their offsets changed to contiguous (clustered) order in a new archive.
// * SrcDstRange: slice of offsets in the source archive, offset in the new archive, and length.
//   - Each range is one or more tiles
//   - the output must not have contiguous entries
//   - It is sorted by new offsets, but not necessarily by source offsets
//
// * The total size of the tile section in the new archive
// * The # of addressed tiles (sum over RunLength)
// * # the number of unique offsets ("tile contents")
//   - this might not be the last SrcDstRange new_offset + length, it's the highest offset (can be in the middle)
func reencodeEntries(dir []EntryV3) ([]EntryV3, []srcDstRange, uint64, uint64, uint64) {
	reencoded := make([]EntryV3, 0, len(dir))
	seenOffsets := make(map[uint64]uint64)
	ranges := make([]srcDstRange, 0)
	addressedTiles := uint64(0)

	dstOffset := uint64(0)
	for _, entry := range dir {
		if val, ok := seenOffsets[entry.Offset]; ok {
			reencoded = append(reencoded, EntryV3{entry.TileID, val, entry.Length, entry.RunLength})
		} else {
			if len(ranges) > 0 {
				lastRange := ranges[len(ranges)-1]
				if lastRange.SrcOffset+lastRange.Length == entry.Offset {
					ranges[len(ranges)-1].Length += uint64(entry.Length)
				} else {
					ranges = append(ranges, srcDstRange{entry.Offset, dstOffset, uint64(entry.Length)})
				}
			} else {
				ranges = append(ranges, srcDstRange{entry.Offset, dstOffset, uint64(entry.Length)})
			}

			reencoded = append(reencoded, EntryV3{entry.TileID, dstOffset, entry.Length, entry.RunLength})
			seenOffsets[entry.Offset] = dstOffset
			dstOffset += uint64(entry.Length)
		}

		addressedTiles += uint64(entry.RunLength)
	}
	return reencoded, ranges, dstOffset, addressedTiles, uint64(len(seenOffsets))
}

// "want the next N bytes, then discard N bytes"
type copyDiscard struct {
	Wanted  uint64
	Discard uint64
}

type overfetchRange struct {
	Rng          srcDstRange
	CopyDiscards []copyDiscard
}

// A single request, where only some of the bytes
// in the requested range we want
type overfetchListItem struct {
	Rng          srcDstRange
	CopyDiscards []copyDiscard
	BytesToNext  uint64 // the "priority"
	prev         *overfetchListItem
	next         *overfetchListItem
	index        int
}

// mergeRanges takes a slice of SrcDstRanges, that:
// * is non-contiguous, and is sorted by DstOffset
// * an Overfetch parameter
//   - overfetch = 0.2 means we can request an extra 20%
//   - overfetch = 1.00 means we can double our total transfer size
//
// Return a list of OverfetchRanges
//
//	Each OverfetchRange is one or more input ranges
//	input ranges are merged in order of smallest byte distance to next range
//	until the overfetch budget is consumed.
//	The list is sorted by Length
func mergeRanges(ranges []srcDstRange, overfetch float32) (*list.List, uint64) {
	totalSize := 0

	shortest := make([]*overfetchListItem, len(ranges))

	// create the heap items
	for i, rng := range ranges {
		var bytesToNext int64
		if i == len(ranges)-1 {
			bytesToNext = math.MaxInt64
		} else {
			bytesToNext = int64(ranges[i+1].SrcOffset) - (int64(rng.SrcOffset) + int64(rng.Length))
			if bytesToNext < 0 {
				bytesToNext = math.MaxInt64
			}
		}

		shortest[i] = &overfetchListItem{
			Rng:          rng,
			BytesToNext:  uint64(bytesToNext),
			CopyDiscards: []copyDiscard{{uint64(rng.Length), 0}},
		}
		totalSize += int(rng.Length)
	}

	// make the list doubly-linked
	for i, item := range shortest {
		if i > 0 {
			item.prev = shortest[i-1]
		}
		if i < len(shortest)-1 {
			item.next = shortest[i+1]
		}
	}

	overfetchBudget := int(float32(totalSize) * overfetch)

	// sort by ascending distance to next range
	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].BytesToNext < shortest[j].BytesToNext
	})

	// while we haven't consumed the budget, merge ranges
	for (len(shortest) > 1) && (overfetchBudget-int(shortest[0].BytesToNext) >= 0) {
		item := shortest[0]

		// merge this item into item.next
		newLength := item.Rng.Length + item.BytesToNext + item.next.Rng.Length
		item.next.Rng = srcDstRange{item.Rng.SrcOffset, item.Rng.DstOffset, newLength}
		item.next.prev = item.prev
		if item.prev != nil {
			item.prev.next = item.next
		}
		item.CopyDiscards[len(item.CopyDiscards)-1].Discard = item.BytesToNext
		item.next.CopyDiscards = append(item.CopyDiscards, item.next.CopyDiscards...)

		shortest = shortest[1:]

		overfetchBudget -= int(item.BytesToNext)
	}

	sort.Slice(shortest, func(i, j int) bool {
		return shortest[i].Rng.Length > shortest[j].Rng.Length
	})

	totalBytes := uint64(0)
	result := list.New()
	for _, x := range shortest {
		result.PushBack(overfetchRange{
			Rng:          x.Rng,
			CopyDiscards: x.CopyDiscards,
		})
		totalBytes += x.Rng.Length
	}

	return result, totalBytes
}

// splitOverfetchRange cuts one merged request into consecutive requests of at
// most maxLen source bytes, so that a single large range can be fetched over
// several connections in parallel instead of dominating the tail of the
// download on one connection.
// Each piece carries its own DstOffset and CopyDiscards, so the bytes written
// to the output are exactly the same as for the unsplit range.
// Discarded bytes at the start or end of a piece are not requested.
func splitOverfetchRange(or overfetchRange, maxLen uint64) []overfetchRange {
	if maxLen == 0 || or.Rng.Length <= maxLen {
		return []overfetchRange{or}
	}

	result := make([]overfetchRange, 0, or.Rng.Length/maxLen+1)
	srcPos := or.Rng.SrcOffset
	dstPos := or.Rng.DstOffset
	var piece overfetchRange
	open := false

	flush := func() {
		if !open {
			return
		}
		last := &piece.CopyDiscards[len(piece.CopyDiscards)-1]
		piece.Rng.Length -= last.Discard
		last.Discard = 0
		result = append(result, piece)
		open = false
	}

	for _, cd := range or.CopyDiscards {
		for wanted := cd.Wanted; wanted > 0; {
			if !open {
				piece = overfetchRange{Rng: srcDstRange{srcPos, dstPos, 0}}
				open = true
			}
			n := min(wanted, maxLen-piece.Rng.Length)
			if l := len(piece.CopyDiscards); l > 0 && piece.CopyDiscards[l-1].Discard == 0 {
				piece.CopyDiscards[l-1].Wanted += n
			} else {
				piece.CopyDiscards = append(piece.CopyDiscards, copyDiscard{n, 0})
			}
			piece.Rng.Length += n
			srcPos += n
			dstPos += n
			wanted -= n
			if piece.Rng.Length == maxLen {
				flush()
			}
		}
		for discard := cd.Discard; discard > 0; {
			if !open {
				srcPos += discard
				break
			}
			n := min(discard, maxLen-piece.Rng.Length)
			piece.CopyDiscards[len(piece.CopyDiscards)-1].Discard += n
			piece.Rng.Length += n
			srcPos += n
			discard -= n
			if piece.Rng.Length == maxLen {
				flush()
			}
		}
	}
	flush()
	return result
}

// splitOverfetchRanges applies splitOverfetchRange to every range of the list,
// keeping the list order, and returns the new list with its total transfer size.
func splitOverfetchRanges(ranges *list.List, maxLen uint64) (*list.List, uint64) {
	result := list.New()
	totalBytes := uint64(0)
	for e := ranges.Front(); e != nil; e = e.Next() {
		for _, piece := range splitOverfetchRange(e.Value.(overfetchRange), maxLen) {
			result.PushBack(piece)
			totalBytes += piece.Rng.Length
		}
	}
	return result, totalBytes
}

// sliceOverfetchRange returns the part of a request between the source bytes
// from and to (relative to its SrcOffset), without leading or trailing
// discarded bytes. It returns false when that part has no wanted bytes.
func sliceOverfetchRange(or overfetchRange, from uint64, to uint64) (overfetchRange, bool) {
	var piece overfetchRange
	open := false
	srcRel := uint64(0)
	dstPos := or.Rng.DstOffset
	for _, cd := range or.CopyDiscards {
		lo := max(srcRel, from)
		hi := min(srcRel+cd.Wanted, to)
		if lo < hi {
			if !open {
				piece = overfetchRange{Rng: srcDstRange{or.Rng.SrcOffset + lo, dstPos + (lo - srcRel), 0}}
				open = true
			} else {
				gap := or.Rng.SrcOffset + lo - (piece.Rng.SrcOffset + piece.Rng.Length)
				piece.CopyDiscards[len(piece.CopyDiscards)-1].Discard += gap
				piece.Rng.Length += gap
			}
			piece.CopyDiscards = append(piece.CopyDiscards, copyDiscard{hi - lo, 0})
			piece.Rng.Length += hi - lo
		}
		srcRel += cd.Wanted + cd.Discard
		dstPos += cd.Wanted
		if srcRel >= to {
			break
		}
	}
	return piece, open
}

// minStealBytes is the smallest number of unread bytes of an in-flight request
// that an idle download thread splits to take over the second half.
const minStealBytes = 4 << 20

// claimChunkBytes is how many bytes a request reader claims at a time;
// a stolen request is cut at a claim boundary.
const claimChunkBytes = 1 << 20

// activeRange is a request being downloaded: its reader claims source bytes
// (relative to or.Rng.SrcOffset) up to end, which another thread may lower.
type activeRange struct {
	or      overfetchRange
	claimed uint64
	end     uint64
}

// rangeScheduler hands the queued requests to the download threads. When the
// queue is empty, an idle thread splits the in-flight request with the most
// unread bytes and downloads its second half on a new connection, as aria2
// does, so the last large requests do not leave all but one connection idle.
type rangeScheduler struct {
	mu       sync.Mutex
	queue    *list.List
	active   map[*activeRange]struct{}
	minSteal uint64
	issued   int64
}

func newRangeScheduler(queue *list.List, minSteal uint64) *rangeScheduler {
	return &rangeScheduler{queue: queue, active: make(map[*activeRange]struct{}), minSteal: minSteal}
}

// next returns the next request to download, or nil when no work is left.
func (s *rangeScheduler) next(fromBack bool) *activeRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		var or overfetchRange
		if s.queue.Len() > 0 {
			if fromBack {
				or = s.queue.Remove(s.queue.Back()).(overfetchRange)
			} else {
				or = s.queue.Remove(s.queue.Front()).(overfetchRange)
			}
		} else {
			var victim *activeRange
			unread := uint64(0)
			for a := range s.active {
				if a.end-a.claimed > unread {
					victim, unread = a, a.end-a.claimed
				}
			}
			if victim == nil || unread < s.minSteal {
				return nil
			}
			split := victim.claimed + unread/2
			piece, ok := sliceOverfetchRange(victim.or, split, victim.end)
			victim.end = split
			if !ok {
				continue
			}
			or = piece
		}
		a := &activeRange{or: or, end: or.Rng.Length}
		s.active[a] = struct{}{}
		s.issued++
		return a
	}
}

// requestsTotal is the number of requests handed out plus those still queued;
// it grows when a request is split.
func (s *rangeScheduler) requestsTotal() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.issued + int64(s.queue.Len())
}

// end returns the source bytes (relative to a.or.Rng.SrcOffset) a must read.
func (s *rangeScheduler) end(a *activeRange) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return a.end
}

// claim reserves up to n more bytes of a for its reader and returns how many;
// 0 means the request is finished.
func (s *rangeScheduler) claim(a *activeRange, n uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	n = min(n, a.end-a.claimed)
	a.claimed += n
	if n == 0 {
		delete(s.active, a)
	}
	return n
}

// rangeOpener opens a reader of source bytes [srcOffset, srcOffset+length).
type rangeOpener func(ctx context.Context, srcOffset uint64, length uint64) (io.ReadCloser, error)

// downloadHooks observe a download.
type downloadHooks struct {
	transferred func(n uint64)                   // bytes read from the source
	written     func(dstOffset uint64, n uint64) // tile bytes written at this tile-data offset
	retried     func(err error)                  // a failed attempt that will be retried
}

// downloadActiveRange reads the claimed bytes of a and writes the wanted ones
// to w at dstBase + their destination offset. A failed connection is reopened
// at the first byte not yet read, following policy.
func downloadActiveRange(ctx context.Context, s *rangeScheduler, a *activeRange, open rangeOpener, w io.WriterAt, dstBase int64, buf []byte, hooks downloadHooks, policy retryPolicy) error {
	cdIdx, inCd := 0, uint64(0)
	dstPos := a.or.Rng.DstOffset
	consumed, pending := uint64(0), uint64(0)
	var r io.ReadCloser
	defer func() {
		if r != nil {
			r.Close()
		}
	}()

	write := func(chunk []byte) error {
		for len(chunk) > 0 {
			cd := a.or.CopyDiscards[cdIdx]
			var k uint64
			if inCd < cd.Wanted {
				k = min(cd.Wanted-inCd, uint64(len(chunk)))
				if _, err := w.WriteAt(chunk[:k], dstBase+int64(dstPos)); err != nil {
					return &finalError{err}
				}
				hooks.written(dstPos, k)
				dstPos += k
			} else {
				k = min(cd.Wanted+cd.Discard-inCd, uint64(len(chunk)))
			}
			inCd += k
			chunk = chunk[k:]
			if inCd == cd.Wanted+cd.Discard {
				cdIdx++
				inCd = 0
			}
		}
		return nil
	}

	return retryRequest(ctx, policy, hooks.retried, func() (bool, error) {
		progressed := false
		for {
			if pending == 0 {
				pending = s.claim(a, uint64(len(buf)))
				if pending == 0 {
					return progressed, nil
				}
			}
			if r == nil {
				var err error
				if r, err = open(ctx, a.or.Rng.SrcOffset+consumed, s.end(a)-consumed); err != nil {
					r = nil
					return progressed, err
				}
			}
			got, err := io.ReadFull(r, buf[:pending])
			if got > 0 {
				progressed = true
				hooks.transferred(uint64(got))
				if werr := write(buf[:got]); werr != nil {
					return progressed, werr
				}
				consumed += uint64(got)
				pending -= uint64(got)
			}
			if err != nil {
				r.Close()
				r = nil
				return progressed, err
			}
		}
	})
}

// Extract a smaller archive from local or remote archive.
// 1. Get the root directory (check that it is clustered)
// 2. Turn the input geometry into a relevance bitmap (using min(maxzoom, headermaxzoom))
// 3. Get all relevant level 1 directories (if any)
// 4. Get all relevant level 2 directories (usually none)
// 5. With the existing directory + relevance bitmap, construct
//   - a new total directory (root + leaf directories)
//   - a sorted slice of byte ranges in the old file required
//
// 6. Merge requested ranges using an overfetch parametter
// 7. write the modified header
// 8. write the root directory.
// 9. get and write the metadata.
// 10. write the leaf directories (if any)
// 11. Get all tiles, and write directly to the output.
//
// maxRequestBytes > 0 splits every tile request larger than that into
// several requests, so that downloadThreads connections stay busy until the end.
//
// The download resumes from <output>.resume when a previous run of the same
// extract was interrupted; failed requests are retried. A non-nil
// progressJSON receives one JSON progress line per second, and the
// exact transfer size for a dry run.
func Extract(ctx context.Context, logger *log.Logger, bucketURL string, key string, minzoom int8, maxzoom int8, regionFile string, bbox string, output string, downloadThreads int, overfetch float32, maxRequestBytes int64, dryRun bool, progressJSON io.Writer) (retErr error) {
	// 1. fetch the header
	start := time.Now()

	var progress *extractProgress
	if !dryRun {
		progress = newExtractProgress(progressJSON)
	}
	// The terminal JSON line names the outcome (done/paused/source_changed/error)
	// and carries exit_code, derived from the error Extract returns.
	defer func() { progress.finishOutcome(retErr) }()
	onRetry := func(err error) {
		progress.addRetry()
		logger.Printf("retrying after error: %v\n", err)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	bucketURL, key, err := NormalizeBucketKey(bucketURL, "", key)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	if httpBucket, ok := bucket.(HTTPBucket); ok && httpBucket.client == http.DefaultClient {
		httpBucket.client = newDownloadClient(downloadThreads)
		bucket = httpBucket
	}
	if httpBucket, ok := bucket.(HTTPBucket); ok {
		httpBucket.client = strictRangeClient{httpBucket.client}
		bucket = httpBucket
	}

	b, etag, err := fetchRange(ctx, bucket, key, 0, HeaderV3LenBytes, "", defaultRetryPolicy, onRetry)
	if err != nil {
		return fmt.Errorf("Failed to read header of %s, %w", key, err)
	}
	// Every later read must come from this same version of the source.
	// A weak ETag cannot be used with If-Match.
	pinnedETag := etag
	if strings.HasPrefix(etag, "W/") {
		pinnedETag = ""
	}

	header, err := DeserializeHeader(b[0:HeaderV3LenBytes])

	if !header.Clustered {
		return fmt.Errorf("source archive must be clustered for extracts")
	}

	sourceMetadataOffset := header.MetadataOffset
	sourceTileDataOffset := header.TileDataOffset

	if minzoom == -1 || int8(header.MinZoom) > minzoom {
		minzoom = int8(header.MinZoom)
	}

	if maxzoom == -1 || int8(header.MaxZoom) < maxzoom {
		maxzoom = int8(header.MaxZoom)
	}

	if minzoom > maxzoom {
		return fmt.Errorf("minzoom cannot be greater than maxzoom")
	}

	var relevantSet *roaring64.Bitmap
	if regionFile != "" || bbox != "" {
		if regionFile != "" && bbox != "" {
			return fmt.Errorf("only one of region and bbox can be specified")
		}

		var multipolygon orb.MultiPolygon
		var centerLon, centerLat float64

		if regionFile != "" {
			dat, _ := ioutil.ReadFile(regionFile)
			multipolygon, err = UnmarshalRegion(dat)
			if err != nil {
				return err
			}

			bound := multipolygon.Bound()
			centerLon = bound.Center().X()
			centerLat = bound.Center().Y()
		} else {
			parts := strings.Split(bbox, ",")
			minLon, err := strconv.ParseFloat(parts[0], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}
			minLat, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}
			maxLon, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}
			maxLat, err := strconv.ParseFloat(parts[3], 64)
			if err != nil {
				return fmt.Errorf("invalid bbox: %w", err)
			}

			multipolygon = bboxToMultiPolygon(minLon, minLat, maxLon, maxLat)

			centerLon = (minLon + maxLon) / 2
			if minLon > maxLon {
				centerLon = (minLon + maxLon + 360) / 2
				if centerLon > 180 {
					centerLon -= 360
				}
			}
			centerLat = (minLat + maxLat) / 2
		}

		bound := multipolygon.Bound()
		header.MinLonE7 = int32(bound.Left() * 10000000)
		header.MinLatE7 = int32(bound.Bottom() * 10000000)
		header.MaxLonE7 = int32(bound.Right() * 10000000)
		header.MaxLatE7 = int32(bound.Top() * 10000000)
		header.CenterLonE7 = int32(centerLon * 10000000)
		header.CenterLatE7 = int32(centerLat * 10000000)

		// 2. construct a relevance bitmap
		boundarySet, interiorSet := bitmapMultiPolygon(uint8(maxzoom), multipolygon)
		relevantSet = boundarySet
		relevantSet.Or(interiorSet)
		generalizeOr(relevantSet, uint8(minzoom))
	} else {
		relevantSet = roaring64.New()
		relevantSet.AddRange(ZxyToID(uint8(minzoom), 0, 0), ZxyToID(uint8(maxzoom)+1, 0, 0))
	}

	// 3. get relevant entries from root
	dirOffset := header.RootOffset
	dirLength := header.RootLength

	rootBytes, _, err := fetchRange(ctx, bucket, key, dirOffset, dirLength, pinnedETag, defaultRetryPolicy, onRetry)
	if err != nil {
		return err
	}

	rootDir := DeserializeEntries(bytes.NewBuffer(rootBytes), header.InternalCompression)

	tileEntries, leaves := RelevantEntries(relevantSet, uint8(maxzoom), rootDir)

	// 4. get all relevant leaf entries

	leafRanges := make([]srcDstRange, 0)
	for _, leaf := range leaves {
		leafRanges = append(leafRanges, srcDstRange{header.LeafDirectoryOffset + leaf.Offset, 0, uint64(leaf.Length)})
	}

	overfetchLeaves, _ := mergeRanges(leafRanges, overfetch)
	numOverfetchLeaves := overfetchLeaves.Len()
	logger.Printf("fetching %d dirs, %d chunks, %d requests\n", len(leaves), len(leafRanges), overfetchLeaves.Len())

	for {
		if overfetchLeaves.Len() == 0 {
			break
		}
		or := overfetchLeaves.Remove(overfetchLeaves.Front()).(overfetchRange)

		chunk, _, err := fetchRange(ctx, bucket, key, or.Rng.SrcOffset, or.Rng.Length, pinnedETag, defaultRetryPolicy, onRetry)
		if err != nil {
			return err
		}

		pos := uint64(0)
		for _, cd := range or.CopyDiscards {

			leafBytes := chunk[pos : pos+cd.Wanted]
			pos += cd.Wanted + cd.Discard
			leafdir := DeserializeEntries(bytes.NewBuffer(leafBytes), header.InternalCompression)
			newEntries, newLeaves := RelevantEntries(relevantSet, uint8(maxzoom), leafdir)

			if len(newLeaves) > 0 {
				panic("This doesn't support leaf level 2+.")
			}
			tileEntries = append(tileEntries, newEntries...)
		}
	}

	sort.Slice(tileEntries, func(i, j int) bool {
		return tileEntries[i].TileID < tileEntries[j].TileID
	})

	logger.Printf("Region tiles %d, result tile entries %d\n", relevantSet.GetCardinality(), len(tileEntries))

	// 6. create the new header and chunk list
	// we now need to re-encode this entry list using cumulative offsets
	reencoded, tileParts, tiledataLength, addressedTiles, tileContents := reencodeEntries(tileEntries)

	overfetchRanges, totalBytes := mergeRanges(tileParts, overfetch)

	if maxRequestBytes > 0 {
		overfetchRanges, totalBytes = splitOverfetchRanges(overfetchRanges, uint64(maxRequestBytes))
	}

	numOverfetchRanges := overfetchRanges.Len()
	logger.Printf("fetching %d tiles, %d chunks, %d requests\n", len(reencoded), len(tileParts), overfetchRanges.Len())

	// TODO: takes up too much RAM
	// construct the directories
	newRootBytes, newLeavesBytes, _ := BuildDirectories(reencoded, 16384-HeaderV3LenBytes, Gzip)

	// 7. write the modified header
	header.RootOffset = HeaderV3LenBytes
	header.RootLength = uint64(len(newRootBytes))
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.LeafDirectoryLength = uint64(len(newLeavesBytes))
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength

	header.TileDataLength = tiledataLength
	header.AddressedTilesCount = addressedTiles
	header.TileEntriesCount = uint64(len(tileEntries))
	header.TileContentsCount = tileContents

	header.MaxZoom = uint8(maxzoom)
	header.MinZoom = uint8(minzoom)
	if header.CenterZoom < header.MinZoom {
		header.CenterZoom = header.MinZoom
	} else if header.CenterZoom > header.MaxZoom {
		header.CenterZoom = header.MaxZoom
	}

	headerBytes := SerializeHeader(header)

	totalActualBytes := uint64(0)
	for _, x := range tileParts {
		totalActualBytes += x.Length
	}
	totalOutputSize := uint64(HeaderV3LenBytes) + uint64(len(newRootBytes)) + header.MetadataLength + uint64(len(newLeavesBytes)) + totalActualBytes

	if dryRun && progressJSON != nil {
		data, _ := json.Marshal(map[string]uint64{"transfer_bytes": totalBytes, "archive_bytes": totalOutputSize, "requests": uint64(numOverfetchRanges)})
		progressJSON.Write(append(data, '\n'))
	}

	if !dryRun {

		// resume a previous run of this same extract, or start over
		journalPath := extractJournalPath(output)
		state := extractJournal{
			Version:          extractJournalVersion,
			Bucket:           bucketURL,
			Key:              key,
			ETag:             etag,
			SourceHeaderHash: sha256Hex(b),
			PlanHash:         extractPlanHash(headerBytes, newRootBytes, newLeavesBytes, tileParts),
			OutputSize:       totalOutputSize,
			TileDataLength:   tiledataLength,
		}
		resuming := false
		previous, err := loadExtractJournal(journalPath)
		if err != nil {
			logger.Printf("Ignoring unreadable resume journal %s: %v\n", journalPath, err)
		} else if previous != nil {
			info, statErr := os.Stat(output)
			if !previous.resumes(&state) {
				logger.Printf("Not resuming: %s was written for another extract or source version\n", journalPath)
			} else if statErr != nil || uint64(info.Size()) != totalOutputSize {
				logger.Printf("Not resuming: %s is missing or has an unexpected size\n", output)
			} else {
				state.Completed = previous.Completed
				resuming = true
			}
		}

		flags := os.O_RDWR | os.O_CREATE
		if !resuming {
			flags |= os.O_TRUNC
			if err := removeExtractJournal(journalPath); err != nil {
				return err
			}
		}
		outfile, err := os.OpenFile(output, flags, 0644)
		if err != nil {
			return err
		}
		defer outfile.Close()

		journal := newJournalWriter(journalPath, outfile, state)

		// set the file size and write empty space for the header for now
		// see comment below
		if err := outfile.Truncate(int64(totalOutputSize)); err != nil {
			return err
		}
		if err := journal.save(); err != nil {
			return err
		}
		_, err = outfile.Write(make([]byte, HeaderV3LenBytes))
		if err != nil {
			return err
		}

		// 8. write the root directory
		_, err = outfile.Write(newRootBytes)
		if err != nil {
			return err
		}

		// 9. get and write the metadata
		metadataBytes, _, err := fetchRange(ctx, bucket, key, sourceMetadataOffset, header.MetadataLength, pinnedETag, defaultRetryPolicy, onRetry)
		if err != nil {
			return err
		}

		_, err = outfile.Write(metadataBytes)
		if err != nil {
			return err
		}

		// 10. write the leaf directories
		_, err = outfile.Write(newLeavesBytes)
		if err != nil {
			return err
		}

		// only request the tile bytes a previous run did not write
		completed := journal.completedSet()
		remaining := list.New()
		remainingBytes := uint64(0)
		for e := overfetchRanges.Front(); e != nil; e = e.Next() {
			for _, piece := range missingOverfetchRanges(e.Value.(overfetchRange), completed) {
				remaining.PushBack(piece)
				remainingBytes += piece.Rng.Length
			}
		}
		if resuming {
			logger.Printf("Resuming %s: %s of %s tile data already written, %d requests left\n", output, humanize.Bytes(completed.total()), humanize.Bytes(tiledataLength), remaining.Len())
		}

		addToBar := func(int64) {}
		if progress == nil && remainingBytes > 0 {
			bar := defaultBytesProgressbar(
				logger,
				int64(remainingBytes),
				"fetching chunks",
			)
			addToBar = func(n int64) { bar.Add64(n) }
		}

		scheduler := newRangeScheduler(remaining, minStealBytes)
		progress.startDownload(totalBytes, totalBytes-min(remainingBytes, totalBytes), scheduler.requestsTotal)

		hooks := downloadHooks{
			transferred: func(n uint64) {
				addToBar(int64(n))
				progress.addTransferred(n)
			},
			written: journal.record,
			retried: onRetry,
		}
		open := func(ctx context.Context, srcOffset uint64, length uint64) (io.ReadCloser, error) {
			r, _, _, err := bucket.NewRangeReaderEtag(ctx, key, int64(sourceTileDataOffset+srcOffset), int64(length), pinnedETag)
			return r, err
		}

		// save the journal periodically while downloading
		stopSaving := make(chan struct{})
		saverDone := make(chan struct{})
		go func() {
			defer close(saverDone)
			ticker := time.NewTicker(extractJournalSaveInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopSaving:
					return
				case <-ticker.C:
					if err := journal.save(); err != nil {
						logger.Printf("Failed to save resume journal, %v\n", err)
					}
				}
			}
		}()

		errs, gctx := errgroup.WithContext(ctx)

		for i := 0; i < downloadThreads; i++ {
			workBack := (i == 0 && downloadThreads > 1)
			errs.Go(func() error {
				buf := make([]byte, claimChunkBytes)
				for {
					a := scheduler.next(workBack)
					if a == nil {
						return nil
					}
					if err := downloadActiveRange(gctx, scheduler, a, open, outfile, int64(header.TileDataOffset), buf, hooks, defaultRetryPolicy); err != nil {
						return err
					}
					progress.addRequestDone()
				}
			})
		}

		err = errs.Wait()
		close(stopSaving)
		<-saverDone
		if err != nil {
			if saveErr := journal.save(); saveErr != nil {
				logger.Printf("Failed to save resume journal, %v\n", saveErr)
			}
			var refresh *RefreshRequiredError
			if errors.As(err, &refresh) {
				// the bytes already written belong to another version of the source
				removeExtractJournal(journalPath)
			} else {
				logger.Printf("Download stopped; run the same command again to resume from %s\n", journalPath)
			}
			return err
		}

		// Write the header when finishing,
		// otherwise a extract cancelled during the tile download section
		// will appear valid with "pmtiles verify"
		// even though the tile contents are corrupted.
		progress.setPhase("finalize")
		outfile.Seek(0, io.SeekStart)
		_, err = outfile.Write(headerBytes)
		if err != nil {
			return err
		}
		if err := outfile.Sync(); err != nil {
			return err
		}
		if err := outfile.Close(); err != nil {
			return err
		}
		if err := removeExtractJournal(journalPath); err != nil {
			logger.Printf("Failed to remove resume journal %s, %v\n", journalPath, err)
		}
		totalBytes = remainingBytes
	}

	logger.Printf("Completed in %v with %v download threads (%v tiles/s).\n", time.Since(start), downloadThreads, float64(len(reencoded))/float64(time.Since(start).Seconds()))
	totalRequests := 2                  // header + root
	totalRequests += numOverfetchLeaves // leaves
	totalRequests++                     // metadata
	totalRequests += numOverfetchRanges
	logger.Printf("Extract required %d total requests.\n", totalRequests)
	logger.Printf("Extract transferred %s (overfetch %v) for an archive size of %s\n", humanize.Bytes(totalBytes), overfetch, humanize.Bytes(totalActualBytes))

	return nil
}

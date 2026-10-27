package pmtiles

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractResumeRangeMerge(t *testing.T) {
	var ranges []extractResumeRange
	ranges = mergeExtractResumeRange(ranges, extractResumeRange{Offset: 10, Length: 5})
	ranges = mergeExtractResumeRange(ranges, extractResumeRange{Offset: 30, Length: 5})
	ranges = mergeExtractResumeRange(ranges, extractResumeRange{Offset: 15, Length: 15})

	assert.Equal(t, []extractResumeRange{{Offset: 10, Length: 25}}, normalizeExtractResumeRanges(ranges))

	progress := newExtractResumeProgress("", extractResumeState{Completed: ranges})
	assert.Equal(t, uint64(25), progress.completedPrefix(10, 25))
	assert.Equal(t, uint64(5), progress.completedPrefix(30, 10))
	assert.Equal(t, uint64(0), progress.completedPrefix(0, 10))
}

func TestOverfetchRangeMissing(t *testing.T) {
	or := overfetchRange{
		Rng: srcDstRange{SrcOffset: 100, DstOffset: 0, Length: 120},
		CopyDiscards: []copyDiscard{
			{Wanted: 10, Discard: 5},
			{Wanted: 20, Discard: 0},
			{Wanted: 30, Discard: 15},
			{Wanted: 40, Discard: 0},
		},
	}

	// Nothing complete: the range is unchanged.
	parts := overfetchRangeMissing(or, func(offset, length uint64) uint64 {
		assert.Equal(t, uint64(0), offset)
		assert.Equal(t, uint64(100), length)
		return 0
	})
	assert.Equal(t, []overfetchRange{or}, parts)

	// Complete prefix: resume at byte 25, partway through the second segment.
	parts = overfetchRangeMissing(or, func(uint64, uint64) uint64 { return 25 })
	assert.Equal(t, []overfetchRange{{
		Rng: srcDstRange{SrcOffset: 130, DstOffset: 25, Length: 90},
		CopyDiscards: []copyDiscard{
			{Wanted: 5, Discard: 0},
			{Wanted: 30, Discard: 15},
			{Wanted: 40, Discard: 0},
		},
	}}, parts)

	// Complete first two segments: resume at tile 3 in the source.
	parts = overfetchRangeMissing(or, func(uint64, uint64) uint64 { return 30 })
	assert.Equal(t, []overfetchRange{{
		Rng: srcDstRange{SrcOffset: 135, DstOffset: 30, Length: 85},
		CopyDiscards: []copyDiscard{
			{Wanted: 30, Discard: 15},
			{Wanted: 40, Discard: 0},
		},
	}}, parts)

	assert.Nil(t, overfetchRangeMissing(or, func(uint64, uint64) uint64 { return 100 }))
}

func TestExtractResumeStateMatches(t *testing.T) {
	state := extractResumeState{
		Version:          extractResumeVersion,
		Bucket:           "file:///tmp",
		Key:              "archive.pmtiles",
		SourceETag:       "etag1",
		SourceHeaderHash: "header",
		PlanHash:         "plan",
		OutputSize:       1000,
		TileDataLength:   500,
		Completed:        []extractResumeRange{{Offset: 0, Length: 100}},
	}

	assert.True(t, state.matches("file:///tmp", "archive.pmtiles", "etag1", "header", "plan", 1000, 500))
	assert.False(t, state.matches("file:///tmp", "archive.pmtiles", "etag2", "header", "plan", 1000, 500))
	assert.False(t, state.matches("file:///tmp", "archive.pmtiles", "etag1", "header2", "plan", 1000, 500))
	assert.False(t, state.matches("file:///tmp", "archive.pmtiles", "etag1", "header", "plan2", 1000, 500))
	assert.False(t, state.matches("file:///tmp", "archive.pmtiles", "etag1", "header", "plan", 1001, 500))
	assert.False(t, state.matches("file:///other", "archive.pmtiles", "etag1", "header", "plan", 1000, 500))

	state.SourceETag = ""
	assert.True(t, state.matches("file:///tmp", "archive.pmtiles", "etag1", "header", "plan", 1000, 500))

	state.Completed = []extractResumeRange{{Offset: 50, Length: 450}}
	assert.True(t, state.matches("file:///tmp", "archive.pmtiles", "", "header", "plan", 1000, 500))
	state.Completed = []extractResumeRange{{Offset: 100, Length: 401}}
	assert.False(t, state.matches("file:///tmp", "archive.pmtiles", "", "header", "plan", 1000, 500))
}

func makeExtractTestArchive(t *testing.T) []byte {
	t.Helper()

	const numTiles = 21
	const gap = 256
	tileData := make([]byte, 0, numTiles*100+(numTiles-1)*gap)
	entries := make([]EntryV3, 0, numTiles)
	for i := 0; i < numTiles; i++ {
		tile := bytes.Repeat([]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)}, 25)
		entries = append(entries, EntryV3{
			TileID:    uint64(i),
			Offset:    uint64(len(tileData)),
			Length:    uint32(len(tile)),
			RunLength: 1,
		})
		tileData = append(tileData, tile...)
		if i < numTiles-1 {
			tileData = append(tileData, bytes.Repeat([]byte{0xff}, gap)...)
		}
	}

	rootBytes, leavesBytes, _ := BuildDirectories(entries, 16384-HeaderV3LenBytes, Gzip)
	metadataBytes, err := SerializeMetadata(map[string]interface{}{"name": "extract resume test"}, Gzip)
	require.NoError(t, err)

	header := HeaderV3{
		RootOffset:          HeaderV3LenBytes,
		RootLength:          uint64(len(rootBytes)),
		MetadataLength:      uint64(len(metadataBytes)),
		LeafDirectoryLength: uint64(len(leavesBytes)),
		TileDataLength:      uint64(len(tileData)),
		AddressedTilesCount: numTiles,
		TileEntriesCount:    numTiles,
		TileContentsCount:   numTiles,
		Clustered:           true,
		InternalCompression: Gzip,
		TileCompression:     NoCompression,
		TileType:            Mvt,
		MinZoom:             0,
		MaxZoom:             2,
		MinLonE7:            -1800000000,
		MinLatE7:            -850000000,
		MaxLonE7:            1800000000,
		MaxLatE7:            850000000,
	}
	header.MetadataOffset = header.RootOffset + header.RootLength
	header.LeafDirectoryOffset = header.MetadataOffset + header.MetadataLength
	header.TileDataOffset = header.LeafDirectoryOffset + header.LeafDirectoryLength

	var archive bytes.Buffer
	archive.Write(SerializeHeader(header))
	archive.Write(rootBytes)
	archive.Write(metadataBytes)
	archive.Write(leavesBytes)
	archive.Write(tileData)
	return archive.Bytes()
}

type extractResumeTestServer struct {
	data                 []byte
	tileDataOffset       uint64
	etag                 string
	failFirstTileRangeAt int

	mu              sync.Mutex
	failed          bool
	tileBytesServed int
}

func (s *extractResumeTestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/archive.pmtiles" {
		http.NotFound(w, r)
		return
	}

	s.mu.Lock()
	etag := s.etag
	s.mu.Unlock()

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(s.data)))
		_, _ = w.Write(s.data)
		return
	}

	var start, end int64
	if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}
	if end >= int64(len(s.data)) {
		end = int64(len(s.data)) - 1
	}

	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && ifMatch != etag {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	body := s.data[start : end+1]
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.data)))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusPartialContent)

	if start >= int64(s.tileDataOffset) {
		s.mu.Lock()
		s.tileBytesServed += len(body)
		if s.failFirstTileRangeAt >= 0 && !s.failed {
			s.failed = true
			if s.failFirstTileRangeAt < len(body) {
				body = body[:s.failFirstTileRangeAt]
			}
		}
		s.mu.Unlock()
	}

	_, _ = w.Write(body)
}

func (s *extractResumeTestServer) Configure(etag string, failFirstTileRangeAt int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.etag = etag
	s.failFirstTileRangeAt = failFirstTileRangeAt
	s.failed = false
	s.tileBytesServed = 0
}

func (s *extractResumeTestServer) TileBytesServed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tileBytesServed
}

func TestExtractResume(t *testing.T) {
	archive := makeExtractTestArchive(t)
	header, err := DeserializeHeader(archive[0:HeaderV3LenBytes])
	require.NoError(t, err)

	tmp := t.TempDir()
	fullOutput := filepath.Join(tmp, "full.pmtiles")
	resumedOutput := filepath.Join(tmp, "resumed.pmtiles")
	testLogger := log.New(io.Discard, "", 0)

	serverState := &extractResumeTestServer{
		data:           archive,
		tileDataOffset: header.TileDataOffset,
		etag:           `"v1"`,
	}
	httpServer := httptest.NewServer(serverState)
	defer httpServer.Close()

	runExtract := func(output string) error {
		return Extract(context.Background(), testLogger, "", httpServer.URL+"/archive.pmtiles", -1, -1, "", "", output, 4, 0.05, false)
	}

	serverState.Configure(`"v1"`, -1)
	require.NoError(t, runExtract(fullOutput))
	assert.NoFileExists(t, extractResumePath(fullOutput))
	fullTileBytes := serverState.TileBytesServed()

	serverState.Configure(`"v1"`, 25)
	require.Error(t, runExtract(resumedOutput))
	require.FileExists(t, extractResumePath(resumedOutput))

	state, err := loadExtractResume(extractResumePath(resumedOutput))
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Greater(t, completedExtractResumeBytes(state.Completed), uint64(0))
	assert.Less(t, completedExtractResumeBytes(state.Completed), header.TileDataLength)

	serverState.Configure(`"v1"`, -1)
	require.NoError(t, runExtract(resumedOutput))
	assert.NoFileExists(t, extractResumePath(resumedOutput))
	assert.Less(t, serverState.TileBytesServed(), fullTileBytes)

	fullBytes, err := os.ReadFile(fullOutput)
	require.NoError(t, err)
	resumedBytes, err := os.ReadFile(resumedOutput)
	require.NoError(t, err)
	assert.Equal(t, fullBytes, resumedBytes)
	assert.NoError(t, Verify(testLogger, resumedOutput))
}

func TestExtractResumeStartsOverWhenSourceChanges(t *testing.T) {
	archive := makeExtractTestArchive(t)
	header, err := DeserializeHeader(archive[0:HeaderV3LenBytes])
	require.NoError(t, err)

	tmp := t.TempDir()
	output := filepath.Join(tmp, "changed.pmtiles")
	testLogger := log.New(io.Discard, "", 0)

	serverState := &extractResumeTestServer{
		data:           archive,
		tileDataOffset: header.TileDataOffset,
		etag:           `"v1"`,
	}
	httpServer := httptest.NewServer(serverState)
	defer httpServer.Close()

	runExtract := func() error {
		return Extract(context.Background(), testLogger, "", httpServer.URL+"/archive.pmtiles", -1, -1, "", "", output, 4, 0.05, false)
	}

	serverState.Configure(`"v1"`, 25)
	require.Error(t, runExtract())
	require.FileExists(t, extractResumePath(output))

	// A different ETag means the partial output cannot be trusted.
	serverState.Configure(`"v2"`, -1)
	require.NoError(t, runExtract())
	assert.NoFileExists(t, extractResumePath(output))
	assert.NoError(t, Verify(testLogger, output))
}

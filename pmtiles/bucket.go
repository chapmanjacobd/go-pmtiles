package pmtiles

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyHttp "github.com/aws/smithy-go/transport/http"
	"github.com/cespare/xxhash/v2"
	"gocloud.dev/blob"
	"google.golang.org/api/googleapi"
)

// Bucket is an abstration over a gocloud or plain HTTP bucket.
type Bucket interface {
	Close() error
	NewRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error)
	NewRangeReaderEtag(ctx context.Context, key string, offset int64, length int64, etag string) (io.ReadCloser, string, int, error)
}

// RefreshRequiredError is an error that indicates the etag has chanced on the remote file
type RefreshRequiredError struct {
	StatusCode int
}

func (m *RefreshRequiredError) Error() string {
	return fmt.Sprintf("HTTP error indicates file has changed: %d", m.StatusCode)
}

// HTTPStatusError is an unexpected HTTP status in reply to a range request.
type HTTPStatusError struct {
	StatusCode int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("HTTP error: %d", e.StatusCode)
}

// ErrRangeNotHonored means the server did not return exactly the requested
// byte range (e.g. a 200 with the whole file), so the bytes cannot be trusted.
var ErrRangeNotHonored = errors.New("server did not return the requested byte range")

type mockBucket struct {
	items map[string][]byte
}

func (m mockBucket) Close() error {
	return nil
}

func (m mockBucket) NewRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	body, _, _, err := m.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err

}
func (m mockBucket) NewRangeReaderEtag(_ context.Context, key string, offset int64, length int64, etag string) (io.ReadCloser, string, int, error) {
	bs, ok := m.items[key]
	if !ok {
		return nil, "", 404, fmt.Errorf("Not found %s", key)
	}

	resultEtag := generateEtag(bs)
	if len(etag) > 0 && resultEtag != etag {
		return nil, "", 412, &RefreshRequiredError{}
	}
	if offset >= int64(len(bs)) {
		return nil, "", 416, &RefreshRequiredError{416}
	}

	end := offset + length
	if end > int64(len(bs)) {
		end = int64(len(bs))
	}
	return io.NopCloser(bytes.NewReader(bs[offset:end])), resultEtag, 206, nil
}

// FileBucket is a bucket backed by a directory on disk
type FileBucket struct {
	path string
}

// NewFileBucket initializes a FileBucket and returns a new instance
func NewFileBucket(path string) *FileBucket {
	return &FileBucket{path: path}
}

func (b FileBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func uintToBytes(n uint64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, n)
	return bs
}

func hasherToEtag(hasher *xxhash.Digest) string {
	sum := uintToBytes(hasher.Sum64())
	return fmt.Sprintf(`"%s"`, hex.EncodeToString(sum))
}

func generateEtag(data []byte) string {
	hasher := xxhash.New()
	hasher.Write(data)
	return hasherToEtag(hasher)
}

func generateEtagFromInts(ns ...int64) string {
	hasher := xxhash.New()
	for _, n := range ns {
		hasher.Write(uintToBytes(uint64(n)))
	}
	return hasherToEtag(hasher)
}

func (b FileBucket) NewRangeReaderEtag(_ context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	name := filepath.Join(b.path, key)
	file, err := os.Open(name)
	defer file.Close()
	if err != nil {
		return nil, "", 404, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, "", 404, err
	}
	newEtag := generateEtagFromInts(info.ModTime().UnixNano(), info.Size())
	if len(etag) > 0 && etag != newEtag {
		return nil, "", 412, &RefreshRequiredError{}
	}
	result := make([]byte, length)
	read, err := file.ReadAt(result, offset)

	if err == io.EOF {
		part := result[0:read]
		return io.NopCloser(bytes.NewReader(part)), newEtag, 206, nil
	}

	if err != nil {
		return nil, "", 500, err
	}
	if read != int(length) {
		return nil, "", 416, fmt.Errorf("Expected to read %d bytes but only read %d", length, read)
	}

	return io.NopCloser(bytes.NewReader(result)), newEtag, 206, nil
}

func (b FileBucket) Close() error {
	return nil
}

// HTTPClient is an interface that lets you swap out the default client with a mock one in tests
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPBucket struct {
	baseURL string
	client  HTTPClient
}

// newDownloadClient returns an HTTP client for bulk range downloads.
// http.DefaultClient keeps only 2 idle connections per host, so with more
// download threads every extra request opens a new TCP+TLS connection.
// HTTP/2 is disabled because it multiplexes all requests to a host over a
// single TCP connection, which caps parallel range downloads at the
// throughput of that one connection.
func newDownloadClient(connections int) *http.Client {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	// same settings as http.DefaultTransport, except the protocols and idle connections
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           idleTimeoutDialer(15*time.Second, downloadIdleTimeout),
		Protocols:             protocols,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   max(connections, 2),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{Transport: transport}
}

// downloadIdleTimeout aborts a download connection that receives no bytes for
// this long, so a stalled request is retried (aria2's --timeout).
const downloadIdleTimeout = 60 * time.Second

// idleTimeoutConn fails a Read that gets no data within timeout.
type idleTimeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (c *idleTimeoutConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

func idleTimeoutDialer(connectTimeout time.Duration, idleTimeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &idleTimeoutConn{conn, idleTimeout}, nil
	}
}

// strictRangeClient rejects any response to a range request that is not
// exactly the requested range, which would otherwise silently corrupt data.
type strictRangeClient struct {
	HTTPClient
}

func (c strictRangeClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.HTTPClient.Do(req)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent) {
		return resp, err
	}
	var first, last int64
	if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &first, &last); err != nil {
		return resp, nil
	}
	if err := checkContentRange(resp, first, last-first+1); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// checkContentRange verifies that a response carries exactly the bytes
// [offset, offset+length) that were requested.
func checkContentRange(resp *http.Response, offset int64, length int64) error {
	if resp.StatusCode == http.StatusOK {
		// a 200 is only the requested range when that range is the whole file
		if offset == 0 && resp.ContentLength == length {
			return nil
		}
		return fmt.Errorf("%w: got 200 for bytes %d-%d", ErrRangeNotHonored, offset, offset+length-1)
	}
	var first, last int64
	contentRange := resp.Header.Get("Content-Range")
	if _, err := fmt.Sscanf(contentRange, "bytes %d-%d/", &first, &last); err != nil || first != offset || last != offset+length-1 {
		return fmt.Errorf("%w: asked for bytes %d-%d, got Content-Range %q", ErrRangeNotHonored, offset, offset+length-1, contentRange)
	}
	return nil
}

func (b HTTPBucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := b.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func (b HTTPBucket) NewRangeReaderEtag(ctx context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	reqURL := b.baseURL + "/" + key

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, "", 500, err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	if len(etag) > 0 {
		req.Header.Set("If-Match", etag)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		if resp != nil {
			return nil, "", resp.StatusCode, err
		} else {
			return nil, "", 1, err
		}
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		if isRefreshRequiredCode(resp.StatusCode) {
			err = &RefreshRequiredError{resp.StatusCode}
		} else {
			err = &HTTPStatusError{resp.StatusCode}
		}
		return nil, "", resp.StatusCode, err
	}

	return resp.Body, resp.Header.Get("ETag"), resp.StatusCode, nil
}

func (b HTTPBucket) Close() error {
	return nil
}

func isRefreshRequiredCode(code int) bool {
	return code == http.StatusPreconditionFailed || code == http.StatusRequestedRangeNotSatisfiable
}

type BucketAdapter struct {
	Bucket *blob.Bucket
}

func (ba BucketAdapter) NewRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	body, _, _, err := ba.NewRangeReaderEtag(ctx, key, offset, length, "")
	return body, err
}

func etagToGeneration(etag string) int64 {
	i, _ := strconv.ParseInt(etag, 10, 64)
	return i
}

func generationToEtag(generation int64) string {
	return strconv.FormatInt(generation, 10)
}

func setProviderEtag(asFunc func(interface{}) bool, etag string) {
	var awsV2Req *s3.GetObjectInput
	var azblobReq *azblob.DownloadStreamOptions
	var gcsHandle **storage.ObjectHandle
	if asFunc(&awsV2Req) {
		awsV2Req.IfMatch = aws.String(etag)
	} else if asFunc(&azblobReq) {
		azEtag := azcore.ETag(etag)
		azblobReq.AccessConditions = &azblob.AccessConditions{
			ModifiedAccessConditions: &container.ModifiedAccessConditions{
				IfMatch: &azEtag,
			},
		}
	} else if asFunc(&gcsHandle) {
		*gcsHandle = (*gcsHandle).If(storage.Conditions{
			GenerationMatch: etagToGeneration(etag),
		})
	}
}

func getProviderErrorStatusCode(err error) int {
	var awsV2Err *smithyHttp.ResponseError
	var azureErr *azcore.ResponseError
	var gcpErr *googleapi.Error

	if errors.As(err, &awsV2Err); awsV2Err != nil {
		return awsV2Err.HTTPStatusCode()
	} else if errors.As(err, &azureErr); azureErr != nil {
		return azureErr.StatusCode
	} else if errors.As(err, &gcpErr); gcpErr != nil {
		return gcpErr.Code
	}
	return 404
}

func getProviderEtag(reader *blob.Reader) string {
	var awsV2Resp s3.GetObjectOutput
	var azureResp azblob.DownloadStreamResponse
	var gcpResp *storage.Reader

	if reader.As(&awsV2Resp) {
		return *awsV2Resp.ETag
	} else if reader.As(&azureResp) {
		return string(*azureResp.ETag)
	} else if reader.As(&gcpResp) {
		return generationToEtag(gcpResp.Attrs.Generation)
	}

	return ""
}

func (ba BucketAdapter) NewRangeReaderEtag(ctx context.Context, key string, offset, length int64, etag string) (io.ReadCloser, string, int, error) {
	reader, err := ba.Bucket.NewRangeReader(ctx, key, offset, length, &blob.ReaderOptions{
		BeforeRead: func(asFunc func(interface{}) bool) error {
			if len(etag) > 0 {
				setProviderEtag(asFunc, etag)
			}
			return nil
		},
	})
	status := 206
	if err != nil {
		status = getProviderErrorStatusCode(err)
		if isRefreshRequiredCode(status) {
			return nil, "", status, &RefreshRequiredError{status}
		}

		return nil, "", status, err
	}

	return reader, getProviderEtag(reader), status, nil
}

func (ba BucketAdapter) Close() error {
	return ba.Bucket.Close()
}

func NormalizeBucketKey(bucket string, prefix string, key string) (string, string, error) {
	if bucket == "" {
		if strings.HasPrefix(key, "http") {
			u, err := url.Parse(key)
			if err != nil {
				return "", "", err
			}
			dir, file := path.Split(u.Path)
			if strings.HasSuffix(dir, "/") {
				dir = dir[:len(dir)-1]
			}
			keyWithQuery := file
			if u.RawQuery != "" {
				keyWithQuery = keyWithQuery + "?" + u.RawQuery
			}
			return u.Scheme + "://" + u.Host + dir, keyWithQuery, nil
		}
		fileprotocol := "file://"
		if string(os.PathSeparator) != "/" {
			fileprotocol += "/"
		}
		if prefix != "" {
			abs, err := filepath.Abs(prefix)
			if err != nil {
				return "", "", err
			}
			return fileprotocol + filepath.ToSlash(abs), key, nil
		}
		abs, err := filepath.Abs(key)
		if err != nil {
			return "", "", err
		}
		return fileprotocol + filepath.ToSlash(filepath.Dir(abs)), filepath.Base(abs), nil
	}
	return bucket, key, nil
}

func OpenBucket(ctx context.Context, bucketURL string, bucketPrefix string) (Bucket, error) {
	if strings.HasPrefix(bucketURL, "http") {
		bucket := HTTPBucket{bucketURL, http.DefaultClient}
		return bucket, nil
	}
	if strings.HasPrefix(bucketURL, "file") {
		fileprotocol := "file://"
		if string(os.PathSeparator) != "/" {
			fileprotocol += "/"
		}
		path := strings.Replace(bucketURL, fileprotocol, "", 1)
		bucket := NewFileBucket(filepath.FromSlash(path))
		return bucket, nil
	}
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}
	if bucketPrefix != "" && bucketPrefix != "/" && bucketPrefix != "." {
		bucket = blob.PrefixedBucket(bucket, path.Clean(bucketPrefix)+string(os.PathSeparator))
	}
	wrappedBucket := BucketAdapter{bucket}
	return wrappedBucket, err
}

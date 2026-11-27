package otlp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/snappy"
)

// snappyFramingHeader is the stream identifier chunk that starts every snappy
// framed stream.
var snappyFramingHeader = []byte{0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y'}

// newSnappyReader returns a reader over the decoded content of a snappy
// request body.
//
// The OpenTelemetry Collector sends "Content-Encoding: snappy" in two shapes.
// Current releases send a raw snappy block, which is what the header means.
// Older ones sent the framed stream format under the same header. Framed
// streams are told apart by their stream identifier, the same way the
// Collector's own receiver does it.
//
// The block format has to be decoded in one piece, so the decoded length it
// declares is checked against maxSize before anything is allocated for it.
func newSnappyReader(r io.Reader, maxSize int64) (io.Reader, error) {
	br := bufio.NewReader(r)
	head, err := br.Peek(len(snappyFramingHeader))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if bytes.Equal(head, snappyFramingHeader) {
		return snappy.NewReader(br), nil
	}

	compressed, err := io.ReadAll(br)
	if err != nil {
		return nil, err
	}
	return decodeSnappyBlock(compressed, maxSize)
}

func decodeSnappyBlock(compressed []byte, maxSize int64) (io.Reader, error) {
	decodedLen, err := snappy.DecodedLen(compressed)
	if err != nil {
		return nil, err
	}
	if int64(decodedLen) > maxSize {
		return nil, fmt.Errorf("decompressed body exceeds maximum size of %d bytes", maxSize)
	}
	decoded, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(decoded), nil
}

// newSnappyFramedReader returns a reader over a snappy framed stream, which is
// what "Content-Encoding: x-snappy-framed" carries.
func newSnappyFramedReader(r io.Reader) io.Reader {
	return snappy.NewReader(r)
}

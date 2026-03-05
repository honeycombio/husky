package compat07

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// capturesDir returns the path to CloudWatch capture data, or skips the test.
// Set CLOUDWATCH_CAPTURES_DIR to point at a directory containing otlp-0.7.0/
// and otlp-1.0.0/ subdirectories of varint-length-delimited capture files.
func capturesDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("CLOUDWATCH_CAPTURES_DIR")
	if dir == "" {
		t.Skip("CLOUDWATCH_CAPTURES_DIR not set")
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Skipf("CLOUDWATCH_CAPTURES_DIR path does not exist: %s", dir)
	}
	return dir
}

// readCaptureFile reads varint-length-delimited ExportMetricsServiceRequest
// records from a CloudWatch Metric Stream capture file, returning all metrics.
func readCaptureFile(t *testing.T, path string) []*metricspb.Metric {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	reader := bufio.NewReader(f)
	var allMetrics []*metricspb.Metric

	for {
		length, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "reading varint length prefix")

		data := make([]byte, length)
		_, err = io.ReadFull(reader, data)
		require.NoError(t, err, "reading record bytes")

		var req collectormetricspb.ExportMetricsServiceRequest
		require.NoError(t, proto.Unmarshal(data, &req), "unmarshaling record")

		for _, rm := range req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				allMetrics = append(allMetrics, sm.GetMetrics()...)
			}
		}
	}

	require.NotEmpty(t, allMetrics, "capture file %s had no metrics", path)
	return allMetrics
}

// findCaptureFiles returns all files under the given subdirectory.
func findCaptureFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, files, "no capture files found in %s", dir)
	return files
}

func TestCloudWatch07_DetectAndConvert(t *testing.T) {
	dir := capturesDir(t)
	files := findCaptureFiles(t, filepath.Join(dir, "otlp-0.7.0"))

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			metrics := readCaptureFile(t, file)

			// 0.7 captures should be detected as having 0.7 data (labels on data points).
			assert.True(t, Has07Data(metrics), "expected Has07Data=true for 0.7 capture")

			converted, err := ConvertMetrics(metrics)
			require.NoError(t, err)
			require.Len(t, converted, len(metrics))

			for i, m := range converted {
				require.NotNilf(t, m.GetData(), "metric %d (%s) has nil Data after conversion", i, m.GetName())

				// CloudWatch sends Summary metrics.
				if summary := m.GetSummary(); summary != nil {
					for j, dp := range summary.GetDataPoints() {
						// Labels should have been converted to attributes.
						assert.NotEmptyf(t, dp.GetAttributes(),
							"metric %d (%s) dp %d: expected attributes after label conversion", i, m.GetName(), j)

						// Unknown field 1 (labels) should be consumed.
						assert.Falsef(t, hasField(dp.ProtoReflect().GetUnknown(), 1),
							"metric %d (%s) dp %d: labels still present as unknown field 1", i, m.GetName(), j)
					}
				}
			}
		})
	}
}

func TestCloudWatch10_Passthrough(t *testing.T) {
	dir := capturesDir(t)
	files := findCaptureFiles(t, filepath.Join(dir, "otlp-1.0.0"))

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			metrics := readCaptureFile(t, file)

			// 1.0 captures should have no 0.7 data.
			assert.False(t, Has07Data(metrics), "expected Has07Data=false for 1.0 capture")

			converted, err := ConvertMetrics(metrics)
			require.NoError(t, err)
			require.Len(t, converted, len(metrics))

			for i, m := range converted {
				require.NotNilf(t, m.GetData(), "metric %d (%s) has nil Data", i, m.GetName())
			}
		})
	}
}

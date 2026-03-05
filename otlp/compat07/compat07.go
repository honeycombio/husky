package compat07

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	shadowmetricspb "github.com/honeycombio/husky/otlp/compat07/internal/shadowpb/metricspb"
)

// ResourceMetricsHas07Data checks whether a ResourceMetrics slice contains any
// 0.7-shaped metric data. A single ExportMetricsServiceRequest is serialized by
// one SDK version, so the entire request is uniformly 0.7 or 1.x. This function
// scans metrics until it finds a definitive signal (0.7 labels/types or 1.x
// attributes) and returns early. Metrics with no dimensions are inconclusive
// and are skipped.
func ResourceMetricsHas07Data(resourceMetrics []*metricspb.ResourceMetrics) bool {
	for _, rm := range resourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				switch check07Metric(m) {
				case detected07:
					return true
				case detected1x:
					return false
				}
			}
		}
	}
	return false
}

// ConvertMetrics converts a slice of Metrics from 0.7 to 1.x shape.
// For each metric:
//   - If it's already fully 1.x, returns it unchanged.
//   - If it contains 0.7 metric types (int_gauge/int_sum/int_histogram in unknown fields),
//     converts to the 1.x equivalent and returns it.
//   - If it contains 0.7 labels on any metric data points, converts those to attributes.
//
// Returns an error only if 0.7 data is present but malformed/unparseable.
// The returned slice is always new, but metrics with recognized 1.x types are
// mutated in place: 0.7 labels are converted to attributes and the corresponding
// unknown field bytes are removed from their data points and exemplars.
func ConvertMetrics(metrics []*metricspb.Metric) ([]*metricspb.Metric, error) {
	result := make([]*metricspb.Metric, len(metrics))
	for i, m := range metrics {
		converted, err := convertMetric(m)
		if err != nil {
			return nil, fmt.Errorf("compat07: metric %q (index %d): %w", m.GetName(), i, err)
		}
		result[i] = converted
	}
	return result, nil
}

// metricVersion is the result of checking a single metric for 0.7 vs 1.x signals.
type metricVersion int

const (
	inconclusive metricVersion = iota // no definitive signal: dimensionless data points, empty metric, or unrecognized unknown fields
	detected07                        // 0.7 metric type or labels found
	detected1x                        // 1.x attributes found
)

// Has07Data is a cheaper check that returns true if any metric in the slice
// contains 0.7 unknown fields, without converting. Useful for
// logging/counting 0.7 traffic.
func Has07Data(metrics []*metricspb.Metric) bool {
	for _, m := range metrics {
		switch check07Metric(m) {
		case detected07:
			return true
		case detected1x:
			return false
		}
	}
	return false
}

// check07Metric inspects a single metric for 0.7 vs 1.x signals.
func check07Metric(m *metricspb.Metric) metricVersion {
	// No recognized 1.x data variant — check for 0.7 metric types
	// (IntGauge=4, IntSum=6, IntHistogram=8) in unknown fields.
	if m.GetData() == nil {
		unknown := m.ProtoReflect().GetUnknown()
		if hasField(unknown, 4) || hasField(unknown, 6) || hasField(unknown, 8) {
			return detected07
		}
		return inconclusive
	}
	// Recognized 1.x type — check data points for 0.7 labels.
	// A batch will not mix attributes and labels, so finding 1.x
	// attributes on any data point means the entire batch is 1.x.
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			if len(dp.GetAttributes()) > 0 {
				return detected1x
			}
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return detected07
			}
		}
	case *metricspb.Metric_Sum:
		for _, dp := range d.Sum.GetDataPoints() {
			if len(dp.GetAttributes()) > 0 {
				return detected1x
			}
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return detected07
			}
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			if len(dp.GetAttributes()) > 0 {
				return detected1x
			}
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return detected07
			}
		}
	case *metricspb.Metric_Summary:
		for _, dp := range d.Summary.GetDataPoints() {
			if len(dp.GetAttributes()) > 0 {
				return detected1x
			}
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return detected07
			}
		}
	}
	return inconclusive
}

// convertMetric handles a single metric, detecting and converting 0.7 data.
func convertMetric(m *metricspb.Metric) (*metricspb.Metric, error) {
	if m.GetData() == nil {
		// No recognized data variant from stable proto, check for 0.7 metric types
		unknownBytes := m.ProtoReflect().GetUnknown()
		if len(unknownBytes) == 0 {
			return m, nil
		}
		return convertUnknownMetricData(m, unknownBytes)
	}
	// Metric type is recognizable with stable proto, but data points may have 0.7 labels.
	// e.g. 0.7 DoubleSum became 1.x Sum, same proto field number but 0.7 has Labels while 1.x has Attributes
	return convertDataPointLabels(m)
}

// convertUnknownMetricData tries to extract IntGauge (field 4), IntSum (field 6),
// or IntHistogram (field 8) from the metric's unknown fields.
func convertUnknownMetricData(m *metricspb.Metric, unknownBytes []byte) (*metricspb.Metric, error) {
	// Create a new metric preserving the original's metadata.
	newMetric := func(remaining []byte) *metricspb.Metric {
		result := &metricspb.Metric{
			Name:        m.GetName(),
			Description: m.GetDescription(),
			Unit:        m.GetUnit(),
			Metadata:    m.GetMetadata(),
		}
		if len(remaining) > 0 {
			result.ProtoReflect().SetUnknown(remaining)
		}
		return result
	}

	// extractField returns all occurrences, but these are oneof fields in the
	// Metric proto — a real 0.7 SDK will only produce one. Using values[0] ought to be OK.

	// Try IntGauge (field 4)
	if values, remaining, err := extractField(unknownBytes, 4); err != nil {
		return nil, fmt.Errorf("extract IntGauge: %w", err)
	} else if len(values) > 0 {
		var ig shadowmetricspb.IntGauge
		if err := proto.Unmarshal(values[0], &ig); err != nil {
			return nil, fmt.Errorf("unmarshal IntGauge: %w", err)
		}
		result := newMetric(remaining)
		result.Data = &metricspb.Metric_Gauge{Gauge: convertIntGauge(&ig)}
		return result, nil
	}

	// Try IntSum (field 6)
	if values, remaining, err := extractField(unknownBytes, 6); err != nil {
		return nil, fmt.Errorf("extract IntSum: %w", err)
	} else if len(values) > 0 {
		var is shadowmetricspb.IntSum
		if err := proto.Unmarshal(values[0], &is); err != nil {
			return nil, fmt.Errorf("unmarshal IntSum: %w", err)
		}
		result := newMetric(remaining)
		result.Data = &metricspb.Metric_Sum{Sum: convertIntSum(&is)}
		return result, nil
	}

	// Try IntHistogram (field 8)
	if values, remaining, err := extractField(unknownBytes, 8); err != nil {
		return nil, fmt.Errorf("extract IntHistogram: %w", err)
	} else if len(values) > 0 {
		var ih shadowmetricspb.IntHistogram
		if err := proto.Unmarshal(values[0], &ih); err != nil {
			return nil, fmt.Errorf("unmarshal IntHistogram: %w", err)
		}
		result := newMetric(remaining)
		result.Data = &metricspb.Metric_Histogram{Histogram: convertIntHistogram(&ih)}
		return result, nil
	}

	// No recognized 0.7 data; pass through unchanged
	return m, nil
}

// convertDataPointLabels walks data points and exemplars of a metric whose type
// is recognizable with stable proto, converting any 0.7 labels/filtered_labels
// from unknown fields to attributes/filtered_attributes.
func convertDataPointLabels(m *metricspb.Metric) (*metricspb.Metric, error) {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			if err := convertNumberDataPointLabels(dp); err != nil {
				return nil, err
			}
		}
	case *metricspb.Metric_Sum:
		for _, dp := range d.Sum.GetDataPoints() {
			if err := convertNumberDataPointLabels(dp); err != nil {
				return nil, err
			}
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			if err := convertHistogramDataPointLabels(dp); err != nil {
				return nil, err
			}
		}
	case *metricspb.Metric_Summary:
		for _, dp := range d.Summary.GetDataPoints() {
			if err := convertSummaryDataPointLabels(dp); err != nil {
				return nil, err
			}
		}
		// ExponentialHistogram did not exist in 0.7, no label conversion needed
	}
	return m, nil
}

// convertNumberDataPointLabels extracts labels (field 1) from a NumberDataPoint's
// unknown fields and appends them as attributes.
func convertNumberDataPointLabels(dp *metricspb.NumberDataPoint) error {
	unknownBytes := dp.ProtoReflect().GetUnknown()
	if len(unknownBytes) == 0 {
		return nil
	}
	attrs, remaining, err := convertLabelsToAttributes(unknownBytes)
	if err != nil {
		return err
	}
	if len(attrs) > 0 {
		dp.Attributes = append(dp.Attributes, attrs...)
		dp.ProtoReflect().SetUnknown(remaining)
	}
	for _, ex := range dp.GetExemplars() {
		if err := convertExemplarFilteredLabels(ex); err != nil {
			return err
		}
	}
	return nil
}

// convertHistogramDataPointLabels is like convertNumberDataPointLabels but for HistogramDataPoint.
func convertHistogramDataPointLabels(dp *metricspb.HistogramDataPoint) error {
	unknownBytes := dp.ProtoReflect().GetUnknown()
	if len(unknownBytes) == 0 {
		return nil
	}
	attrs, remaining, err := convertLabelsToAttributes(unknownBytes)
	if err != nil {
		return err
	}
	if len(attrs) > 0 {
		dp.Attributes = append(dp.Attributes, attrs...)
		dp.ProtoReflect().SetUnknown(remaining)
	}
	for _, ex := range dp.GetExemplars() {
		if err := convertExemplarFilteredLabels(ex); err != nil {
			return err
		}
	}
	return nil
}

// convertSummaryDataPointLabels is like convertNumberDataPointLabels but for SummaryDataPoint.
func convertSummaryDataPointLabels(dp *metricspb.SummaryDataPoint) error {
	unknownBytes := dp.ProtoReflect().GetUnknown()
	if len(unknownBytes) == 0 {
		return nil
	}
	attrs, remaining, err := convertLabelsToAttributes(unknownBytes)
	if err != nil {
		return err
	}
	if len(attrs) > 0 {
		dp.Attributes = append(dp.Attributes, attrs...)
		dp.ProtoReflect().SetUnknown(remaining)
	}
	return nil
}

// convertExemplarFilteredLabels extracts filtered_labels (field 1) from an
// Exemplar's unknown fields and appends them as filtered_attributes.
func convertExemplarFilteredLabels(ex *metricspb.Exemplar) error {
	unknownBytes := ex.ProtoReflect().GetUnknown()
	if len(unknownBytes) == 0 {
		return nil
	}
	attrs, remaining, err := convertLabelsToAttributes(unknownBytes)
	if err != nil {
		return err
	}
	if len(attrs) > 0 {
		ex.FilteredAttributes = append(ex.FilteredAttributes, attrs...)
		ex.ProtoReflect().SetUnknown(remaining)
	}
	return nil
}

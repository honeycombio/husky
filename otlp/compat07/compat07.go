package compat07

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	shadowmetricspb "github.com/honeycombio/husky/otlp/compat07/internal/shadowpb/metricspb"
)

// DetectAndConvertMetrics checks a slice of Metrics for 0.7-era data.
// For each metric:
//   - If it contains 0.7 data (int_gauge/int_sum/int_histogram in unknown fields),
//     converts to the 1.x equivalent and returns it.
//   - If it contains 0.7-era labels on data points, converts those to attributes.
//   - If it's already fully 1.x, returns it unchanged.
//
// Returns an error only if 0.7 data is present but malformed/unparseable.
// The input slice is not modified; individual data points within recognized
// metrics with recognized types may be mutated (labels converted to attributes in place).
func DetectAndConvertMetrics(metrics []*metricspb.Metric) ([]*metricspb.Metric, bool, error) {
	result := make([]*metricspb.Metric, len(metrics))
	var any07 bool
	for i, m := range metrics {
		converted, was07, err := convertMetric(m)
		if err != nil {
			return nil, false, fmt.Errorf("compat07: metric %q (index %d): %w", m.GetName(), i, err)
		}
		result[i] = converted
		if was07 {
			any07 = true
		}
	}
	return result, any07, nil
}

// Has07Data is a cheaper check that returns true if any metric in the slice
// contains 0.7-era unknown fields, without converting. Useful for
// logging/counting 0.7 traffic.
func Has07Data(metrics []*metricspb.Metric) bool {
	for _, m := range metrics {
		// Check for 0.7 metric types (IntGauge=4, IntSum=6, IntHistogram=8)
		if m.GetData() == nil {
			unknown := m.ProtoReflect().GetUnknown()
			if hasField(unknown, 4) || hasField(unknown, 6) || hasField(unknown, 8) {
				return true
			}
		}
		// Check for 0.7 labels on data points (field 1 in unknown fields)
		if hasLabelsInDataPoints(m) {
			return true
		}
	}
	return false
}

// convertMetric handles a single metric, detecting and converting 0.7 data.
// Returns the (possibly converted) metric and whether any 0.7 data was found.
func convertMetric(m *metricspb.Metric) (*metricspb.Metric, bool, error) {
	if m.GetData() == nil {
		// No recognized data variant from stable proto, check for 0.7 metric types
		unknownBytes := m.ProtoReflect().GetUnknown()
		if len(unknownBytes) == 0 {
			return m, false, nil
		}
		return convertUnknownMetricData(m, unknownBytes)
	}
	// Metric type is recognizable with stable proto, but data points may have 0.7-era labels.
	// e.g. 0.7 DoubleSum became 1.x Sum, same proto field number but 0.7 has Labels while 1.x has Attributes
	return convertDataPointLabels(m)
}

// convertUnknownMetricData tries to extract IntGauge (field 4), IntSum (field 6),
// or IntHistogram (field 8) from the metric's unknown fields.
func convertUnknownMetricData(m *metricspb.Metric, unknownBytes []byte) (*metricspb.Metric, bool, error) {
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

	// Try IntGauge (field 4)
	if values, remaining, err := extractField(unknownBytes, 4); err != nil {
		return nil, false, fmt.Errorf("extract IntGauge: %w", err)
	} else if len(values) > 0 {
		var ig shadowmetricspb.IntGauge
		if err := proto.Unmarshal(values[0], &ig); err != nil {
			return nil, false, fmt.Errorf("unmarshal IntGauge: %w", err)
		}
		result := newMetric(remaining)
		result.Data = &metricspb.Metric_Gauge{Gauge: convertIntGauge(&ig)}
		return result, true, nil
	}

	// Try IntSum (field 6)
	if values, remaining, err := extractField(unknownBytes, 6); err != nil {
		return nil, false, fmt.Errorf("extract IntSum: %w", err)
	} else if len(values) > 0 {
		var is shadowmetricspb.IntSum
		if err := proto.Unmarshal(values[0], &is); err != nil {
			return nil, false, fmt.Errorf("unmarshal IntSum: %w", err)
		}
		result := newMetric(remaining)
		result.Data = &metricspb.Metric_Sum{Sum: convertIntSum(&is)}
		return result, true, nil
	}

	// Try IntHistogram (field 8)
	if values, remaining, err := extractField(unknownBytes, 8); err != nil {
		return nil, false, fmt.Errorf("extract IntHistogram: %w", err)
	} else if len(values) > 0 {
		var ih shadowmetricspb.IntHistogram
		if err := proto.Unmarshal(values[0], &ih); err != nil {
			return nil, false, fmt.Errorf("unmarshal IntHistogram: %w", err)
		}
		result := newMetric(remaining)
		result.Data = &metricspb.Metric_Histogram{Histogram: convertIntHistogram(&ih)}
		return result, true, nil
	}

	// No recognized 0.7 data; pass through unchanged
	return m, false, nil
}

// convertDataPointLabels walks data points and exemplars of a metric whose type
// is recognizable with stable proto, converting any 0.7-era labels/filtered_labels
// from unknown fields to attributes/filtered_attributes.
func convertDataPointLabels(m *metricspb.Metric) (*metricspb.Metric, bool, error) {
	var converted bool
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			if c, err := convertNumberDataPointLabels(dp); err != nil {
				return nil, false, err
			} else if c {
				converted = true
			}
		}
	case *metricspb.Metric_Sum:
		for _, dp := range d.Sum.GetDataPoints() {
			if c, err := convertNumberDataPointLabels(dp); err != nil {
				return nil, false, err
			} else if c {
				converted = true
			}
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			if c, err := convertHistogramDataPointLabels(dp); err != nil {
				return nil, false, err
			} else if c {
				converted = true
			}
		}
	case *metricspb.Metric_Summary:
		for _, dp := range d.Summary.GetDataPoints() {
			if c, err := convertSummaryDataPointLabels(dp); err != nil {
				return nil, false, err
			} else if c {
				converted = true
			}
		}
		// ExponentialHistogram did not exist in 0.7, no label conversion needed
	}
	return m, converted, nil
}

// convertNumberDataPointLabels extracts labels (field 1) from a NumberDataPoint's
// unknown fields and appends them as attributes.
func convertNumberDataPointLabels(dp *metricspb.NumberDataPoint) (bool, error) {
	unknownBytes := dp.ProtoReflect().GetUnknown()
	if len(unknownBytes) == 0 {
		return false, nil
	}
	attrs, remaining, err := convertLabelsToAttributes(unknownBytes)
	if err != nil {
		return false, err
	}
	converted := len(attrs) > 0
	if converted {
		dp.Attributes = append(dp.Attributes, attrs...)
		dp.ProtoReflect().SetUnknown(remaining)
	}
	for _, ex := range dp.GetExemplars() {
		if err := convertExemplarFilteredLabels(ex); err != nil {
			return false, err
		}
	}
	return converted, nil
}

// convertHistogramDataPointLabels is like convertNumberDataPointLabels but for HistogramDataPoint.
func convertHistogramDataPointLabels(dp *metricspb.HistogramDataPoint) (bool, error) {
	unknownBytes := dp.ProtoReflect().GetUnknown()
	if len(unknownBytes) == 0 {
		return false, nil
	}
	attrs, remaining, err := convertLabelsToAttributes(unknownBytes)
	if err != nil {
		return false, err
	}
	converted := len(attrs) > 0
	if converted {
		dp.Attributes = append(dp.Attributes, attrs...)
		dp.ProtoReflect().SetUnknown(remaining)
	}
	for _, ex := range dp.GetExemplars() {
		if err := convertExemplarFilteredLabels(ex); err != nil {
			return false, err
		}
	}
	return converted, nil
}

// convertSummaryDataPointLabels is like convertNumberDataPointLabels but for SummaryDataPoint.
func convertSummaryDataPointLabels(dp *metricspb.SummaryDataPoint) (bool, error) {
	unknownBytes := dp.ProtoReflect().GetUnknown()
	if len(unknownBytes) == 0 {
		return false, nil
	}
	attrs, remaining, err := convertLabelsToAttributes(unknownBytes)
	if err != nil {
		return false, err
	}
	converted := len(attrs) > 0
	if converted {
		dp.Attributes = append(dp.Attributes, attrs...)
		dp.ProtoReflect().SetUnknown(remaining)
	}
	return converted, nil
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

// hasLabelsInDataPoints checks if any data point or exemplar in the metric has
// labels/filtered_labels (field 1) in its unknown fields.
func hasLabelsInDataPoints(m *metricspb.Metric) bool {
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return true
			}
			for _, ex := range dp.GetExemplars() {
				if hasField(ex.ProtoReflect().GetUnknown(), 1) {
					return true
				}
			}
		}
	case *metricspb.Metric_Sum:
		for _, dp := range d.Sum.GetDataPoints() {
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return true
			}
			for _, ex := range dp.GetExemplars() {
				if hasField(ex.ProtoReflect().GetUnknown(), 1) {
					return true
				}
			}
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return true
			}
			for _, ex := range dp.GetExemplars() {
				if hasField(ex.ProtoReflect().GetUnknown(), 1) {
					return true
				}
			}
		}
	case *metricspb.Metric_Summary:
		for _, dp := range d.Summary.GetDataPoints() {
			if hasField(dp.ProtoReflect().GetUnknown(), 1) {
				return true
			}
		}
	}
	return false
}

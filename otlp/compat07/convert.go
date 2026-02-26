package compat07

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	shadowcommonpb "github.com/honeycombio/opentelemetry-proto/compat07/internal/shadowpb/commonpb"
	shadowmetricspb "github.com/honeycombio/opentelemetry-proto/compat07/internal/shadowpb/metricspb"
)

// convertStringKeyValue converts a shadow StringKeyValue to an upstream KeyValue.
func convertStringKeyValue(skv *shadowcommonpb.StringKeyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: skv.GetKey(),
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: skv.GetValue()},
		},
	}
}

// convertIntExemplar converts a shadow IntExemplar to an upstream Exemplar.
func convertIntExemplar(ie *shadowmetricspb.IntExemplar) *metricspb.Exemplar {
	e := &metricspb.Exemplar{
		TimeUnixNano: ie.GetTimeUnixNano(),
		Value:        &metricspb.Exemplar_AsInt{AsInt: ie.GetValue()},
		SpanId:       ie.GetSpanId(),
		TraceId:      ie.GetTraceId(),
	}
	for _, fl := range ie.GetFilteredLabels() {
		e.FilteredAttributes = append(e.FilteredAttributes, convertStringKeyValue(fl))
	}
	return e
}

// convertIntDataPoint converts a shadow IntDataPoint to an upstream NumberDataPoint.
func convertIntDataPoint(idp *shadowmetricspb.IntDataPoint) *metricspb.NumberDataPoint {
	ndp := &metricspb.NumberDataPoint{
		StartTimeUnixNano: idp.GetStartTimeUnixNano(),
		TimeUnixNano:      idp.GetTimeUnixNano(),
		Value:             &metricspb.NumberDataPoint_AsInt{AsInt: idp.GetValue()},
	}
	for _, l := range idp.GetLabels() {
		ndp.Attributes = append(ndp.Attributes, convertStringKeyValue(l))
	}
	for _, ie := range idp.GetExemplars() {
		ndp.Exemplars = append(ndp.Exemplars, convertIntExemplar(ie))
	}
	return ndp
}

// convertIntGauge converts a shadow IntGauge to an upstream Gauge.
func convertIntGauge(ig *shadowmetricspb.IntGauge) *metricspb.Gauge {
	g := &metricspb.Gauge{}
	for _, dp := range ig.GetDataPoints() {
		g.DataPoints = append(g.DataPoints, convertIntDataPoint(dp))
	}
	return g
}

// convertIntSum converts a shadow IntSum to an upstream Sum.
func convertIntSum(is *shadowmetricspb.IntSum) *metricspb.Sum {
	s := &metricspb.Sum{
		AggregationTemporality: metricspb.AggregationTemporality(is.GetAggregationTemporality()),
		IsMonotonic:            is.GetIsMonotonic(),
	}
	for _, dp := range is.GetDataPoints() {
		s.DataPoints = append(s.DataPoints, convertIntDataPoint(dp))
	}
	return s
}

// convertIntHistogramDataPoint converts a shadow IntHistogramDataPoint to an upstream HistogramDataPoint.
func convertIntHistogramDataPoint(ihdp *shadowmetricspb.IntHistogramDataPoint) *metricspb.HistogramDataPoint {
	sum := float64(ihdp.GetSum())
	hdp := &metricspb.HistogramDataPoint{
		StartTimeUnixNano: ihdp.GetStartTimeUnixNano(),
		TimeUnixNano:      ihdp.GetTimeUnixNano(),
		Count:             ihdp.GetCount(),
		Sum:               &sum,
		BucketCounts:      ihdp.GetBucketCounts(),
		ExplicitBounds:    ihdp.GetExplicitBounds(),
	}
	for _, l := range ihdp.GetLabels() {
		hdp.Attributes = append(hdp.Attributes, convertStringKeyValue(l))
	}
	for _, ie := range ihdp.GetExemplars() {
		hdp.Exemplars = append(hdp.Exemplars, convertIntExemplar(ie))
	}
	return hdp
}

// convertIntHistogram converts a shadow IntHistogram to an upstream Histogram.
func convertIntHistogram(ih *shadowmetricspb.IntHistogram) *metricspb.Histogram {
	h := &metricspb.Histogram{
		AggregationTemporality: metricspb.AggregationTemporality(ih.GetAggregationTemporality()),
	}
	for _, dp := range ih.GetDataPoints() {
		h.DataPoints = append(h.DataPoints, convertIntHistogramDataPoint(dp))
	}
	return h
}

// convertLabelsToAttributes extracts StringKeyValue labels (field 1) from
// unknown field bytes and converts them to KeyValue attributes.
// Returns the converted attributes, remaining unknown bytes, and any error.
func convertLabelsToAttributes(unknownBytes []byte) ([]*commonpb.KeyValue, []byte, error) {
	labelBytes, remaining, err := extractField(unknownBytes, 1)
	if err != nil {
		return nil, nil, err
	}
	var attrs []*commonpb.KeyValue
	for _, lb := range labelBytes {
		var skv shadowcommonpb.StringKeyValue
		if err := proto.Unmarshal(lb, &skv); err != nil {
			return nil, nil, fmt.Errorf("compat07: unmarshal label: %w", err)
		}
		attrs = append(attrs, convertStringKeyValue(&skv))
	}
	return attrs, remaining, nil
}

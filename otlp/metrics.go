package otlp

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	collectorMetrics "github.com/honeycombio/husky/proto/otlp/collector/metrics/v1"
	common "github.com/honeycombio/husky/proto/otlp/common/v1"
	metrics "github.com/honeycombio/husky/proto/otlp/metrics/v1"
	resource "github.com/honeycombio/husky/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// TranslateTraceRequestFromReader translates an OTLP/HTTP request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the HTTP headers
func TranslateMetricseRequestFromReader(body io.ReadCloser, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	if err := ri.ValidateMetricsHeaders(); err != nil {
		return nil, err
	}
	request := &collectorMetrics.ExportMetricsServiceRequest{}
	if err := parseOtlpRequestBody(body, ri.ContentEncoding, request); err != nil {
		return nil, ErrFailedParseBody
	}
	return TranslateMetricsRequest(request, ri)
}

// TranslateTraceRequest translates an OTLP/gRPC request into Honeycomb-friendly structure
// RequestInfo is the parsed information from the gRPC metadata
func TranslateMetricsRequest(request *collectorMetrics.ExportMetricsServiceRequest, ri RequestInfo) (*TranslateOTLPRequestResult, error) {
	const defaultDataset = "unknown_metrics"

	if err := ri.ValidateMetricsHeaders(); err != nil {
		return nil, err
	}

	dataPointsOverwritten := 0

	var batches []Batch
	eventsByKey := make(map[string]Event)
	var events []Event
	var dataset string
	var totalSize int

	for _, resourceMetric := range request.ResourceMetrics {

		resourceAttrs := make(map[string]interface{})

		if resourceMetric.Resource != nil {
			addAttributesToMap(resourceAttrs, resourceMetric.Resource.Attributes)
		}

		// need to get some clarity about dataset logic for now, we need to keep doing what we're doing in shepherd
		dataset = ri.Dataset
		if dataset == "" {
			dataset = defaultDataset
		}

		totalSize += proto.Size(resourceMetric)

		for _, scopeMetric := range resourceMetric.GetScopeMetrics() {
			// TODO: handle datapoint flags that may indicate datapoints with a null value (requires OTLP >0.9.0)
			// https://app.asana.com/0/1199917178609623/1200450178355226/f
			for _, metric := range scopeMetric.GetMetrics() {
				switch metric.Data.(type) {
				case *metrics.Metric_IntGauge:
					dataPointsOverwritten += addNumberDataPointsToEvents(metric.Name, resourceMetric, eventsByKey, repackageDeprecatedOTLPIntDataPoints(metric.GetIntGauge().GetDataPoints()))
				case *metrics.Metric_Gauge:
					dataPointsOverwritten += addNumberDataPointsToEvents(metric.Name, resourceMetric, eventsByKey, metric.GetGauge().GetDataPoints())
				case *metrics.Metric_IntSum:
					dataPointsOverwritten += addNumberDataPointsToEvents(metric.Name, resourceMetric, eventsByKey, repackageDeprecatedOTLPIntDataPoints(metric.GetIntSum().GetDataPoints()))
				case *metrics.Metric_Sum:
					dataPointsOverwritten += addNumberDataPointsToEvents(metric.Name, resourceMetric, eventsByKey, metric.GetSum().GetDataPoints())
				case *metrics.Metric_IntHistogram:
					dataPointsOverwritten += addHistogramDataPointsToEvents(metric.Name, resourceMetric, eventsByKey, repackageDeprecatedOTLPIntHistogramDataPoints(metric.GetIntHistogram().GetDataPoints()))
				case *metrics.Metric_Histogram:
					dataPointsOverwritten += addHistogramDataPointsToEvents(metric.Name, resourceMetric, eventsByKey, metric.GetHistogram().GetDataPoints())
				case *metrics.Metric_Summary:
					dataPointsOverwritten += addSummaryDataPointsToEvents(metric.Name, resourceMetric, eventsByKey, metric.GetSummary().GetDataPoints())
				case nil:
				default:
					// need to make sure this still generates a Sentry error
					continue
				}
			}
		}

	}

	for _, event := range eventsByKey {
		events = append(events, event)
	}

	batches = append(batches, Batch{
		Dataset:   dataset,
		SizeBytes: totalSize,
		Events:    events,
	})

	return &TranslateOTLPRequestResult{
		RequestSize: proto.Size(request),
		Batches:     batches,
	}, nil
}

type dataPointWithLabelsOrAttributes interface {
	GetLabels() []*common.StringKeyValue
	GetAttributes() []*common.KeyValue
}

func attributesToString(attributes []*common.KeyValue) string {
	strs := make([]string, 0, len(attributes))
	for i := range attributes {
		strs = append(strs, fmt.Sprintf("%s=%s", attributes[i].Key, attributes[i].Value.String()))
	}
	sort.Strings(strs)
	return strings.Join(strs, ",")
}

// createEventKey creates a unique key for the event by combining the timestamp plus the
// key value pairs from the resource and the labels in a comma delimited string.
// E.g. `service.name=string_value:"otel-collector",1626905565881738000,host=foo`
func createEventKey(resourceKey string, ts time.Time, dp dataPointWithLabelsOrAttributes) string {
	strs := []string{
		resourceKey,
		strconv.Itoa(int(ts.UnixNano())),
	}

	labels := dp.GetLabels()
	for i := range labels {
		strs = append(strs, fmt.Sprintf("%s=%s", labels[i].Key, labels[i].Value))
	}

	attributes := dp.GetAttributes()
	for i := range attributes {
		strs = append(strs, fmt.Sprintf("%s=%s", attributes[i].Key, attributes[i].Value.String()))
	}

	return strings.Join(strs, ",")
}

// repackageDeprecatedOTLPIntDataPoints converts an IntDataPoint into a NumberDataPoint. This allows us to use
// the same code to handle both types later on.
func repackageDeprecatedOTLPIntDataPoints(dataPoints []*metrics.IntDataPoint) (wrapped []*metrics.NumberDataPoint) {
	for _, dp := range dataPoints {
		wrapped = append(wrapped, &metrics.NumberDataPoint{
			Labels:       dp.GetLabels(),
			TimeUnixNano: dp.GetTimeUnixNano(),
			Value:        &metrics.NumberDataPoint_AsInt{dp.GetValue()},
			// Note: we're not saving StartTimeUnixNano or Exemplars, so drop this data here
		})
	}
	return wrapped
}

// repackageDeprecatedOTLPIntHistogramDataPoints converts an IntHistogramDataPoint into a HistogramDataPoint.
// This allows us to use the same code to handle both types later on.
func repackageDeprecatedOTLPIntHistogramDataPoints(datapoints []*metrics.IntHistogramDataPoint) (wrapped []*metrics.HistogramDataPoint) {
	for _, dp := range datapoints {
		val := float64(dp.GetSum())
		wrapped = append(wrapped, &metrics.HistogramDataPoint{
			Labels:         dp.GetLabels(),
			TimeUnixNano:   dp.GetTimeUnixNano(),
			Count:          dp.GetCount(),
			Sum:            &val,
			BucketCounts:   dp.GetBucketCounts(),
			ExplicitBounds: dp.GetExplicitBounds(),
			// Note: we're not saving StartTimeUnixNano or Exemplars, so drop this data here
		})
	}
	return wrapped
}

var percentiles = map[string]float64{
	".p001": 0.001,
	".p01":  0.01,
	".p05":  0.5,
	".p10":  0.10,
	".p25":  0.25,
	".p50":  0.50,
	".p75":  0.75,
	".p90":  0.90,
	".p95":  0.95,
	".p99":  0.99,
	".p999": 0.999,
}

func calculateQuantiles(count uint64, bucketCounts []uint64, bounds []float64) map[string]float64 {
	quantiles := make(map[string]float64)
	if len(bounds) > 0 && len(bucketCounts) > 0 {
		var cumulativeTotal uint64
		bucketQuantiles := make([]float64, 0, len(bucketCounts))
		for _, bc := range bucketCounts {
			cumulativeTotal += bc
			bucketQuantiles = append(bucketQuantiles, float64(cumulativeTotal)/float64(count))
		}

		for key, percentile := range percentiles {
			highestBound := bounds[0]
			for i, quantile := range bucketQuantiles {
				if quantile > percentile {
					break
				}

				switch {
				case i < len(bounds):
					highestBound = bounds[i]
				default:
					// this is an odd scenario that we are not entirely sure creates
					// valid data. We can use this to flag when investigation is needed
					// potentially asking the customer for permission to look at the data
				}
			}
			quantiles[key] = highestBound
		}
	}

	return quantiles
}

func buildEventFromDataPoint(dataPoint dataPointWithLabelsOrAttributes, data map[string]any, resource *resource.Resource, ts time.Time) Event {
	eventAttrs := map[string]interface{}{
		"meta.signal_type":     "metric",
		"meta.annotation_type": "metric_event",
	}

	for _, label := range dataPoint.GetLabels() {
		data[label.GetKey()] = label.GetValue()
	}

	// copy basic event attrs to data
	for k, v := range eventAttrs {
		data[k] = v
	}

	addAttributesToMap(data, dataPoint.GetAttributes())
	addAttributesToMap(data, resource.GetAttributes())
	event := Event{
		Attributes: data,
		Timestamp:  ts,
		SampleRate: 1,
	}
	return event
}

func addNumberDataPointsToEvents(metricName string, resourceMetric *metrics.ResourceMetrics, eventsByKey map[string]Event, otlpDataPoints []*metrics.NumberDataPoint) int {
	resourceKey := attributesToString(resourceMetric.GetResource().GetAttributes())
	dataPointsOverwritten := 0

	for _, dataPoint := range otlpDataPoints {
		ts := time.Unix(0, int64(dataPoint.GetTimeUnixNano())).UTC().Truncate(1 * time.Second)
		eventKey := createEventKey(resourceKey, ts, dataPoint)

		var value any
		switch dataPoint.GetValue().(type) {
		case *metrics.NumberDataPoint_AsDouble:
			value = dataPoint.GetAsDouble()
		case *metrics.NumberDataPoint_AsInt:
			value = dataPoint.GetAsInt()
		}

		if _, ok := eventsByKey[eventKey]; ok {
			// datapoint already exists, add metric name & value
			// NOTE: it is possible to overwrite a value here if we receive multiple
			// data points with the same label set and timestamp
			if _, present := eventsByKey[eventKey].Attributes[metricName]; present {
				dataPointsOverwritten++ // track how often we're overwriting
			}

			eventsByKey[eventKey].Attributes[metricName] = value
		} else {
			// create new data
			data := make(map[string]any)
			data[metricName] = value

			eventsByKey[eventKey] = buildEventFromDataPoint(dataPoint, data, resourceMetric.GetResource(), ts)
		}
	}

	return dataPointsOverwritten
}

func addHistogramDataPointsToEvents(metricName string, resourceMetric *metrics.ResourceMetrics, eventsByKey map[string]Event, histogramDatapoints []*metrics.HistogramDataPoint) int {
	resourceKey := attributesToString(resourceMetric.GetResource().GetAttributes())
	dataPointsOverwritten := 0

	for _, dataPoint := range histogramDatapoints {
		ts := time.Unix(0, int64(dataPoint.GetTimeUnixNano())).UTC().Truncate(1 * time.Second)
		eventKey := createEventKey(resourceKey, ts, dataPoint)

		if _, ok := eventsByKey[eventKey]; ok {
			// datapoint already exists, add metric aggregates
			// NOTE: it is possible to overwrite a value here if we recevive multiple data points with the same label set and timestamp
			if _, present := eventsByKey[eventKey].Attributes[metricName]; present {
				dataPointsOverwritten++ // track how often we're overwriting
			}

			eventsByKey[eventKey].Attributes[metricName+".count"] = int64(dataPoint.GetCount()) // truncate to int64 from uint64
			eventsByKey[eventKey].Attributes[metricName+".sum"] = dataPoint.GetSum()
			eventsByKey[eventKey].Attributes[metricName+".avg"] = dataPoint.GetSum() / float64(dataPoint.GetCount())
			for key, val := range calculateQuantiles(dataPoint.GetCount(), dataPoint.GetBucketCounts(), dataPoint.GetExplicitBounds()) {
				eventsByKey[eventKey].Attributes[metricName+key] = val
			}
		} else {
			data := make(map[string]any)
			data[metricName+".count"] = int64(dataPoint.GetCount()) // truncate to int64 from uint64
			data[metricName+".sum"] = dataPoint.GetSum()
			data[metricName+".avg"] = dataPoint.GetSum() / float64(dataPoint.GetCount())
			for key, val := range calculateQuantiles(dataPoint.Count, dataPoint.BucketCounts, dataPoint.ExplicitBounds) {
				data[metricName+key] = val
			}

			eventsByKey[eventKey] = buildEventFromDataPoint(dataPoint, data, resourceMetric.GetResource(), ts)
		}
	}
	return dataPointsOverwritten
}

func addSummaryDataPointsToEvents(metricName string, resourceMetric *metrics.ResourceMetrics, eventsByKey map[string]Event, summaryDataPoints []*metrics.SummaryDataPoint) int {
	resourceKey := attributesToString(resourceMetric.GetResource().GetAttributes())
	dataPointsOverwritten := 0

	for _, dataPoint := range summaryDataPoints {
		ts := time.Unix(0, int64(dataPoint.GetTimeUnixNano())).UTC().Truncate(1 * time.Second)
		eventKey := createEventKey(resourceKey, ts, dataPoint)

		if _, ok := eventsByKey[eventKey]; ok {
			// datapoint already exists, just add metric aggregates
			if _, present := eventsByKey[eventKey].Attributes[metricName]; present {
				dataPointsOverwritten++ // track how often we're overwriting
			}
			addSummaryDatapointToMap(metricName, dataPoint, eventsByKey[eventKey].Attributes)
		} else {
			// create new datapoint and add labels
			data := make(map[string]any)
			addSummaryDatapointToMap(metricName, dataPoint, data)
			eventsByKey[eventKey] = buildEventFromDataPoint(dataPoint, data, resourceMetric.GetResource(), ts)
		}
	}
	return dataPointsOverwritten
}

func addSummaryDatapointToMap(metricName string, dp *metrics.SummaryDataPoint, data map[string]any) {
	data[metricName+".count"] = int64(dp.GetCount()) // truncate to int64 from uint64
	data[metricName+".sum"] = dp.GetSum()
	data[metricName+".avg"] = dp.GetSum() / float64(dp.GetCount())

	for _, qv := range dp.GetQuantileValues() {
		switch qv.GetQuantile() {
		case 0.0:
			data[metricName+".min"] = qv.GetValue()
		case 1.0:
			data[metricName+".max"] = qv.GetValue()
		default:
			data[formatQuantileKey(metricName, qv.GetQuantile())] = qv.GetValue()
		}
	}
}

// formatQuantileKey formats a quantile value (range between 0.0 and 1.0) into a field name. The quantile
// value does not include the preceeding 0./1. and trims any unneccesary trailing 0s.
// eg 0.50 returns <metric-name>.p50, 0.01 returns <metric-name>.p01, 0.999 returns <metric-name>.p999
func formatQuantileKey(metricName string, quantile float64) string {
	return fmt.Sprintf(
		"%s.p%s",
		metricName,
		strings.TrimSuffix(strconv.FormatFloat(quantile, 'f', 3, 64)[2:], "0"),
	)
}
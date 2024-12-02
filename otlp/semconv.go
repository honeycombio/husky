package otlp

var descriptions = map[string]string{
	"http.request.method":          "HTTP request method.",
	"http.request.header":          "HTTP request headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values.",
	"http.request.method_original": "Original HTTP method sent by the client in the request line.",
	"http.request.resend_count":    "The ordinal number of request resending attempt (for any reason, including redirects).",
	"http.response.header":         "HTTP response headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values.",
	"http.response.status_code":    "[HTTP response status code](https://tools.ietf.org/html/rfc7231#section-6).",
	"http.route":                   "The matched route, that is, the path template in the format used by the respective server framework.",
}

// GetDescriptionForOtelAttributeName returns an informative description of a given OpenTelemetry attribute name.
func GetDescriptionForOtelAttributeName(name string) string {
	if description, ok := descriptions[name]; ok {
		return description
	}

	return ""
}

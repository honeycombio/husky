package otlp

// GetDescriptionForOtelAttributeName returns an informative description of a given OpenTelemetry attribute name.
func GetDescriptionForOtelAttributeName(name string) string {
	switch name {
	case "http.request.method":
		return "HTTP request method."
	case "http.request.header":
		return "HTTP request headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values."
	case "http.request.method_original":
		return "Original HTTP method sent by the client in the request line."
	case "http.request.resend_count":
		return "The ordinal number of request resending attempt (for any reason, including redirects)."
	case "http.response.header":
		return "HTTP response headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values."
	case "http.response.status_code":
		return "[HTTP response status code](https://tools.ietf.org/html/rfc7231#section-6)."
	case "http.route":
		return "The matched route, that is, the path template in the format used by the respective server framework."
	default:
		return ""
	}
}

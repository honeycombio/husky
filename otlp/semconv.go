package otlp

func GetDescriptionForOtelAttributeName(name string) string {
	switch name {
	case "http.request.method":
		return "HTTP request method."
	case "http.request.header":
		return "HTTP request headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values."
	default:
		return ""
	}
}

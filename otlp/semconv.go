package otlp

func GetDescriptionForOtelAttributeName(name string) string {
	switch name {
	case "http.request.method":
		return "HTTP request method."
	case "http.request.header":
		return "HTTP request headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values."
	case "http.response.body.size":
		return "The size of the response payload body in bytes. This is the number of bytes transferred excluding headers and" +
			"is often, but not always, present as the [Content-Length](https://www.rfc-editor.org/rfc/rfc9110.html#field.content-length)" +
			"header. For requests using transport encoding, this should be the compressed size."
	default:
		return ""
	}
}

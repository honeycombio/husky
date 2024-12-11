package otlp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetDescriptionForOtelAttributeName(t *testing.T) {
	testCases := []struct {
		attrName            string
		expectedDescription string
	}{
		{
			attrName:            "some.random.attribute",
			expectedDescription: "",
		},
		{
			attrName:            "http.request.header",
			expectedDescription: "HTTP request headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values.",
		}, {
			attrName:            "http.request.method",
			expectedDescription: "HTTP request method.",
		}, {
			attrName:            "http.request.method_original",
			expectedDescription: "Original HTTP method sent by the client in the request line.",
		}, {
			attrName:            "http.request.resend_count",
			expectedDescription: "The ordinal number of request resending attempt (for any reason, including redirects).",
		}, {
			attrName:            "http.response.header",
			expectedDescription: "HTTP response headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values.",
		}, {
			attrName:            "http.response.status_code",
			expectedDescription: "[HTTP response status code](https://tools.ietf.org/html/rfc7231#section-6).",
		}, {
			attrName:            "http.route",
			expectedDescription: "The matched route, that is, the path template in the format used by the respective server framework.",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.attrName, func(t *testing.T) {
			assert.Equal(t, tC.expectedDescription, GetDescriptionForOtelAttributeName(tC.attrName))
			assert.LessOrEqual(t, len(GetDescriptionForOtelAttributeName(tC.attrName)), 255, "Description is limited to 255 characters.")
		})
	}
}

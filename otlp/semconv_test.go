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
			attrName:            "http.request.method",
			expectedDescription: "HTTP request method.",
		},
		{
			attrName:            "http.request.header",
			expectedDescription: "HTTP request headers, `<key>` being the normalized HTTP Header name (lowercase), the value being the header values.",
		},
		{
			attrName: "http.response.body.size",
			expectedDescription: "The size of the response payload body in bytes. This is the number of bytes transferred excluding headers and" +
				"is often, but not always, present as the [Content-Length](https://www.rfc-editor.org/rfc/rfc9110.html#field.content-length)" +
				"header. For requests using transport encoding, this should be the compressed size.",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.attrName, func(t *testing.T) {
			assert.Equal(t, tC.expectedDescription, GetDescriptionForOtelAttributeName(tC.attrName))
		})
	}
}

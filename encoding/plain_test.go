package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidationInvalid(t *testing.T) {
	h := NewPlain(false, 100)
	metrics := map[string][]byte{
		"incorrectFields":                []byte("incorrect fields 21300.00 12351123"),
		"stringValue":                    []byte("incorrect_value two 12351123"),
		"stringTime":                     []byte("incorrect_time 1.0 two"),
		"incompleteValue":                []byte("incomplete "),
		"incompleteValueOff":             []byte("   incomplete "),
		"incompleteValueDot":             []byte(".......incomplete "),
		"incompleteValueOffNoTrail":      []byte(" incomplete"),
		"incompleteValueNoTrail":         []byte("incomplete"),
		"incompleteValueWithValAndTrail": []byte("incomplete 2020 "),
		"metricNameTooLong":              []byte("metric.testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest 10.00 1000"),
		"randomBinary":                   []byte("\x10\x68\xcc\x9c\x2c\xa6\x26\xbb\x8a\x1d\x1f\x4d\xfd\x51\xe2\x9e\xd8\xbf\xef\x69\x6b\x60\xaa\x2d\xec\xdf\x23\xb1\xd8\x2c\x1b\x52\x01\x4a\x52\x76\x07\x7f\xb7\xb6"),
	}
	for test, metric := range metrics {
		t.Run(test, func(t *testing.T) {
			_, err := h.load(metric, Tags{})
			assert.Error(t, err)
		})
	}
}
func TestValidationValid(t *testing.T) {
	h := NewPlain(false, 1000)
	metrics := map[string]([]byte){
		"normal":         []byte("test.metric 10.00 1000"),
		"spaces":         []byte("   test.metric   10.00  1000"),
		"dotted":         []byte(".test.metric 10.00 1000"),
		"dotted_space":   []byte("    .test.metric     10.00      1000"),
		"graphiteTag":    []byte("test.metric;applicationName=wootwootApp 10.00 1000"),
		"graphiteTags":   []byte("test.metric;applicationName=wootwootApp;applicationType=hype 10.00 1000"),
		"graphiteTagsv2": []byte("test.metric;applicationName=wootwootApp;applicationType=hype;gogogo=amazing 10.00 1000"),
	}

	tags := map[string](Tags){
		"normal":       {},
		"spaces":       {},
		"dotted":       {},
		"dotted_space": {},
		"graphiteTag": {
			"applicationName": "wootwootApp",
		},
		"graphiteTags": {
			"applicationName": "wootwootApp",
			"applicationType": "hype",
		},
		"graphiteTagsv2": {
			"applicationName": "wootwootApp",
			"applicationType": "hype",
			"gogogo":          "amazing",
		},
	}

	for test, metric := range metrics {
		t.Run(test, func(t *testing.T) {
			refD := Datapoint{Name: "test.metric", Value: 10.0, Timestamp: 1000, Tags: tags[test]}
			d, err := h.load(metric, tags[test])
			assert.NoError(t, err)
			assert.Equal(t, refD, d)
		})
	}
}

func TestAddGraphiteTagToMetadata(t *testing.T) {
	graphiteTags := map[string]string{
		"graphiteTag":    "applicationName=wootwootApp",
		"graphiteTags":   "applicationName=wootwootApp;applicationType=hype",
		"graphiteTagsv2": "applicationName=wootwootApp;applicationType=hype;gogogo=amazing",
	}

	tags := map[string](Tags){

		"graphiteTag": {
			"applicationName": "wootwootApp",
		},
		"graphiteTags": {
			"applicationName": "wootwootApp",
			"applicationType": "hype",
		},
		"graphiteTagsv2": {
			"applicationName": "wootwootApp",
			"applicationType": "hype",
			"gogogo":          "amazing",
		},
	}
	for key, value := range graphiteTags {
		t.Run(key, func(t *testing.T) {
			emptyTags := make(Tags)
			err := addGraphiteTagToTags(value, emptyTags)
			assert.NoError(t, err)
			assert.Equal(t, tags[key], emptyTags)
		})
	}

}
func BenchmarkAddGraphiteTagToMetadata(B *testing.B) {
	metric := "k1=v1;k2=v2;k3=v3"
	tags := make(Tags)

	B.Run(",normal", func(B *testing.B) {
		for i := 0; i < B.N; i++ {
			addGraphiteTagToTags(metric, tags)
		}
	})
}

func BenchmarkPlainLoadPackets(B *testing.B) {
	metric := []byte("abcde.test.test.test.test.test.test.test.test.test.ineed.a.one.hundred.byte.metric 21300.00 12351123")
	h := NewPlain(false, 0)
	B.Run("Normal", func(B *testing.B) {
		for i := 0; i < B.N; i++ {
			h.load(metric, Tags{})
		}
	})

}

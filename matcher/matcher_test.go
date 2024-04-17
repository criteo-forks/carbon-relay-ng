package matcher

import (
	"testing"
)

func BenchmarkMatchPrefix(b *testing.B) {
	matcher, _ := New("abcde_fghij.klmnopqrst", "", "", "")
	metric70 := []byte("abcde_fghij.klmnopqrst.uv_wxyz.1234567890abcdefg 12345.6789 1234567890") // key = 48, val = 10, ts = 10 -> 70
	for i := 0; i < b.N; i++ {
		matcher.Match(metric70)
	}
}

func BenchmarkMatchSubstr(b *testing.B) {
	matcher, _ := New("", "1234567890abc", "", "")
	metric70 := []byte("abcde_fghij.klmnopqrst.uv_wxyz.1234567890abcdefg 12345.6789 1234567890") // key = 48, val = 10, ts = 10 -> 70
	for i := 0; i < b.N; i++ {
		matcher.Match(metric70)
	}
}

func BenchmarkMatchRegex(b *testing.B) {
	matcher, _ := New("", "", "abcde_(fghij|foo).[^\\.]+.\\.*.\\.*", "")
	metric70 := []byte("abcde_fghij.klmnopqrst.uv_wxyz.1234567890abcdefg 12345.6789 1234567890") // key = 48, val = 10, ts = 10 -> 70
	for i := 0; i < b.N; i++ {
		matcher.Match(metric70)
	}
}

func TestNotRegex(t *testing.T) {
	matcher, _ := New("", "", "", "counter\\.sum")
	metric70 := []byte("abcde_fghij.counter.sum.uv_wxyz.1234567890abcdefg 12345.6789 1234567890") // key = 48, val = 10, ts = 10 -> 70
	match := matcher.Match(metric70)
	if match {
		t.Errorf("Metric should not match %s %s", metric70, matcher.notRegex)
	}
}

func TestSameRegexNotRegex(t *testing.T) {
	regex := "counter.\\sum"
	matcher, _ := New("", "", regex, regex)
	metric70 := []byte("abcde_fghij.counter.sum.uv_wxyz.1234567890abcdefg 12345.6789 1234567890") // key = 48, val = 10, ts = 10 -> 70
	match := matcher.Match(metric70)
	if match {
		t.Errorf("Metric %s should match %s, but not match %s,", metric70, matcher.regex, matcher.notRegex)
	}
}

func TestRegexNotRegex(t *testing.T) {
	matcher, _ := New("", "", "toto", "plop")
	metric70 := []byte("abcde_fghij.toto.uv_wxyz.1234567890abcdefg 12345.6789 1234567890") // key = 48, val = 10, ts = 10 -> 70
	match := matcher.Match(metric70)
	if !match {
		t.Errorf("Metric %s should match %s, but not match %s,", metric70, matcher.regex, matcher.notRegex)
	}

	metric70 = []byte("abcde_fghij.toto.uv_wxyz.plop 12345.6789 1234567890") // key = 48, val = 10, ts = 10 -> 70
	match = matcher.Match(metric70)
	if match {
		t.Errorf("Metric %s should match %s, but not match %s,", metric70, matcher.regex, matcher.notRegex)
	}
}

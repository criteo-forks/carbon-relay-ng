package encoding

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
)

var (
	errFieldsNum         = errors.New("incorrect number of fields in metric")
	errTwoConsecutiveDot = errors.New("two consecutive dots")
	errBadTags           = errors.New("bad tags")
	errBadGraphiteTag    = errors.New("bad graphiteTag")
	errNoTags            = errors.New("no tags")
	errBadMetricPath     = errors.New("bad metricPath")
	errEmptyline         = errors.New("empty line")
	errFmtNullInKey      = errors.New("null char in key")
	errFmtNotAscii       = errors.New("non-ascii char")
	errIncomplete        = errors.New("incomplete graphite datapoint")
	errTooLong           = errors.New("path too long for metric")
)

const PlainFormat FormatName = "plain"
const dotChar uint8 = '.'

type PlainAdapter struct {
	Validate  bool
	MaxLength int
}

func NewPlain(validate bool, maxLength int) PlainAdapter {
	return PlainAdapter{Validate: validate, MaxLength: maxLength}
}

func (p PlainAdapter) parseKey(firstPartDataPoint string) (string, error) {
	metricPath, err := parseMetricPath(firstPartDataPoint)
	if err != nil {
		return "", errBadMetricPath
	}

	return metricPath, nil
}
func putGraphiteTagInTags(metricPath string, firstPartDataPoint string, tags Tags) error {
	if len(metricPath) != len(firstPartDataPoint) {
		err := addGraphiteTagToTags(firstPartDataPoint[len(metricPath)+1:], tags)
		if err != nil {
			return errBadTags
		}
	}
	return nil
}

func parseMetricPath(key string) (string, error) {
	var previousChar uint8 = ' '
	i := 0
	for ; i < len(key) && key[i] != ';'; i++ {
		if key[i] == 0 {
			return "", errFmtNullInKey
		}
		if key[i] > unicode.MaxASCII {
			return "", errFmtNotAscii
		}
		if key[i] == dotChar && key[i] == previousChar {
			return "", errTwoConsecutiveDot
		}
		previousChar = key[i]
	}
	return key[:i], nil

}

// graphite tag follow the pattern key1=value1;key2=value2
func addGraphiteTagToTags(graphiteTag string, tags Tags) error {
	for index := 0; index < len(graphiteTag); index++ { //index++ is to remove the ; char for the next iteration
		//begin part to set the key
		tmp := strings.IndexByte(graphiteTag[index:], '=')
		if tmp == -1 {
			return errBadGraphiteTag
		}
		equalsIndex := index + tmp
		if index >= equalsIndex {
			return errBadGraphiteTag
		}
		key := graphiteTag[index:equalsIndex]
		//end part to set the key
		//begin part to set the value
		tmp = strings.IndexByte(graphiteTag[index:], ';')
		if tmp == -1 {
			index = len(graphiteTag) // at the end there is no ;
		} else {
			index = tmp + index
		}
		startValue := equalsIndex + 1
		if startValue >= index {
			return errBadGraphiteTag
		}
		value := graphiteTag[startValue:index]
		//end part to set the value
		tags[key] = value
	}
	return nil
}

func (p PlainAdapter) KindS() string {
	return string(PlainFormat)
}

func (p PlainAdapter) Kind() FormatName {
	return PlainFormat
}

func (p PlainAdapter) Dump(dp Datapoint) []byte {
	return []byte(dp.String())
}

func (p PlainAdapter) Load(msgbuf []byte, tags Tags) (Datapoint, error) {
	return p.load(msgbuf, tags)
}

func (p PlainAdapter) load(msgbuf []byte, tags Tags) (Datapoint, error) {
	d := Datapoint{}
	if tags == nil {
		return d, errNoTags
	}
	if len(msgbuf) == 0 {
		return Datapoint{}, errEmptyline
	}
	msg := string(msgbuf)
	start := 0
	for msg[start] == ' ' {
		start++
	}
	if msg[start] == '.' {
		start++
	}
	firstSpace := strings.IndexByte(msg[start:], ' ')
	if firstSpace == -1 {
		return d, errFieldsNum
	}
	firstSpace += start

	if firstSpace == len(msg)-1 {
		return d, errIncomplete
	}
	if p.MaxLength > 0 && firstSpace >= p.MaxLength {
		return d, errTooLong
	}
	var err error
	d.Name, err = p.parseKey(msg[start:firstSpace])
	if err != nil {
		return d, err
	}
	err = putGraphiteTagInTags(d.Name, msg[start:firstSpace], tags)
	if err != nil {
		return d, err
	}
	d.Tags = tags
	for msg[firstSpace] == ' ' {
		firstSpace++
	}
	nextSpace := strings.IndexByte(msg[firstSpace:], ' ')
	if nextSpace == -1 {
		return d, errFieldsNum
	}
	nextSpace += firstSpace

	if nextSpace == len(msg)-1 {
		return d, errIncomplete
	}

	v, err := strconv.ParseFloat(msg[firstSpace:nextSpace], 64)
	if err != nil {
		return d, err
	}
	d.Value = v
	for msg[nextSpace] == ' ' {
		nextSpace++
	}
	timestampLen := nextSpace
	for timestampLen < len(msgbuf) && msg[timestampLen] != ' ' {
		timestampLen++
	}

	ts, err := strconv.ParseUint(msg[nextSpace:timestampLen], 10, 32)
	if err != nil {
		return d, err
	}
	d.Timestamp = ts
	if err != nil {
		return d, err
	}
	return d, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

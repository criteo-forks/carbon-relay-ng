package input

import (
	"bufio"
	"fmt"
	"github.com/graphite-ng/carbon-relay-ng/metrics"
	"go.uber.org/zap"
	"io"

	"github.com/graphite-ng/carbon-relay-ng/encoding"
)

type Input interface {
	Name() string
	Format() encoding.FormatName
	Handler() encoding.FormatAdapter
	Start(d Dispatcher) error
	Stop() error
}

type BaseInput struct {
	Dispatcher Dispatcher
	name       string
	handler    encoding.FormatAdapter
	logger     *zap.Logger
}

func (b *BaseInput) Name() string {
	return b.name
}
func (b *BaseInput) Handler() encoding.FormatAdapter {
	return b.handler
}
func (b *BaseInput) Format() encoding.FormatName {
	return b.handler.Kind()
}

func (b *BaseInput) handleReader(r io.Reader, tags encoding.Tags) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		taintedTags := make(encoding.Tags)

		// Copy from the original map to the target map
		for key, value := range tags {
			taintedTags[key] = value
		}

		// Use taintedTags.
		err := b.handle(scanner.Bytes(), taintedTags)
		if err != nil {
			b.logger.Warn("Handling issue", zap.Error(err))
		}

	}
	return scanner.Err()
}

func (b *BaseInput) handle(msg []byte, tags encoding.Tags) error {
	if len(msg) == 0 {
		return nil
	}
	d, err := b.handler.Load(msg, tags)
	if err != nil {
		metrics.DroppedMetrics.WithLabelValues(err.Error()).Inc()
		return fmt.Errorf("error while processing `%s`: %s", string(truncate(msg)), err)
	}
	b.Dispatcher.Dispatch(d)
	return nil
}

type Dispatcher interface {
	// Dispatch runs data validation and processing
	// implementations must not reuse buf after returning
	Dispatch(dp encoding.Datapoint)
	// IncNumInvalid marks protocol-level decoding failures
	// does not apply to carbon as the protocol is trivial and any parse failure
	// is a message failure (handled in Dispatch)
	IncNumInvalid()
}

func truncate(msg []byte) []byte {
	if len(msg) > 100 {
		return append(msg[:100], []byte("...")...)
	}
	return msg
}

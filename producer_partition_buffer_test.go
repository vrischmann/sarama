package sarama

import "testing"

func TestPPBSuccess(t *testing.T) {
	p := &asyncProducer{
		conf:      NewConfig(),
		errors:    make(chan *ProducerError),
		successes: make(chan *ProducerMessage),
	}

	buf := newPPB(p)

	for i := 0; i < 10000; i++ {
		buf.add(&ProducerMessage{})
		if !buf.hasNext() {
			t.Error("Didn't have next message")
		}
		if buf.next() == nil {
			t.Error("Next message was nil")
		}
		buf.markSuccess(1)
		if buf.full() {
			t.Error("Buffer was full")
		}
	}
}

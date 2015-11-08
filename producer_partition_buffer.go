package sarama

const minQueueLen = 16
const maxBuffer = 1024 // TODO: configurable

type ppbMessage struct {
	in      *ProducerMessage
	out     *Message
	retries int
}

type producerPartitionBuffer struct {
	buf                    []ppbMessage
	head, tail, count, cur int

	parent *asyncProducer
}

func newPPB(parent *asyncProducer) *producerPartitionBuffer {
	return &producerPartitionBuffer{
		buf:    make([]ppbMessage, minQueueLen),
		parent: parent,
	}
}

func (ppb *producerPartitionBuffer) full() bool {
	return ppb.count == maxBuffer
}

func (ppb *producerPartitionBuffer) hasNext() bool {
	return ppb.count > 0 && ppb.cur < ppb.count
}

func (ppb *producerPartitionBuffer) resize() {
	newBuf := make([]ppbMessage, ppb.count*2)

	if ppb.tail > ppb.head {
		copy(newBuf, ppb.buf[ppb.head:ppb.tail])
	} else {
		copy(newBuf, ppb.buf[ppb.head:len(ppb.buf)])
		copy(newBuf[len(ppb.buf)-ppb.head:], ppb.buf[:ppb.tail])
	}

	ppb.head = 0
	ppb.tail = ppb.count
	ppb.buf = newBuf
}

func (ppb *producerPartitionBuffer) add(in *ProducerMessage) {
	var err error
	var key, val []byte

	if in.Key != nil {
		if key, err = in.Key.Encode(); err != nil {
			ppb.parent.returnError(in, err)
			return
		}
	}

	if in.Value != nil {
		if val, err = in.Value.Encode(); err != nil {
			ppb.parent.returnError(in, err)
			return
		}
	}

	if ppb.count == len(ppb.buf) {
		ppb.resize()
	}

	ppb.buf[ppb.tail].in = in
	ppb.buf[ppb.tail].out = &Message{Codec: CompressionNone, Key: key, Value: val}
	ppb.buf[ppb.tail].retries = 0

	ppb.tail = (ppb.tail + 1) % len(ppb.buf)
	ppb.count++
}

func (ppb *producerPartitionBuffer) next() *Message {
	msg := ppb.buf[(ppb.head+ppb.cur)%len(ppb.buf)].out
	ppb.cur++
	return msg
}

func (ppb *producerPartitionBuffer) markSuccess(count int) {
	for i := 0; i < count; i++ {
		if ppb.parent.conf.Producer.Return.Successes {
			ppb.buf[ppb.head].in.clear()
			ppb.parent.successes <- ppb.buf[ppb.head].in
		}
		ppb.buf[ppb.head].in = nil
		ppb.buf[ppb.head].out = nil
		ppb.head = (ppb.head + 1) % len(ppb.buf)
	}

	ppb.count -= count
	ppb.cur -= count
	if len(ppb.buf) > minQueueLen && ppb.count*4 == len(ppb.buf) {
		ppb.resize()
	}
}

func (ppb *producerPartitionBuffer) markFailure(count int, err error) {
	abandoned := 0

	cur := ppb.head
	for i := 0; i < count; i++ {
		ppb.buf[cur].retries++

		if ppb.buf[cur].retries == ppb.parent.conf.Producer.Retry.Max {
			abandoned++
		}

		cur = (cur + 1) % len(ppb.buf)
	}

	for i := 0; i < abandoned; i++ {
		ppb.parent.returnError(ppb.buf[ppb.head].in, err)
		ppb.buf[ppb.head].in = nil
		ppb.buf[ppb.head].out = nil
		ppb.head = (ppb.head + 1) % len(ppb.buf)
	}

	ppb.count -= abandoned
	ppb.cur -= abandoned
	if len(ppb.buf) > minQueueLen && ppb.count*4 == len(ppb.buf) {
		ppb.resize()
	}
}

func (ppb *producerPartitionBuffer) markImmediateFailure(count int, err error) {
	for i := 0; i < count; i++ {
		ppb.parent.returnError(ppb.buf[ppb.head].in, err)
		ppb.buf[ppb.head].in = nil
		ppb.buf[ppb.head].out = nil
		ppb.head = (ppb.head + 1) % len(ppb.buf)
	}

	ppb.count -= count
	ppb.cur -= count
	if len(ppb.buf) > minQueueLen && ppb.count*4 == len(ppb.buf) {
		ppb.resize()
	}
}

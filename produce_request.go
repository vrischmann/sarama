package sarama

import "github.com/prometheus/client_golang/prometheus"

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any of the constants defined here are valid. On broker versions
// prior to 0.8.2.0 any other positive int16 is also valid (the broker will wait for that many
// acknowledgements) but in 0.8.2.0 and later this will raise an exception (it has been replaced
// by setting the `min.isr` value in the brokers configuration).
type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
)

type ProduceRequest struct {
	RequiredAcks RequiredAcks
	Timeout      int32
	Version      int16 // v1 requires Kafka 0.9, v2 requires Kafka 0.10
	msgSets      map[string]map[int32]*MessageSet
}

var (
	batchSizeHisto = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: promNamespace,
			Subsystem: "produce",
			Name:      "batch_size",
			Help:      "Batch size histogram",
		},
		[]string{"topic"},
	)
	compressionRatioHisto = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: promNamespace,
			Subsystem: "produce",
			Name:      "compression_ratio",
			Help:      "Compression ratio histogram",
		},
		[]string{"topic"},
	)
	recordSendCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: "produce",
			Name:      "record_send",
			Help:      "Records sent counter",
		},
		[]string{"topic"},
	)
	recordPerRequestHisto = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: promNamespace,
			Subsystem: "produce",
			Name:      "records_per_request",
			Help:      "Records per request histogram",
		},
		[]string{"topic"},
	)
)

func (r *ProduceRequest) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.RequiredAcks))
	pe.putInt32(r.Timeout)
	err := pe.putArrayLength(len(r.msgSets))
	if err != nil {
		return err
	}

	for topic, partitions := range r.msgSets {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		topicRecordCount := int64(0)
		for id, msgSet := range partitions {
			startOffset := pe.offset()
			pe.putInt32(id)
			pe.push(&lengthField{})
			err = msgSet.encode(pe)
			if err != nil {
				return err
			}
			err = pe.pop()
			if err != nil {
				return err
			}

			for _, messageBlock := range msgSet.Messages {
				// Is this a fake "message" wrapping real messages?
				if messageBlock.Msg.Set != nil {
					topicRecordCount += int64(len(messageBlock.Msg.Set.Messages))
				} else {
					// A single uncompressed message
					topicRecordCount++
				}
				// Better be safe than sorry when computing the compression ratio
				if messageBlock.Msg.compressedSize != 0 {
					compressionRatio := float64(len(messageBlock.Msg.Value)) / float64(messageBlock.Msg.compressedSize)
					compressionRatioHisto.With(makeTopicPromLabels(topic)).Observe(compressionRatio)
				}
			}
			batchSize := int64(pe.offset() - startOffset)
			batchSizeHisto.With(makeTopicPromLabels(topic)).Observe(float64(batchSize))
		}
		if topicRecordCount > 0 {
			recordSendCounter.With(makeTopicPromLabels(topic)).Add(float64(topicRecordCount))
			recordPerRequestHisto.With(makeTopicPromLabels(topic)).Observe(float64(topicRecordCount))
		}
	}

	return nil
}

func (r *ProduceRequest) decode(pd packetDecoder, version int16) error {
	requiredAcks, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.RequiredAcks = RequiredAcks(requiredAcks)
	if r.Timeout, err = pd.getInt32(); err != nil {
		return err
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	r.msgSets = make(map[string]map[int32]*MessageSet)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.msgSets[topic] = make(map[int32]*MessageSet)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			messageSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}
			msgSetDecoder, err := pd.getSubset(int(messageSetSize))
			if err != nil {
				return err
			}
			msgSet := &MessageSet{}
			err = msgSet.decode(msgSetDecoder)
			if err != nil {
				return err
			}
			r.msgSets[topic][partition] = msgSet
		}
	}
	return nil
}

func (r *ProduceRequest) key() int16 {
	return 0
}

func (r *ProduceRequest) version() int16 {
	return r.Version
}

func (r *ProduceRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	default:
		return minVersion
	}
}

func (r *ProduceRequest) AddMessage(topic string, partition int32, msg *Message) {
	if r.msgSets == nil {
		r.msgSets = make(map[string]map[int32]*MessageSet)
	}

	if r.msgSets[topic] == nil {
		r.msgSets[topic] = make(map[int32]*MessageSet)
	}

	set := r.msgSets[topic][partition]

	if set == nil {
		set = new(MessageSet)
		r.msgSets[topic][partition] = set
	}

	set.addMessage(msg)
}

func (r *ProduceRequest) AddSet(topic string, partition int32, set *MessageSet) {
	if r.msgSets == nil {
		r.msgSets = make(map[string]map[int32]*MessageSet)
	}

	if r.msgSets[topic] == nil {
		r.msgSets[topic] = make(map[int32]*MessageSet)
	}

	r.msgSets[topic][partition] = set
}

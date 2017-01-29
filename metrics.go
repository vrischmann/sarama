package sarama

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	promNamespace = "sarama"
)

func makeTopicPromLabels(topic string) prometheus.Labels {
	return prometheus.Labels{"topic": topic}
}

func (b *Broker) makePromLabels() prometheus.Labels {
	return prometheus.Labels{"broker": strconv.Itoa(int(b.ID()))}
}

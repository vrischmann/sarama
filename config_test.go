package sarama

import "testing"

func TestDefaultConfigValidates(t *testing.T) {
	config := NewConfig()
	if err := config.Validate(); err != nil {
		t.Error(err)
	}
}

func TestInvalidClientIDConfigValidates(t *testing.T) {
	config := NewConfig()
	config.ClientID = "foo:bar"
	if err := config.Validate(); string(err.(ConfigurationError)) != "ClientID is invalid" {
		t.Error("Expected invalid ClientID, got ", err)
	}
}

func TestEmptyClientIDConfigValidates(t *testing.T) {
	config := NewConfig()
	config.ClientID = ""
	if err := config.Validate(); string(err.(ConfigurationError)) != "ClientID is invalid" {
		t.Error("Expected invalid ClientID, got ", err)
	}
}

func TestLZ4ConfigValidation(t *testing.T) {
	config := NewConfig()
	config.Producer.Compression = CompressionLZ4
	if err := config.Validate(); string(err.(ConfigurationError)) != "lz4 compression requires Version >= V0_10_0_0" {
		t.Error("Expected invalid lz4/kakfa version error, got ", err)
	}
	config.Version = V0_10_0_0
	if err := config.Validate(); err != nil {
		t.Error("Expected lz4 to work, got ", err)
	}
}

package tracelogconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

func NewFactory() connector.Factory {
	// OpenTelemetry connector factory to make a factory for connectors

	return connector.NewFactory(
		typeStr,
		createDefaultConfig,
		connector.WithTracesToLogs(createTracesToLogsConnector, component.StabilityLevelAlpha),
	)
}

const (
	typeStr = "tracelog"
)

func createDefaultConfig() component.Config {
	return &Config{}
}

func createTracesToLogsConnector(ctx context.Context, params connector.CreateSettings, cfg component.Config, nextConsumer consumer.Logs) (connector.Traces, error) {
	c, err := newConnector(params.Logger, cfg)
	if err != nil {
		return nil, err
	}
	c.logsConsumer = nextConsumer
	return c, nil
}

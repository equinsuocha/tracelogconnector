package tracelogconnector

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/metadata"
)

func TestConsumeTrace(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	assert.NoError(t, component.UnmarshalConfig(cm, cfg))
	c, err := newConnector(zaptest.NewLogger(t), cfg)
	ctx := metadata.NewIncomingContext(context.Background(), nil)
	require.NoError(t, err)
	ls := &consumertest.LogsSink{}
	c.logsConsumer = ls
	c.ConsumeTraces(ctx, buildTraces())

	assert.Eventually(t, func() bool {
		allLogs := ls.AllLogs()[0]
		rLogs := allLogs.ResourceLogs()
		for i := 0; i < rLogs.Len(); i++ {
			sLogs := rLogs.At(i).ScopeLogs()
			for i := 0; i < sLogs.Len(); i++ {
				logs := sLogs.At(i).LogRecords()
				for i := 0; i < logs.Len(); i++ {
					log := logs.At(i)
					if v, success := log.Attributes().Get("custom_attr.my_custom_attribute"); success == true {
						return v.AsString() == "my-custom-attr-value"
					}
				}
			}
		}
		return false
	},
		time.Second,
		time.Millisecond*10,
	)
}

func buildTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	rspans := traces.ResourceSpans().AppendEmpty()
	rspans.Resource().Attributes().PutStr("region", "test")
	rspans.Resource().Attributes().PutStr("service.name", "test-service")
	sspans := rspans.ScopeSpans().AppendEmpty()
	sspans.Scope().SetName("instrumentation-test")
	span := sspans.Spans().AppendEmpty()
	span.Attributes().PutBool("root", true)
	span.Attributes().PutStr("custom_attr.my_custom_attribute", "my-custom-attr-value")
	return traces
}

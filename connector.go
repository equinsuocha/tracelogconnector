package tracelogconnector

import (
	"context"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type connectorImp struct {
	config       Config
	pConfig      ProcessedConfig
	logsConsumer consumer.Logs
	logger       *zap.Logger
	component.StartFunc
	component.ShutdownFunc
}

type HasAttributes interface {
	Attributes() pcommon.Map
}

func newConnector(logger *zap.Logger, config component.Config) (*connectorImp, error) {
	logger.Info("Building tracelogconnector connector")
	cfg := config.(*Config)
	pConfig := cfg.GetProcessedConfig()

	return &connectorImp{
		config:  *cfg,
		pConfig: pConfig,
		logger:  logger,
	}, nil
}

func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// loop through the levels of spans of the one trace consumed
	logs := plog.NewLogs()

	rLogsSlice := logs.ResourceLogs()
	rLogsSlice.EnsureCapacity(td.ResourceSpans().Len())
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rSpan := td.ResourceSpans().At(i)
		rLogs := rLogsSlice.AppendEmpty()
		if c.pConfig.HasResourceReMatch {
			rSpan.Resource().Attributes().CopyTo(rLogs.Resource().Attributes())
			c.GetResourceReMatch(rLogs.Resource().Attributes())
		}
		c.copyResourceAttrs(&rLogs, &rSpan)

		sLogsSlice := rLogs.ScopeLogs()
		sLogsSlice.EnsureCapacity(rSpan.ScopeSpans().Len())
		for j := 0; j < rSpan.ScopeSpans().Len(); j++ {
			sSpan := rSpan.ScopeSpans().At(j)
			sLogs := sLogsSlice.AppendEmpty()
			c.copyScopeAttrs(&sLogs, &sSpan)

			logSlice := sLogs.LogRecords()
			logSlice.EnsureCapacity(sSpan.Spans().Len())
			for k := 0; k < sSpan.Spans().Len(); k++ {
				span := sSpan.Spans().At(k)
				log := logSlice.AppendEmpty()
				if c.pConfig.HasAttrReMatch {
					span.Attributes().CopyTo(log.Attributes())
					c.GetResourceReMatch(log.Attributes())
				}
				c.copySpanAttrs(&log, &span)
			}
		}
	}
	c.logsConsumer.ConsumeLogs(ctx, logs)
	return nil
}

func (c *connectorImp) copyResourceAttrs(rLogs *plog.ResourceLogs, rSpan *ptrace.ResourceSpans) {
	for _, ac := range c.pConfig.Resource {
		val, exists := rSpan.Resource().Attributes().Get(ac.Source)
		if exists {
			setAttributeType(val, ac.Dest, rLogs.Resource())
		} else {
			if len(ac.Default) > 0 {
				setAttributeType(pcommon.NewValueStr(ac.Default), ac.Dest, rLogs.Resource())
			}
		}
	}
}

func (c *connectorImp) copyScopeAttrs(sLogs *plog.ScopeLogs, sSpan *ptrace.ScopeSpans) {
	for _, ac := range c.pConfig.Scope {
		if ac.Source == "name" {
			sLogs.Scope().SetName(sSpan.Scope().Name())
		}
		if ac.Source == "version" {
			sLogs.Scope().SetVersion(sSpan.Scope().Version())
		}
	}
}

func (c *connectorImp) copySpanAttrs(log *plog.LogRecord, span *ptrace.Span) {
	log.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	log.SetTraceID(span.TraceID())
	log.SetSpanID(span.SpanID())
	log.SetTimestamp(span.StartTimestamp())
	log.Attributes().PutStr("span.name", span.Name())
	log.Attributes().PutStr("span.kind", span.Kind().String())
	log.Attributes().PutStr("span.status", span.Status().Code().String())
	log.Attributes().PutInt("span.duration", int64(span.EndTimestamp()-span.StartTimestamp()))

	for _, ac := range c.pConfig.Attributes {
		val, exists := span.Attributes().Get(ac.Source)
		if exists {
			setAttributeType(val, ac.Dest, log)
		} else {
			if len(ac.Default) > 0 {
				setAttributeType(pcommon.NewValueStr(ac.Default), ac.Dest, log)
			}
		}
	}
}

func (c *connectorImp) GetAttrReMatch(attrs pcommon.Map) {
	getReMatch(&attrs, &c.pConfig.AttrReMatch)
}

func (c *connectorImp) GetResourceReMatch(attrs pcommon.Map) {
	getReMatch(&attrs, &c.pConfig.ResourceReMatch)
}

func getReMatch(attrs *pcommon.Map, re *[]regexp.Regexp) {
	attrs.RemoveIf(func(k string, v pcommon.Value) bool {
		remove := true
		for i := 0; i < len(*re); i++ {
			remove = !(*re)[i].MatchString(k)
			if !remove {
				break
			}
		}
		return remove
	})
}

func setAttributeType[T HasAttributes](val pcommon.Value, key string, dest T) {
	switch val.Type() {
	case pcommon.ValueTypeStr:
		dest.Attributes().PutStr(key, val.AsString())
		return
	case pcommon.ValueTypeInt:
		dest.Attributes().PutInt(key, val.Int())
		return
	case pcommon.ValueTypeDouble:
		dest.Attributes().PutDouble(key, val.Double())
		return
	case pcommon.ValueTypeBool:
		dest.Attributes().PutBool(key, val.Bool())
		return
	case pcommon.ValueTypeMap:
		destMap := dest.Attributes().PutEmptyMap(key)
		val.Map().CopyTo(destMap)
		return
	case pcommon.ValueTypeSlice:
		destSlice := dest.Attributes().PutEmptySlice(key)
		val.Slice().CopyTo(destSlice)
		return
	}
}

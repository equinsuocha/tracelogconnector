package tracelogconnector

import (
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	assert.Equal(t, &Config{}, cfg)
	pCfg := cfg.GetProcessedConfig()
	assert.Equal(t, &ProcessedConfig{
		Config: *cfg,
	}, &pCfg)
}

func TestConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	assert.NoError(t, component.UnmarshalConfig(cm, cfg))
	assert.Equal(t,
		&Config{
			Resource: []AttrConfig{
				{
					Source: "env",
				},
				{
					Source: "k8s.pod.name",
				},
				{
					Source: "region",
				},
				{
					Source: "service.name",
				},
			},
			Attributes: []AttrConfig{
				{
					Source: "http.host",
				},
				{
					Source: "http.method",
				},
				{
					Source: "http.route",
				},
				{
					Source: "http.status_code",
				},
				{
					Source: "http.target",
				},
				{
					Source: "http.user_agent",
				},
				{
					Source: `^custom_attr\.`,
					Match:  "regexp",
				},
				{
					Source: "span.kind",
				},
			},
		},
		cfg,
	)

	cfg.Normalize()
	re, err := regexp.Compile(`^custom_attr\.`)

	assert.NoError(t, err)

	assert.Equal(t,
		&Config{
			Resource: []AttrConfig{
				{
					Source: "env",
					Dest:   "env",
					Match:  "strict",
				},
				{
					Source: "k8s.pod.name",
					Dest:   "k8s.pod.name",
					Match:  "strict",
				},
				{
					Source: "region",
					Dest:   "region",
					Match:  "strict",
				},
				{
					Source: "service.name",
					Dest:   "service.name",
					Match:  "strict",
				},
			},
			Attributes: []AttrConfig{
				{
					Source: "http.host",
					Dest:   "http.host",
					Match:  "strict",
				},
				{
					Source: "http.method",
					Dest:   "http.method",
					Match:  "strict",
				},
				{
					Source: "http.route",
					Dest:   "http.route",
					Match:  "strict",
				},
				{
					Source: "http.status_code",
					Dest:   "http.status_code",
					Match:  "strict",
				},
				{
					Source: "http.target",
					Dest:   "http.target",
					Match:  "strict",
				},
				{
					Source: "http.user_agent",
					Dest:   "http.user_agent",
					Match:  "strict",
				},
				{
					Source: `^custom_attr\.`,
					Match:  "regexp",
					re:     *re,
				},
				{
					Source: "span.kind",
					Dest:   "span.kind",
					Match:  "strict",
				},
			},
		},
		cfg,
	)

	pCfg := cfg.GetProcessedConfig()
	processedConfigStruct := Config{
		Resource: []AttrConfig{
			{
				Source: "env",
				Dest:   "env",
				Match:  "strict",
			},
			{
				Source: "k8s.pod.name",
				Dest:   "k8s.pod.name",
				Match:  "strict",
			},
			{
				Source: "region",
				Dest:   "region",
				Match:  "strict",
			},
			{
				Source: "service.name",
				Dest:   "service.name",
				Match:  "strict",
			},
		},
		Attributes: []AttrConfig{
			{
				Source: "http.host",
				Dest:   "http.host",
				Match:  "strict",
			},
			{
				Source: "http.method",
				Dest:   "http.method",
				Match:  "strict",
			},
			{
				Source: "http.route",
				Dest:   "http.route",
				Match:  "strict",
			},
			{
				Source: "http.status_code",
				Dest:   "http.status_code",
				Match:  "strict",
			},
			{
				Source: "http.target",
				Dest:   "http.target",
				Match:  "strict",
			},
			{
				Source: "http.user_agent",
				Dest:   "http.user_agent",
				Match:  "strict",
			},
			{
				Source: "span.kind",
				Dest:   "span.kind",
				Match:  "strict",
			},
		},
	}

	assert.Equal(t,
		&ProcessedConfig{
			Config:             processedConfigStruct,
			HasAttrReMatch:     true,
			HasResourceReMatch: false,
			AttrReMatch:        []regexp.Regexp{*re},
		},
		&pCfg,
	)
}

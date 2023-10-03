package tracelogconnector

import (
	"fmt"
	"regexp"

	"k8s.io/utils/strings/slices"
)

type AttrConfig struct {
	Dest    string `mapstructure:"dest"`
	Default string `mapstructure:"default"`
	Match   string `mapstructure:"match"`
	Source  string `mapstructure:"source"`
	re      regexp.Regexp
}

type Config struct {
	Attributes []AttrConfig `mapstructure:"attributes"`
	Resource   []AttrConfig `mapstructure:"resource"`
	Scope      []AttrConfig `mapstructure:"scope"`
}

type ProcessedConfig struct {
	Config
	HasAttrReMatch     bool
	HasResourceReMatch bool
	AttrReMatch        []regexp.Regexp
	ResourceReMatch    []regexp.Regexp
}

func (c *Config) GetProcessedConfig() (pc ProcessedConfig) {
	for _, a := range c.Resource {
		if a.Match == "strict" {
			pc.Resource = append(pc.Resource, a)
		} else {
			pc.ResourceReMatch = append(pc.ResourceReMatch, a.re)
		}
	}

	if len(pc.ResourceReMatch) > 0 {
		pc.HasResourceReMatch = true
	}

	pc.Scope = c.Scope

	for _, a := range c.Attributes {
		if a.Match == "strict" {
			pc.Attributes = append(pc.Attributes, a)
		} else {
			pc.AttrReMatch = append(pc.AttrReMatch, a.re)
		}
	}

	if len(pc.AttrReMatch) > 0 {
		pc.HasAttrReMatch = true
	}

	return pc
}

func (c *Config) Validate() error {
	if err := c.Normalize(); err != nil {
		return err
	}

	if err := validateAttributes(&c.Resource); err != nil {
		return err
	}

	if err := validateScopeAttributes(&c.Scope); err != nil {
		return err
	}

	if err := validateAttributes(&c.Attributes); err != nil {
		return err
	}

	return nil
}

func (c *Config) Normalize() error {
	for i := 0; i < len(c.Attributes); i++ {
		a := &c.Attributes[i]
		a.normalizeDest()
		a.normalizeMatch()
		if err := a.compileRe(); err != nil {
			return err
		}
	}

	for i := 0; i < len(c.Resource); i++ {
		a := &c.Resource[i]
		a.normalizeDest()
		a.normalizeMatch()
		if err := a.compileRe(); err != nil {
			return err
		}
	}

	return nil
}

func (c *AttrConfig) normalizeDest() {
	if len(c.Dest) == 0 {
		c.Dest = c.Source
	}
}

func (c *AttrConfig) normalizeMatch() {
	if len(c.Match) == 0 {
		c.Match = "strict"
	}
}

func (c *AttrConfig) compileRe() error {
	if c.Match == "regexp" {
		re, err := regexp.Compile(c.Source)
		if err != nil {
			return err
		}
		c.re = *re
	}
	return nil
}

func validateScopeAttributes(a *[]AttrConfig) error {
	allowedScopeAttrs := []string{"name", "version"}

	for _, cfg := range *a {
		if !slices.Contains(allowedScopeAttrs, cfg.Source) {
			return fmt.Errorf(
				"source key value %s in not allowed for scope attributes, allowed values are %v",
				cfg.Source,
				allowedScopeAttrs,
			)
		} else {
			if len(cfg.Dest) > 0 || len(cfg.Default) > 0 || len(cfg.Match) > 0 {
				return fmt.Errorf("scope attributes don't support dest, default, match config keys")
			}
		}
	}

	return nil
}

func validateAttributes(a *[]AttrConfig) error {
	allowedMatchKeyValues := []string{"strict", "regexp"}

	for _, attrCfg := range *a {
		if !slices.Contains(allowedMatchKeyValues, attrCfg.Match) {
			return fmt.Errorf(
				"match key value %s in not allowed, allowed values are %v",
				attrCfg.Match,
				allowedMatchKeyValues,
			)
		}
	}

	return nil
}

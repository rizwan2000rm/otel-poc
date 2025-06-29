package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/service"

	// Import Arrow proto from otel-arrow
	arrowpb "github.com/open-telemetry/otel-arrow/collector/gen/otel/arrow/v1"
)

// Config defines the exporter config
type Config struct {
	component.ExportersSettings `mapstructure:",squash"`
	LogFile                     string `mapstructure:"log_file"`
}

// Exporter implements the custom Arrow exporter
type ArrowFileExporter struct {
	cfg     *Config
	logFile *os.File
}

// NewFactory registers the exporter with the collector
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		"customexporter",
		func() component.Config {
			return &Config{
				ExportersSettings: component.NewExportersSettings(component.NewID("customexporter")),
				LogFile:           "arrow_data.log",
			}
		},
		exporter.WithTraces(func(ctx context.Context, set exporter.CreateSettings, cfg component.Config) (exporter.Traces, error) {
			return createExporter(ctx, cfg.(*Config))
		}),
	)
}

// createExporter creates the exporter instance
func createExporter(ctx context.Context, cfg *Config) (exporter.Traces, error) {
	f, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	exp := &ArrowFileExporter{
		cfg:     cfg,
		logFile: f,
	}

	return exporterhelper.NewTracesExporter(
		ctx,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithConsumeTraces(exp.ConsumeTraces),
	)
}

// Start is a no-op for now
func (e *ArrowFileExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

// Shutdown closes the log file
func (e *ArrowFileExporter) Shutdown(ctx context.Context) error {
	return e.logFile.Close()
}

// ConsumeTraces receives pdata.Traces — must extract Arrow payload
func (e *ArrowFileExporter) ConsumeTraces(ctx context.Context, td exporterhelper.Traces) error {
	// Instead of pdata.Traces, this should really receive *arrowpb.ExportTraceServiceRequest,
	// but since exporterhelper uses pdata.Traces, you must work with upstream otelarrowreceiver instead.
	return errors.New("this exporter expects to be wired to otel-arrow protocol and receive raw arrowpb payloads")
}

// This function is only useful if you're receiving the raw Arrow payload directly
func (e *ArrowFileExporter) ExportArrowRequest(req *arrowpb.ExportTraceServiceRequest) error {
	payload := req.GetArrowPayload()
	if len(payload) == 0 {
		return errors.New("empty Arrow payload")
	}

	_, err := e.logFile.Write(payload)
	return err
}

// Main entry point
func main() {
	if err := service.Run(service.Settings{
		BuildInfo: component.BuildInfo{
			Command:     "otelcol-custom",
			Description: "Custom Collector with Arrow Exporter",
			Version:     "0.1.0",
		},
		Factories: func() (component.Factories, error) {
			factories, err := service.DefaultComponents()
			if err != nil {
				return component.Factories{}, err
			}

			exporterFactories := factories.Exporters
			exporterFactories["customexporter"] = NewFactory()

			return component.Factories{
				Receivers:  factories.Receivers,
				Exporters:  exporterFactories,
				Processors: factories.Processors,
				Extensions: factories.Extensions,
			}, nil
		}(),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "collector failed: %v\n", err)
		os.Exit(1)
	}
}

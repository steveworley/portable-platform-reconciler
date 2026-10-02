package ecs

import (
	"context"
	"fmt"
	"log/slog"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"

	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/runtime"
)

func init() {
	runtime.Register(RuntimeType, factory,
		runtime.AnyOf(
			runtime.EnvSet("ECS_CONTAINER_METADATA_URI_V4"),
			runtime.EnvHasPrefix("AWS_EXECUTION_ENV", "AWS_ECS"),
		),
		runtime.WithTargetValidator(ValidateTarget),
	)
}

// factory builds the adapter from configuration. "simulated" mode uses a
// seeded in-memory simulation with a working remediator; "aws" mode uses the
// real ECS client (read-only) with a LogRemediator.
func factory(cfg config.Config) (runtime.RuntimeAdapter, error) {
	logger := slog.Default()
	switch cfg.ECS.Mode {
	case config.ECSModeSimulated, "":
		sim := NewSeededSimulation()
		return New(sim, WithLogger(logger), WithRemediator(sim.Remediator())), nil
	case config.ECSModeAWS:
		var opts []func(*awsconfig.LoadOptions) error
		if cfg.ECS.Region != "" {
			opts = append(opts, awsconfig.WithRegion(cfg.ECS.Region))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
		if err != nil {
			return nil, fmt.Errorf("load aws config: %w", err)
		}
		return New(ecs.NewFromConfig(awsCfg), WithLogger(logger), WithRemediator(NewLogRemediator(logger))), nil
	}
	return nil, fmt.Errorf("unknown ecs.mode %q", cfg.ECS.Mode)
}

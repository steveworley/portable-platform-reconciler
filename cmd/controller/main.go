// Command controller is the portable platform reconciler: one binary that
// reconciles observability policy and golden-signal alerts for workloads on
// any registered runtime into a swappable alert backend.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	// Register runtimes and alert backends. Each package registers itself in init().
	_ "github.com/example/portable-platform-reconciler/internal/alerting/memory"
	_ "github.com/example/portable-platform-reconciler/internal/alerting/newrelic"
	_ "github.com/example/portable-platform-reconciler/internal/runtime/ecs"
	_ "github.com/example/portable-platform-reconciler/internal/runtime/kubernetes"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `Usage: controller [flags]
       controller run [flags]       run the controller (default)
       controller prune [flags]     delete managed alert rules from a backend
       controller version

Common flags:
  --config path        YAML config file (empty = defaults)
  --log-format fmt     json or text (default json)
  --log-level level    debug, info, warn or error (default info)

Prune flags:
  --backend name       alert backend to prune (default: alerting.backend)
  --keep-none          delete every rule this controller manages (default mode)
  --orphans            keep rules whose policy still exists in policies.dir
  --dry-run=true       only report what would be deleted (default true)
`

type options struct {
	configPath string
	logFormat  string
	logLevel   string
	backend    string
	keepNone   bool
	orphans    bool
	dryRun     bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	if cmd == "version" {
		fmt.Fprintln(stdout, "controller", version)
		return nil
	}
	if cmd != "run" && cmd != "prune" {
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}

	var o options
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	fs.StringVar(&o.configPath, "config", "", "")
	fs.StringVar(&o.logFormat, "log-format", "json", "")
	fs.StringVar(&o.logLevel, "log-level", "info", "")
	fs.StringVar(&o.backend, "backend", "", "")
	fs.BoolVar(&o.keepNone, "keep-none", false, "")
	fs.BoolVar(&o.orphans, "orphans", false, "")
	fs.BoolVar(&o.dryRun, "dry-run", true, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	logger, err := newLogger(stderr, o.logFormat, o.logLevel)
	if err != nil {
		return err
	}
	if cmd == "prune" {
		return runPrune(ctx, o, logger, stdout)
	}
	return run(ctx, o, logger)
}

func newLogger(w io.Writer, format, level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid --log-level %q: %w", level, err)
	}
	hopts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(w, hopts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, hopts)), nil
	}
	return nil, fmt.Errorf("invalid --log-format %q (want json or text)", format)
}

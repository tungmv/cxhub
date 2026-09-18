package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cxhub/internal/config"
	"cxhub/internal/gateway"
	"cxhub/internal/provider"
)

const exampleConfig = `gateway:
  host: 127.0.0.1
  port: 8787
backends:
  cliproxy:
    type: openai-compatible
    base_url: http://127.0.0.1:8317/v1
    api_key: ${CLIPROXY_API_KEY}
    requires_api_key: false
profiles:
  orchestrator:
    targets:
      - backend: cliproxy
        model: YOUR_MODEL
`

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "init":
		exitOnError(initCommand(os.Args[2:]))
	case "start":
		exitOnError(startCommand(os.Args[2:]))
	case "stop":
		exitOnError(stopCommand(os.Args[2:]))
	case "status":
		exitOnError(statusCommand(os.Args[2:]))
	case "models":
		exitOnError(modelsCommand(os.Args[2:]))
	case "doctor":
		exitOnError(doctorCommand(os.Args[2:]))
	case "config":
		if len(os.Args) >= 3 && os.Args[2] == "validate" {
			exitOnError(validateCommand(os.Args[3:]))
			return
		}
		usage()
		os.Exit(2)
	default:
		usage()
		os.Exit(2)
	}
}

func initCommand(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	path := fs.String("config", config.DefaultPath(), "configuration path")
	configureCodex := fs.Bool("configure-codex", false, "add/update the cxhub provider in Codex config")
	home, _ := os.UserHomeDir()
	codexConfig := fs.String("codex-config", filepath.Join(home, ".codex", "config.toml"), "Codex config path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := os.Stat(*path); err == nil {
		return fmt.Errorf("refusing to overwrite existing config %q", *path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(*path, []byte(exampleConfig), 0o600); err != nil {
		return err
	}
	fmt.Printf("created %s\n", *path)
	if *configureCodex {
		changed, err := configureCodexFile(*codexConfig)
		if err != nil {
			return err
		}
		if changed {
			fmt.Printf("updated %s (backup: %s.bak-cxhub)\n", *codexConfig, *codexConfig)
		} else {
			fmt.Printf("Codex config already points at cxhub: %s\n", *codexConfig)
		}
	} else {
		fmt.Println("Codex configuration was not modified. Use --configure-codex to add the local Responses provider.")
	}
	return nil
}

func startCommand(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	path := fs.String("config", config.DefaultPath(), "configuration path")
	verbose := fs.Bool("verbose", false, "enable debug logging")
	pidFile := fs.String("pid-file", defaultPIDPath(), "PID file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	if *verbose || cfg.Logging.Verbose {
		level.Set(slog.LevelDebug)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	providers := make(map[string]provider.Provider, len(cfg.Backends))
	for name, backend := range cfg.Backends {
		providers[name] = provider.NewOpenAICompatible(name, backend, provider.DefaultHTTPClient(cfg.RequestTimeoutDuration()))
	}
	server := gateway.NewServer(cfg, providers, logger)
	pidLock, err := writePID(*pidFile)
	if err != nil {
		return err
	}
	defer func() {
		_ = pidLock.Close()
		_ = os.Remove(*pidFile)
	}()
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Start() }()
	fmt.Printf("cxhub listening on http://%s\n", cfg.Address())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func stopCommand(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	pidFile := fs.String("pid-file", defaultPIDPath(), "PID file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	data, err := os.ReadFile(*pidFile)
	if err != nil {
		return fmt.Errorf("read PID file: %w", err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil || pid < 1 {
		return fmt.Errorf("invalid PID file %q", *pidFile)
	}
	if !processIsCXHub(pid) {
		return fmt.Errorf("PID %d is not an identifiable cxhub process; refusing to signal it", pid)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("stop process %d: %w", pid, err)
	}
	fmt.Printf("sent SIGTERM to %d\n", pid)
	return nil
}

func statusCommand(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8787/status", "status URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(*url)
	if err != nil {
		return fmt.Errorf("status request: %w", err)
	}
	defer resp.Body.Close()
	_, err = os.Stdout.ReadFrom(resp.Body)
	fmt.Println()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status returned HTTP %d", resp.StatusCode)
	}
	return err
}

func modelsCommand(args []string) error {
	fs := flag.NewFlagSet("models", flag.ContinueOnError)
	path := fs.String("config", config.DefaultPath(), "configuration path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	profiles := make([]string, 0, len(cfg.Profiles))
	for name := range cfg.Profiles {
		profiles = append(profiles, name)
	}
	sort.Strings(profiles)
	for _, name := range profiles {
		fmt.Println(name)
	}
	return nil
}

func doctorCommand(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	path := fs.String("config", config.DefaultPath(), "configuration path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if missing := config.MissingEnvironmentReferences(*path); len(missing) > 0 {
		fmt.Printf("missing environment variables: %v\n", missing)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, backend := range cfg.Backends {
		p := provider.NewOpenAICompatible(name, backend, provider.DefaultHTTPClient(5*time.Second))
		if err := p.Health(ctx); err != nil {
			fmt.Printf("backend %s: unhealthy (%v)\n", name, err)
		} else {
			fmt.Printf("backend %s: healthy\n", name)
		}
	}
	return nil
}

func validateCommand(args []string) error {
	fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
	path := fs.String("config", config.DefaultPath(), "configuration path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := config.Load(*path); err != nil {
		return err
	}
	fmt.Printf("valid: %s\n", *path)
	return nil
}

func usage() {
	fmt.Println("usage: cxhub {init|start|stop|status|models|doctor|config validate} [flags]")
}

func defaultPIDPath() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "cxhub.pid"
	}
	return filepath.Join(dir, "cxhub", "cxhub.pid")
}

func writePID(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("cxhub is already running")
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return lock, nil
}

func processIsCXHub(pid int) bool {
	output, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	return err == nil && strings.Contains(strings.ToLower(string(output)), "cxhub")
}

func configureCodexFile(path string) (bool, error) {
	original, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read Codex config: %w", err)
	}
	text := string(original)
	if strings.Contains(text, "[model_providers.cxhub]") {
		if strings.Contains(text, `base_url = "http://127.0.0.1:8787/v1"`) {
			return false, nil
		}
		return false, fmt.Errorf("Codex config already has [model_providers.cxhub]; refusing to merge it automatically")
	}
	lines := strings.Split(text, "\n")
	foundProvider := false
	foundModel := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") {
			break
		}
		if strings.HasPrefix(trimmed, "model_provider =") {
			lines[i] = `model_provider = "cxhub"`
			foundProvider = true
		}
		if strings.HasPrefix(trimmed, "model =") {
			lines[i] = `model = "orchestrator"`
			foundModel = true
		}
	}
	if !foundProvider {
		lines = append([]string{`model_provider = "cxhub"`, `model = "orchestrator"`, ""}, lines...)
	} else if !foundModel {
		lines = append([]string{`model = "orchestrator"`}, lines...)
	}
	updated := strings.TrimRight(strings.Join(lines, "\n"), "\n") + "\n\n[model_providers.cxhub]\nname = \"cxhub\"\nbase_url = \"http://127.0.0.1:8787/v1\"\nwire_api = \"responses\"\nrequires_openai_auth = false\nsupports_websockets = false\n"
	if err := writeAtomic(path+".bak-cxhub", original, 0o600); err != nil {
		return false, fmt.Errorf("write Codex config backup: %w", err)
	}
	if err := writeAtomic(path, []byte(updated), 0o600); err != nil {
		return false, fmt.Errorf("write Codex config: %w", err)
	}
	return true, nil
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".cxhub-atomic-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "cxhub:", err)
		os.Exit(1)
	}
}

package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Environment string

const (
	EnvDryRun  Environment = "dry-run"
	EnvTestnet Environment = "testnet"
	EnvLive    Environment = "live"
)

type Config struct {
	APIKey                   string
	SecretKey                string
	Environment              Environment
	RESTBaseURL              string
	WSBaseURL                string
	Host                     string
	Port                     int
	Symbols                  []string
	StateDir                 string
	WindowDuration           time.Duration
	WindowMaxTicks           int
	ReplaceMinInterval       time.Duration
	OrderBudgetRatio         float64
	MaxProtectionDistancePct float64
}

func Load() (Config, error) {
	loadEnvFile()

	home, _ := os.UserHomeDir()
	stateDir := filepath.Join(home, ".binance-chase-trader", "state")
	env := Environment(getEnv("CHASER_ENV", getEnv("BINANCE_ENV", string(EnvDryRun))))

	cfg := Config{
		APIKey:                   getEnv("BINANCE_API_KEY", ""),
		SecretKey:                getEnv("BINANCE_SECRET_KEY", ""),
		Environment:              env,
		Host:                     getEnv("BINANCE_DAEMON_HOST", "127.0.0.1"),
		Port:                     getEnvInt("BINANCE_DAEMON_PORT", 8765),
		Symbols:                  splitSymbols(getEnv("CHASER_SYMBOLS", "XAGUSDT,XAUUSDT")),
		StateDir:                 getEnv("CHASER_STATE_DIR", stateDir),
		WindowDuration:           time.Duration(getEnvInt("CHASER_WINDOW_SECONDS", 60)) * time.Second,
		WindowMaxTicks:           getEnvInt("CHASER_WINDOW_MAX_TICKS", 1000),
		ReplaceMinInterval:       time.Duration(getEnvInt("CHASER_REPLACE_MIN_INTERVAL_MS", 1000)) * time.Millisecond,
		OrderBudgetRatio:         getEnvFloat("CHASER_ORDER_BUDGET_RATIO", 0.2),
		MaxProtectionDistancePct: getEnvFloat("CHASER_PROTECTION_MAX_DISTANCE_PCT", 0.5),
	}

	if err := cfg.Finalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) Finalize() error {
	switch c.Environment {
	case EnvDryRun:
		c.RESTBaseURL = getEnv("BINANCE_REST_BASE", "https://fapi.binance.com")
		c.WSBaseURL = getEnv("BINANCE_WS_BASE", "wss://fstream.binance.com")
	case EnvTestnet:
		c.RESTBaseURL = getEnv("BINANCE_REST_BASE", "https://demo-fapi.binance.com")
		c.WSBaseURL = getEnv("BINANCE_WS_BASE", "wss://fstream.binancefuture.com")
	case EnvLive:
		c.RESTBaseURL = getEnv("BINANCE_REST_BASE", "https://fapi.binance.com")
		c.WSBaseURL = getEnv("BINANCE_WS_BASE", "wss://fstream.binance.com")
	default:
		return fmt.Errorf("未知环境: %s", c.Environment)
	}

	if c.Environment != EnvDryRun && (c.APIKey == "" || c.SecretKey == "") {
		return errors.New("testnet/live 模式必须配置 BINANCE_API_KEY 和 BINANCE_SECRET_KEY")
	}
	if c.OrderBudgetRatio <= 0 || c.OrderBudgetRatio > 1 {
		c.OrderBudgetRatio = 0.2
	}
	if c.MaxProtectionDistancePct <= 0 {
		c.MaxProtectionDistancePct = 0.5
	}
	return nil
}

func (c Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c Config) IsDryRun() bool {
	return c.Environment == EnvDryRun
}

func ApplyCommonFlags(cfg *Config, symbols string, stateDir string, env string, host string, port int) {
	if symbols != "" {
		cfg.Symbols = splitSymbols(symbols)
	}
	if stateDir != "" {
		cfg.StateDir = stateDir
	}
	if env != "" {
		cfg.Environment = Environment(env)
	}
	if host != "" {
		cfg.Host = host
	}
	if port > 0 {
		cfg.Port = port
	}
}

func getEnv(name string, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvFloat(name string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}

func splitSymbols(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.ToUpper(strings.TrimSpace(p))
		if s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return []string{"XAGUSDT", "XAUUSDT"}
	}
	return out
}

func loadEnvFile() {
	home, _ := os.UserHomeDir()
	candidates := []string{
		filepath.Join(".", ".env"),
		filepath.Join(home, ".binance-chase", ".env"),
		filepath.Join(home, ".hermes", ".env"),
	}
	for _, path := range candidates {
		if loadOneEnv(path) {
			return
		}
	}
}

func loadOneEnv(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		key := strings.TrimSpace(parts[0])
		val := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
	return true
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/fc470505146/binance-chase-trader/internal/config"
	"github.com/fc470505146/binance-chase-trader/internal/control"
	"github.com/fc470505146/binance-chase-trader/internal/domain"
	"github.com/fc470505146/binance-chase-trader/internal/service"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		must(runServe(os.Args[2:]))
	case "order":
		must(runOrder(os.Args[2:]))
	case "window":
		must(runSimple("window", os.Args[2:]))
	case "tasks":
		must(runSimple("tasks", os.Args[2:]))
	case "plans":
		must(runSimple("plans", os.Args[2:]))
	case "cancel":
		must(runCancel(os.Args[2:]))
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func runServe(args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	env := fs.String("env", string(cfg.Environment), "运行环境: dry-run/testnet/live")
	symbols := fs.String("symbols", strings.Join(cfg.Symbols, ","), "逗号分隔的 symbol 列表")
	host := fs.String("host", cfg.Host, "本地控制服务 host")
	port := fs.Int("port", cfg.Port, "本地控制服务 port")
	stateDir := fs.String("state-dir", cfg.StateDir, "状态文件目录")
	if err := fs.Parse(args); err != nil {
		return err
	}
	config.ApplyCommonFlags(&cfg, *symbols, *stateDir, *env, *host, *port)
	if err := cfg.Finalize(); err != nil {
		return err
	}

	svc, err := service.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctrl := control.NewServer(cfg.Addr(), svc)
	errCh := make(chan error, 2)
	go func() { errCh <- ctrl.Run(ctx) }()
	go func() { errCh <- svc.Run(ctx) }()

	log.Printf("chaser serve 已启动 env=%s addr=%s symbols=%s state=%s", cfg.Environment, cfg.Addr(), strings.Join(cfg.Symbols, ","), cfg.StateDir)
	err = <-errCh
	if err == context.Canceled {
		return nil
	}
	return err
}

func runOrder(args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	clientFlags, rest, err := parseClientArgs(args, cfg.Host, cfg.Port)
	if err != nil {
		return err
	}
	tp, err := requiredFloat(clientFlags.values, "tp")
	if err != nil {
		return err
	}
	sl, err := requiredFloat(clientFlags.values, "sl")
	if err != nil {
		return err
	}
	if len(rest) != 4 {
		return fmt.Errorf("用法: chaser order <symbol> <BUY|SELL> <qty> <LONG|SHORT> --tp <price> --sl <price>")
	}
	qty, err := strconv.ParseFloat(rest[2], 64)
	if err != nil {
		return fmt.Errorf("quantity 格式错误: %w", err)
	}
	payload := domain.OrderRequest{
		Symbol:       strings.ToUpper(rest[0]),
		Side:         domain.Side(strings.ToUpper(rest[1])),
		Quantity:     qty,
		PositionSide: domain.PositionSide(strings.ToUpper(rest[3])),
		TakeProfit:   tp,
		StopLoss:     sl,
	}
	return callAndPrint(fmt.Sprintf("%s:%d", clientFlags.host, clientFlags.port), "order", payload)
}

func runSimple(command string, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	clientFlags, rest, err := parseClientArgs(args, cfg.Host, cfg.Port)
	if err != nil {
		return err
	}
	payload := map[string]string{}
	if command == "window" && len(rest) > 0 {
		payload["symbol"] = strings.ToUpper(rest[0])
	}
	return callAndPrint(fmt.Sprintf("%s:%d", clientFlags.host, clientFlags.port), command, payload)
}

func runCancel(args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	clientFlags, rest, err := parseClientArgs(args, cfg.Host, cfg.Port)
	if err != nil {
		return err
	}
	if len(rest) != 1 {
		return fmt.Errorf("用法: chaser cancel <taskId>")
	}
	return callAndPrint(fmt.Sprintf("%s:%d", clientFlags.host, clientFlags.port), "cancel", map[string]string{"taskId": rest[0]})
}

func callAndPrint(addr string, command string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := control.Call(addr, control.Request{Command: command, Payload: raw})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(resp.Data)
}

func usage() {
	fmt.Print(`binance-chase-trader Go CLI

用法:
  chaser serve [--env dry-run|testnet|live] [--symbols XAGUSDT,XAUUSDT]
  chaser order <symbol> <BUY|SELL> <qty> <LONG|SHORT> --tp <price> --sl <price>
  chaser window [symbol]
  chaser tasks
  chaser plans
  chaser cancel <taskId>

说明:
  默认环境是 dry-run，不会真实下单。
  实盘必须显式使用: chaser serve --env live
`)
}

func must(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, "错误:", err)
	os.Exit(1)
}

type clientFlags struct {
	host   string
	port   int
	values map[string]string
}

func parseClientArgs(args []string, defaultHost string, defaultPort int) (clientFlags, []string, error) {
	flags := clientFlags{
		host:   defaultHost,
		port:   defaultPort,
		values: map[string]string{},
	}
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			rest = append(rest, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if name == "" {
			continue
		}
		var val string
		if strings.Contains(name, "=") {
			parts := strings.SplitN(name, "=", 2)
			name, val = parts[0], parts[1]
		} else {
			if i+1 >= len(args) {
				return flags, rest, fmt.Errorf("参数 %s 缺少值", arg)
			}
			i++
			val = args[i]
		}
		switch name {
		case "host":
			flags.host = val
		case "port":
			p, err := strconv.Atoi(val)
			if err != nil {
				return flags, rest, fmt.Errorf("port 格式错误: %w", err)
			}
			flags.port = p
		default:
			flags.values[name] = val
		}
	}
	return flags, rest, nil
}

func requiredFloat(values map[string]string, key string) (float64, error) {
	raw := values[key]
	if raw == "" {
		return 0, fmt.Errorf("缺少 --%s", key)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s 格式错误: %w", key, err)
	}
	return v, nil
}

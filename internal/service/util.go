package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/fc470505146/binance-chase-trader/internal/domain"
)

func newID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func newClientOrderID(prefix string) string {
	raw := fmt.Sprintf("chaser_%s_%d", prefix, time.Now().UnixNano())
	if len(raw) <= 36 {
		return raw
	}
	return raw[:36]
}

func cloneMarkets(in map[string]domain.MarketSnapshot) map[string]domain.MarketSnapshot {
	out := make(map[string]domain.MarketSnapshot, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneHistory(in map[string][]domain.MarketTick) map[string][]domain.MarketTick {
	out := make(map[string][]domain.MarketTick, len(in))
	for k, v := range in {
		out[k] = append([]domain.MarketTick(nil), v...)
	}
	return out
}

func cloneTasks(in map[string]*domain.ChaseTask) map[string]*domain.ChaseTask {
	out := make(map[string]*domain.ChaseTask, len(in))
	for k, v := range in {
		if v == nil {
			continue
		}
		cp := *v
		out[k] = &cp
	}
	return out
}

func clonePlans(in map[string]*domain.ProtectionPlan) map[string]*domain.ProtectionPlan {
	out := make(map[string]*domain.ProtectionPlan, len(in))
	for k, v := range in {
		if v == nil {
			continue
		}
		cp := *v
		out[k] = &cp
	}
	return out
}

func normalizeSymbol(symbol string) string {
	return strings.ToUpper(strings.TrimSpace(symbol))
}

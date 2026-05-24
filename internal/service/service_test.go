package service

import (
	"context"
	"testing"
	"time"

	"github.com/fc470505146/binance-chase-trader/internal/config"
	"github.com/fc470505146/binance-chase-trader/internal/domain"
)

func testService(t *testing.T) *Service {
	t.Helper()
	cfg := config.Config{
		Environment:        config.EnvDryRun,
		RESTBaseURL:        "https://fapi.binance.com",
		WSBaseURL:          "wss://fstream.binance.com",
		Host:               "127.0.0.1",
		Port:               0,
		Symbols:            []string{"XAGUSDT"},
		StateDir:           t.TempDir(),
		WindowDuration:     60 * time.Second,
		WindowMaxTicks:     1000,
		ReplaceMinInterval: time.Second,
		OrderBudgetRatio:   0.2,
	}
	svc, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestSubmitEntryBlocksConcurrentSameSideChase(t *testing.T) {
	svc := testService(t)
	req := domain.OrderRequest{
		Symbol:       "XAGUSDT",
		Side:         domain.SideBuy,
		Quantity:     3,
		PositionSide: domain.PositionLong,
		TakeProfit:   80,
		StopLoss:     76,
	}

	if _, err := svc.SubmitEntry(context.Background(), req); err != nil {
		t.Fatalf("first submit failed: %v", err)
	}
	if _, err := svc.SubmitEntry(context.Background(), req); err == nil {
		t.Fatal("expected concurrent same-side entry to be blocked")
	}
}

func TestFilledEntriesCreateMultipleProtectionPlans(t *testing.T) {
	svc := testService(t)
	req := domain.OrderRequest{
		Symbol:       "XAGUSDT",
		Side:         domain.SideBuy,
		Quantity:     3,
		PositionSide: domain.PositionLong,
		TakeProfit:   80,
		StopLoss:     76,
	}

	result1, err := svc.SubmitEntry(context.Background(), req)
	if err != nil {
		t.Fatalf("first submit failed: %v", err)
	}
	task1 := findTask(t, svc, result1.TaskID)
	svc.OnOrderUpdate(domain.OrderUpdate{
		Symbol:        "XAGUSDT",
		ClientOrderID: task1.ClientOrderID,
		OrderID:       task1.OrderID,
		Status:        "FILLED",
		FilledQty:     3,
		OrigQty:       3,
	})

	req.TakeProfit = 82
	req.StopLoss = 77
	req.Quantity = 2
	result2, err := svc.SubmitEntry(context.Background(), req)
	if err != nil {
		t.Fatalf("second submit after fill failed: %v", err)
	}
	task2 := findTask(t, svc, result2.TaskID)
	svc.OnOrderUpdate(domain.OrderUpdate{
		Symbol:        "XAGUSDT",
		ClientOrderID: task2.ClientOrderID,
		OrderID:       task2.OrderID,
		Status:        "FILLED",
		FilledQty:     2,
		OrigQty:       2,
	})

	plans := svc.Plans()
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}
	if plans[0].ProtectedQty+plans[1].ProtectedQty != 5 {
		t.Fatalf("expected protected qty sum 5, got %v + %v", plans[0].ProtectedQty, plans[1].ProtectedQty)
	}
}

func findTask(t *testing.T, svc *Service, id string) *domain.ChaseTask {
	t.Helper()
	for _, task := range svc.Tasks() {
		if task.ID == id {
			return task
		}
	}
	t.Fatalf("task not found: %s", id)
	return nil
}

package domain

import "time"

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type PositionSide string

const (
	PositionLong  PositionSide = "LONG"
	PositionShort PositionSide = "SHORT"
)

type TaskKind string

const (
	TaskEntry TaskKind = "ENTRY"
	TaskExit  TaskKind = "EXIT"
)

type TaskStatus string

const (
	TaskPending         TaskStatus = "PENDING"
	TaskPlaced          TaskStatus = "PLACED"
	TaskChasing         TaskStatus = "CHASING"
	TaskPartiallyFilled TaskStatus = "PARTIALLY_FILLED"
	TaskFilled          TaskStatus = "FILLED"
	TaskCanceled        TaskStatus = "CANCELED"
	TaskFailed          TaskStatus = "FAILED"
	TaskDone            TaskStatus = "DONE"
)

type PlanStatus string

const (
	PlanPending         PlanStatus = "PENDING"
	PlanArmed           PlanStatus = "ARMED"
	PlanTriggered       PlanStatus = "TRIGGERED"
	PlanClosed          PlanStatus = "CLOSED"
	PlanNeedsProtection PlanStatus = "NEEDS_PROTECTION"
	PlanNeedsReconcile  PlanStatus = "NEEDS_RECONCILE"
)

type TriggerType string

const (
	TriggerTakeProfit TriggerType = "TAKE_PROFIT"
	TriggerStopLoss   TriggerType = "STOP_LOSS"
)

type MarketSnapshot struct {
	Symbol    string    `json:"symbol"`
	Bid       float64   `json:"bid"`
	Ask       float64   `json:"ask"`
	Mid       float64   `json:"mid"`
	MarkPrice float64   `json:"markPrice"`
	EventTime time.Time `json:"eventTime"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type MarketTick struct {
	Time      time.Time `json:"time"`
	Bid       float64   `json:"bid,omitempty"`
	Ask       float64   `json:"ask,omitempty"`
	MarkPrice float64   `json:"markPrice,omitempty"`
}

type WindowState struct {
	Symbol    string            `json:"symbol"`
	Market    MarketSnapshot    `json:"market"`
	History   []MarketTick      `json:"history"`
	Tasks     []*ChaseTask      `json:"tasks,omitempty"`
	Plans     []*ProtectionPlan `json:"plans,omitempty"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

type ChaseTask struct {
	ID            string       `json:"id"`
	Kind          TaskKind     `json:"kind"`
	Symbol        string       `json:"symbol"`
	Side          Side         `json:"side"`
	PositionSide  PositionSide `json:"positionSide"`
	Quantity      float64      `json:"quantity"`
	FilledQty     float64      `json:"filledQty"`
	RemainingQty  float64      `json:"remainingQty"`
	TargetPrice   float64      `json:"targetPrice"`
	Status        TaskStatus   `json:"status"`
	PlanID        string       `json:"planId,omitempty"`
	ClientOrderID string       `json:"clientOrderId,omitempty"`
	OrderID       int64        `json:"orderId,omitempty"`
	TakeProfit    float64      `json:"takeProfit,omitempty"`
	StopLoss      float64      `json:"stopLoss,omitempty"`
	ReplaceCount  int          `json:"replaceCount"`
	LastReplaceAt time.Time    `json:"lastReplaceAt,omitempty"`
	LastError     string       `json:"lastError,omitempty"`
	CreatedAt     time.Time    `json:"createdAt"`
	UpdatedAt     time.Time    `json:"updatedAt"`
}

type ProtectionPlan struct {
	ID            string       `json:"id"`
	Symbol        string       `json:"symbol"`
	PositionSide  PositionSide `json:"positionSide"`
	EntryTaskID   string       `json:"entryTaskId"`
	ExitTaskID    string       `json:"exitTaskId,omitempty"`
	EntryQty      float64      `json:"entryQty"`
	ProtectedQty  float64      `json:"protectedQty"`
	TakeProfit    float64      `json:"takeProfit"`
	StopLoss      float64      `json:"stopLoss"`
	TriggerSource string       `json:"triggerSource"`
	TriggerType   TriggerType  `json:"triggerType,omitempty"`
	TriggerPrice  float64      `json:"triggerPrice,omitempty"`
	Status        PlanStatus   `json:"status"`
	CreatedAt     time.Time    `json:"createdAt"`
	UpdatedAt     time.Time    `json:"updatedAt"`
}

type OrderUpdate struct {
	Symbol        string       `json:"symbol"`
	ClientOrderID string       `json:"clientOrderId"`
	OrderID       int64        `json:"orderId"`
	Side          Side         `json:"side"`
	PositionSide  PositionSide `json:"positionSide"`
	Status        string       `json:"status"`
	ExecutionType string       `json:"executionType"`
	OrigQty       float64      `json:"origQty"`
	FilledQty     float64      `json:"filledQty"`
	LastQty       float64      `json:"lastQty"`
	Price         float64      `json:"price"`
	AvgPrice      float64      `json:"avgPrice"`
	EventTime     time.Time    `json:"eventTime"`
}

type OrderRequest struct {
	Symbol       string       `json:"symbol"`
	Side         Side         `json:"side"`
	Quantity     float64      `json:"quantity"`
	PositionSide PositionSide `json:"positionSide"`
	TakeProfit   float64      `json:"takeProfit"`
	StopLoss     float64      `json:"stopLoss"`
}

type OrderResult struct {
	TaskID string `json:"taskId"`
	PlanID string `json:"planId,omitempty"`
}

func OppositeSide(side Side) Side {
	if side == SideBuy {
		return SideSell
	}
	return SideBuy
}

func CloseSide(pos PositionSide) Side {
	if pos == PositionLong {
		return SideSell
	}
	return SideBuy
}

func IsValidSide(side Side) bool {
	return side == SideBuy || side == SideSell
}

func IsValidPositionSide(side PositionSide) bool {
	return side == PositionLong || side == PositionShort
}

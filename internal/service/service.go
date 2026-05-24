package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fc470505146/binance-chase-trader/internal/binance"
	"github.com/fc470505146/binance-chase-trader/internal/config"
	"github.com/fc470505146/binance-chase-trader/internal/domain"
	"github.com/fc470505146/binance-chase-trader/internal/store"
)

type Service struct {
	cfg     config.Config
	client  *binance.Client
	streams *binance.Streams
	store   *store.Store
	replace *ReplaceLimiter
	orders  *TokenBucket

	mu          sync.Mutex
	markets     map[string]domain.MarketSnapshot
	history     map[string][]domain.MarketTick
	tasks       map[string]*domain.ChaseTask
	plans       map[string]*domain.ProtectionPlan
	clientIndex map[string]string
	orderIndex  map[int64]string
	limit10s    int
	limit1m     int
}

func New(cfg config.Config) (*Service, error) {
	st := store.New(cfg.StateDir)
	if err := st.Ensure(); err != nil {
		return nil, err
	}
	snap, err := st.Load()
	if err != nil {
		return nil, err
	}

	client := binance.NewClient(cfg)
	s := &Service{
		cfg:         cfg,
		client:      client,
		streams:     binance.NewStreams(cfg, client),
		store:       st,
		replace:     NewReplaceLimiter(cfg.ReplaceMinInterval),
		orders:      NewTokenBucket(60, 5),
		markets:     snap.Markets,
		history:     snap.History,
		tasks:       snap.Tasks,
		plans:       snap.Plans,
		clientIndex: map[string]string{},
		orderIndex:  map[int64]string{},
	}
	if s.markets == nil {
		s.markets = map[string]domain.MarketSnapshot{}
	}
	if s.history == nil {
		s.history = map[string][]domain.MarketTick{}
	}
	if s.tasks == nil {
		s.tasks = map[string]*domain.ChaseTask{}
	}
	if s.plans == nil {
		s.plans = map[string]*domain.ProtectionPlan{}
	}
	s.rebuildIndexes()
	return s, nil
}

func (s *Service) Run(ctx context.Context) error {
	s.configureRateLimits(ctx)
	s.streams.RunMarket(ctx, s.cfg.Symbols, s.OnMarket)
	s.streams.RunUser(ctx, s.OnOrderUpdate)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.reconcile(ctx)
		}
	}
}

func (s *Service) SubmitEntry(ctx context.Context, req domain.OrderRequest) (domain.OrderResult, error) {
	req.Symbol = strings.ToUpper(strings.TrimSpace(req.Symbol))
	if req.Symbol == "" {
		return domain.OrderResult{}, errors.New("symbol 不能为空")
	}
	if !domain.IsValidSide(req.Side) {
		return domain.OrderResult{}, fmt.Errorf("无效 side: %s", req.Side)
	}
	if !domain.IsValidPositionSide(req.PositionSide) {
		return domain.OrderResult{}, fmt.Errorf("无效 positionSide: %s", req.PositionSide)
	}
	if req.Quantity <= 0 {
		return domain.OrderResult{}, errors.New("quantity 必须大于 0")
	}
	if req.TakeProfit <= 0 || req.StopLoss <= 0 {
		return domain.OrderResult{}, errors.New("固定 takeProfit/stopLoss 必须同时提供")
	}
	if req.PositionSide == domain.PositionLong && req.Side != domain.SideBuy {
		return domain.OrderResult{}, errors.New("LONG 开仓必须使用 BUY")
	}
	if req.PositionSide == domain.PositionShort && req.Side != domain.SideSell {
		return domain.OrderResult{}, errors.New("SHORT 开仓必须使用 SELL")
	}

	now := time.Now()
	task := &domain.ChaseTask{
		ID:            newID("task"),
		Kind:          domain.TaskEntry,
		Symbol:        req.Symbol,
		Side:          req.Side,
		PositionSide:  req.PositionSide,
		Quantity:      req.Quantity,
		RemainingQty:  req.Quantity,
		Status:        domain.TaskPending,
		ClientOrderID: newClientOrderID("entry"),
		TakeProfit:    req.TakeProfit,
		StopLoss:      req.StopLoss,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	s.mu.Lock()
	if s.hasActiveEntryLocked(req.Symbol, req.PositionSide) {
		s.mu.Unlock()
		return domain.OrderResult{}, fmt.Errorf("%s %s 已有开仓追单在途，首版同向开仓追单串行执行", req.Symbol, req.PositionSide)
	}
	s.tasks[task.ID] = task
	s.clientIndex[task.ClientOrderID] = task.ID
	s.mu.Unlock()

	if err := s.persist("task_created", task); err != nil {
		log.Printf("保存 task 失败: %v", err)
	}
	if err := s.placeTask(ctx, task.ID); err != nil {
		return domain.OrderResult{}, err
	}
	return domain.OrderResult{TaskID: task.ID}, nil
}

func (s *Service) CancelTask(ctx context.Context, taskID string) error {
	s.mu.Lock()
	task := s.tasks[taskID]
	if task == nil {
		s.mu.Unlock()
		return fmt.Errorf("找不到 task: %s", taskID)
	}
	orderID := task.OrderID
	symbol := task.Symbol
	task.Status = domain.TaskCanceled
	task.UpdatedAt = time.Now()
	s.mu.Unlock()

	if orderID != 0 {
		if err := s.client.CancelOrder(ctx, symbol, orderID); err != nil {
			s.adaptOrderBudget()
			return err
		}
		s.adaptOrderBudget()
	}
	return s.persist("task_canceled", task)
}

func (s *Service) OnMarket(snapshot domain.MarketSnapshot) {
	if snapshot.Symbol == "" {
		return
	}
	var tasksToCheck []string
	var plansToTrigger []string

	s.mu.Lock()
	current := s.markets[snapshot.Symbol]
	if snapshot.Bid > 0 {
		current.Bid = snapshot.Bid
	}
	if snapshot.Ask > 0 {
		current.Ask = snapshot.Ask
	}
	if current.Bid > 0 && current.Ask > 0 {
		current.Mid = (current.Bid + current.Ask) / 2
	}
	if snapshot.MarkPrice > 0 {
		current.MarkPrice = snapshot.MarkPrice
	}
	if !snapshot.EventTime.IsZero() {
		current.EventTime = snapshot.EventTime
	}
	current.Symbol = snapshot.Symbol
	current.UpdatedAt = time.Now()
	s.markets[snapshot.Symbol] = current
	s.appendTickLocked(snapshot.Symbol, current)

	for _, task := range s.tasks {
		if task.Symbol == snapshot.Symbol && isActiveTask(task.Status) {
			tasksToCheck = append(tasksToCheck, task.ID)
		}
	}
	for _, plan := range s.plans {
		if plan.Symbol == snapshot.Symbol && plan.Status == domain.PlanArmed && shouldTrigger(plan, current.MarkPrice) {
			plansToTrigger = append(plansToTrigger, plan.ID)
		}
	}
	s.mu.Unlock()

	for _, taskID := range tasksToCheck {
		s.maybeReplace(context.Background(), taskID)
	}
	for _, planID := range plansToTrigger {
		if err := s.triggerPlan(context.Background(), planID); err != nil {
			log.Printf("触发保护失败 %s: %v", planID, err)
		}
	}
}

func (s *Service) OnOrderUpdate(update domain.OrderUpdate) {
	s.mu.Lock()
	taskID := s.clientIndex[update.ClientOrderID]
	if taskID == "" && update.OrderID != 0 {
		taskID = s.orderIndex[update.OrderID]
	}
	task := s.tasks[taskID]
	if task == nil {
		s.mu.Unlock()
		return
	}

	task.OrderID = update.OrderID
	if update.ClientOrderID != "" {
		task.ClientOrderID = update.ClientOrderID
		s.clientIndex[update.ClientOrderID] = task.ID
	}
	if update.OrderID != 0 {
		s.orderIndex[update.OrderID] = task.ID
	}
	if update.FilledQty > task.FilledQty {
		task.FilledQty = update.FilledQty
	}
	task.RemainingQty = math.Max(0, task.Quantity-task.FilledQty)
	task.UpdatedAt = time.Now()

	switch update.Status {
	case "PARTIALLY_FILLED":
		task.Status = domain.TaskPartiallyFilled
	case "FILLED":
		task.Status = domain.TaskFilled
	case "CANCELED", "EXPIRED", "REJECTED":
		task.Status = domain.TaskCanceled
	default:
		if task.Status == domain.TaskPlaced || task.Status == domain.TaskPending {
			task.Status = domain.TaskChasing
		}
	}

	var plan *domain.ProtectionPlan
	if task.Kind == domain.TaskEntry && task.FilledQty > 0 {
		plan = s.ensurePlanLocked(task, task.FilledQty)
	}
	if task.Kind == domain.TaskExit && task.Status == domain.TaskFilled && task.PlanID != "" {
		if p := s.plans[task.PlanID]; p != nil {
			p.Status = domain.PlanClosed
			p.ProtectedQty = 0
			p.UpdatedAt = time.Now()
			plan = p
		}
	}
	s.mu.Unlock()

	if err := s.persist("order_update", update); err != nil {
		log.Printf("保存订单更新失败: %v", err)
	}
	if plan != nil {
		_ = s.persist("plan_updated", plan)
	}
}

func (s *Service) Window(symbol string) domain.WindowState {
	symbol = strings.ToUpper(symbol)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.windowLocked(symbol)
}

func (s *Service) Windows() []domain.WindowState {
	s.mu.Lock()
	defer s.mu.Unlock()
	symbols := make([]string, 0, len(s.markets))
	seen := map[string]bool{}
	for _, symbol := range s.cfg.Symbols {
		symbol = strings.ToUpper(symbol)
		symbols = append(symbols, symbol)
		seen[symbol] = true
	}
	for symbol := range s.markets {
		if !seen[symbol] {
			symbols = append(symbols, symbol)
		}
	}
	sort.Strings(symbols)
	out := make([]domain.WindowState, 0, len(symbols))
	for _, symbol := range symbols {
		out = append(out, s.windowLocked(symbol))
	}
	return out
}

func (s *Service) Tasks() []*domain.ChaseTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*domain.ChaseTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		cp := *task
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *Service) Plans() []*domain.ProtectionPlan {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*domain.ProtectionPlan, 0, len(s.plans))
	for _, plan := range s.plans {
		cp := *plan
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *Service) placeTask(ctx context.Context, taskID string) error {
	s.mu.Lock()
	task := s.tasks[taskID]
	if task == nil {
		s.mu.Unlock()
		return fmt.Errorf("找不到 task: %s", taskID)
	}
	task.TargetPrice = s.targetPriceLocked(task.Symbol, task.Side)
	task.Status = domain.TaskPlaced
	task.UpdatedAt = time.Now()
	s.mu.Unlock()

	if err := s.orders.Wait(ctx); err != nil {
		return err
	}
	resp, err := s.client.PlaceQueueLimit(ctx, task)
	s.adaptOrderBudget()
	if err != nil {
		s.markTaskFailed(taskID, err)
		return err
	}

	s.mu.Lock()
	task = s.tasks[taskID]
	if task != nil {
		task.OrderID = resp.OrderID
		if resp.ClientOrderID != "" {
			task.ClientOrderID = resp.ClientOrderID
			s.clientIndex[task.ClientOrderID] = task.ID
		}
		task.Status = domain.TaskChasing
		task.UpdatedAt = time.Now()
		s.orderIndex[task.OrderID] = task.ID
	}
	s.mu.Unlock()
	return s.persist("task_placed", task)
}

func (s *Service) maybeReplace(ctx context.Context, taskID string) {
	s.mu.Lock()
	task := s.tasks[taskID]
	if task == nil || !isActiveTask(task.Status) || task.RemainingQty <= 0 {
		s.mu.Unlock()
		return
	}
	target := s.targetPriceLocked(task.Symbol, task.Side)
	if target <= 0 || nearlyEqual(target, task.TargetPrice) {
		s.mu.Unlock()
		return
	}
	key := task.Symbol + ":" + string(task.PositionSide)
	if !s.replace.Allow(key) {
		s.mu.Unlock()
		return
	}
	orderID := task.OrderID
	symbol := task.Symbol
	s.mu.Unlock()

	if orderID != 0 {
		if err := s.client.CancelOrder(ctx, symbol, orderID); err != nil {
			s.adaptOrderBudget()
			log.Printf("撤单失败 task=%s order=%d: %v", taskID, orderID, err)
			return
		}
		s.adaptOrderBudget()
	}

	s.mu.Lock()
	task = s.tasks[taskID]
	if task != nil {
		task.ReplaceCount++
		task.ClientOrderID = newClientOrderID("replace")
		s.clientIndex[task.ClientOrderID] = task.ID
		task.UpdatedAt = time.Now()
	}
	s.mu.Unlock()

	if err := s.placeTask(ctx, taskID); err != nil {
		log.Printf("重挂失败 task=%s: %v", taskID, err)
	}
}

func (s *Service) triggerPlan(ctx context.Context, planID string) error {
	s.mu.Lock()
	plan := s.plans[planID]
	if plan == nil {
		s.mu.Unlock()
		return fmt.Errorf("找不到 protection plan: %s", planID)
	}
	if plan.Status != domain.PlanArmed {
		s.mu.Unlock()
		return nil
	}
	market := s.markets[plan.Symbol]
	triggerType := triggerType(plan, market.MarkPrice)
	if triggerType == "" {
		s.mu.Unlock()
		return nil
	}
	now := time.Now()
	plan.Status = domain.PlanTriggered
	plan.TriggerType = triggerType
	plan.TriggerPrice = market.MarkPrice
	plan.UpdatedAt = now

	if entry := s.tasks[plan.EntryTaskID]; entry != nil && isActiveTask(entry.Status) && entry.OrderID != 0 {
		entry.Status = domain.TaskCanceled
		entry.UpdatedAt = now
		go func(symbol string, orderID int64) {
			if err := s.client.CancelOrder(context.Background(), symbol, orderID); err != nil {
				log.Printf("保护触发撤剩余开仓失败 %s %d: %v", symbol, orderID, err)
			}
		}(entry.Symbol, entry.OrderID)
	}

	exitTask := &domain.ChaseTask{
		ID:            newID("task"),
		Kind:          domain.TaskExit,
		Symbol:        plan.Symbol,
		Side:          domain.CloseSide(plan.PositionSide),
		PositionSide:  plan.PositionSide,
		Quantity:      plan.ProtectedQty,
		RemainingQty:  plan.ProtectedQty,
		Status:        domain.TaskPending,
		PlanID:        plan.ID,
		ClientOrderID: newClientOrderID("exit"),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	plan.ExitTaskID = exitTask.ID
	s.tasks[exitTask.ID] = exitTask
	s.clientIndex[exitTask.ClientOrderID] = exitTask.ID
	s.mu.Unlock()

	if err := s.persist("plan_triggered", plan); err != nil {
		log.Printf("保存 plan trigger 失败: %v", err)
	}
	return s.placeTask(ctx, exitTask.ID)
}

func (s *Service) ensurePlanLocked(task *domain.ChaseTask, protectedQty float64) *domain.ProtectionPlan {
	for _, plan := range s.plans {
		if plan.EntryTaskID == task.ID {
			if protectedQty > plan.ProtectedQty {
				plan.ProtectedQty = protectedQty
			}
			plan.Status = domain.PlanArmed
			plan.UpdatedAt = time.Now()
			return plan
		}
	}
	now := time.Now()
	plan := &domain.ProtectionPlan{
		ID:            newID("plan"),
		Symbol:        task.Symbol,
		PositionSide:  task.PositionSide,
		EntryTaskID:   task.ID,
		EntryQty:      task.Quantity,
		ProtectedQty:  protectedQty,
		TakeProfit:    task.TakeProfit,
		StopLoss:      task.StopLoss,
		TriggerSource: "markPrice",
		Status:        domain.PlanArmed,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	s.plans[plan.ID] = plan
	return plan
}

func (s *Service) reconcile(ctx context.Context) {
	if s.cfg.IsDryRun() {
		return
	}
	for _, symbol := range s.cfg.Symbols {
		positions, err := s.client.Positions(ctx, symbol)
		if err != nil {
			log.Printf("reconcile 持仓失败 %s: %v", symbol, err)
			continue
		}
		s.markNeedsProtection(symbol, positions)
	}
	_ = s.persist("reconcile", map[string]any{"symbols": s.cfg.Symbols})
}

func (s *Service) configureRateLimits(ctx context.Context) {
	info, err := s.client.ExchangeInfo(ctx)
	if err != nil {
		log.Printf("读取 exchangeInfo 失败，使用默认限频预算: %v", err)
		return
	}
	capacity := 0
	refill := 0.0
	for _, rl := range info.RateLimits {
		if rl.RateLimitType != "ORDERS" || rl.Limit <= 0 {
			continue
		}
		seconds := intervalSeconds(rl.Interval, rl.IntervalNum)
		if seconds <= 0 {
			continue
		}
		candidateCapacity := int(math.Max(1, math.Floor(float64(rl.Limit)*s.cfg.OrderBudgetRatio)))
		candidateRefill := float64(rl.Limit) * s.cfg.OrderBudgetRatio / seconds
		if capacity == 0 || candidateCapacity < capacity {
			capacity = candidateCapacity
		}
		if refill == 0 || candidateRefill < refill {
			refill = candidateRefill
		}
		if rl.Interval == "SECOND" && rl.IntervalNum == 10 {
			s.limit10s = rl.Limit
		}
		if rl.Interval == "MINUTE" && rl.IntervalNum == 1 {
			s.limit1m = rl.Limit
		}
	}
	if capacity > 0 && refill > 0 {
		s.orders.Update(capacity, refill)
		log.Printf("订单限频预算已设置: capacity=%d refill=%.2f/s ratio=%.2f", capacity, refill, s.cfg.OrderBudgetRatio)
	}
}

func (s *Service) adaptOrderBudget() {
	usage := s.client.LastRateUsage()
	if usage.StatusCode == 0 {
		return
	}
	if usage.StatusCode == 429 || usage.StatusCode == 418 {
		s.orders.Update(1, 0.2)
		log.Printf("Binance 返回 %d，订单限频预算降至最低，retry-after=%s", usage.StatusCode, usage.RetryAfter)
		return
	}
	if s.limit10s > 0 && usage.OrderCount10s >= int(float64(s.limit10s)*0.8) {
		s.orders.Update(1, 0.5)
		log.Printf("10s 订单计数接近阈值: %d/%d，临时降速", usage.OrderCount10s, s.limit10s)
		return
	}
	if s.limit1m > 0 && usage.OrderCount1m >= int(float64(s.limit1m)*0.8) {
		s.orders.Update(1, 0.5)
		log.Printf("1m 订单计数接近阈值: %d/%d，临时降速", usage.OrderCount1m, s.limit1m)
	}
}

func (s *Service) markNeedsProtection(symbol string, positions []binance.PositionRisk) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range positions {
		posSide := domain.PositionSide(p.PositionSide)
		if !domain.IsValidPositionSide(posSide) {
			continue
		}
		amt := parseFloat(p.PositionAmt)
		if math.Abs(amt) <= 0 {
			continue
		}
		protected := 0.0
		for _, plan := range s.plans {
			if plan.Symbol == symbol && plan.PositionSide == posSide && plan.Status == domain.PlanArmed {
				protected += plan.ProtectedQty
			}
		}
		if protected+1e-9 >= math.Abs(amt) {
			continue
		}
		now := time.Now()
		plan := &domain.ProtectionPlan{
			ID:            newID("plan"),
			Symbol:        symbol,
			PositionSide:  posSide,
			ProtectedQty:  math.Abs(amt) - protected,
			TriggerSource: "markPrice",
			Status:        domain.PlanNeedsProtection,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		s.plans[plan.ID] = plan
	}
}

func (s *Service) windowLocked(symbol string) domain.WindowState {
	tasks := make([]*domain.ChaseTask, 0)
	for _, task := range s.tasks {
		if symbol == "" || task.Symbol == symbol {
			cp := *task
			tasks = append(tasks, &cp)
		}
	}
	plans := make([]*domain.ProtectionPlan, 0)
	for _, plan := range s.plans {
		if symbol == "" || plan.Symbol == symbol {
			cp := *plan
			plans = append(plans, &cp)
		}
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].CreatedAt.Before(tasks[j].CreatedAt) })
	sort.Slice(plans, func(i, j int) bool { return plans[i].CreatedAt.Before(plans[j].CreatedAt) })
	history := append([]domain.MarketTick(nil), s.history[symbol]...)
	if history == nil {
		history = []domain.MarketTick{}
	}
	market := s.markets[symbol]
	if market.Symbol == "" {
		market.Symbol = symbol
	}
	return domain.WindowState{
		Symbol:    symbol,
		Market:    market,
		History:   history,
		Tasks:     tasks,
		Plans:     plans,
		UpdatedAt: time.Now(),
	}
}

func (s *Service) persist(event string, data any) error {
	if err := s.store.Append(event, data); err != nil {
		return err
	}
	s.mu.Lock()
	snap := store.Snapshot{
		Markets: cloneMarkets(s.markets),
		History: cloneHistory(s.history),
		Tasks:   cloneTasks(s.tasks),
		Plans:   clonePlans(s.plans),
	}
	s.mu.Unlock()
	return s.store.Save(snap)
}

func (s *Service) targetPriceLocked(symbol string, side domain.Side) float64 {
	market := s.markets[symbol]
	if side == domain.SideBuy {
		return market.Bid
	}
	return market.Ask
}

func (s *Service) hasActiveEntryLocked(symbol string, side domain.PositionSide) bool {
	for _, task := range s.tasks {
		if task.Symbol == symbol && task.PositionSide == side && task.Kind == domain.TaskEntry && isActiveTask(task.Status) {
			return true
		}
	}
	return false
}

func (s *Service) appendTickLocked(symbol string, snapshot domain.MarketSnapshot) {
	tick := domain.MarketTick{
		Time:      time.Now(),
		Bid:       snapshot.Bid,
		Ask:       snapshot.Ask,
		MarkPrice: snapshot.MarkPrice,
	}
	h := append(s.history[symbol], tick)
	cutoff := time.Now().Add(-s.cfg.WindowDuration)
	start := 0
	for start < len(h) && h[start].Time.Before(cutoff) {
		start++
	}
	h = h[start:]
	if len(h) > s.cfg.WindowMaxTicks {
		h = h[len(h)-s.cfg.WindowMaxTicks:]
	}
	s.history[symbol] = h
}

func (s *Service) rebuildIndexes() {
	for _, task := range s.tasks {
		if task.ClientOrderID != "" {
			s.clientIndex[task.ClientOrderID] = task.ID
		}
		if task.OrderID != 0 {
			s.orderIndex[task.OrderID] = task.ID
		}
	}
}

func (s *Service) markTaskFailed(taskID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task := s.tasks[taskID]; task != nil {
		task.Status = domain.TaskFailed
		task.LastError = err.Error()
		task.UpdatedAt = time.Now()
	}
}

func isActiveTask(status domain.TaskStatus) bool {
	switch status {
	case domain.TaskPending, domain.TaskPlaced, domain.TaskChasing, domain.TaskPartiallyFilled:
		return true
	default:
		return false
	}
}

func shouldTrigger(plan *domain.ProtectionPlan, price float64) bool {
	return triggerType(plan, price) != ""
}

func triggerType(plan *domain.ProtectionPlan, price float64) domain.TriggerType {
	if price <= 0 {
		return ""
	}
	switch plan.PositionSide {
	case domain.PositionLong:
		if plan.TakeProfit > 0 && price >= plan.TakeProfit {
			return domain.TriggerTakeProfit
		}
		if plan.StopLoss > 0 && price <= plan.StopLoss {
			return domain.TriggerStopLoss
		}
	case domain.PositionShort:
		if plan.TakeProfit > 0 && price <= plan.TakeProfit {
			return domain.TriggerTakeProfit
		}
		if plan.StopLoss > 0 && price >= plan.StopLoss {
			return domain.TriggerStopLoss
		}
	}
	return ""
}

func nearlyEqual(a float64, b float64) bool {
	if a == 0 || b == 0 {
		return false
	}
	return math.Abs(a-b) < 1e-12
}

func parseFloat(raw string) float64 {
	v, _ := strconv.ParseFloat(raw, 64)
	return v
}

func intervalSeconds(interval string, n int) float64 {
	if n <= 0 {
		n = 1
	}
	switch interval {
	case "SECOND":
		return float64(n)
	case "MINUTE":
		return float64(n * 60)
	case "HOUR":
		return float64(n * 3600)
	case "DAY":
		return float64(n * 86400)
	default:
		return 0
	}
}

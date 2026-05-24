package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fc470505146/binance-chase-trader/internal/domain"
)

type Snapshot struct {
	Version int                               `json:"version"`
	SavedAt time.Time                         `json:"savedAt"`
	Markets map[string]domain.MarketSnapshot  `json:"markets"`
	History map[string][]domain.MarketTick    `json:"history"`
	Tasks   map[string]*domain.ChaseTask      `json:"tasks"`
	Plans   map[string]*domain.ProtectionPlan `json:"plans"`
}

type Event struct {
	Time time.Time `json:"time"`
	Type string    `json:"type"`
	Data any       `json:"data,omitempty"`
}

type Store struct {
	dir          string
	snapshotPath string
	eventsPath   string
	mu           sync.Mutex
}

func New(dir string) *Store {
	return &Store{
		dir:          dir,
		snapshotPath: filepath.Join(dir, "snapshot.json"),
		eventsPath:   filepath.Join(dir, "events.jsonl"),
	}
}

func (s *Store) Ensure() error {
	return os.MkdirAll(s.dir, 0o755)
}

func (s *Store) Load() (Snapshot, error) {
	var snap Snapshot
	data, err := os.ReadFile(s.snapshotPath)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{
			Version: 1,
			Markets: map[string]domain.MarketSnapshot{},
			History: map[string][]domain.MarketTick{},
			Tasks:   map[string]*domain.ChaseTask{},
			Plans:   map[string]*domain.ProtectionPlan{},
		}, nil
	}
	if err != nil {
		return snap, err
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		return snap, err
	}
	if snap.Markets == nil {
		snap.Markets = map[string]domain.MarketSnapshot{}
	}
	if snap.History == nil {
		snap.History = map[string][]domain.MarketTick{}
	}
	if snap.Tasks == nil {
		snap.Tasks = map[string]*domain.ChaseTask{}
	}
	if snap.Plans == nil {
		snap.Plans = map[string]*domain.ProtectionPlan{}
	}
	return snap, nil
}

func (s *Store) Save(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Ensure(); err != nil {
		return err
	}
	snap.Version = 1
	snap.SavedAt = time.Now()

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.snapshotPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.snapshotPath)
}

func (s *Store) Append(eventType string, data any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Ensure(); err != nil {
		return err
	}
	f, err := os.OpenFile(s.eventsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	event := Event{
		Time: time.Now(),
		Type: eventType,
		Data: data,
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("写事件日志失败: %w", err)
	}
	return nil
}

package control

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"github.com/fc470505146/binance-chase-trader/internal/domain"
	"github.com/fc470505146/binance-chase-trader/internal/service"
)

type Request struct {
	Command string          `json:"command"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type Response struct {
	OK    bool   `json:"ok"`
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

type Server struct {
	addr string
	svc  *service.Service
}

func NewServer(addr string, svc *service.Service) *Server {
	return &Server{addr: addr, svc: svc}
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go s.handle(ctx, conn)
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	var req Request
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&req); err != nil {
		write(conn, Response{OK: false, Error: err.Error()})
		return
	}
	resp := s.dispatch(ctx, req)
	write(conn, resp)
}

func (s *Server) dispatch(ctx context.Context, req Request) Response {
	switch strings.ToLower(strings.TrimSpace(req.Command)) {
	case "order":
		var payload domain.OrderRequest
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		result, err := s.svc.SubmitEntry(ctx, payload)
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: result}
	case "window":
		var payload struct {
			Symbol string `json:"symbol"`
		}
		_ = json.Unmarshal(req.Payload, &payload)
		if payload.Symbol != "" {
			return Response{OK: true, Data: s.svc.Window(payload.Symbol)}
		}
		return Response{OK: true, Data: s.svc.Windows()}
	case "tasks":
		return Response{OK: true, Data: s.svc.Tasks()}
	case "plans":
		return Response{OK: true, Data: s.svc.Plans()}
	case "cancel":
		var payload struct {
			TaskID string `json:"taskId"`
		}
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		if payload.TaskID == "" {
			return Response{OK: false, Error: "taskId 不能为空"}
		}
		if err := s.svc.CancelTask(ctx, payload.TaskID); err != nil {
			return Response{OK: false, Error: err.Error()}
		}
		return Response{OK: true, Data: map[string]string{"taskId": payload.TaskID}}
	default:
		return Response{OK: false, Error: fmt.Sprintf("未知命令: %s", req.Command)}
	}
}

func write(conn net.Conn, resp Response) {
	enc := json.NewEncoder(conn)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}

func Call(addr string, req Request) (Response, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return Response{}, err
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return Response{}, err
	}
	return resp, nil
}

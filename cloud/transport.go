// MCP JSON-RPC 2.0 transport layer over stdio.
//
// Reads JSON-RPC messages from stdin, dispatches to handlers,
// and writes JSON-RPC responses/notifications to stdout.
// All logging goes to stderr.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// jsonrpcRequest represents a JSON-RPC 2.0 request.
type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// jsonrpcResponse represents a JSON-RPC 2.0 response.
type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// jsonrpcNotification represents a JSON-RPC 2.0 notification (no id field).
type jsonrpcNotification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// MethodHandler is called for each incoming request/notification.
type MethodHandler func(id json.RawMessage, params json.RawMessage) (interface{}, error)

// Transport manages MCP JSON-RPC 2.0 communication over stdio.
type Transport struct {
	mu       sync.Mutex
	out      *json.Encoder
	stdout   *os.File
	handlers map[string]MethodHandler
}

// NewTransport creates a new MCP transport reading from stdin, writing to stdout.
func NewTransport() *Transport {
	return &Transport{
		out:      json.NewEncoder(os.Stdout),
		stdout:   os.Stdout,
		handlers: make(map[string]MethodHandler),
	}
}

// RegisterHandler registers a handler for a given method name.
func (t *Transport) RegisterHandler(method string, h MethodHandler) {
	t.handlers[method] = h
}

// Run starts the main loop, reading JSON-RPC messages from stdin.
func (t *Transport) Run() error {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			return nil
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "transport: read error: %v\n", err)
			return err
		}
		t.handleMessage(line)
	}
}

// handleMessage processes a single JSON-RPC message line.
func (t *Transport) handleMessage(raw []byte) {
	// Try to unmarshal as request first
	var req jsonrpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		fmt.Fprintf(os.Stderr, "transport: json decode error: %v\n", err)
		return
	}

	// If no method field, might be a notification or invalid
	if req.Method == "" {
		// Try as notification (no id)
		var notif jsonrpcNotification
		if err := json.Unmarshal(raw, &notif); err != nil || notif.Method == "" {
			fmt.Fprintf(os.Stderr, "transport: received message without method\n")
			return
		}
		t.handleNotification(raw, &notif)
		return
	}

	t.handleRequest(&req)
}

// handleRequest processes a JSON-RPC request.
func (t *Transport) handleRequest(req *jsonrpcRequest) {
	h, ok := t.handlers[req.Method]
	if !ok {
		t.sendError(req.ID, -32601, fmt.Sprintf("Method not found: %s", req.Method))
		return
	}

	result, err := h(req.ID, req.Params)
	if err != nil {
		t.sendError(req.ID, -32603, err.Error())
		return
	}

	// If ID is nil, it's a notification - no response needed
	if len(req.ID) == 0 || string(req.ID) == "null" {
		return
	}

	t.sendResponse(req.ID, result)
}

// handleNotification processes a JSON-RPC notification (no id, no response expected).
func (t *Transport) handleNotification(raw []byte, notif *jsonrpcNotification) {
	h, ok := t.handlers[notif.Method]
	if !ok {
		return // silently ignore unknown notifications
	}
	// Call handler but discard result
	h(nil, notif.Params)
}

// sendResponse writes a JSON-RPC success response.
func (t *Transport) sendResponse(id json.RawMessage, result interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	resp := jsonrpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
	if result == nil {
		resp.Result = struct{}{}
	}
	t.out.Encode(resp)
	t.stdout.Sync()
}

// sendError writes a JSON-RPC error response.
func (t *Transport) sendError(id json.RawMessage, code int, message string) {
	if len(id) == 0 || string(id) == "null" {
		return // can't respond to notifications
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	resp := jsonrpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &jsonrpcError{
			Code:    code,
			Message: message,
		},
	}
	t.out.Encode(resp)
	t.stdout.Sync()
}

// SendNotification sends a JSON-RPC notification to the client.
func (t *Transport) SendNotification(method string, params interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	notif := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
	}
	if params != nil {
		notif["params"] = params
	}
	t.out.Encode(notif)
	t.stdout.Sync()
}

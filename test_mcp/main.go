package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      int         `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <robot-mcp-path>\n", os.Args[0])
		os.Exit(1)
	}

	cmd := exec.Command(os.Args[1], "--client-only")
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	cmd.Start()
	defer cmd.Process.Kill()
	br := bufio.NewReader(stdout)

	// 1. Initialize
	send(stdin, rpcRequest{JSONRPC: "2.0", Method: "initialize", ID: 1, Params: map[string]interface{}{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]interface{}{},
		"clientInfo":      map[string]interface{}{"name": "test", "version": "1.0"},
	}})
	resp := readWithTimeout(br, 10*time.Second)
	fmt.Printf("INIT: %s\n", resp.Result)
	send(stdin, rpcRequest{JSONRPC: "2.0", Method: "notifications/initialized", ID: 2})
	time.Sleep(time.Second)
	drain(br)

	send(stdin, rpcRequest{JSONRPC: "2.0", Method: "tools/list", ID: 3})
	resp = readWithTimeout(br, 10*time.Second)
	fmt.Printf("TOOLS: %s\n", resp.Result)

	// 3. Test tools (skip subscribe — daima owns audio)
	tests := []struct {
		name string
		args map[string]interface{}
	}{
		{"robot_get_battery", nil},
		{"robot_get_sensors", nil},
		{"robot_set_volume", map[string]interface{}{"level": float64(4)}},
		{"robot_cancel_playback", nil},
		{"robot_drive_on_charger", nil},
	}

	for i, t := range tests {
		params := map[string]interface{}{}
		if t.args != nil {
			for k, v := range t.args {
				params[k] = v
			}
		}
		send(stdin, rpcRequest{
			JSONRPC: "2.0", Method: "tools/call", ID: 100 + i,
			Params: map[string]interface{}{"name": t.name, "arguments": params},
		})
		resp = readWithTimeout(br, 10*time.Second)
		if resp.Error != nil {
			fmt.Printf("  %-30s ERROR: %s\n", t.name, string(resp.Error))
		} else {
			fmt.Printf("  %-30s OK: %s\n", t.name, string(resp.Result))
		}
	}

	// 4. Test app intents
	fmt.Printf("\n=== APP INTENTS ===\n")
	intents := []string{
		"intent_greeting_hello",
		"intent_meet_victor",
		"intent_imperative_affirmative",
		"intent_imperative_negative",
		"intent_global_stop",
	}
	for _, intent := range intents {
		send(stdin, rpcRequest{
			JSONRPC: "2.0", Method: "tools/call", ID: 200,
			Params: map[string]interface{}{
				"name": "robot_app_intent",
				"arguments": map[string]interface{}{"intent": intent},
			},
		})
		resp = readWithTimeout(br, 10*time.Second)
		if resp.Error != nil {
			fmt.Printf("  %-35s ERROR: %s\n", intent, string(resp.Error))
		} else {
			fmt.Printf("  %-35s OK: %s\n", intent, string(resp.Result))
		}
		time.Sleep(500 * time.Millisecond)
		drain(br)
	}

	cmd.Process.Kill()
}

func send(w io.Writer, req rpcRequest) {
	b, _ := json.Marshal(req)
	w.Write(append(b, '\n'))
}

func readWithTimeout(br *bufio.Reader, timeout time.Duration) rpcResponse {
	type result struct {
		resp rpcResponse
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		r, e := readResult(br)
		ch <- result{r, e}
	}()
	select {
	case r := <-ch:
		return r.resp
	case <-time.After(timeout):
		return rpcResponse{Error: json.RawMessage(`"timeout"`)}
	}
}

func readResult(br *bufio.Reader) (rpcResponse, error) {
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			return rpcResponse{Error: json.RawMessage(fmt.Sprintf(`"read error: %v"`, err))}, err
		}
		var resp rpcResponse
		if json.Unmarshal(line, &resp) != nil {
			continue
		}
		if resp.ID != 0 || resp.Result != nil || resp.Error != nil {
			return resp, nil
		}
	}
}

func drain(br *bufio.Reader) {
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			return
		}
		// check if notification (no id)
		var check struct {
			ID int `json:"id"`
		}
		if json.Unmarshal(line, &check) == nil && check.ID == 0 {
			continue
		}
		return
	}
}

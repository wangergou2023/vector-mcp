// vector-mcp: stripped vic-cloud (gRPC gateway) + MCP server
// Replaces /anki/bin/vic-cloud. No VOSK, no cloud services.
package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/digital-dream-labs/vector-cloud/internal/log"
)

const (
	serverName    = "vector-mcp"
	serverVersion = "1.0.0"
)

func randomString() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 20)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		b[i] = charset[n.Int64()]
	}
	return string(b)
}

func main() {
	clientOnly := flag.Bool("client-only", false, "Run as MCP client only (no gRPC gateway)")
	flag.Parse()

	if !*clientOnly {
		go mainGateway()
		for i := 0; i < 50; i++ {
			c, err := net.DialTimeout("tcp", "localhost:443", 100*time.Millisecond)
			if err == nil { c.Close(); break }
			time.Sleep(200 * time.Millisecond)
		}
		log.Println("vector-mcp: gateway ready, starting MCP")

		os.MkdirAll("/run/vic-cloud", 0755)
		if _, err := os.Stat("/run/vic-cloud/perRuntimeToken"); os.IsNotExist(err) {
			os.WriteFile("/run/vic-cloud/perRuntimeToken", []byte(randomString()), 0644)
		}
	} else {
		log.Println("vector-mcp: client-only mode")
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	// Robot client (connects to our own gateway, skip TLS verification since localhost)
	robot, err := NewRobotClient("localhost:443", "/run/vic-cloud/perRuntimeToken", true)
	if err != nil {
		log.Println("Failed to create robot client:", err)
	}
	if robot != nil {
		defer robot.Close()
		go func() {
			for {
				if robot.conn == nil {
					log.Println("Reconnecting robot client...")
					if err := robot.ConnectWithRetry(true); err == nil {
						go robot.eventStreamLoop()
					}
				}
				time.Sleep(10 * time.Second)
			}
		}()
	}

	tools := NewToolRegistry(robot)
	listenSpeakerSocket(robot)
	transport := NewTransport()
	audio := NewAudioSubscriber(transport, robot)

	// MCP lifecycle
	transport.RegisterHandler("initialize", func(id json.RawMessage, params json.RawMessage) (interface{}, error) {
		return map[string]interface{}{
			"protocolVersion": "2025-03-26",
			"serverInfo": map[string]interface{}{
				"name":    serverName,
				"version": serverVersion,
			},
			"capabilities": map[string]interface{}{
				"tools": map[string]interface{}{},
			},
			"instructions": "Vector robot control server with MCP tools.",
		}, nil
	})

	transport.RegisterHandler("notifications/initialized", func(id json.RawMessage, params json.RawMessage) (interface{}, error) {
		log.Println("vector-mcp: client initialized")
		return nil, nil
	})

	transport.RegisterHandler("tools/list", func(id json.RawMessage, params json.RawMessage) (interface{}, error) {
		return map[string]interface{}{"tools": allTools()}, nil
	})

	transport.RegisterHandler("tools/call", func(id json.RawMessage, params json.RawMessage) (interface{}, error) {
		var callParams struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
		}
		if err := json.Unmarshal(params, &callParams); err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		switch callParams.Name {
		case "robot_subscribe_audio":
			return handleSubscribeAudio(audio)
		case "robot_unsubscribe_audio":
			return handleUnsubscribeAudio(audio)
		case "mic_get_direction":
			return handleMicGetDirection(audio)
		}
		handler := tools.Handler(callParams.Name)
		if handler == nil {
			return ToolCallResult{Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Unknown tool: %s", callParams.Name)}}, IsError: true}, nil
		}
		result, err := handler(callParams.Arguments)
		if err != nil {
			fmt.Fprintf(os.Stderr, "robot-mcp: tool %s error: %v\n", callParams.Name, err)
			return ToolCallResult{Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Error: %v", err)}}, IsError: true}, nil
		}
		fmt.Fprintf(os.Stderr, "robot-mcp: tool %s result: [%s]\n", callParams.Name, result)
		return ToolCallResult{Content: []ToolContent{{Type: "text", Text: result}}}, nil
	})

	log.Println("vector-mcp: ready, waiting for MCP client")
	go func() {
		if err := transport.Run(); err != nil {
			log.Println("transport error:", err)
		}
	}()

	<-signalCh
	log.Println("Received signal, shutting down")
	audio.Stop()
}

func handleSubscribeAudio(audio *AudioSubscriber) (interface{}, error) {
	if err := audio.Start(); err != nil {
		return ToolCallResult{Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Failed to subscribe: %v", err)}}, IsError: true}, nil
	}
	return ToolCallResult{Content: []ToolContent{{Type: "text", Text: "Subscribed to audio stream."}}}, nil
}

func handleUnsubscribeAudio(audio *AudioSubscriber) (interface{}, error) {
	if err := audio.Stop(); err != nil {
		return ToolCallResult{Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Failed to unsubscribe: %v", err)}}, IsError: true}, nil
	}
	return ToolCallResult{Content: []ToolContent{{Type: "text", Text: "Unsubscribed from audio stream."}}}, nil
}

func handleMicGetDirection(audio *AudioSubscriber) (interface{}, error) {
	d := audio.GetLatestDirection()
	return ToolCallResult{Content: []ToolContent{{Type: "text", Text: fmt.Sprintf("Direction: index=%d degrees=%.0f selectedDirection=%d confidence=%d activeState=%d power=%.4f noiseFloor=%.4f",
		d.Direction, d.Degrees, d.SelectedDirection, d.Confidence, d.ActiveState, d.LatestPowerValue, d.LatestNoiseFloor)}}}, nil
}

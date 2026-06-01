// Audio: creates a DGRAM client socket bound to /dev/socket/mic_sock_rmcp
// and connected to /dev/socket/mic_sock (vic-anim's server).
// This matches how vic-cloudless's NewUnixgramClient works.
//
// Vic-anim records client addresses from recvfrom() and sends audio TO them.
// So we MUST bind to a specific client path and send a probe so vic-anim learns us.

package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	micSockPath       = "/dev/socket/mic_sock"
	micSockClientPath = "/dev/socket/mic_sock_rmcp" // our client address
)

type AudioSubscriber struct {
	mu          sync.Mutex
	active      bool
	conn        net.Conn
	done        chan struct{}
	transport   *Transport
	robot       *RobotClient
	dirMu       sync.RWMutex
	lastDir     MicDirectionData
	lastDirTime time.Time
}

type MicDirectionData struct {
	Timestamp          uint32    `json:"timestamp"`
	Direction          uint16    `json:"direction"`
	Confidence         int16     `json:"confidence"`
	SelectedDirection  uint16    `json:"selectedDirection"`
	SelectedConfidence int16     `json:"selectedConfidence"`
	ActiveState        int32     `json:"activeState"`
	LatestPowerValue   float32   `json:"latestPowerValue"`
	LatestNoiseFloor   float32   `json:"latestNoiseFloor"`
	Degrees            float64   `json:"degrees"`
}

// 12 directions = clock positions, 0=forward, step=-30°
func dirToDegrees(d uint16) float64 {
	return math.Mod(float64(int(d))*30.0+360, 360)
}

func (s *AudioSubscriber) GetLatestDirection() MicDirectionData {
	s.dirMu.RLock()
	defer s.dirMu.RUnlock()
	return s.lastDir
}

func NewAudioSubscriber(t *Transport, robot *RobotClient) *AudioSubscriber {
	return &AudioSubscriber{transport: t, robot: robot}
}

func (s *AudioSubscriber) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active { return nil }

	// Bind to a specific client path (vic-anim sends TO this address)
	syscall.Unlink(micSockClientPath)
	cliAddr, err := net.ResolveUnixAddr("unixgram", micSockClientPath)
	if err != nil {
		return fmt.Errorf("mic_sock client addr: %w", err)
	}
	srvAddr, err := net.ResolveUnixAddr("unixgram", micSockPath)
	if err != nil {
		return fmt.Errorf("mic_sock server addr: %w", err)
	}

	conn, err := net.DialUnix("unixgram", cliAddr, srvAddr)
	if err != nil {
		return fmt.Errorf("mic_sock dial: %w", err)
	}

	// Send CLAD ConnectionCheck (tag=3, empty body) so vic-anim records our client address
	// Repeat a few times to ensure delivery
	for i := 0; i < 3; i++ {
		conn.Write([]byte{3})
	}

	s.conn = conn
	s.active = true
	s.done = make(chan struct{})
	go s.readLoop()
	fmt.Fprintf(os.Stderr, "vector-mcp: audio subscriber started (mic_sock client at %s)\n", micSockClientPath)
	return nil
}

func (s *AudioSubscriber) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active { return nil }
	s.active = false
	close(s.done)
	if s.conn != nil { s.conn.Close() }
	return nil
}

func (s *AudioSubscriber) readLoop() {
	buf := make([]byte, 65536)
	var timestamp uint64
	for {
		select {
		case <-s.done:
			return
		default:
		}
		n, err := s.conn.Read(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "robot-mcp: mic read: %v\n", err)
			return
		}
		if n < 1 { continue }

		tag := buf[0]
		switch tag {
		case 1: // MessageTag_Audio
			if n < 3 { continue }
			dataLen := binary.LittleEndian.Uint16(buf[1:3])
			pcmStart := 3
			pcmEnd := int(pcmStart) + int(dataLen)*2
			if pcmEnd > n { continue }
			pcm := buf[pcmStart:pcmEnd]

			timestamp++
			encoded := base64.StdEncoding.EncodeToString(pcm)
			s.transport.SendNotification("notifications/audio/chunk", map[string]interface{}{
				"data": encoded, "sampleRate": 16000, "channels": 1,
				"format": "pcm_s16le", "bytes": len(pcm), "timestamp": timestamp,
			})

		case 2: // MessageTag_AudioDone — robot detected end of speech
			d := s.GetLatestDirection()
			params := map[string]interface{}{
				"timestamp": timestamp,
				"direction": d.Direction,
				"selectedDirection": d.SelectedDirection,
				"confidence": d.Confidence,
				"degrees": d.Degrees,
			}
			if s.robot != nil {
				rs := s.robot.GetRobotState()
				if rs != nil {
					pd := rs.GetProxData()
					if pd != nil {
						params["proxDistanceMm"] = pd.GetDistanceMm()
						params["proxFoundObject"] = pd.GetFoundObject()
						params["proxUnobstructed"] = pd.GetUnobstructed()
					}
					params["cliffDetected"] = (rs.GetStatus() & 0x4000) != 0
					params["robotStatus"] = rs.GetStatus()
					params["headAngleDeg"] = rs.GetHeadAngleRad() * 180.0 / 3.141592653
				}
			}
			s.transport.SendNotification("notifications/audio/done", params)
			fmt.Fprintf(os.Stderr, "robot-mcp: audio done (robot VAD) dir=%d deg=%.0f cliff=%v\n", d.Direction, d.Degrees, params["cliffDetected"])

		case 0xDA: // MessageTag_MicDirection — sound source localization
			if n < 77 { continue }
			var d MicDirectionData
			d.Timestamp          = binary.LittleEndian.Uint32(buf[1:5])
			d.Direction          = binary.LittleEndian.Uint16(buf[5:7])
			d.Confidence         = int16(binary.LittleEndian.Uint16(buf[7:9]))
			d.SelectedDirection  = binary.LittleEndian.Uint16(buf[9:11])
			d.SelectedConfidence = int16(binary.LittleEndian.Uint16(buf[11:13]))
			// confidenceList[13] at offset 13..65 (52 bytes), skip
			d.ActiveState        = int32(binary.LittleEndian.Uint32(buf[65:69]))
			d.LatestPowerValue   = math.Float32frombits(binary.LittleEndian.Uint32(buf[69:73]))
			d.LatestNoiseFloor   = math.Float32frombits(binary.LittleEndian.Uint32(buf[73:77]))
			d.Degrees            = dirToDegrees(d.Direction)

			s.dirMu.Lock()
			s.lastDir = d
			s.lastDirTime = time.Now()
			s.dirMu.Unlock()
		}
	}
}

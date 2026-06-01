// gRPC client for Vector robot's ExternalInterface service.
// Connects to vic-gateway on port 443 with TLS and Bearer token auth.

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	extint "github.com/digital-dream-labs/vector-cloud/internal/proto/external_interface"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// RobotClient wraps a gRPC connection to the Vector robot.
type RobotClient struct {
	conn     *grpc.ClientConn
	client   extint.ExternalInterfaceClient
	target   string
	token    string
	bcStream extint.ExternalInterface_BehaviorControlClient
	bcCancel context.CancelFunc

	sensorMu  sync.RWMutex
	robotState *extint.RobotState
}

func (rc *RobotClient) bgCtx() context.Context {
	return context.Background()
}

// tokenAuth implements credentials.PerRPCCredentials for Bearer token auth.
type tokenAuth struct {
	token string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + t.token,
	}, nil
}

func (tokenAuth) RequireTransportSecurity() bool { return true }

// NewRobotClient creates a new robot client from a token file path.
func NewRobotClient(target string, tokenPath string, skipVerify bool) (*RobotClient, error) {
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("read token file %s: %w", tokenPath, err)
	}
	return NewRobotClientWithToken(target, string(token), skipVerify)
}

// NewRobotClientWithToken creates a new robot client with a raw token string.
func NewRobotClientWithToken(target string, token string, skipVerify bool) (*RobotClient, error) {
	rc := &RobotClient{
		target: target,
		token:  token,
	}

	if err := rc.connect(skipVerify); err != nil {
		fmt.Fprintf(os.Stderr, "robot-mcp: initial connect failed: %v (will retry)\n", err)
	}
	return rc, nil
}

// connectWithRetry keeps retrying gRPC connection with backoff.
func (rc *RobotClient) ConnectWithRetry(skipVerify bool) error {
	backoff := 1 * time.Second
	for i := 0; i < 10; i++ {
		if rc.conn != nil {
			rc.conn.Close()
		}
		if err := rc.connect(skipVerify); err == nil {
			fmt.Fprintf(os.Stderr, "robot-mcp: connected after %d retries\n", i)
			return nil
		}
		fmt.Fprintf(os.Stderr, "robot-mcp: connect attempt %d/10 failed, retrying in %v...\n", i+1, backoff)
		time.Sleep(backoff)
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
	return fmt.Errorf("failed to connect after 10 attempts")
}

// connect establishes a gRPC connection to the robot.
func (rc *RobotClient) connect(skipVerify bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tlsCfg := &tls.Config{}
	if skipVerify {
		tlsCfg.InsecureSkipVerify = true
	}

	conn, err := grpc.DialContext(ctx, rc.target,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithPerRPCCredentials(tokenAuth{rc.token}),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", rc.target, err)
	}

	rc.conn = conn
	rc.client = extint.NewExternalInterfaceClient(conn)

	// Protocol version handshake
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	resp, err := rc.client.ProtocolVersion(ctx2, &extint.ProtocolVersionRequest{
		ClientVersion:  int64(extint.ProtocolVersion_PROTOCOL_VERSION_CURRENT),
		MinHostVersion: int64(extint.ProtocolVersion_PROTOCOL_VERSION_MINIMUM),
	})
	if err != nil {
		conn.Close()
		return fmt.Errorf("protocol version check: %w", err)
	}
	if resp.Result != extint.ProtocolVersionResponse_SUCCESS {
		conn.Close()
		return fmt.Errorf("protocol version mismatch: %v", resp.Result)
	}

	fmt.Fprintf(os.Stderr, "robot: connected to %s (protocol OK)\n", rc.target)

	// Start sensor data event stream
	go rc.eventStreamLoop()

	return nil
}

func (rc *RobotClient) eventStreamLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := rc.client.EventStream(ctx, &extint.EventRequest{
		ListType: &extint.EventRequest_WhiteList{
			WhiteList: &extint.FilterList{List: []string{"RobotState"}},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "robot-mcp: EventStream: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "robot-mcp: EventStream started\n")

	for {
		resp, err := stream.Recv()
		if err != nil {
			fmt.Fprintf(os.Stderr, "robot-mcp: EventStream recv: %v\n", err)
			return
		}
		if resp.GetEvent() == nil {
			continue
		}
		rs := resp.GetEvent().GetRobotState()
		if rs != nil {
			rc.sensorMu.Lock()
			rc.robotState = rs
			rc.sensorMu.Unlock()
		}
	}
}

func (rc *RobotClient) GetRobotState() *extint.RobotState {
	rc.sensorMu.RLock()
	defer rc.sensorMu.RUnlock()
	return rc.robotState
}

// Close closes the gRPC connection.
func (rc *RobotClient) Close() error {
	if rc.conn != nil {
		return rc.conn.Close()
	}
	return nil
}

// Connected returns true if the client has an active connection.
func (rc *RobotClient) Connected() bool {
	return rc.conn != nil && rc.client != nil
}

// Robot wheelbase half for turning radius calculation (mm)
const wheelbaseHalf = 24.0

// ---- Convenience methods (clad / fire-and-forget proto only) ----

// DriveWheels controls individual wheel speeds (fire-and-forget proto).
func (rc *RobotClient) DriveWheels(leftMmps, rightMmps, leftMmps2, rightMmps2 float64) (*extint.DriveWheelsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rc.client.DriveWheels(ctx, &extint.DriveWheelsRequest{
		LeftWheelMmps:   float32(leftMmps),
		RightWheelMmps:  float32(rightMmps),
		LeftWheelMmps2:  float32(leftMmps2),
		RightWheelMmps2: float32(rightMmps2),
	})
}

// DriveStraightTimed drives straight by acquiring BehaviorControl, running wheels for calculated duration, then releasing.
func (rc *RobotClient) DriveStraightTimed(speedMmps, distMm float64) error {
	if distMm <= 0 {
		return fmt.Errorf("distance must be positive")
	}
	s := math.Abs(speedMmps)
	if s < 20 {
		s = 20
	}
	if s > 300 {
		s = 300
	}
	if speedMmps < 0 {
		s = -s
	}
	durationMs := math.Abs(distMm / speedMmps * 1000)
	if durationMs > 30000 {
		durationMs = 30000
	}

	if err := rc.StartForegroundActivity(); err != nil {
		return err
	}
	defer rc.StopForegroundActivity()

	if _, err := rc.DriveWheels(s, s, 200, 200); err != nil {
		return err
	}
	time.Sleep(time.Duration(durationMs) * time.Millisecond)
	if _, err := rc.DriveWheels(0, 0, 0, 0); err != nil {
		return err
	}
	return nil
}

// TurnInPlaceTimed turns in place by acquiring BC, running wheels at opposite speeds for calculated duration.
func (rc *RobotClient) TurnInPlaceTimed(angleRad, speedRadPerSec float64) error {
	if math.Abs(angleRad) < 0.01 {
		return nil
	}
	dir := 1.0
	if angleRad < 0 {
		dir = -1.0
	}
	wheelSpeed := speedRadPerSec * wheelbaseHalf
	durationMs := math.Abs(angleRad / speedRadPerSec * 1000)
	if durationMs > 30000 {
		durationMs = 30000
	}

	if err := rc.StartForegroundActivity(); err != nil {
		return err
	}
	defer rc.StopForegroundActivity()

	if _, err := rc.DriveWheels(dir*wheelSpeed, -dir*wheelSpeed, 200, 200); err != nil {
		return err
	}
	time.Sleep(time.Duration(durationMs) * time.Millisecond)
	if _, err := rc.DriveWheels(0, 0, 0, 0); err != nil {
		return err
	}
	return nil
}

// MoveHead moves the head motor at given speed (clad, fire-and-forget).
func (rc *RobotClient) MoveHead(speedRadPerSec float64) (*extint.MoveHeadResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return rc.client.MoveHead(ctx, &extint.MoveHeadRequest{
		SpeedRadPerSec: float32(speedRadPerSec),
	})
}

// MoveHeadTimed moves head by acquiring BC, running MoveHead for calculated duration to reach target angle.
func (rc *RobotClient) MoveHeadTimed(angleRad, speedRadPerSec float64) error {
	if math.Abs(angleRad) < 0.01 {
		return nil
	}
	dir := 1.0
	if angleRad < 0 {
		dir = -1.0
		angleRad = -angleRad
	}
	durationMs := angleRad / speedRadPerSec * 1000
	if durationMs > 10000 {
		durationMs = 10000
	}

	if err := rc.StartForegroundActivity(); err != nil {
		return err
	}
	defer rc.StopForegroundActivity()

	if _, err := rc.MoveHead(dir * speedRadPerSec); err != nil {
		return err
	}
	time.Sleep(time.Duration(durationMs) * time.Millisecond)
	if _, err := rc.MoveHead(0); err != nil {
		return err
	}
	return nil
}

// MoveLift moves the lift motor at given speed (clad, fire-and-forget).
func (rc *RobotClient) MoveLift(speedRadPerSec float64) (*extint.MoveLiftResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return rc.client.MoveLift(ctx, &extint.MoveLiftRequest{
		SpeedRadPerSec: float32(speedRadPerSec),
	})
}

// MoveLiftTimed moves lift by acquiring BC, running MoveLift for calculated duration to reach target height.
func (rc *RobotClient) MoveLiftTimed(heightMm, speedRadPerSec float64) error {
	targetRad := heightMm * 2 * math.Pi / 100.0
	if targetRad < 0.01 {
		return nil
	}
	durationMs := targetRad / speedRadPerSec * 1000
	if durationMs > 10000 {
		durationMs = 10000
	}

	if err := rc.StartForegroundActivity(); err != nil {
		return err
	}
	defer rc.StopForegroundActivity()

	if _, err := rc.MoveLift(speedRadPerSec); err != nil {
		return err
	}
	time.Sleep(time.Duration(durationMs) * time.Millisecond)
	if _, err := rc.MoveLift(0); err != nil {
		return err
	}
	return nil
}

// StopAllMotors stops all motor movement immediately (fire-and-forget proto).
func (rc *RobotClient) StopAllMotors() (*extint.StopAllMotorsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return rc.client.StopAllMotors(ctx, &extint.StopAllMotorsRequest{})
}

// BatteryState returns the current battery state.
func (rc *RobotClient) BatteryState() (*extint.BatteryStateResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return rc.client.BatteryState(ctx, &extint.BatteryStateRequest{})
}

// ---- Audio Playback ----

const cancelFile = "/tmp/daima_playback_cancel"

// CancelPlayback signals any in-progress PlayPCM to abort.
func CancelPlayback() {
	os.WriteFile(cancelFile, nil, 0644)
}

func isPlaybackCancelled() bool {
	_, err := os.Stat(cancelFile)
	return err == nil
}

// SetMasterVolume sets the robot's master volume level (0-4).
func (rc *RobotClient) SetMasterVolume(level extint.MasterVolumeLevel) (*extint.MasterVolumeResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return rc.client.SetMasterVolume(ctx, &extint.MasterVolumeRequest{VolumeLevel: level})
}

// AppIntent sends a high-level intent to the robot (e.g. "intent_system_charger")
func (rc *RobotClient) AppIntent(intent string) (*extint.AppIntentResponse, error) {
	return rc.client.AppIntent(context.Background(), &extint.AppIntentRequest{Intent: intent})
}

// PlayAnimation plays a raw animation by filename (e.g. "anim_holiday_hny_fireworks_01").
// Must have BehaviorControl active (StartForegroundActivity) before calling.
func (rc *RobotClient) PlayAnimation(name string, loops int) (*extint.PlayAnimationResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return rc.client.PlayAnimation(ctx, &extint.PlayAnimationRequest{
		Animation: &extint.Animation{Name: name},
		Loops:     uint32(loops),
	})
}

// StartForegroundActivity requests behavior control with DEFAULT priority.
// Keeps the control context alive until StopForegroundActivity is called.
func (rc *RobotClient) StartForegroundActivity() error {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := rc.client.BehaviorControl(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("behavior control: %w", err)
	}

	err = stream.Send(&extint.BehaviorControlRequest{
		RequestType: &extint.BehaviorControlRequest_ControlRequest{
			ControlRequest: &extint.ControlRequest{
				Priority: extint.ControlRequest_DEFAULT,
			},
		},
	})
	if err != nil {
		cancel()
		return fmt.Errorf("control request: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		cancel()
		return fmt.Errorf("control recv: %w", err)
	}
	if resp.GetControlGrantedResponse() == nil {
		cancel()
		return fmt.Errorf("control not granted")
	}

	rc.bcStream = stream
	rc.bcCancel = cancel

	// Drain responses in background (e.g. ControlLost)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()

	return nil
}

// StopForegroundActivity releases behavior control, allowing the robot to resume
// autonomous behaviors.
func (rc *RobotClient) StopForegroundActivity() error {
	if rc.bcStream == nil {
		return nil
	}
	defer func() {
		rc.bcCancel()
		rc.bcStream = nil
		rc.bcCancel = nil
	}()

	return rc.bcStream.Send(&extint.BehaviorControlRequest{
		RequestType: &extint.BehaviorControlRequest_ControlRelease{
			ControlRelease: &extint.ControlRelease{},
		},
	})
}

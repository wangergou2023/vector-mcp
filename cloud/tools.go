// MCP tool definitions and handlers for robot control.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	extint "github.com/digital-dream-labs/vector-cloud/internal/proto/external_interface"
)

var (
	appIntentMu   sync.Mutex
	lastAppIntent time.Time
)

// ---- Tool Definition ----

type ToolInputSchema struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Required   []string               `json:"required,omitempty"`
}

type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema ToolInputSchema `json:"inputSchema"`
}

// ToolHandler is called when a tool is invoked.
type ToolHandler func(args map[string]interface{}) (string, error)

// ToolCallResult is the response format for tools/call.
type ToolCallResult struct {
	Content []ToolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// allTools returns all registered MCP tools.
func allTools() []ToolDef {
	return []ToolDef{
		{Name: "robot_drive_straight", Description: "Drive straight forward or backward. Speed in mm/s, distance in mm.", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"speed_mmps": map[string]interface{}{"type": "number", "description": "Speed mm/s (±200)"}, "dist_mm": map[string]interface{}{"type": "number", "description": "Distance mm (50-2000)"}}}},
		{Name: "robot_turn_in_place", Description: "Turn in place. Angle in radians (π=180°).", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"angle_rad": map[string]interface{}{"type": "number", "description": "Angle rad (π=180°)"}, "speed_rad_per_sec": map[string]interface{}{"type": "number", "description": "Speed rad/s (default 2.0)"}}}},
		{Name: "robot_set_head_angle", Description: "Move head to angle. 0=level, -0.3=down, +0.3=up.", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"angle_rad": map[string]interface{}{"type": "number", "description": "Head angle rad"}}, Required: []string{"angle_rad"}}},
		{Name: "robot_set_lift_height", Description: "Move lift arm. 0=down, 90=up.", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"height_mm": map[string]interface{}{"type": "number", "description": "Height mm (0-100)"}}, Required: []string{"height_mm"}}},
		{Name: "robot_drive_wheels", Description: "Raw wheel control. Left/right speed in mm/s.", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"left_mmps": map[string]interface{}{"type": "number"}, "right_mmps": map[string]interface{}{"type": "number"}}, Required: []string{"left_mmps", "right_mmps"}}},
		{Name: "robot_stop", Description: "Stop all motors immediately.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_get_battery", Description: "Get battery voltage, level, charging status.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_get_sensors", Description: "Get proximity and cliff sensor data.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_set_volume", Description: "Set master volume level 0-4.", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"level": map[string]interface{}{"type": "number", "description": "0=low...4=high"}}, Required: []string{"level"}}},
		{Name: "robot_drive_on_charger", Description: "Drive onto charging station.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_drive_off_charger", Description: "Drive off charging station.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_play_animation", Description: "Play animation by name (Happy01, Sad01, Greeting01, WeatherStars01, etc).", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"name": map[string]interface{}{"type": "string", "description": "Animation name"}, "loops": map[string]interface{}{"type": "number", "description": "Loop count (1-3)"}}, Required: []string{"name"}}},
		{Name: "robot_cancel_playback", Description: "Cancel in-progress audio playback.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_activity_start", Description: "Acquire behavior control (needed before some actions).", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_activity_end", Description: "Release behavior control.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_subscribe_audio", Description: "Subscribe to microphone audio stream.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_unsubscribe_audio", Description: "Unsubscribe from microphone audio stream.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "mic_get_direction", Description: "Get sound source direction from microphone array.", InputSchema: ToolInputSchema{Type: "object"}},
		{Name: "robot_app_intent", Description: "Trigger built-in behavior by name (intent_greeting_hello, intent_system_charger, intent_global_stop, etc).", InputSchema: ToolInputSchema{Type: "object", Properties: map[string]interface{}{"intent": map[string]interface{}{"type": "string"}}, Required: []string{"intent"}}},
	}
}

// ---- Tool Handler Registry ----

type ToolRegistry struct {
	robot    *RobotClient
	handlers map[string]ToolHandler
}

func NewToolRegistry(robot *RobotClient) *ToolRegistry {
	tr := &ToolRegistry{
		robot:    robot,
		handlers: make(map[string]ToolHandler),
	}
	tr.registerAll()
	return tr
}

func (tr *ToolRegistry) robotConnected() bool {
	return tr.robot != nil && tr.robot.Connected()
}

func (tr *ToolRegistry) registerAll() {
	tr.handlers["robot_drive_straight"]  = tr.handleDriveStraight
	tr.handlers["robot_turn_in_place"]   = tr.handleTurnInPlace
	tr.handlers["robot_set_head_angle"]  = tr.handleSetHeadAngle
	tr.handlers["robot_set_lift_height"] = tr.handleSetLiftHeight
	tr.handlers["robot_drive_wheels"]    = tr.handleDriveWheels
	tr.handlers["robot_stop"]            = tr.handleStop
	tr.handlers["robot_get_battery"]     = tr.handleGetBattery
	tr.handlers["robot_play_pcm"]        = tr.handlePlayPCM
	tr.handlers["robot_set_volume"]      = tr.handleSetVolume
	tr.handlers["robot_drive_on_charger"] = tr.handleDriveOnCharger
	tr.handlers["robot_drive_off_charger"] = tr.handleDriveOffCharger
	tr.handlers["robot_play_animation"]   = tr.handlePlayAnimation
	tr.handlers["robot_cancel_playback"]  = tr.handleCancelPlayback
	tr.handlers["robot_activity_start"]   = tr.handleActivityStart
	tr.handlers["robot_activity_end"]     = tr.handleActivityEnd
	tr.handlers["robot_get_sensors"]      = tr.handleGetSensors
	tr.handlers["robot_app_intent"]       = tr.handleAppIntent
}

func (tr *ToolRegistry) Handler(name string) ToolHandler {
	return tr.handlers[name]
}

// ---- Tool Implementations ----

func (tr *ToolRegistry) handleDriveStraight(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	speed := getFloat(args, "speed_mmps", 80)
	dist := getFloat(args, "dist_mm", 300)
	if speed < -300 || speed > 300 { return "", fmt.Errorf("speed must be -300..300 mm/s") }
	if dist < 10 || dist > 5000 { return "", fmt.Errorf("distance must be 10..5000 mm") }
	if err := tr.robot.DriveStraightTimed(speed, dist); err != nil {
		return "", err
	}
	return fmt.Sprintf("Drove %.0fmm at %.0fmm/s", dist, speed), nil
}

func (tr *ToolRegistry) handleTurnInPlace(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	angle := getFloat(args, "angle_rad", 3.14159)
	speed := getFloat(args, "speed_rad_per_sec", 2.0)
	if angle < -6.283 || angle > 6.283 { return "", fmt.Errorf("angle must be -2π..2π rad") }
	if err := tr.robot.TurnInPlaceTimed(angle, speed); err != nil {
		return "", err
	}
	return fmt.Sprintf("Turned %.1f°", angle*180.0/3.14159), nil
}

func (tr *ToolRegistry) handleSetHeadAngle(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	angle := getFloat(args, "angle_rad", 0)
	if angle < -0.5 || angle > 0.7 { return "", fmt.Errorf("head angle must be -0.5..0.7 rad") }
	if err := tr.robot.MoveHeadTimed(angle, 1.5); err != nil {
		return "", err
	}
	return fmt.Sprintf("Head moved to %.1f°", angle*180.0/3.14159), nil
}

func (tr *ToolRegistry) handleSetLiftHeight(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	height := getFloat(args, "height_mm", 0)
	if height < 0 || height > 100 { return "", fmt.Errorf("lift height must be 0..100 mm") }
	if err := tr.robot.MoveLiftTimed(height, 3.0); err != nil {
		return "", err
	}
	return fmt.Sprintf("Lift moved to %.0fmm", height), nil
}

func (tr *ToolRegistry) handleDriveWheels(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	left := getFloat(args, "left_mmps", 0)
	right := getFloat(args, "right_mmps", 0)
	resp, err := tr.robot.DriveWheels(left, right, 200, 200)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("DriveWheels: status=%v", resp.Status.Code), nil
}

func (tr *ToolRegistry) handleStop(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	resp, err := tr.robot.StopAllMotors()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("StopAllMotors: status=%v", resp.Status.Code), nil
}

func (tr *ToolRegistry) handleGetBattery(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	resp, err := tr.robot.BatteryState()
	if err != nil {
		return "", err
	}
	levelStr := mapBatteryLevel(resp.BatteryLevel)
	j, _ := json.Marshal(map[string]interface{}{
		"battery_volts":       resp.BatteryVolts,
		"battery_level":       levelStr,
		"is_charging":         resp.IsCharging,
		"is_on_charger":       resp.IsOnChargerPlatform,
		"suggested_charger_s": resp.SuggestedChargerSec,
	})
	return string(j), nil
}

func (tr *ToolRegistry) handlePlayPCM(args map[string]interface{}) (string, error) {
	return "", fmt.Errorf("use Unix socket /tmp/daima_spk.sock instead of MCP for playback")
}

func (tr *ToolRegistry) handleSetVolume(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	level := int(getFloat(args, "level", 2))
	if level < 0 {
		level = 0
	}
	if level > 4 {
		level = 4
	}

	resp, err := tr.robot.SetMasterVolume(extint.MasterVolumeLevel(level))
	if err != nil {
		return "", err
	}
	names := []string{"low", "medium_low", "medium", "medium_high", "high"}
	return fmt.Sprintf("SetMasterVolume: status=%v level=%s", resp.Status.Code, names[level]), nil
}

func (tr *ToolRegistry) handleDriveOnCharger(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	resp, err := tr.robot.AppIntent("intent_system_charger")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Charger intent sent: status=%v", resp.Status.Code), nil
}

func (tr *ToolRegistry) handleDriveOffCharger(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	if err := tr.robot.DriveStraightTimed(-60, 100); err != nil {
		return "", err
	}
	return "Backed off charger", nil
}

func (tr *ToolRegistry) handlePlayAnimation(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	name := getString(args, "name", "")
	if name == "" {
		return "", fmt.Errorf("missing animation name")
	}
	loops := int(getFloat(args, "loops", 1))

	// Map common animation requests to known working animation names
	animName := name
	switch strings.ToLower(name) {
	case "weatherstars01", "stars", "fireworks", "firework", "fire", "firework01", "star", "celebrate":
		animName = "anim_holiday_hny_fireworks_01"
	case "greeting01", "greeting", "greet", "hello":
		animName = "anim_greeting_hello_01"
	case "happy01", "happy", "happiness":
		animName = "anim_feedback_goodrobot_01"
	case "sad01", "sad":
		animName = "anim_feedback_badrobot_01"
	case "surprise01", "surprise":
		animName = "anim_greeting_hello_02"
	case "love01", "love":
		animName = "anim_feedback_iloveyou_01"
	case "sleep01", "sleep":
		animName = "anim_gotosleep_getin_01"
	case "meet01", "meet", "victor":
		animName = "anim_meetvictor_sayname_01"
	}

	if err := tr.robot.StartForegroundActivity(); err != nil {
		return "", err
	}
	defer tr.robot.StopForegroundActivity()

	resp, err := tr.robot.PlayAnimation(animName, loops)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("PlayAnimation %s: status=%v", animName, resp.Status.Code), nil
}

func (tr *ToolRegistry) handleCancelPlayback(args map[string]interface{}) (string, error) {
	CancelPlayback()
	return "Playback cancel signal sent", nil
}

func (tr *ToolRegistry) handleActivityStart(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	if err := tr.robot.StartForegroundActivity(); err != nil {
		return "", err
	}
	return "Activity started (behavior control acquired)", nil
}

func (tr *ToolRegistry) handleActivityEnd(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	if err := tr.robot.StopForegroundActivity(); err != nil {
		return "", err
	}
	return "Activity ended (behavior control released)", nil
}

const robotStatusCliff = uint32(0x4000)

func (tr *ToolRegistry) handleAppIntent(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	intent := getString(args, "intent", "")
	if intent == "" {
		return "", fmt.Errorf("missing intent")
	}
	// throttle: prevent back-to-back intents from clogging the clad channel
	appIntentMu.Lock()
	elapsed := time.Since(lastAppIntent)
	wait := 1500*time.Millisecond - elapsed
	if wait > 0 {
		time.Sleep(wait)
	}
	resp, err := tr.robot.AppIntent(intent)
	lastAppIntent = time.Now()
	appIntentMu.Unlock()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("AppIntent %s: status=%v", intent, resp.Status.Code), nil
}

func (tr *ToolRegistry) handleGetSensors(args map[string]interface{}) (string, error) {
	if !tr.robotConnected() {
		return "", fmt.Errorf("robot not connected")
	}
	rs := tr.robot.GetRobotState()
	if rs == nil {
		return "", fmt.Errorf("sensor data not available yet (waiting for EventStream)")
	}

	cliff := (rs.GetStatus()&robotStatusCliff) != 0

	var prox string
	if rs.GetProxData() != nil {
		pd := rs.GetProxData()
		prox = fmt.Sprintf("distance=%dmm found=%v obstructed=%v sig=%.2f",
			pd.GetDistanceMm(), pd.GetFoundObject(), pd.GetUnobstructed(), pd.GetSignalQuality())
	} else {
		prox = "no data"
	}

	return fmt.Sprintf("Proximity: %s\nCliff detected: %v\nStatus flags: 0x%04X\nHead angle: %.2f°\nLift height: %.1fmm",
		prox, cliff, rs.GetStatus(),
		rs.GetHeadAngleRad()*180.0/3.14159, rs.GetLiftHeightMm()), nil
}

func mapBatteryLevel(l extint.BatteryLevel) string {
	switch l {
	case extint.BatteryLevel_BATTERY_LEVEL_LOW:
		return "low"
	case extint.BatteryLevel_BATTERY_LEVEL_NOMINAL:
		return "nominal"
	case extint.BatteryLevel_BATTERY_LEVEL_FULL:
		return "full"
	default:
		return "unknown"
	}
}

// ---- Helper ----

func getFloat(args map[string]interface{}, key string, defaultVal float64) float64 {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	switch val := v.(type) {
	case float64:
		return val
	case json.Number:
		f, _ := val.Float64()
		return f
	default:
		fmt.Fprintf(os.Stderr, "tools: unexpected type %T for key %s\n", v, key)
		return defaultVal
	}
}

func getString(args map[string]interface{}, key string, defaultVal string) string {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	switch val := v.(type) {
	case string:
		return val
	default:
		return defaultVal
	}
}



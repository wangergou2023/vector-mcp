// audio_socket.go — Unix socket for PCM playback (dauma → robot-mcp → speaker)

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"time"

	extint "github.com/digital-dream-labs/vector-cloud/internal/proto/external_interface"
)

const spkSockPath = "/tmp/daima_spk.sock"

type pcmJob struct {
	seq  uint32
	text string
	pcm  []byte
	rxAt time.Time
}

var spkRobot *RobotClient
var spkQueue = make(chan pcmJob, 16)

func listenSpeakerSocket(robot *RobotClient) {
	spkRobot = robot
	os.Remove(spkSockPath)
	ln, err := net.Listen("unix", spkSockPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "robot-mcp: speaker socket listen: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stderr, "robot-mcp: speaker socket ready at %s\n", spkSockPath)

	go func() {
		var buffer []pcmJob
		nextSeq := uint32(0)
		for {
			var job pcmJob
			gotJob := false
			if len(buffer) == 0 {
				job = <-spkQueue
				buffer = append(buffer, job)
				gotJob = true
			} else {
				select {
				case job = <-spkQueue:
					buffer = append(buffer, job)
					gotJob = true
				default:
				}
			}

			if gotJob {
				sort.Slice(buffer, func(i, j int) bool { return buffer[i].seq < buffer[j].seq })
			}
			for len(buffer) > 0 && buffer[0].seq == nextSeq {
				pj := buffer[0]
				qWait := time.Since(pj.rxAt)
				fmt.Fprintf(os.Stderr, "robot-mcp: PLAY[%d] qwait=%dms text=[%s]\n", pj.seq, qWait.Milliseconds(), pj.text)
				spkPlay(pj.pcm)
				buffer = buffer[1:]
				nextSeq++
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go func(conn net.Conn) {
				defer conn.Close()
				t0 := time.Now()
				var seq uint32
				if err := binary.Read(conn, binary.LittleEndian, &seq); err != nil {
					return
				}
				var textLen uint16
				if err := binary.Read(conn, binary.LittleEndian, &textLen); err != nil {
					return
				}
				text := ""
				if textLen > 0 && textLen < 4096 {
					tb := make([]byte, textLen)
					io.ReadFull(conn, tb)
					text = string(tb)
				}
				pcm, _ := io.ReadAll(conn)
				rxMs := time.Since(t0).Milliseconds()
				if len(pcm) > 0 {
					fmt.Fprintf(os.Stderr, "robot-mcp: sock recv seq=%d text=[%s] pcm=%dKB rx=%dms\n", seq, text, len(pcm)/1024, rxMs)
					spkQueue <- pcmJob{seq, text, pcm, t0}
				}
			}(conn)
		}
	}()
}

func spkPlay(pcm []byte) {
	if spkRobot == nil {
		return
	}

	audioLen := len(pcm)
	audioSec := float64(audioLen) / float64(16000*2)
	chunkCount := (audioLen + 1024 - 1) / 1024
	fmt.Fprintf(os.Stderr, "robot-mcp: spkPlay start len=%dKB dur=%.1fs chunks=%d\n", audioLen/1024, audioSec, chunkCount)
	spkStart := time.Now()

	t0 := time.Now()
	client, err := spkRobot.client.ExternalAudioStreamPlayback(spkRobot.bgCtx())
	if err != nil {
		fmt.Fprintf(os.Stderr, "robot-mcp: audio stream: %v\n", err)
		return
	}

	// Drain responses in background to prevent engine IPC channel deadlock.
	// Must close cleanly before next sentence creates a new stream.
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			if _, err := client.Recv(); err != nil {
				return
			}
		}
	}()
	defer func() {
		client.CloseSend()
		tRecv := time.Now()
		<-recvDone
		fmt.Fprintf(os.Stderr, "robot-mcp: spkPlay recv drain took %dms\n", time.Since(tRecv).Milliseconds())
	}()

	connectMs := time.Since(t0).Milliseconds()

	t0 = time.Now()
	_ = client.Send(&extint.ExternalAudioStreamRequest{
		AudioRequestType: &extint.ExternalAudioStreamRequest_AudioStreamPrepare{
			AudioStreamPrepare: &extint.ExternalAudioStreamPrepare{
				AudioFrameRate: 16000,
				AudioVolume:    100,
			},
		},
	})
	prepareMs := time.Since(t0).Milliseconds()

	const maxChunk = 1024
	const chunkThrottle = 20 * time.Millisecond

	t0 = time.Now()
	sent := 0
	for offset := 0; offset < audioLen; offset += maxChunk {
		end := offset + maxChunk
		if end > audioLen {
			end = audioLen
		}
		if err := client.Send(&extint.ExternalAudioStreamRequest{
			AudioRequestType: &extint.ExternalAudioStreamRequest_AudioStreamChunk{
				AudioStreamChunk: &extint.ExternalAudioStreamChunk{
					AudioChunkSizeBytes: uint32(end - offset),
					AudioChunkSamples:   pcm[offset:end],
				},
			},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "robot-mcp: spkPlay chunk send err: %v\n", err)
			return
		}
		sent++
		if sent%100 == 0 {
			fmt.Fprintf(os.Stderr, "robot-mcp: spkPlay progress %d/%d chunks (%.0f%%)\n",
				sent, chunkCount, float64(sent)/float64(chunkCount)*100)
		}
		time.Sleep(chunkThrottle)
	}
	chunksMs := time.Since(t0).Milliseconds()
	fmt.Fprintf(os.Stderr, "robot-mcp: spkPlay all %d chunks sent in %dms\n", sent, chunksMs)

	t0 = time.Now()
	_ = client.Send(&extint.ExternalAudioStreamRequest{
		AudioRequestType: &extint.ExternalAudioStreamRequest_AudioStreamComplete{
			AudioStreamComplete: &extint.ExternalAudioStreamComplete{},
		},
	})
	completeMs := time.Since(t0).Milliseconds()

	remain := time.Duration(audioSec*float64(time.Second)) - time.Since(spkStart)
	if remain < 50*time.Millisecond {
		remain = 50 * time.Millisecond
	}
	t0 = time.Now()
	time.Sleep(remain)
	sleepMs := time.Since(t0).Milliseconds()

	totalMs := time.Since(spkStart).Milliseconds()
	fmt.Fprintf(os.Stderr, "robot-mcp: spkPlay done total=%dms (connect=%dms prepare=%dms chunks=%dms complete=%dms sleep=%dms)\n",
		totalMs, connectMs, prepareMs, chunksMs, completeMs, sleepMs)
}

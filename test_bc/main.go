package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	extint "github.com/digital-dream-labs/vector-cloud/internal/proto/external_interface"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	addr := "localhost:443"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	token, _ := os.ReadFile("/run/vic-cloud/perRuntimeToken")

	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithPerRPCCredentials(tokenAuth{string(token)}),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("FAIL: dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := extint.NewExternalInterfaceClient(conn)
	fmt.Println("connected")

	ctx := context.Background()

	// Step 1: Acquire BehaviorControl
	stream, err := client.BehaviorControl(ctx)
	if err != nil {
		fmt.Printf("FAIL: BC stream: %v\n", err)
		os.Exit(1)
	}
	err = stream.Send(&extint.BehaviorControlRequest{
		RequestType: &extint.BehaviorControlRequest_ControlRequest{
			ControlRequest: &extint.ControlRequest{
				Priority: extint.ControlRequest_DEFAULT,
			},
		},
	})
	if err != nil {
		fmt.Printf("FAIL: send ControlRequest: %v\n", err)
		os.Exit(1)
	}
	resp, err := stream.Recv()
	if err != nil {
		fmt.Printf("FAIL: recv: %v\n", err)
		os.Exit(1)
	}
	if resp.GetControlGrantedResponse() == nil {
		fmt.Println("FAIL: not granted")
		os.Exit(1)
	}
	fmt.Println("ControlGranted!")

	// Step 2: Drive forward for 2 seconds
	fmt.Println("Driving forward...")
	driveCtx, _ := context.WithTimeout(ctx, 3*time.Second)
	_, err = client.DriveWheels(driveCtx, &extint.DriveWheelsRequest{
		LeftWheelMmps:  80,
		RightWheelMmps: 80,
		LeftWheelMmps2: 200,
		RightWheelMmps2: 200,
	})
	if err != nil {
		fmt.Printf("DriveWheels error: %v\n", err)
	} else {
		fmt.Println("DriveWheels OK")
	}
	time.Sleep(2 * time.Second)

	// Step 3: Stop
	_, err = client.DriveWheels(ctx, &extint.DriveWheelsRequest{
		LeftWheelMmps: 0, RightWheelMmps: 0,
		LeftWheelMmps2: 0, RightWheelMmps2: 0,
	})
	if err != nil {
		fmt.Printf("Stop error: %v\n", err)
	} else {
		fmt.Println("Stop OK")
	}

	// Step 4: Release control
	stream.Send(&extint.BehaviorControlRequest{
		RequestType: &extint.BehaviorControlRequest_ControlRelease{
			ControlRelease: &extint.ControlRelease{},
		},
	})
	fmt.Println("ControlReleased. Did robot move?")
}

type tokenAuth struct{ token string }

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + t.token}, nil
}
func (tokenAuth) RequireTransportSecurity() bool { return true }

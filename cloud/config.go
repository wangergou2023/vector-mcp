// +build linux darwin

package main

import (
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	Port       = 443
	SocketPath = "/dev/socket/"
	IsOnRobot  = true
)

func checkAuth(_ http.ResponseWriter, r *http.Request) (string, error) {
	auth, ok := r.Header["Authorization"]
	if !ok {
		return "", status.Error(codes.Unauthenticated, "No auth token")
	}
	if len(auth) != 1 {
		return "", status.Error(codes.Unauthenticated, "Too many auth tokens")
	}
	authHeader := auth[0]
	if !strings.HasPrefix(authHeader, "Bearer ") && !strings.HasPrefix(authHeader, "Basic ") {
		return "", status.Error(codes.Unauthenticated, "Unknown auth header")
	}
	return authHeader, nil
}

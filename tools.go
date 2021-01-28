package main

import (
	"net"
	"context"
)

func UnixDial(context context.Context, addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}

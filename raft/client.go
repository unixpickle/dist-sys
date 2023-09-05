package raft

import (
	"errors"
	"math/rand"

	"github.com/google/uuid"
	"github.com/unixpickle/dist-sys/simulator"
)

var ErrClientTimeout = errors.New("raft server did not respond")

type Client[C Command] struct {
	Handle  *simulator.Handle
	Network simulator.Network
	Port    *simulator.Port
	Servers []*simulator.Port

	SendTimeout float64
}

// Send attempts to send the command to one of the servers,
// following redirects as necessary, and returns the result
// or an error if it times out.
func (c *Client[C]) Send(command C) (Result, error) {
	timerStream := c.Handle.Stream()
	timer := c.Handle.Schedule(timerStream, nil, c.SendTimeout)

	defer c.Handle.Cancel(timer)

	id := uuid.NewString()

	server := c.Servers[rand.Intn(len(c.Servers))]
	for {
		c.Network.Send(c.Handle, &simulator.Message{
			Source: c.Port,
			Dest:   server,
			Message: &CommandMessage[C]{
				Command: command,
				ID:      id,
			},
			Size: float64(command.Size()),
		})
		for {
			resp := c.Handle.Poll(timerStream, c.Port.Incoming)
			if resp.Stream == timerStream {
				return nil, ErrClientTimeout
			}
			rawMsg := resp.Message.(*simulator.Message)
			if obj, ok := rawMsg.Message.(*CommandResponse); ok {
				if obj.ID != id {
					continue
				}
				// TODO: handle object here.
				if obj.Redirect != nil {
					server = nil
					for _, p := range c.Servers {
						if p.Node == obj.Redirect {
							server = p
						}
					}
					if server == nil {
						panic("unknown redirect server")
					}
				} else {
					return obj.Result, nil
				}
			}
		}
	}
}

package raft

import (
	"errors"
	"math/rand"

	"github.com/google/uuid"
	"github.com/unixpickle/dist-sys/simulator"
)

var (
	ErrClientTimeout = errors.New("raft server did not respond")
	ErrLeaderUnknown = errors.New("raft leader is not currently known")
)

type Client[C Command] struct {
	Handle  *simulator.Handle
	Network simulator.Network
	Port    *simulator.Port
	Servers []*simulator.Port

	SendTimeout float64

	timerStream *simulator.EventStream
	timer       *simulator.Timer
	leader      *simulator.Port
}

// Send attempts to send the message to the servers and get
// the result of the command.
//
// It retries until the command is executed, or the given
// number of tries is exceeded.
//
// If numAttempts is 0, then retries are executed forever.
func (c *Client[C]) Send(command C, numAttempts int) (Result, error) {
	c.timerStream = c.Handle.Stream()
	c.timer = c.Handle.Schedule(c.timerStream, nil, c.SendTimeout)
	defer func() {
		defer c.Handle.Cancel(c.timer)
	}()

	id := uuid.NewString()

	if c.leader == nil {
		c.leader = c.Servers[rand.Intn(len(c.Servers))]
	}

	msg := &CommandMessage[C]{
		Command: command,
		ID:      id,
	}

	madeAttempts := 0
	for {
		c.sendToLeader(msg)
		res, redir, err := c.waitForResult(id)
		if err != nil {
			madeAttempts++
			if madeAttempts == numAttempts {
				return nil, err
			}
			if err == ErrLeaderUnknown {
				// Wait some time for the cluster to come back up.
				c.Handle.Sleep(c.SendTimeout)
			}
			c.timer = c.Handle.Schedule(c.timerStream, nil, c.SendTimeout)
			c.leader = c.Servers[rand.Intn(len(c.Servers))]
		} else if redir != nil {
			c.leader = nil
			for _, p := range c.Servers {
				if p.Node == redir {
					c.leader = p
				}
			}
			if c.leader == nil {
				panic("unknown redirect server")
			}
		} else {
			return res, nil
		}
	}
}

func (c *Client[C]) sendToLeader(msg *CommandMessage[C]) {
	c.Network.Send(c.Handle, &simulator.Message{
		Source:  c.Port,
		Dest:    c.leader,
		Message: msg,
		Size:    float64(msg.Command.Size() + len(msg.ID)),
	})
}

func (c *Client[C]) waitForResult(id string) (res Result, redir *simulator.Node, err error) {
	for {
		resp := c.Handle.Poll(c.timerStream, c.Port.Incoming)
		if resp.Stream == c.timerStream {
			return nil, nil, ErrClientTimeout
		}
		rawMsg := resp.Message.(*simulator.Message)
		if obj, ok := rawMsg.Message.(*CommandResponse); ok {
			if obj.ID != id {
				continue
			}
			if obj.Redirect != nil {
				return nil, obj.Redirect, nil
			} else if obj.LeaderUnknown {
				return nil, nil, ErrLeaderUnknown
			} else {
				return obj.Result, nil, nil
			}
		}
	}
}

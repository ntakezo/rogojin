package states

import (
	"context"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

const waitInQueue workflows.State = "wait-in-queue"

const queueCookieTopic = "queue-cookie"

// WaitInQueue acquires a shared queue cookie. After a short random wait, the
// first task to clear the queue mints the cookie and shares it on the bus; tasks
// behind it reuse whatever was already published instead of minting their own.
func (c *Context) WaitInQueue(ctx context.Context) (*workflows.State, error) {
	topic := comms.NewTopic[string](c.running.bus, queueCookieTopic)

	sub, err := topic.On(ctx)
	if err != nil {
		return nil, err
	}
	defer sub.Close()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(rand.Intn(100)) * time.Millisecond):
	}

	select {
	case cookie := <-sub.C(): // a task ahead of us already shared one
		// the topic is only ever written through the typed Emit, so the
		// payload always asserts back to string.
		c.running.queueCookie = cookie.(string)
	default: // we are first through the queue: mint and share it
		c.running.queueCookie = uuid.NewString()
		if err := topic.Emit(ctx, c.running.queueCookie); err != nil {
			return nil, err
		}
	}

	return workflows.Next(addToCart), nil
}

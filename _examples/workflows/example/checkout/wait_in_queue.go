package example_checkout

import (
	"context"
	"math/rand"
	"time"

	http "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
	"github.com/ntakezo/rogojin/_examples/workflows/example/common"
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

	// The cookie must ride on the wire, not just in the snapshot: install it
	// into the client's jar so every later request presents it.
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	if err := common.SetCookies(client, c.static.ProductURL,
		&http.Cookie{Name: "queue", Value: c.running.queueCookie}); err != nil {
		return nil, err
	}

	return workflows.Next(login), nil
}

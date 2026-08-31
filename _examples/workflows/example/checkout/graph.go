package example_checkout

import (
	"time"

	"github.com/ntakezo/rogojin/workflows"
)

// Graph wires the checkout states into a workflow graph bound to this context.
// Each handler is a method value closing over c, so state flows through the
// receiver rather than a threaded input. Cross-cutting policy is declared
// here, not in handler bodies: transient network failures retry with
// exponential backoff, and retrying SubmitCheckout is safe because the submit
// itself rides in the effect log — a re-run skips a placed order.
func (c *Context) Graph() workflows.Graph {
	transient := workflows.ExpBackoff(500*time.Millisecond, 2, 5*time.Second)
	return workflows.NewGraph(getHomepage,
		workflows.On(getHomepage, c.GetHomepage, workflows.Retry(3, transient)),
		workflows.On(waitInQueue, c.WaitInQueue),
		// The verification mail either arrives promptly or something upstream
		// is wrong; the deadline turns a silent hang into a failed state.
		workflows.On(login, c.Login, workflows.Timeout(2*time.Minute)),
		workflows.On(followLink, c.FollowLink, workflows.Retry(3, transient)),
		workflows.On(addToCart, c.AddToCart, workflows.Retry(3, transient)),
		workflows.On(submitCheckout, c.SubmitCheckout,
			workflows.Retry(3, workflows.ExpBackoff(time.Second, 2, 10*time.Second))),
	)
}

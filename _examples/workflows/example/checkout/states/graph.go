package states

import "github.com/ntakezo/rogojin/workflows"

// Graph wires the checkout states into a workflow graph bound to this context.
// Each handler is a method value closing over c, so state flows through the
// receiver rather than a threaded input.
func (c *Context) Graph() workflows.Graph {
	return workflows.NewGraph(getHomepage, workflows.States{
		getHomepage:    c.GetHomepage,
		waitInQueue:    c.WaitInQueue,
		addToCart:      c.AddToCart,
		submitCheckout: c.SubmitCheckout,
	})
}

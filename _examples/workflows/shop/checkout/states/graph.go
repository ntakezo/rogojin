package states

import "github.com/ntakezo/rogojin/workflows"

// Graph binds the states to this context: each handler is a method value
// closing over c. Derived from the states package — edits here are overwritten.
func (c *Context) Graph() workflows.Graph {
	return workflows.NewGraph(getHomepage, workflows.States{
		addToCart:      c.AddToCart,
		getHomepage:    c.GetHomepage,
		getProduct:     c.GetProduct,
		submitCheckout: c.SubmitCheckout,
	})
}

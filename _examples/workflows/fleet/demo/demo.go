// Package fleet_demo is the fleet demo's workflow: two states and one
// effect, written to be resumed by a different node than the one that
// started it. The durable struct rides the snapshot, so the greeting
// composed on one node is there when another resumes the run; the greeting
// itself is composed through an effect, so it happens once fleet-wide.
package fleet_demo

import (
	"context"
	"fmt"
	"os"

	"github.com/ntakezo/rogojin/workflows"
)

const Name = "fleet-demo"

const (
	greet  workflows.State = "greet"
	finish workflows.State = "finish"
)

type Input struct {
	Order string `json:"order"`
}

// New builds the registered module. node is this process's name in the
// printed output — the store's claim rows carry the task manager's node id,
// this one just keeps the demo's prints attributable.
func New(node string) *workflows.Module[Input] {
	return workflows.NewModule(Name, func(in Input, deps workflows.Deps) (workflows.Instance, error) {
		r := &run{in: in, node: node}
		r.Persist(&r.d)
		return r, nil
	})
}

// run is one task's instance.
type run struct {
	workflows.Base
	in   Input
	node string
	d    struct{ Greeting string }
}

func (r *run) Graph() workflows.Graph {
	return workflows.NewGraph(greet,
		workflows.On(greet, r.Greet),
		workflows.On(finish, r.Finish),
	)
}

// Greet composes the greeting through an effect, so it happens once
// fleet-wide: the node that resumes this run reads the recorded result
// instead of composing a second greeting.
func (r *run) Greet(ctx context.Context) (*workflows.State, error) {
	greeting, err := workflows.Do(ctx, &r.Base, "compose-greeting", func(ctx context.Context) (string, error) {
		fmt.Printf("[%s] composing the greeting — an effect, so it runs once fleet-wide\n", r.node)
		return fmt.Sprintf("order %s, greeted by %s", r.in.Order, r.node), nil
	})
	if err != nil {
		return nil, err
	}
	r.d.Greeting = greeting
	return workflows.Next(finish), nil
}

// Finish prints the greeting — or, with FLEET_CRASH set, dies the way a
// yanked power cord would: no teardown, no release, the claim simply stops
// renewing.
func (r *run) Finish(ctx context.Context) (*workflows.State, error) {
	if os.Getenv("FLEET_CRASH") != "" {
		fmt.Printf("[%s] crashing before finishing — the claim stops renewing now\n", r.node)
		os.Exit(1)
	}
	fmt.Printf("[%s] finished: %s\n", r.node, r.d.Greeting)
	return nil, nil
}

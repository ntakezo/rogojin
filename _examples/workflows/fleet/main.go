// Command fleet is the two-process demo: run it twice against one database
// and watch the second process finish what the first one died holding.
//
//	go build -o fleet .
//	FLEET_CRASH=1 ./fleet -db /tmp/fleet.db -node a   # checkpoints, then "loses power"
//	./fleet -db /tmp/fleet.db -node b                 # steals the task and finishes it
//
// Node a creates a task, runs its first state — recording an effect and a
// durable greeting — and crashes inside the second. Its claim on the task
// stops renewing, so it expires; node b polls for claimable work, claims the
// task, resumes at the checkpointed state, and finishes. The effect does not
// run again: its log survives in the store, which is the whole trick.
//
// The same binary works over postgres by pointing -db at a DSN and swapping
// sqlite.Open/NewTasks for the postgres constructors.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/sqlite"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

const workflowName = "fleet-demo"

// leaseTTL is deliberately short so the dead node's claim lapses while you
// watch; production keeps the 30s default.
const leaseTTL = 2 * time.Second

const (
	greet  workflows.State = "greet"
	finish workflows.State = "finish"
)

type input struct {
	Order string `json:"order"`
}

// run is one task's instance. The durable struct rides the snapshot, so the
// greeting composed on one node is there when another resumes the run.
type run struct {
	workflows.Base
	in   input
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
		fmt.Printf("[%s] crashing before finishing — the claim will lapse in %s\n", r.node, leaseTTL)
		os.Exit(1)
	}
	fmt.Printf("[%s] finished: %s\n", r.node, r.d.Greeting)
	return nil, nil
}

func main() {
	dsn := flag.String("db", "fleet.db", "database the fleet shares")
	node := flag.String("node", fmt.Sprintf("node-%d", os.Getpid()), "this process's name in task claims")
	flag.Parse()
	ctx := context.Background()

	db, err := sqlite.Open(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	repo, err := sqlite.NewTasks(db)
	if err != nil {
		log.Fatal(err)
	}

	svc, err := tasks.NewManager(ctx, repo, comms.NewBus(),
		tasks.WithNode(*node), tasks.WithLeaseTTL(leaseTTL))
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close()

	module := workflows.NewModule(workflowName, func(in input, deps workflows.Deps) (workflows.Instance, error) {
		r := &run{in: in, node: *node}
		r.Persist(&r.d)
		return r, nil
	})
	if err := svc.RegisterWorkflow(workflowName, module); err != nil {
		log.Fatal(err)
	}

	// A node's loop in miniature: finish what a dead peer left claimable, or
	// start fresh work when the store holds none.
	recovered, err := svc.RecoverAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	pending := 0
	for _, t := range recovered {
		if !workflows.Status(t.Status).Terminal() {
			pending++
		}
	}

	if pending == 0 {
		t, err := svc.CreateTask(ctx, workflowName, input{Order: "999"})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[%s] created task %s\n", *node, t.ID)
		if _, err := t.Start(ctx); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Pending work exists but may still be claimed by its owner; poll until a
	// lapsed claim frees it. This inline loop is what WithRecoverySweep runs
	// for a long-lived node.
	fmt.Printf("[%s] %d unfinished task(s) in the store; waiting for a claim to lapse\n", *node, pending)
	deadline := time.Now().Add(5 * leaseTTL)
	for time.Now().Before(deadline) {
		claimable, err := svc.RecoverClaimable(ctx)
		if err != nil {
			log.Fatal(err)
		}
		for _, t := range claimable {
			rec := t.Record()
			fmt.Printf("[%s] claiming task %s, checkpointed at state %q by %q\n", *node, rec.ID, rec.State, rec.OwnerNode)
			switch _, err := t.Start(ctx); {
			case err == nil:
				return
			case errors.Is(err, tasks.ErrClaimHeld):
				fmt.Printf("[%s] another node won task %s\n", *node, rec.ID)
			default:
				log.Fatal(err)
			}
		}
		time.Sleep(leaseTTL / 4)
	}
	fmt.Printf("[%s] nothing became claimable; is the owner still alive?\n", *node)
}

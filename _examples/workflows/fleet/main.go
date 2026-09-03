// Command fleet is the two-process demo: run it twice against one database
// and watch the second process finish what the first one died holding.
//
//	go build -o fleet .
//	FLEET_CRASH=1 ./fleet -db /tmp/fleet.db -node a   # checkpoints, then "loses power"
//	./fleet -db /tmp/fleet.db -node b                 # steals the task and finishes it
//
// Node a creates a task, runs its first state — recording an effect and a
// durable greeting — and crashes inside the second. Its claim on the task
// stops renewing, so it expires; node b's recovery sweep claims the task,
// resumes it at the checkpointed state, and finishes. The effect does not
// run again: its log survives in the store, which is the whole trick.
//
// The workflow itself lives in the demo package; this file is the node
// wiring. The same binary works over postgres by pointing -db at a DSN and
// swapping sqlite.Open/NewTasks for the postgres constructors.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	fleet_demo "github.com/ntakezo/rogojin/_examples/workflows/fleet/demo"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/sqlite"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

// leaseTTL is deliberately short so the dead node's claim lapses while you
// watch; production keeps the 30s default.
const leaseTTL = 2 * time.Second

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

	// The sweep is the recovery loop: every interval it lists tasks whose
	// claims have lapsed and starts them on background contexts. Nothing
	// below polls for work — this option is the whole takeover story.
	svc, err := tasks.NewManager(ctx, repo, comms.NewBus(),
		tasks.WithNode(*node), tasks.WithLeaseTTL(leaseTTL),
		tasks.WithRecoverySweep(leaseTTL/4, func(taskID string, err error) {
			fmt.Printf("[%s] sweep %s: %v\n", *node, taskID, err)
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close()

	if err := svc.RegisterWorkflow(fleet_demo.Name, fleet_demo.New(*node)); err != nil {
		log.Fatal(err)
	}

	// Fresh work or a dead peer's: the store decides. With unfinished tasks
	// present the sweep will claim them once their leases lapse; this process
	// only has to watch for them turning terminal.
	recovered, err := svc.RecoverAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	var pending []string
	for _, t := range recovered {
		if !workflows.Status(t.Status).Terminal() {
			pending = append(pending, t.ID)
		}
	}

	if len(pending) == 0 {
		t, err := svc.CreateTask(ctx, fleet_demo.Name, fleet_demo.Input{Order: "999"})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[%s] created task %s\n", *node, t.ID)
		if _, err := t.Start(ctx); err != nil {
			log.Fatal(err)
		}
		return
	}

	fmt.Printf("[%s] %d unfinished task(s) in the store; the sweep takes over once their claims lapse\n", *node, len(pending))
	deadline := time.Now().Add(10 * leaseTTL)
	for time.Now().Before(deadline) {
		remaining := pending[:0]
		for _, id := range pending {
			t, err := svc.RecoverTask(ctx, id)
			if err != nil {
				log.Fatal(err)
			}
			if !t.LiveStatus().Terminal() {
				remaining = append(remaining, id)
			}
		}
		if pending = remaining; len(pending) == 0 {
			return
		}
		time.Sleep(leaseTTL / 4)
	}
	fmt.Printf("[%s] %s never finished; is the owner still alive?\n", *node, plural(pending))
}

// plural renders the leftover ids as "task t1" or "tasks t1, t2".
func plural(ids []string) string {
	if len(ids) == 1 {
		return "task " + ids[0]
	}
	return "tasks " + strings.Join(ids, ", ")
}

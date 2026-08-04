// Command run wires the shop domain onto a task service and runs one
// task end to end. It is a starting point — edit the input, swap the
// persistence, and add proxies to taste.
package main

import (
	"context"
	"log"

	"github.com/ntakezo/rogojin/_examples/shop"
	"github.com/ntakezo/rogojin/_examples/shop/checkout"
	"github.com/ntakezo/rogojin/_examples/shop/checkout/states"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/persistence/proxysqlite"
	"github.com/ntakezo/rogojin/persistence/tasksqlite"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/tasks"
)

func main() {
	ctx := context.Background()
	repo, err := tasksqlite.NewSQLite("shop.db")
	if err != nil {
		log.Fatal(err)
	}
	defer repo.Close()
	svc := tasks.NewService(repo, comms.NewBus())
	proxyRepo, err := proxysqlite.NewSQLite("shop-proxies.db")
	if err != nil {
		log.Fatal(err)
	}
	defer proxyRepo.Close()
	manager, err := proxies.NewManager(ctx, proxyRepo, proxies.NewRoundRobin(), proxies.Exclusive(), nil)
	if err != nil {
		log.Fatal(err)
	}
	// Nothing leases a proxy until a workflow's config below carries the manager.
	_ = manager

	// Every workflow the domain holds, registered together, each from its own
	// config. Fill one field in per workflow — checkout.Config takes the
	// manager built above: Register refuses a workflow whose config is unset.
	if err := shop.Register(svc, shop.Configs{}); err != nil {
		log.Fatal(err)
	}

	// The input one task runs against. Shape states.Input to your target and
	// fill it in here.
	task, err := svc.CreateTask(ctx, checkout.Name, states.Input{})
	if err != nil {
		log.Fatal(err)
	}
	output, err := task.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("task %s finished with status %q, output %s", task.ID(), task.Status(), output)
}

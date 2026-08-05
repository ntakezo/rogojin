// Package shop is the shop domain: the workflows that drive it,
// the requests they share, and the registration that puts them on a service.
package shop

import (
	"errors"
	"reflect"

	"github.com/ntakezo/rogojin/_examples/workflows/shop/checkout"
	"github.com/ntakezo/rogojin/_examples/workflows/shop/signup"
	"github.com/ntakezo/rogojin/tasks"
)

// Configs is what the domain builds its workflows from, one field per workflow,
// so a workflow asking for its own dependencies is handed them. A workflow that
// asks for anything is refused when its field is left unset. Derived from the
// workflow packages beside it — edits here are overwritten.
type Configs struct {
	Checkout checkout.Config
	Signup   signup.Config
}

// Register puts every workflow in this domain on svc, each built from its own
// config. Derived from the workflow packages beside it — edits are overwritten.
func Register(svc tasks.Service, configs Configs) error {
	// Checked before anything is registered, so a config left unset leaves the
	// service as it was rather than half a domain on it.
	if reflect.ValueOf(configs.Checkout).IsZero() {
		return errors.New("shop: checkout config is not set — fill Configs.Checkout in before registering")
	}
	if reflect.ValueOf(configs.Signup).IsZero() {
		return errors.New("shop: signup config is not set — fill Configs.Signup in before registering")
	}

	if err := svc.RegisterWorkflow(checkout.Name, checkout.New(configs.Checkout)); err != nil {
		return err
	}
	if err := svc.RegisterWorkflow(signup.Name, signup.New(configs.Signup)); err != nil {
		return err
	}
	return nil
}

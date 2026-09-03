// Package nodeid names the running process for claim ownership. The default
// identity is minted once per process and carries a random suffix on top of
// host and pid: a restarted process must present as a new node, never inherit
// its dead predecessor's claims — host/pid alone would recycle.
package nodeid

import (
	"fmt"
	"os"
	"sync"

	"github.com/google/uuid"
)

// Default returns this process's node identity, "host/pid/8hex", computed
// once. The host and pid make a claim row legible to an operator; the random
// suffix makes the identity unique across restarts.
var Default = sync.OnceValue(func() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s/%d/%.8s", host, os.Getpid(), uuid.NewString())
})

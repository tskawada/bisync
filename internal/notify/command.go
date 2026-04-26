package notify

import (
	"fmt"
	"os/exec"

	"github.com/tskawada/bisync/internal/config"
)

type command struct {
	cmd    string
	events map[EventType]bool
}

func newCommand(h config.NotifyHandlerConfig) *command {
	evts := make(map[EventType]bool, len(h.Events))
	for _, e := range h.Events {
		evts[EventType(e)] = true
	}
	return &command{cmd: h.Command, events: evts}
}

func (c *command) HandlesEvent(et EventType) bool { return c.events[et] }

func (c *command) Send(p *Payload) error {
	cmd := exec.Command(c.cmd)
	cmd.Env = append(cmd.Environ(),
		"BISYNC_EVENT="+string(p.Event),
		"BISYNC_TIMESTAMP="+p.Timestamp,
		"BISYNC_NODE="+p.Node,
		"BISYNC_PEER="+p.Peer,
		"BISYNC_SYNC_PAIR="+p.SyncPair,
		"BISYNC_PATH="+p.Path,
		"BISYNC_MESSAGE="+p.Message,
	)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("notify command %q: %w", c.cmd, err)
	}
	return nil
}

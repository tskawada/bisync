package notify

import (
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/config"
)

// EventType identifies the kind of notification event.
type EventType string

const (
	EventConflict        EventType = "conflict"
	EventError           EventType = "error"
	EventTransferFailure EventType = "transfer_failure"
	EventPeerUnreachable EventType = "peer_unreachable"
	EventRecoveryComplete EventType = "recovery_complete"
)

// Payload is the data attached to a notification.
type Payload struct {
	Event     EventType         `json:"event"`
	Timestamp string            `json:"timestamp"`
	Node      string            `json:"node"`
	Peer      string            `json:"peer"`
	SyncPair  string            `json:"sync_pair"`
	Path      string            `json:"path"`
	Message   string            `json:"message"`
	Details   map[string]string `json:"details,omitempty"`
}

// Handler dispatches notifications for a specific set of event types.
type Handler interface {
	Send(p *Payload) error
	HandlesEvent(et EventType) bool
}

// Notifier dispatches events to all registered handlers.
type Notifier struct {
	cfg      *config.Config
	handlers []Handler
}

// New creates a Notifier from the config.
func New(cfg *config.Config) *Notifier {
	n := &Notifier{cfg: cfg}
	for _, h := range cfg.Notify.Handlers {
		switch h.Type {
		case "webhook":
			n.handlers = append(n.handlers, newWebhook(h))
		case "command":
			n.handlers = append(n.handlers, newCommand(h))
		}
	}
	return n
}

func (n *Notifier) emit(et EventType, syncPair, path, message string, details map[string]string) {
	p := &Payload{
		Event:     et,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Node:      n.cfg.Node.Name,
		Peer:      n.cfg.Peer.Name,
		SyncPair:  syncPair,
		Path:      path,
		Message:   message,
		Details:   details,
	}
	for _, h := range n.handlers {
		if !h.HandlesEvent(et) {
			continue
		}
		if err := h.Send(p); err != nil {
			log.Error().Err(err).Str("event", string(et)).Msg("notification handler failed")
		}
	}
}

// Conflict sends a conflict notification.
func (n *Notifier) Conflict(syncPair, path, message string, details map[string]string) {
	n.emit(EventConflict, syncPair, path, message, details)
}

// Error sends an error notification.
func (n *Notifier) Error(syncPair, path, message string) {
	n.emit(EventError, syncPair, path, message, nil)
}

// TransferFailure sends a transfer failure notification.
func (n *Notifier) TransferFailure(syncPair, path, message string) {
	n.emit(EventTransferFailure, syncPair, path, message, nil)
}

// PeerUnreachable sends a peer-unreachable notification.
func (n *Notifier) PeerUnreachable(message string) {
	n.emit(EventPeerUnreachable, "", "", message, nil)
}

// RecoveryComplete sends a recovery-complete notification.
func (n *Notifier) RecoveryComplete(message string) {
	n.emit(EventRecoveryComplete, "", "", message, nil)
}

package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tskawada/bisync/internal/config"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

type webhook struct {
	url    string
	events map[EventType]bool
}

func newWebhook(h config.NotifyHandlerConfig) *webhook {
	evts := make(map[EventType]bool, len(h.Events))
	for _, e := range h.Events {
		evts[EventType(e)] = true
	}
	return &webhook{url: h.URL, events: evts}
}

func (w *webhook) HandlesEvent(et EventType) bool { return w.events[et] }

func (w *webhook) Send(p *Payload) error {
	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("webhook marshal: %w", err)
	}
	resp, err := httpClient.Post(w.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook: server returned %d", resp.StatusCode)
	}
	return nil
}

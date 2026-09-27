package websocket

import "sync"

type Hub struct {
	mu      sync.RWMutex
	clients map[int64]map[*client]struct{}
}

func NewHub() *Hub { return &Hub{clients: make(map[int64]map[*client]struct{})} }

func (h *Hub) register(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[c.userID] == nil {
		h.clients[c.userID] = make(map[*client]struct{})
	}
	h.clients[c.userID][c] = struct{}{}
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients[c.userID], c)
	if len(h.clients[c.userID]) == 0 {
		delete(h.clients, c.userID)
	}
}

// deliver never waits for a slow socket; a full queue closes that connection.
func (h *Hub) deliver(userID int64, payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients[userID] {
		select {
		case c.send <- payload:
		default:
			c.close()
		}
	}
}

func (h *Hub) CloseAll() {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, userClients := range h.clients {
		for c := range userClients {
			c.close()
		}
	}
}

func (h *Hub) UserIDs() []int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]int64, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	return ids
}

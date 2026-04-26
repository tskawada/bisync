// Package grpc implements the bisync peer-communication layer.
// The wire format uses JSON over HTTP/2 (standard gRPC framing) so that
// no protoc code-generation step is required to build the project.
// The proto/bisync.proto file documents the canonical schema.
package grpc

// ChangelogEntry mirrors the proto ChangelogEntry message.
type ChangelogEntry struct {
	ID           int64  `json:"id"`
	SyncPair     string `json:"sync_pair"`
	Path         string `json:"path"`
	EventType    string `json:"event_type"`
	RenameTo     string `json:"rename_to,omitempty"`
	FileSize     int64  `json:"file_size,omitempty"`
	FileMtime    int64  `json:"file_mtime,omitempty"`
	FileMode     uint32 `json:"file_mode,omitempty"`
	IsDir        bool   `json:"is_dir,omitempty"`
	HLCTimestamp string `json:"hlc_timestamp"`
	NodeName     string `json:"node_name"`
}

// ChangelogRequest requests unsynced entries from a peer.
type ChangelogRequest struct {
	SyncPair string `json:"sync_pair,omitempty"`
	SinceHLC string `json:"since_hlc,omitempty"`
}

// ChangelogResponse carries the peer's unsynced entries.
type ChangelogResponse struct {
	Entries    []*ChangelogEntry `json:"entries"`
	CurrentHLC string            `json:"current_hlc"`
}

// AckRequest acknowledges that a set of entries have been transferred.
type AckRequest struct {
	EntryIDs []int64 `json:"entry_ids"`
	NodeName string  `json:"node_name"`
}

// AckResponse is the reply to AckRequest.
type AckResponse struct {
	Success bool `json:"success"`
}

// DeleteRequest instructs the peer to delete a file.
type DeleteRequest struct {
	SyncPair string `json:"sync_pair"`
	Path     string `json:"path"`
	NodeName string `json:"node_name"`
}

// DeleteResponse is the reply to DeleteRequest.
type DeleteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// RenameRequest instructs the peer to rename a file.
type RenameRequest struct {
	SyncPair string `json:"sync_pair"`
	FromPath string `json:"from_path"`
	ToPath   string `json:"to_path"`
	NodeName string `json:"node_name"`
}

// RenameResponse is the reply to RenameRequest.
type RenameResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// PingRequest is a health-check message.
type PingRequest struct {
	NodeName string `json:"node_name"`
}

// PingResponse is the reply to PingRequest.
type PingResponse struct {
	NodeName string `json:"node_name"`
	Status   string `json:"status"` // "ok", "syncing", "error"
}

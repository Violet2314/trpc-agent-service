// Package worker assembles and executes tenant-specific Agent runners.
package worker

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Event is the platform-neutral projection of a tRPC-Agent-Go event.
type Event struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	ToolName    string `json:"tool_name,omitempty"`
	Error       string `json:"error,omitempty"`
	UsageTokens int    `json:"usage_tokens,omitempty"`
}

// Executor runs one debounced batch while the caller owns the session lock.
type Executor interface {
	Execute(
		context.Context,
		tenant.Snapshot,
		string,
		[]storage.UserEvent,
	) (<-chan Event, error)
}

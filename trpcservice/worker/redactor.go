package worker

import (
	"regexp"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const redactedValue = "${REDACTED}"

// PolicyRedactor applies built-in secret patterns plus tenant patterns.
type PolicyRedactor struct {
	builtins []*regexp.Regexp
}

// NewPolicyRedactor constructs the Worker output redactor.
func NewPolicyRedactor() *PolicyRedactor {
	return &PolicyRedactor{builtins: []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(api[_-]?key|token|password|secret)\b\s*[:=]\s*[^\s,;]+`),
		regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+`),
		regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}\b`),
	}}
}

// Redact replaces matches without interpreting the replacement as a regexp
// capture reference.
func (r *PolicyRedactor) Redact(snapshot tenant.Snapshot, value string) string {
	for _, pattern := range r.builtins {
		value = pattern.ReplaceAllStringFunc(value, func(string) string {
			return redactedValue
		})
	}
	for _, expression := range snapshot.Tenant.Policy.RedactPatterns {
		pattern, err := regexp.Compile(expression)
		if err != nil {
			continue
		}
		value = pattern.ReplaceAllStringFunc(value, func(string) string {
			return redactedValue
		})
	}
	return value
}

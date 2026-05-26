package objstore

import (
	"net/url"
)

// safeID escapes a caller-supplied identifier (workflow ID,
// namespace name, task queue name, etc.) so that it's safe to embed
// in an S3 / object-store key. Object stores like MinIO reject keys
// with leading slashes, consecutive slashes, control characters, and
// some other reserved sequences. URL PathEscape produces a
// deterministic, reversible mapping that handles those cases without
// breaking the "/"-as-prefix-delimiter that List() relies on.
//
// Use this on every component of a constructed key that comes from
// caller input (anything other than the literal segments and the
// integer fields we control). Components we know are
// pre-hex-encoded UUIDs (hostID, table version sums) don't strictly
// need this, but applying it uniformly is cheaper than reasoning
// about each call site.
func safeID(s string) string {
	return url.PathEscape(s)
}

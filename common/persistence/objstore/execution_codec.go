package objstore

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
)

// JSON envelopes for the workflow execution state stored on a
// per-(namespace, workflowID, runID) basis. Every [commonpb.DataBlob]
// gets wrapped in [blobEnv] so we keep encoding + bytes together
// without inheriting protobuf's JSON quirks.

// blobEnv wraps a *commonpb.DataBlob for on-disk storage. Keys are
// short ("e", "d") to keep the wire format compact since the field
// repeats many times in a snapshot.
type blobEnv struct {
	Encoding string `json:"e,omitempty"`
	Data     []byte `json:"d,omitempty"` // base64-encoded by encoding/json
}

func blobToEnv(b *commonpb.DataBlob) *blobEnv {
	if b == nil {
		return nil
	}
	return &blobEnv{
		Encoding: b.EncodingType.String(),
		Data:     b.Data,
	}
}

func envToBlob(e *blobEnv) *commonpb.DataBlob {
	if e == nil {
		return nil
	}
	// EncodingTypeFromString handles both the SCREAMING_CASE form
	// (ENCODING_TYPE_PROTO3) and Temporal's PascalCase shorthand
	// (Proto3) — EncodingType.String() emits the latter, so we use
	// the resolver to round-trip cleanly.
	enc, _ := enumspb.EncodingTypeFromString(e.Encoding)
	return &commonpb.DataBlob{
		EncodingType: enc,
		Data:         e.Data,
	}
}

// chasmEnv wraps an [persistence.InternalChasmNode]. CHASM nodes
// carry both metadata and data blobs; we persist both.
type chasmEnv struct {
	Metadata *blobEnv `json:"m,omitempty"`
	Data     *blobEnv `json:"d,omitempty"`
}

func chasmToEnv(n persistence.InternalChasmNode) chasmEnv {
	return chasmEnv{
		Metadata: blobToEnv(n.Metadata),
		Data:     blobToEnv(n.Data),
	}
}

func envToChasm(e chasmEnv) persistence.InternalChasmNode {
	return persistence.InternalChasmNode{
		Metadata: envToBlob(e.Metadata),
		Data:     envToBlob(e.Data),
	}
}

// workflowEnv is the JSON-encoded form of a workflow execution. It
// holds enough state to reconstruct an [persistence.InternalWorkflowMutableState]
// on Get and to apply an [persistence.InternalWorkflowMutation] in
// place on Update.
//
// Field naming uses short JSON keys deliberately — the envelope
// is serialized many times per workflow (every Update writes it
// again) and bytes matter for S3 storage costs at scale.
type workflowEnv struct {
	NamespaceID string `json:"ns"`
	WorkflowID  string `json:"wid"`
	RunID       string `json:"rid"`

	// Versioning fields.
	NextEventID      int64 `json:"nei,omitempty"`
	StartVersion     int64 `json:"sv,omitempty"`
	LastWriteVersion int64 `json:"lwv,omitempty"`
	DBRecordVersion  int64 `json:"dbrv,omitempty"`

	// Core typed blobs.
	ExecutionInfo  *blobEnv `json:"ei,omitempty"`
	ExecutionState *blobEnv `json:"es,omitempty"`
	Checksum       *blobEnv `json:"cs,omitempty"`

	// Map-of-blob mutable state.
	ActivityInfos       map[int64]*blobEnv  `json:"act,omitempty"`
	TimerInfos          map[string]*blobEnv `json:"tim,omitempty"`
	ChildExecutionInfos map[int64]*blobEnv  `json:"chl,omitempty"`
	RequestCancelInfos  map[int64]*blobEnv  `json:"rc,omitempty"`
	SignalInfos         map[int64]*blobEnv  `json:"sig,omitempty"`
	ChasmNodes          map[string]chasmEnv `json:"chsm,omitempty"`

	// SignalRequestedIDs is stored as a sorted slice — both Snapshot
	// (map-set) and MutableState (slice) project onto it cleanly.
	SignalRequestedIDs []string `json:"srids,omitempty"`

	// BufferedEvents is a list of event blobs awaiting flush.
	BufferedEvents []*blobEnv `json:"be,omitempty"`

	// LastUpdate marker used for debugging and trail-tracking; not
	// part of any CAS predicate.
	UpdatedAt int64 `json:"ts,omitempty"`
}

// snapshotToEnv converts a fresh [persistence.InternalWorkflowSnapshot]
// (used in Create/Set) into a [workflowEnv] for storage.
func snapshotToEnv(s *persistence.InternalWorkflowSnapshot) *workflowEnv {
	env := &workflowEnv{
		NamespaceID:      s.NamespaceID,
		WorkflowID:       s.WorkflowID,
		RunID:            s.RunID,
		NextEventID:      s.NextEventID,
		StartVersion:     s.StartVersion,
		LastWriteVersion: s.LastWriteVersion,
		DBRecordVersion:  s.DBRecordVersion,
		ExecutionInfo:    blobToEnv(s.ExecutionInfoBlob),
		ExecutionState:   blobToEnv(s.ExecutionStateBlob),
		Checksum:         blobToEnv(s.Checksum),
	}

	if len(s.ActivityInfos) > 0 {
		env.ActivityInfos = make(map[int64]*blobEnv, len(s.ActivityInfos))
		for k, v := range s.ActivityInfos {
			env.ActivityInfos[k] = blobToEnv(v)
		}
	}
	if len(s.TimerInfos) > 0 {
		env.TimerInfos = make(map[string]*blobEnv, len(s.TimerInfos))
		for k, v := range s.TimerInfos {
			env.TimerInfos[k] = blobToEnv(v)
		}
	}
	if len(s.ChildExecutionInfos) > 0 {
		env.ChildExecutionInfos = make(map[int64]*blobEnv, len(s.ChildExecutionInfos))
		for k, v := range s.ChildExecutionInfos {
			env.ChildExecutionInfos[k] = blobToEnv(v)
		}
	}
	if len(s.RequestCancelInfos) > 0 {
		env.RequestCancelInfos = make(map[int64]*blobEnv, len(s.RequestCancelInfos))
		for k, v := range s.RequestCancelInfos {
			env.RequestCancelInfos[k] = blobToEnv(v)
		}
	}
	if len(s.SignalInfos) > 0 {
		env.SignalInfos = make(map[int64]*blobEnv, len(s.SignalInfos))
		for k, v := range s.SignalInfos {
			env.SignalInfos[k] = blobToEnv(v)
		}
	}
	if len(s.ChasmNodes) > 0 {
		env.ChasmNodes = make(map[string]chasmEnv, len(s.ChasmNodes))
		for k, v := range s.ChasmNodes {
			env.ChasmNodes[k] = chasmToEnv(v)
		}
	}
	if len(s.SignalRequestedIDs) > 0 {
		env.SignalRequestedIDs = sortedKeys(s.SignalRequestedIDs)
	}

	return env
}

// envToMutableState rebuilds the response shape every GetWorkflowExecution
// caller expects.
func envToMutableState(env *workflowEnv) *persistence.InternalWorkflowMutableState {
	out := &persistence.InternalWorkflowMutableState{
		ExecutionInfo:   envToBlob(env.ExecutionInfo),
		ExecutionState:  envToBlob(env.ExecutionState),
		NextEventID:     env.NextEventID,
		DBRecordVersion: env.DBRecordVersion,
		Checksum:        envToBlob(env.Checksum),
	}

	if len(env.ActivityInfos) > 0 {
		out.ActivityInfos = make(map[int64]*commonpb.DataBlob, len(env.ActivityInfos))
		for k, v := range env.ActivityInfos {
			out.ActivityInfos[k] = envToBlob(v)
		}
	}
	if len(env.TimerInfos) > 0 {
		out.TimerInfos = make(map[string]*commonpb.DataBlob, len(env.TimerInfos))
		for k, v := range env.TimerInfos {
			out.TimerInfos[k] = envToBlob(v)
		}
	}
	if len(env.ChildExecutionInfos) > 0 {
		out.ChildExecutionInfos = make(map[int64]*commonpb.DataBlob, len(env.ChildExecutionInfos))
		for k, v := range env.ChildExecutionInfos {
			out.ChildExecutionInfos[k] = envToBlob(v)
		}
	}
	if len(env.RequestCancelInfos) > 0 {
		out.RequestCancelInfos = make(map[int64]*commonpb.DataBlob, len(env.RequestCancelInfos))
		for k, v := range env.RequestCancelInfos {
			out.RequestCancelInfos[k] = envToBlob(v)
		}
	}
	if len(env.SignalInfos) > 0 {
		out.SignalInfos = make(map[int64]*commonpb.DataBlob, len(env.SignalInfos))
		for k, v := range env.SignalInfos {
			out.SignalInfos[k] = envToBlob(v)
		}
	}
	if len(env.ChasmNodes) > 0 {
		out.ChasmNodes = make(map[string]persistence.InternalChasmNode, len(env.ChasmNodes))
		for k, v := range env.ChasmNodes {
			out.ChasmNodes[k] = envToChasm(v)
		}
	}
	if len(env.SignalRequestedIDs) > 0 {
		out.SignalRequestedIDs = append([]string(nil), env.SignalRequestedIDs...)
	}
	if len(env.BufferedEvents) > 0 {
		out.BufferedEvents = make([]*commonpb.DataBlob, 0, len(env.BufferedEvents))
		for _, e := range env.BufferedEvents {
			out.BufferedEvents = append(out.BufferedEvents, envToBlob(e))
		}
	}

	return out
}

// applyMutation folds an [persistence.InternalWorkflowMutation] into
// an existing [workflowEnv]. Upsert maps overwrite; Delete maps
// remove. The buffered-events list is replaced when ClearBufferedEvents
// is set; otherwise NewBufferedEvents is appended.
//
// Note: this function does NOT enforce the Condition / DBRecordVersion
// predicate — that's the caller's responsibility before invoking
// (UpdateWorkflowExecution does the check against the stored env,
// then calls applyMutation, then writes via CAS on the blob ETag).
func applyMutation(env *workflowEnv, m *persistence.InternalWorkflowMutation) {
	if m.ExecutionInfoBlob != nil {
		env.ExecutionInfo = blobToEnv(m.ExecutionInfoBlob)
	}
	if m.ExecutionStateBlob != nil {
		env.ExecutionState = blobToEnv(m.ExecutionStateBlob)
	}
	if m.Checksum != nil {
		env.Checksum = blobToEnv(m.Checksum)
	}
	env.NextEventID = m.NextEventID
	env.StartVersion = m.StartVersion
	env.LastWriteVersion = m.LastWriteVersion
	env.DBRecordVersion = m.DBRecordVersion

	env.ActivityInfos = applyInt64BlobMap(env.ActivityInfos, m.UpsertActivityInfos, m.DeleteActivityInfos)
	env.TimerInfos = applyStringBlobMap(env.TimerInfos, m.UpsertTimerInfos, m.DeleteTimerInfos)
	env.ChildExecutionInfos = applyInt64BlobMap(env.ChildExecutionInfos, m.UpsertChildExecutionInfos, m.DeleteChildExecutionInfos)
	env.RequestCancelInfos = applyInt64BlobMap(env.RequestCancelInfos, m.UpsertRequestCancelInfos, m.DeleteRequestCancelInfos)
	env.SignalInfos = applyInt64BlobMap(env.SignalInfos, m.UpsertSignalInfos, m.DeleteSignalInfos)
	env.ChasmNodes = applyChasmMap(env.ChasmNodes, m.UpsertChasmNodes, m.DeleteChasmNodes)

	// SignalRequestedIDs is a set; flatten upsert/delete onto the
	// stored sorted slice.
	if len(m.UpsertSignalRequestedIDs) > 0 || len(m.DeleteSignalRequestedIDs) > 0 {
		set := make(map[string]struct{}, len(env.SignalRequestedIDs))
		for _, id := range env.SignalRequestedIDs {
			set[id] = struct{}{}
		}
		for id := range m.UpsertSignalRequestedIDs {
			set[id] = struct{}{}
		}
		for id := range m.DeleteSignalRequestedIDs {
			delete(set, id)
		}
		env.SignalRequestedIDs = sortedKeys(set)
	}

	// Buffered events: clear-then-append semantics.
	if m.ClearBufferedEvents {
		env.BufferedEvents = nil
	}
	if m.NewBufferedEvents != nil {
		env.BufferedEvents = append(env.BufferedEvents, blobToEnv(m.NewBufferedEvents))
	}
}

func applyInt64BlobMap(
	current map[int64]*blobEnv,
	upserts map[int64]*commonpb.DataBlob,
	deletes map[int64]struct{},
) map[int64]*blobEnv {
	if len(upserts) == 0 && len(deletes) == 0 {
		return current
	}
	if current == nil {
		current = make(map[int64]*blobEnv, len(upserts))
	}
	for k, v := range upserts {
		current[k] = blobToEnv(v)
	}
	for k := range deletes {
		delete(current, k)
	}
	if len(current) == 0 {
		return nil
	}
	return current
}

func applyStringBlobMap(
	current map[string]*blobEnv,
	upserts map[string]*commonpb.DataBlob,
	deletes map[string]struct{},
) map[string]*blobEnv {
	if len(upserts) == 0 && len(deletes) == 0 {
		return current
	}
	if current == nil {
		current = make(map[string]*blobEnv, len(upserts))
	}
	for k, v := range upserts {
		current[k] = blobToEnv(v)
	}
	for k := range deletes {
		delete(current, k)
	}
	if len(current) == 0 {
		return nil
	}
	return current
}

func applyChasmMap(
	current map[string]chasmEnv,
	upserts map[string]persistence.InternalChasmNode,
	deletes map[string]struct{},
) map[string]chasmEnv {
	if len(upserts) == 0 && len(deletes) == 0 {
		return current
	}
	if current == nil {
		current = make(map[string]chasmEnv, len(upserts))
	}
	for k, v := range upserts {
		current[k] = chasmToEnv(v)
	}
	for k := range deletes {
		delete(current, k)
	}
	if len(current) == 0 {
		return nil
	}
	return current
}

// sortedKeys returns the keys of a `map[string]struct{}` set, sorted
// lexicographically. Used to serialize sets as stable JSON slices.
func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

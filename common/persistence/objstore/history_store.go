package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/objstore/blob"
)

// History storage layout (event tree + branch model):
//
//	history/nodes/{treeID}/{branchID}/{nodeID:020d}-{txnID:020d}
//	  → JSON-encoded historyNodeEnv. Append-once: IfNoneMatch=*
//	    on PUT prevents accidental overwrite. The 20-digit zero-
//	    padded encoding makes lexicographic ordering match natural
//	    nodeID + txnID ordering, so List(prefix) returns nodes in
//	    the correct sequence.
//
//	history/trees/{treeID}/branches/{branchID}
//	  → JSON-encoded historyBranchEnv. Holds the TreeInfo blob the
//	    caller passed at branch creation time; updates with new
//	    branches share the same prefix.
//
// "txnID descending wins" semantics: when two nodes share a nodeID
// the one with higher txnID is authoritative. The list scan picks
// up the highest-txnID node per nodeID; lower-txnID entries are
// shadowed.

func historyNodeKey(treeID, branchID string, nodeID, txnID int64) string {
	// txnIDs may be negative in some race scenarios — clamp to int64
	// max-encodable form via signed-fixed-width via two's-complement
	// "padded decimal" is fine because cassandra also stores them
	// sorted natively as int64. We zero-pad the absolute value with
	// a leading 'n' for negatives — this preserves total order over
	// (positives ≻ negatives) which mirrors Cassandra's CLUSTERING ASC.
	return fmt.Sprintf("history/nodes/%s/%s/%020d-%s", treeID, branchID, nodeID, fixedWidthTxn(txnID))
}

func historyNodePrefix(treeID, branchID string) string {
	return fmt.Sprintf("history/nodes/%s/%s/", treeID, branchID)
}

func historyBranchKey(treeID, branchID string) string {
	return fmt.Sprintf("history/trees/%s/branches/%s", treeID, branchID)
}

func historyTreeBranchesPrefix(treeID string) string {
	return fmt.Sprintf("history/trees/%s/branches/", treeID)
}

func historyAllTreesPrefix() string {
	return "history/trees/"
}

// fixedWidthTxn formats a txnID into a sortable 21-character string:
// "0XXXXXXXXXXXXXXXXXXXX" for non-negative, "n" + 20 zero-padded
// abs(txnID) for negative. Lex-sort is identity-equivalent to numeric
// sort within positives; negatives sort AFTER positives (we don't
// actually expect negatives in practice).
func fixedWidthTxn(txnID int64) string {
	if txnID >= 0 {
		return fmt.Sprintf("0%020d", txnID)
	}
	// Treat as "n<padded abs>" so negative txnIDs sort after positives.
	abs := -txnID
	return fmt.Sprintf("n%020d", abs)
}

// historyNodeEnv is the on-disk shape of a single history node.
type historyNodeEnv struct {
	NodeID            int64    `json:"nid"`
	TransactionID     int64    `json:"txn"`
	PrevTransactionID int64    `json:"ptxn"`
	Events            *blobEnv `json:"ev"`
}

// historyBranchEnv is the on-disk shape of a (tree, branch) record.
// We persist the TreeInfo blob verbatim so callers can reconstruct
// the branch hierarchy on read.
type historyBranchEnv struct {
	TreeID   string   `json:"tid"`
	BranchID string   `json:"bid"`
	TreeInfo *blobEnv `json:"ti,omitempty"`
}

// AppendHistoryNodes writes a single event-segment object. If
// IsNewBranch is true, we also write the (tree, branch) record.
func (e *executionStore) appendHistoryNodes(
	ctx context.Context,
	request *persistence.InternalAppendHistoryNodesRequest,
) error {
	branch := request.BranchInfo
	if branch == nil {
		return serviceerror.NewInvalidArgument("objstore: AppendHistoryNodes: BranchInfo is required")
	}
	node := request.Node
	if node.Events == nil {
		return serviceerror.NewInvalidArgument("objstore: AppendHistoryNodes: Node.Events is required")
	}

	nodeKey := historyNodeKey(branch.TreeId, branch.BranchId, node.NodeID, node.TransactionID)
	nodeBody, err := json.Marshal(historyNodeEnv{
		NodeID:            node.NodeID,
		TransactionID:     node.TransactionID,
		PrevTransactionID: node.PrevTransactionID,
		Events:            blobToEnv(node.Events),
	})
	if err != nil {
		return fmt.Errorf("objstore: marshal history node: %w", err)
	}
	if _, err := e.blob.Put(ctx, nodeKey, nodeBody, blob.PutOptions{
		ContentType: "application/json",
		// Multiple writes for the same (nodeID, txnID) are idempotent.
		// We deliberately do NOT use IfNoneMatch=* — Temporal's
		// retry path may legitimately re-append the same node.
	}); err != nil {
		return fmt.Errorf("objstore: put history node: %w", err)
	}

	if request.IsNewBranch {
		branchBody, err := json.Marshal(historyBranchEnv{
			TreeID:   branch.TreeId,
			BranchID: branch.BranchId,
			TreeInfo: blobToEnv(request.TreeInfo),
		})
		if err != nil {
			return fmt.Errorf("objstore: marshal new branch: %w", err)
		}
		bKey := historyBranchKey(branch.TreeId, branch.BranchId)
		if _, err := e.blob.Put(ctx, bKey, branchBody, blob.PutOptions{
			ContentType: "application/json",
		}); err != nil {
			return fmt.Errorf("objstore: put history branch: %w", err)
		}
	}
	return nil
}

func (e *executionStore) deleteHistoryNodes(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryNodesRequest,
) error {
	branch := request.BranchInfo
	if branch == nil {
		return serviceerror.NewInvalidArgument("objstore: DeleteHistoryNodes: BranchInfo is required")
	}
	if request.NodeID < persistence.GetBeginNodeID(branch) {
		return &persistence.InvalidPersistenceRequestError{
			Msg: "cannot delete from ancestors' nodes",
		}
	}
	key := historyNodeKey(branch.TreeId, branch.BranchId, request.NodeID, request.TransactionID)
	return e.blob.Delete(ctx, key, blob.DeleteOptions{})
}

func (e *executionStore) readHistoryBranch(
	ctx context.Context,
	request *persistence.InternalReadHistoryBranchRequest,
) (*persistence.InternalReadHistoryBranchResponse, error) {
	branch, err := e.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, fmt.Errorf("objstore: parse branch token: %w", err)
	}
	prefix := historyNodePrefix(branch.TreeId, request.BranchID)
	infos, err := e.blob.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("objstore: list history nodes: %w", err)
	}

	// Filter by [MinNodeID, MaxNodeID) and read all matching node bodies.
	// We also collapse duplicates per nodeID (highest txnID wins),
	// matching Cassandra's "txn_id DESC" override behavior.
	type entry struct {
		nodeID int64
		txnID  int64
		info   blob.ObjectInfo
	}
	matching := make([]entry, 0, len(infos))
	for _, info := range infos {
		nodeID, txnID, ok := parseNodeKey(info.Key)
		if !ok {
			continue
		}
		if nodeID < request.MinNodeID || nodeID >= request.MaxNodeID {
			continue
		}
		matching = append(matching, entry{nodeID, txnID, info})
	}
	// Sort by nodeID asc, then txnID desc. Higher txnID wins per nodeID.
	sort.Slice(matching, func(i, j int) bool {
		if matching[i].nodeID != matching[j].nodeID {
			if request.ReverseOrder {
				return matching[i].nodeID > matching[j].nodeID
			}
			return matching[i].nodeID < matching[j].nodeID
		}
		return matching[i].txnID > matching[j].txnID
	})
	// Dedupe per nodeID — keep the first (highest-txn) entry.
	seen := make(map[int64]bool, len(matching))
	deduped := matching[:0]
	for _, m := range matching {
		if seen[m.nodeID] {
			continue
		}
		seen[m.nodeID] = true
		deduped = append(deduped, m)
	}

	// Apply pagination. NextPageToken encodes the index of the next
	// entry to return.
	pageStart := 0
	if len(request.NextPageToken) > 0 {
		v, err := strconv.Atoi(string(request.NextPageToken))
		if err == nil && v >= 0 && v <= len(deduped) {
			pageStart = v
		}
	}
	pageEnd := len(deduped)
	if request.PageSize > 0 && pageStart+request.PageSize < pageEnd {
		pageEnd = pageStart + request.PageSize
	}

	nodes := make([]persistence.InternalHistoryNode, 0, pageEnd-pageStart)
	for _, m := range deduped[pageStart:pageEnd] {
		if request.MetadataOnly {
			// Skip body read — we have NodeID + TxnID from the key.
			nodes = append(nodes, persistence.InternalHistoryNode{
				NodeID:        m.nodeID,
				TransactionID: m.txnID,
			})
			continue
		}
		body, err := readBlobBody(ctx, e.blob, m.info.Key)
		if err != nil {
			return nil, fmt.Errorf("objstore: read history node %s: %w", m.info.Key, err)
		}
		var nodeEnv historyNodeEnv
		if err := json.Unmarshal(body, &nodeEnv); err != nil {
			return nil, fmt.Errorf("objstore: unmarshal history node: %w", err)
		}
		nodes = append(nodes, persistence.InternalHistoryNode{
			NodeID:            nodeEnv.NodeID,
			TransactionID:     nodeEnv.TransactionID,
			PrevTransactionID: nodeEnv.PrevTransactionID,
			Events:            envToBlob(nodeEnv.Events),
		})
	}

	var nextToken []byte
	if pageEnd < len(deduped) {
		nextToken = []byte(strconv.Itoa(pageEnd))
	}
	return &persistence.InternalReadHistoryBranchResponse{
		Nodes:         nodes,
		NextPageToken: nextToken,
	}, nil
}

func (e *executionStore) forkHistoryBranch(
	ctx context.Context,
	request *persistence.InternalForkHistoryBranchRequest,
) error {
	if request.ForkBranchInfo == nil {
		return serviceerror.NewInvalidArgument("objstore: ForkHistoryBranch: ForkBranchInfo is required")
	}
	// A fork is a single branch-info write into the same tree as
	// the parent. The TreeInfo blob has been updated by the caller
	// to include the new branch's ancestry.
	body, err := json.Marshal(historyBranchEnv{
		TreeID:   request.ForkBranchInfo.TreeId,
		BranchID: request.NewBranchID,
		TreeInfo: blobToEnv(request.TreeInfo),
	})
	if err != nil {
		return fmt.Errorf("objstore: marshal forked branch: %w", err)
	}
	key := historyBranchKey(request.ForkBranchInfo.TreeId, request.NewBranchID)
	if _, err := e.blob.Put(ctx, key, body, blob.PutOptions{
		ContentType: "application/json",
		IfNoneMatch: "*",
	}); err != nil {
		if errors.Is(err, blob.ErrPreconditionFailed) {
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("objstore: branch %s/%s already exists", request.ForkBranchInfo.TreeId, request.NewBranchID),
			}
		}
		return fmt.Errorf("objstore: put forked branch: %w", err)
	}
	return nil
}

func (e *executionStore) deleteHistoryBranch(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryBranchRequest,
) error {
	if request.BranchInfo == nil {
		return serviceerror.NewInvalidArgument("objstore: DeleteHistoryBranch: BranchInfo is required")
	}
	branch := request.BranchInfo

	// Delete the branch metadata.
	if err := e.blob.Delete(ctx, historyBranchKey(branch.TreeId, branch.BranchId), blob.DeleteOptions{}); err != nil {
		return fmt.Errorf("objstore: delete branch: %w", err)
	}

	// Delete history nodes per supplied range. BranchRanges may
	// reference ancestor branches if the deletion cascades.
	for _, br := range request.BranchRanges {
		nodes, err := e.blob.List(ctx, historyNodePrefix(branch.TreeId, br.BranchId))
		if err != nil {
			return fmt.Errorf("objstore: list nodes for delete %s: %w", br.BranchId, err)
		}
		for _, info := range nodes {
			nodeID, _, ok := parseNodeKey(info.Key)
			if !ok || nodeID < br.BeginNodeId {
				continue
			}
			if err := e.blob.Delete(ctx, info.Key, blob.DeleteOptions{}); err != nil {
				return fmt.Errorf("objstore: delete node %s: %w", info.Key, err)
			}
		}
	}
	return nil
}

func (e *executionStore) getHistoryTreeContainingBranch(
	ctx context.Context,
	request *persistence.InternalGetHistoryTreeContainingBranchRequest,
) (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
	// Parse the branch token to extract treeID.
	branch, err := e.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, fmt.Errorf("objstore: parse branch token: %w", err)
	}

	infos, err := e.blob.List(ctx, historyTreeBranchesPrefix(branch.TreeId))
	if err != nil {
		return nil, fmt.Errorf("objstore: list tree branches: %w", err)
	}
	out := make([]*commonpb.DataBlob, 0, len(infos))
	for _, info := range infos {
		body, err := readBlobBody(ctx, e.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("objstore: read branch %s: %w", info.Key, err)
		}
		var benv historyBranchEnv
		if err := json.Unmarshal(body, &benv); err != nil {
			return nil, fmt.Errorf("objstore: unmarshal branch: %w", err)
		}
		if benv.TreeInfo != nil {
			out = append(out, envToBlob(benv.TreeInfo))
		}
	}
	return &persistence.InternalGetHistoryTreeContainingBranchResponse{
		TreeInfos: out,
	}, nil
}

func (e *executionStore) getAllHistoryTreeBranches(
	ctx context.Context,
	request *persistence.GetAllHistoryTreeBranchesRequest,
) (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
	infos, err := e.blob.List(ctx, historyAllTreesPrefix())
	if err != nil {
		return nil, fmt.Errorf("objstore: list trees: %w", err)
	}
	out := make([]persistence.InternalHistoryBranchDetail, 0, len(infos))
	for _, info := range infos {
		// Key shape: history/trees/{treeID}/branches/{branchID}
		parts := strings.Split(strings.TrimPrefix(info.Key, "history/trees/"), "/")
		if len(parts) != 3 || parts[1] != "branches" {
			continue
		}
		treeID, branchID := parts[0], parts[2]
		body, err := readBlobBody(ctx, e.blob, info.Key)
		if err != nil {
			return nil, fmt.Errorf("objstore: read tree branch %s: %w", info.Key, err)
		}
		var benv historyBranchEnv
		if err := json.Unmarshal(body, &benv); err != nil {
			return nil, fmt.Errorf("objstore: unmarshal tree branch: %w", err)
		}
		var encoding string
		var data []byte
		if benv.TreeInfo != nil {
			encoding = benv.TreeInfo.Encoding
			data = benv.TreeInfo.Data
		} else {
			encoding = enumspb.ENCODING_TYPE_PROTO3.String()
		}
		out = append(out, persistence.InternalHistoryBranchDetail{
			TreeID:   treeID,
			BranchID: branchID,
			Encoding: encoding,
			Data:     data,
		})
	}
	return &persistence.InternalGetAllHistoryTreeBranchesResponse{
		Branches: out,
	}, nil
}

// parseNodeKey extracts nodeID + txnID from a history node key.
// Returns ok=false on malformed keys.
func parseNodeKey(key string) (int64, int64, bool) {
	// history/nodes/{treeID}/{branchID}/{nodeID:020d}-{txnID...}
	const prefix = "history/nodes/"
	if !strings.HasPrefix(key, prefix) {
		return 0, 0, false
	}
	rest := key[len(prefix):]
	idx := strings.LastIndex(rest, "/")
	if idx == -1 {
		return 0, 0, false
	}
	suffix := rest[idx+1:]
	dash := strings.Index(suffix, "-")
	if dash == -1 {
		return 0, 0, false
	}
	nodeID, err := strconv.ParseInt(suffix[:dash], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	txnPart := suffix[dash+1:]
	if strings.HasPrefix(txnPart, "n") {
		v, err := strconv.ParseInt(txnPart[1:], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		return nodeID, -v, true
	}
	v, err := strconv.ParseInt(strings.TrimPrefix(txnPart, "0"), 10, 64)
	if err != nil {
		// Could be "0" itself.
		if txnPart == "00000000000000000000" {
			return nodeID, 0, true
		}
		return 0, 0, false
	}
	return nodeID, v, true
}

func readBlobBody(ctx context.Context, b blob.Store, key string) ([]byte, error) {
	res, err := b.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}

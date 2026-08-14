# Temporal, embedded, on object storage

Design note. Status: proposal. Audience: one reader who wrote shale, forked temporal, and built the
objstore prototype.

---

## TL;DR

- The question has two readings and they have different answers. **(a) Temporal server with an
  object-store persistence plugin** is done in prototype and is roughly 3 weeks from a demo, 8 to 10
  weeks from something you would trust with `kill -9`. **(b) An embedded durable-execution engine whose
  only durable dependency is a bucket** does not exist anywhere in the industry, and the reason is
  latency, not capability.
- **The objstore prototype's problem is not performance, it is atomicity.** `UpdateWorkflowExecution`
  decomposes one Cassandra `LOGGED BATCH` into 2 GETs and 5 independent PUTs on a representative update
  (`common/persistence/objstore/execution_store.go:187,219,236,243,264-274`) with no rollback. The PUTs
  are strictly ordered and **the order is wrong** (section 2.3). Three of the intermediate states are
  unrecoverable corruption, not retryable no-ops.
- **`common/persistence/objstore/execution_store.go` contains zero references to `request.RangeID`.**
  Verified by grep: the count is 0. Cassandra fences five write paths with `IF range_id = ?`. The
  prototype fences none. A history host that has lost its shard lease can still write mutable state,
  history nodes, and history tasks.
- **The "53/53 conformance on MinIO" claim does not survive contact with the repo.** temporal-s3 has no
  conformance runner, no Makefile target, no CI job, no result artifact. The same number 53 is also
  driftwood's PASS count (53 of 55), timestamped before the objstore work; *hypothesis, unverified*, that
  it was carried over. Separately, the `temporalio/features` corpus is a *vacuous oracle for this class of
  bug*: it never crash-kills a history host mid-write, so a torn-write defect passes it by construction.
- **shale's `Transact(pinKey, fn)` is exactly the missing primitive, and Temporal's row set maps onto it
  cleanly.** Every row Cassandra puts in one batch is shard-scoped, so one hash tag `{h/<cluster>/<shardID>}`
  collapses the whole atomic set into one storage unit. `IF range_id = ?` becomes a read-check.
  `IF db_record_version = ?` becomes another read-check. The three simultaneous conditions all fit.
- **On a metered bucket the economics are decided by batching, and the gap is a bit over one order of
  magnitude.** One object per record costs **$42.00 per million workflow tasks** on S3 Standard at the op
  counts read out of the code. An LSM over the same bucket costs **$0.80 per million at 1000 WFT/s**
  (16 units, 100 ms flush window), and less as throughput rises because the flush window is fixed. That is
  ~52x, not 100x. The parallel fan-out case is where the real gap lives: scheduling 100 activities in one
  transaction is 205 serial PUTs (6.2 s) against 1 WAL PUT.
- **On self-hosted MinIO, which is what the prototype was tested on and what the reader will run, the
  request bill is zero and the whole cost argument evaporates.** What survives is latency, and the strict
  commit latency of a shale `Transact` is unmeasured on any object store. R1 in section 8 is the
  experiment; run it before believing any number in section 5.
- **S3 Express One Zone is strictly better than Standard on both axes for this workload**, which is
  counterintuitive and is the single most actionable fact for anyone on AWS: 6.4 ms PUT vs about 30 ms,
  and $0.00113 per 1000 PUTs vs $0.005. It costs you a single AZ and it costs you the "runs on any bucket"
  pitch.
- **Recommendation: one path, T1-prime then T3.** Build a shale-backed persistence plugin
  (`shalestore`) for the unmodified Temporal server, single node, shale embedded in-process with the
  slatedb backend against a bucket. Validate it with the whole Temporal server, which is the only real
  test oracle available. Then, and only then, reuse the same key layout under a purpose-built engine.
  **Skip T2 (multi-node embedded Temporal on shale) entirely**: the shard-ownership mapping does not
  collapse, the two leases stay nested, and the one genuine prize (storage locality) does not pay for 16
  to 23 weeks. T1-prime is **17 to 23 weeks** to a measured, crash-tested `temporalbox`, priced in 7.
- **T1-prime is strict-durability-only, and that is a structural fact, not a setting.** shale's slate
  factory pins `AwaitDurable=true` on the R=1 path (`backends/slate/factory.go:760-783`), and shale's
  owner-side CAS holds a per-**node** mutex across that durable commit (`pkg/cluster/cas.go:190,310,318`).
  So one node's strict `Transact` ceiling is 1/commit-latency across all shards, with no group commit
  available on the CAS path. Two shale-side changes (stripe `casCommitMu`, reach relaxed durability at
  R>=2) are on the critical path for anything past class (a). They are named in 4.8 and priced in 7.

---

## 1. The question, restated

> What would it take to implement Temporal in an embedded plus object-storage manner, similar to
> shale/slatedb?

That sentence contains two questions with different answers, different effort profiles, and different
failure modes. Separating them is most of the work.

### Reading (a): Temporal server, unchanged topology, object-store persistence

Four services (frontend, history, matching, worker), ringpop membership, the whole gRPC API, the whole
SDK ecosystem. The only change is that the persistence plugin talks to a bucket instead of Cassandra or
Postgres. "Embedded" here means *no separate database to operate*, not *no separate server to operate*.

This is what temporal-s3 already does. The hook is real:
`temporal.WithCustomDataStoreFactory` (`temporal/server_option.go:145`), marked experimental in source
and absent from Temporal's public persistence docs. The one shipped third-party driver in the wild is
`manetu/temporal-yugabyte` for YCQL.

Reading (a) also has a degenerate single-process form, which is the interesting one: Temporal already
ships `temporaltest/internal.LiteServer`, a single-process, all-four-services, zero-external-dependency
Go library with `NumHistoryShards: 1` and static membership. Point that at a bucket and you have
"Temporal in a box, durable on S3, no database."

### Reading (b): an embedded durable-execution engine

A library you link into your process. `import "durable"`, hand it a bucket, get workflows. Its only
durable dependency is object storage, the way slatedb's only durable dependency is object storage. No
Cassandra, no Postgres, no separate cluster, and in the strong form no separate server process at all.

**Nobody has shipped this.** Every production durable-execution or streaming system that stores in object
storage puts a small fast durable log in front of it:

| System | Hot durable tier | Object storage role |
|---|---|---|
| Restate | Bifrost, replicated across nodes | RocksDB snapshots only |
| Golem | Redis (hot oplog) | cold oplog chunks |
| Neon | Paxos safekeepers | off the commit path entirely |
| Turbopuffer | S3 as WAL, but capped at 1 WAL entry/sec/namespace | primary |
| WarpStream | none, and it shows: 400 to 600 ms p99 produce on Standard, under 150 ms only on Express | primary |
| Turso | S3 Express plus cross-tenant batching | primary |

Turso is the closest existing proof that a bucket *can* be the sole durable store, and it required both
Express and aggressive batching. That is the shape of the answer to reading (b), and section 5 derives
why.

The rest of this document answers both, and the recommendation in section 7 sequences them so that the
work is shared rather than duplicated.

---

## 2. Where things stand

### 2.1 What the prototype is

Branch `s3-persistence`, HEAD `899fca7`, about 6600 LOC under `common/persistence/objstore/`. Uncommitted
work adds `visibility_store.go` and its test.

The surface is **complete, not stubbed**. All nine `persistence.DataStoreFactory` methods return real
stores (`common/persistence/objstore/factory.go:73-124`), and the uncommitted visibility work adds the
tenth extension point with all fourteen `VisibilityStore` methods. The `factory.go` docstrings still say
"Unimplemented stubs landing in #288/#289/#290" (`factory.go:16-19,96-99`); that comment is stale.
`ensureBlobReady` (`factory.go:134`) is dead code.

The port is four verbs (`common/persistence/objstore/blob/store.go:46-71`):

```go
type Store interface {
    Put(ctx, key string, body []byte, opts PutOptions) (PutResult, error)   // IfMatch / IfNoneMatch
    Get(ctx, key string) (GetResult, error)
    Delete(ctx, key string, opts DeleteOptions) error                       // IfMatch
    List(ctx, prefix string) ([]ObjectInfo, error)                          // lexicographic, no cursor
}
```

Backends: `s3`, `filefs`, `memfs`, `sharedmem`. Storage model: **one JSON object per logical record**,
with per-object ETag CAS as the only concurrency primitive. Thirteen top-level key prefixes (full schema
in the appendix).

The CAS is portable in shape but not in token type. S3 (If-None-Match since 2024-08-20, If-Match since
Nov 2024, CopyObject since 2025-10-29), R2, Azure Blob, and MinIO all speak ETag plus If-Match with 412
on failure. GCS does not: ETag preconditions there apply only to reads, and writes use
`x-goog-if-generation-match` with generation 0 meaning create-if-absent. The port is typed on ETag
strings, so GCS needs a token-type abstraction, not just a new backend.

### 2.2 What "53/53 conformance on MinIO" actually means

The commit message on `899fca7` says it. The repo does not support it.

- No conformance runner, no Makefile target, no CI job, no result artifact exists anywhere in
  temporal-s3.
- The same number appears in **driftwood**, the earlier from-scratch server:
  `tests/conformance/baseline/round-g-post.tsv` records 53 PASS, 1 FAIL, 1 HUNG out of 55 features from
  the `temporalio/features` Go corpus, timestamped hours *before* the temporal-s3 work began.
  *Hypothesis, unverified:* the 53 was carried over from there. The denominators differ (53/55 versus a
  claimed 53/53), so an equally consistent reading is a 53-feature subset run against MinIO that all
  passed. Either way the artifact does not exist.
- So the "on MinIO" half is unverifiable from this repo. Treat it as an unrecorded local run.

More important than the provenance is what the corpus **cannot** detect:

| Not covered by the features corpus | Why it matters here |
|---|---|
| Any crash injection mid-write | The prototype's entire defect class is torn multi-PUT writes. A functional suite passes them by construction. |
| Visibility list and count queries | 14 methods, freshly written, unexercised |
| Replication DLQ | 5 methods, unexercised |
| Cluster-metadata membership and expiry | Implemented (expiry-in-value plus read filter plus prune, `cluster_metadata_store.go:52,177-179,234-252`) and never run |
| Namespace lifecycle (create, rename, delete) | Rename is a 3-key atomic operation the prototype cannot do |
| History pagination | The prototype's page token is a numeric index into a freshly recomputed slice |
| Multi-shard behavior | Runs at low shard counts |

**Verdict on the conformance claim: it is evidence that the happy path works and evidence of nothing
else.** Green here was never reachable-red for the bugs that matter.

### 2.3 The correctness cliffs, honestly

Three defects, ranked. Each is verified by reading the code, not inferred.

#### Cliff 1: no fencing at all

```
$ grep -c "RangeID" common/persistence/objstore/execution_store.go
0
```

Cassandra appends `templateUpdateLeaseQuery ... IF range_id = ?` to the *same* `LoggedBatch` as the
mutable-state update and runs `MapExecuteBatchCAS` (`common/persistence/cassandra/mutable_state_store.go:701-714`).
SQL takes a shared lock on the `shards` row inside the transaction. The prototype does neither. Nothing
stops a history host that has lost its shard lease from continuing to write.

This is benign at `NumHistoryShards: 1` in a single process, and fatal in anything else.

#### Cliff 2: history nodes are written after mutable state, which is the wrong order

Cassandra (`common/persistence/cassandra/execution_store.go:110-125`) writes history nodes **first**, as
unconditional blind upserts, and *then* calls `MutableStateStore.UpdateWorkflowExecution`. That ordering
is deliberate. Extra or partial history nodes are the repairable direction: a retry writes the same
`(nodeID, txnID)` idempotently or a higher `txnID`, and the read path follows the prev-txn chain from the
highest `txnID`, shadowing the orphan. `trimHistoryNode` in `common/persistence/execution_manager.go`
compensates exactly this direction.

The prototype inverts it. `UpdateWorkflowExecution` puts the snapshot at
`common/persistence/objstore/execution_store.go:219`, writes the task map at `:236`, and appends history
nodes at `:243` and `:248`. A crash between `:219` and `:243` leaves mutable state whose `NextEventID`
points past events that do not exist. There is no compensation for missing nodes below `NextEventID`.
That is unrecoverable `DataLoss`.

**This is the highest-value three days of work in the whole document.** It is a reordering plus a
read-time chain filter, and it is a prerequisite for every topology below.

#### Cliff 3: the CAS is a self-read, and it checks the wrong predicate

`UpdateWorkflowExecution` reads the snapshot at `:187`, mutates it, and writes it back with
`IfMatch: etag` at `:219`. Because the ETag came from the read it just did, the CAS answers "did anyone
write during my own 20 ms window", not "is my view stale". Meanwhile the actual staleness check is done
in Go against `NextEventID`:

```go
// execution_store.go:204
if mutation.Condition != 0 && env.NextEventID != mutation.Condition {
```

with a comment that explicitly declines to use `DBRecordVersion`, which is the predicate Cassandra uses
(`common/persistence/cassandra/mutable_state_store.go:123`). And the current-run refresh at `:264-274`
is best-effort with a comment tolerating lost writes.

#### Torn-write classification

The distinction that decides severity is **whether Temporal has a timeout-driven retry above the record**.

Unrecoverable:

| Torn at | Result |
|---|---|
| `CreateWorkflowExecution` BrandNew, after the `current_run` PUT (`:130`), before the snapshot PUT (`:133`) | The workflow ID is claimed by a runID with no mutable state. Every retry gets `CurrentWorkflowConditionFailedError` populated from the orphan pointer. **The workflow ID is permanently unusable.** |
| Any create or update, after the snapshot PUT, before `writeHistoryTaskMap` (`:162`, `:236`) | Mutable state records a scheduled workflow task, activity, or timer, but no transfer or timer task exists to dispatch it. Nothing reconciles mutable state against the task queues. **The execution silently stalls forever.** This is precisely the invariant the `LoggedBatch` exists to protect. |
| Any create or update, after tasks, before `appendHistoryNodes` (`:172`, `:243`, `:248`) | `NextEventID` past nonexistent events. See cliff 2. |

Retryable no-op:

| Torn at | Why it is safe |
|---|---|
| `AppendHistoryNodes` alone, before mutable state advances | Idempotent overwrite plus txn-id chaining. This is why Cassandra writes it first. |
| `CreateTasks` on the matching plane | A lost matching task is covered by the workflow-task or activity-task timeout in the history plane, which re-creates it. Matching is a cache of work, not a system of record. |
| Visibility writes | Eventually consistent by construction. |
| Replication tasks | Re-derivable from history. |
| `UpdateShard` | Single-object CAS, atomic by definition. |

**Does rangeID fencing fix any of this? No, and conflating the two is the prototype's core error.**
Fencing prevents interleaving. Temporal already strengthens that further with a per-workflow in-memory
mutable-state lock on the owning history host, so writes to one execution are serial. The failure mode is
never "writer B slipped a PUT between writer A's PUTs". It is **"writer A stopped halfway"**: OOM kill,
pod eviction, context deadline, crash. Fencing is silent on that.

### 2.4 Performance shape, for the record

Read the op counts out of the code, do not estimate them. No `errgroup`, no `go func`, no `sync.WaitGroup`
appears anywhere under `common/persistence/objstore/`, so every one of these is serial.

| Operation | Ops | Where |
|---|---|---|
| `CreateWorkflowExecution` (BrandNew) | 7 PUT | `execution_store.go:130,133`; `execution_tasks.go:79` x3; `history_store.go:115,134` |
| `UpdateWorkflowExecution` | 2 GET + 5 PUT | `execution_store.go:187,219,236,243,264,274`; `execution_tasks.go:79` x2 |
| Timer queue poll | 1 full-prefix LIST + 1 GET per task | `execution_tasks.go:91` |
| Range-complete tasks | 1 full-prefix LIST + 1 DELETE per task | `execution_tasks.go:178` |
| Matching, sync match | **0** | never reaches persistence |
| Matching, backlog | 1 full-prefix LIST + 1 GET per task | `task_store.go:319` |

The `List(prefix)` port has **no start-after cursor** by design
(`blob/store.go:64-70`: "Callers needing pagination should narrow the prefix"), so every queue poll is
O(backlog), not O(page), and every pagination token is a numeric index into a freshly recomputed slice.
S3 has `StartAfter` and `ContinuationToken`; the port simply does not expose them. That is a small,
high-value fix.

---

## 3. What Temporal actually demands of storage

Boiled to primitives, ignoring the ten years of surface area.

### 3.1 The contract

Nine `DataStore` interfaces vended by `DataStoreFactory` (`common/persistence/persistence_interface.go:32`):
`ShardStore` (`:54`), `TaskStore` (`:64`), `FairTaskStore`, `MetadataStore` (`:86`),
`ClusterMetadataStore` (`:102`), `ExecutionStore` (`:116`), `Queue` (`:170`), `QueueV2` (`:809`),
`NexusEndpointStore` (`:188`). Visibility is separately pluggable
(`common/persistence/visibility/store/visibility_store.go:19`). Above them sit Manager wrappers that only
serialize and validate; a backend implements the stores, not the managers.

**One of the nine carries essentially all the difficulty.**

### 3.2 The load-bearing requirement

A **shard-scoped atomic multi-row conditional write**.

Every `ExecutionStore` mutation (`Create`, `Update`, `ConflictResolve`, `Set`, `AddHistoryTasks`) writes,
in one atomic step:

- the mutable-state row `(namespaceID, workflowID, runID)`,
- the current-execution pointer row,
- N history-task rows across up to five categories (transfer, timer, visibility, replication, outbound),

gated on **three simultaneous compare-and-set conditions**:

1. the shard's `range_id` is unchanged (fencing),
2. the execution's `db_record_version` matches (OCC), CASed against `DBRecordVersion - 1`
   (`common/persistence/cassandra/mutable_state_store.go:734`),
3. the current-execution row's `current_run_id` matches (workflow-ID uniqueness).

`ConflictResolveWorkflowExecution` is the widest: up to **three** executions plus the current-run pointer
plus all their tasks, all atomic. `UpdateWorkflowExecution` with continue-as-new spans **two** executions.

In Cassandra every one of those rows lives in a single partition keyed by `shard_id`, so a `LOGGED BATCH`
plus LWT gives atomicity and all three conditions in one round trip. In SQL it is a transaction holding a
shared lock on the `shards` row.

### 3.3 The asymmetry that makes an object-store port possible at all

**Workflow history events are not in that transaction.** `AppendHistoryNodes` runs as separate,
unconditional, blind upserts *before* the mutable-state write in both real backends
(`common/persistence/cassandra/execution_store.go:110-125`, `common/persistence/sql/execution.go:338-348`).
Dangling nodes are repaired at read time by transaction-id chaining plus a `trimHistoryNode` compensation
on conflict.

So the storage contract is genuinely two-tier:

| Tier | Requirement | Object store gives you |
|---|---|---|
| History events | monotonic append, idempotent overwrite | free |
| Mutable state plus tasks plus current-run | real multi-key CAS | per object only |

This asymmetry is the single most important fact for the port. It is also why cliff 2 above is a cliff:
the prototype gets tier 1 for free and then throws the freebie away by writing it last.

### 3.4 The single-writer shard invariant, and why it is load-bearing

`rangeID` is a monotonically increasing per-shard fencing token, acquired by a conditional update and then
carried as a precondition on every subsequent write. `renewRangeLocked`
(`service/history/shard/context_impl.go:1152-1157`) drains every in-flight request *before* bumping it,
and the code says why: "requests are conditioned on rangeID."

Two consequences that simplify everything downstream:

- **No range read inside a transaction is ever needed.** The write path does single-key reads plus N
  writes. There are no phantoms to protect against, because there is only one writer per shard. This is
  the reason shale's inability to `ScanPrefix` inside a CAS transaction does not block anything.
- **`rangeID` does double duty as a task-ID block allocator.** `nextTaskID = rangeID << rangeSizeBits`
  (`service/history/shard/task_key_generator.go:152-154`) pre-allocates 2^20 task IDs per acquisition. A
  fencing token that is not also monotonic-per-acquisition cannot replace it.

Membership is **not** the fence. `ownership.verifyOwnership`
(`service/history/shard/ownership.go:135-149`) is an advisory pre-check that returns `ShardOwnershipLost`
before you even try. The actual fence is always the `rangeID` CAS. Remember this in section 6.

### 3.5 The error taxonomy, which is a correctness surface

Temporal classifies write errors three ways (`service/history/shard/context_impl.go:1386-1432`):

1. **Definitely not committed** (`WorkflowConditionFailedError`, `ConditionFailedError`,
   `ResourceExhausted`, `NotFound`): return, caller retries safely.
2. **Ownership lost**: stop the shard engine.
3. **Unknown** (the `default` branch): re-acquire the shard in the background to obtain a *fresh rangeID*,
   so a subsequent read is authoritative about whether the write landed.

Any new backend must map its errors into these three buckets. Misclassifying bucket 3 as bucket 1
double-applies a workflow task. This is the highest-risk mapping work in any topology.

---

## 4. The gap map

Substrate change under evaluation:

| | prototype (objstore) | proposed (shale) |
|---|---|---|
| unit of write | one object per record | one KV entry per record |
| atomicity | one object (ETag CAS) | one **storage unit** (`Transact`, N keys) |
| ordering | lexicographic `List(prefix)`, no cursor, no bound | byte-ordered `ScanPrefix`, no cursor, no bound |
| fencing | ETag on the object you just read | value read-check on any key in the same unit |
| durability | S3 ack | backend's (slatedb `AwaitDurable`, flush interval) |
| write amplification | O(records) object PUTs | O(1) WAL object per flush window per unit |

Layout convention. Two hash tags carry the design; `ring.ShardKey` hashes only the contents of the first
`{...}`, so everything under one tag lands in one storage unit and is `Transact`-eligible together.

**Every caller-supplied component is escaped, tag contents included**, exactly as the prototype's HEAD
commit does (`899fca7`, `esc()` = `url.PathEscape`, `common/persistence/objstore/keys.go:21`). Two
distinct failures if you drop it. (1) *Ambiguity*: workflow IDs may contain `/`, so `wfID="a/b", runID="c"`
and `wfID="a", runID="b/c"` produce the same `ms/` key and one execution overwrites another. Visibility
search-attribute values are worse: arbitrary user text. (2) *Tag corruption*: shale hashes only "the
contents between the first matching pair of braces" (shale `docs/SPEC.md`) and does **not** validate them,
so a task-queue name containing `}` truncates the tag and two queues can hash to the same unit or to the
wrong one. Co-location by hash tag is the invariant the whole one-`Transact`-per-mutation argument rests
on; it must never take a raw user string. Percent-encoding covers both, since it escapes `/`, `{`, `}`.

```
{h/<cluster>/<shardID>}/...      history plane   (tag = Temporal's history shard)
{q/<nsID>/<type>/<tqName>}/...   matching plane  (tag = one task-queue partition)
{m}/...                          global metadata (namespaces, cluster metadata, nexus)
{v/<nsID>}/...                   visibility      (tag = namespace)
```

Integer encoding is `be8(x)` = 8-byte big-endian of `x ^ (1<<63)`, not the prototype's `%020d` decimal
text. Same ordering, 8 bytes instead of 20, and it does not break on a negative fire time. Note that the
prototype's `fixedWidthTxn` (`common/persistence/objstore/history_store.go:59-71`) already special-cases
negatives and sorts them *after* positives, which is wrong and currently unreachable.

**Is the shard tag legitimate?** Yes. SQL's `history_node` primary key is
`(shard_id, tree_id, branch_id, node_id, txn_id)` and `InternalAppendHistoryNodesRequest` carries
`ShardID` with the comment "Used in sharded data stores to identify which shard to use"
(`common/persistence/persistence_interface.go:549-550`). Cassandra chose `((tree_id), branch_id, node_id, txn_id)`
instead, which is exactly why Cassandra *cannot* put history events in the mutable-state batch and SQL
could have. On shale we take the SQL layout and collect the atomicity Cassandra gave up.

Legend: **Y** = direct primitive. **P** = partial, documented workaround. **N** = missing, needs new shale
API or an out-of-band mechanism.

### 4.1 ShardStore

| Method | Key | Primitive | shale | Cost |
|---|---|---|---|---|
| `GetOrCreateShard` | `{h/c/s}/shard` | read, create-if-absent | Y | `Transact`: `tx.Get` records `ExpectAbsent` on miss, then `tx.Put` |
| `UpdateShard` | `{h/c/s}/shard` | CAS on `range_id` | Y | exactly `IF range_id = ?` |
| `AssertShardOwnership` | `{h/c/s}/shard` | read and compare | Y | free when folded into the caller's `Transact` |

### 4.2 ExecutionStore, mutation path

All five write methods reduce to **one** `Transact` pinned on `{h/c/s}/shard`.

| Method | Keys in one tx | shale | Cost |
|---|---|---|---|
| `CreateWorkflowExecution` | `shard` (read-check only), `cur/<ns>/<wf>`, `ms/<ns>/<wf>/<run>`, `q/<cat>/<be8 fire><be8 taskID>` xN, `hn/<tree>/<branch>/<be8 node><be8i txn>` xM | **Y** | 3 routed Gets + 1 `CommitCAS` carrying 2+N+M write ops. `ExpectAbsent` gives `IF NOT EXISTS`; value read-checks give the rangeID fence and `IF current_run_id = ?` |
| `UpdateWorkflowExecution` | same, plus a whole second execution for continue-as-new | **Y** | the CAN run shares `(nsID, wfID)` so it hashes to the same Temporal shard and the same tag: no cross-shard violation |
| `ConflictResolveWorkflowExecution` | up to three executions plus current-run plus all tasks plus all history | **Y** | all three runs share `wfID`, so one tag. Write set may reach a few hundred ops; see risk R2 |
| `SetWorkflowExecution` | one execution's rows | Y | same tx, no OCC predicate |
| `AddHistoryTasks` | `q/<cat>/...` xN | Y | same tx |
| `DeleteWorkflowExecution` | `ms/...` and optional history | Y | tombstone at R>1, see 4.8 |
| `DeleteCurrentWorkflowExecution` | `cur/<ns>/<wf>` | Y | `tx.Get` + `tx.Delete` in one tx |

The whole thing is one function:

```go
err := kv.Transact(shardTag(cluster, shardID), func(tx *cluster.Tx) error {
    sh, err := tx.Get(shardInfoKey)          // read-check == IF range_id = ?
    if err != nil { return err }
    if rangeIDOf(sh) != req.RangeID { return persistence.ErrShardOwnershipLost }

    ms, err := tx.Get(msKey)                 // read-check == IF db_record_version = ?
    if err != nil { return err }
    if versionOf(ms) != req.Condition { return &persistence.WorkflowConditionFailedError{...} }

    cur, err := tx.Get(curRunKey)            // read-check == IF current_run_id = ?
    if err != nil { return err }
    if runIDOf(cur) != req.PrevRunID { return &persistence.CurrentWorkflowConditionFailedError{...} }

    tx.Put(msKey, msBytes)
    tx.Put(curRunKey, curBytes)
    for _, t := range tasks   { tx.Put(taskKey(t), t.Bytes()) }
    for _, n := range histNodes { tx.Put(nodeKey(n), n.Bytes()) }
    return nil
})
```

That is a faithful translation of the Cassandra `LOGGED BATCH` plus LWT. Three shale constraints to
respect:

- `tx.ScanPrefix` inside a CAS transaction is unsupported (no phantom protection over a value-based read
  set). Fine: per 3.4, the write path never range-reads.
- Read checks compare value *bytes*, so ABA is possible. Immaterial: `db_record_version` and `range_id`
  are monotonic, so an identical byte image implies an identical logical version.
- **`tx.Get(msKey)` costs the wire twice.** `recordRead` (`pkg/cluster/castx.go:332-341`) snapshots the
  observed bytes into the read-check, and a later `Put` of the same key does not retract it, so the commit
  carries both the old and the new mutable state. Immaterial in-process (no RPC, see 4.8), but it is why
  `db_record_version` wants its own small key rather than living inside the mutable-state blob.

### 4.3 ExecutionStore, read and queue path

| Method | Prefix | Primitive | shale | Cost |
|---|---|---|---|---|
| `GetWorkflowExecution` | `{h/c/s}/ms/<ns>/<wf>/<run>` | point read | Y | 1 routed Get |
| `GetCurrentExecution` | `{h/c/s}/cur/<ns>/<wf>` | point read | Y | 1 routed Get |
| `ListConcreteExecutions` | `{h/c/s}/ms/` | prefix scan, paged | **P** | no limit, no start-after. Re-scan and skip until `key > token`, O(offset) per page. Admin path only |
| `GetHistoryTasks` | `{h/c/s}/q/<cat>/` | ordered range scan `[min,max)` with limit | **P** | scan, skip to `>= be8(minFire)`, `Close()` once past max or at `BatchSize`. Correct. **Fatal caveat at R>1: tombstones are in the scan** |
| `CompleteHistoryTask` | one key | delete | Y | 1 op |
| `RangeCompleteHistoryTasks` | `{h/c/s}/q/<cat>/` `[min,max)` | **range delete** | **N** | scan outside a tx, then one `Transact` with N `tx.Delete`. Two RTTs regardless of N, up to the message-size cap. Phantom-safe only because of the single-writer invariant |
| Replication DLQ (5 methods) | `{h/c/s}/dlq/<src>/<be8 taskID>` | as above | P | `IsEmpty` = scan plus immediate `Close()` |

### 4.4 ExecutionStore, history V2

| Method | Key | shale | Cost |
|---|---|---|---|
| `AppendHistoryNodes` | `{h/c/s}/hn/<tree>/<branch>/<be8 node><be8i txn>` | Y | **now co-committed with mutable state**, which is the correctness win. `be8i` = inverted txnID so the highest sorts first, reproducing Cassandra's `CLUSTERING ORDER BY (... txn_id DESC)` |
| `ReadHistoryBranch` | `{h/c/s}/hn/<tree>/<branch>/` | P | same no-cursor tax; page token = last key, skip-to-key on resume |
| `DeleteHistoryNodes` | one key | Y | |
| `ForkHistoryBranch` | `{h/c/s}/ht/<tree>/<branch>` | Y | fork target shares the tree and wfID, so same tag |
| `DeleteHistoryBranch` | `{h/c/s}/hn/<tree>/<branch>/` | **N** | bulk delete of 10^4 to 10^6 nodes: scan plus chunked `Transact`. Scavenger rate |
| `GetHistoryTreeContainingBranch` | `{h/c/s}/ht/<tree>/` | Y | small |
| `GetAllHistoryTreeBranches` | all shards | **N** | genuinely cross-shard. `Cluster.Aggregate` (unordered per-node fan-out). The interface doc already warns "branches may be skipped or duplicated across pages", so unordered is in contract. Scavenger only |

### 4.5 TaskStore (matching)

Tag `{q/<nsID>/<type>/<tqName>}` mirrors SQL's `PRIMARY KEY (range_hash, task_queue_id, task_id)` and the
four `IF range_id = ?` sites in `common/persistence/cassandra/matching_task_store_queue.go:77,90,106,115`.

**Subqueue is part of the key, not an afterthought.** `InternalCreateTasksRequest`'s task carries
`Subqueue int` (`common/persistence/persistence_interface.go:314`), `GetTasks` and `CompleteTasksLessThan`
are both scoped to one subqueue, and the prototype already keys and filters on it
(`common/persistence/objstore/task_store.go:85-86,331,404`). Drop it from the key and reads have to scan
and discard other subqueues, while `CompleteTasksLessThan` range-deletes across all of them. Subqueues are
also the fair-task mechanism, so this is the same segment `FairTaskStore` leans on.

| Method | Key | shale | Cost |
|---|---|---|---|
| `CreateTaskQueue` / `Get` / `Update` / `Delete` | `{q/...}/meta` | Y | version CAS via read-check |
| `CreateTasks` | `{q/...}/t/<subqueue>/<be8 pass><be8 taskID>` xN | **Y** | one `Transact`: `tx.Get(meta)` for the fence plus N `tx.Put`. Matches the Cassandra batch exactly |
| `GetTasks` | `{q/...}/t/<subqueue>/` | P | scans the per-subqueue prefix; same no-cursor tax. In a healthy cluster nearly never hit, because a sync match writes zero rows |
| `CompleteTasksLessThan` | `{q/...}/t/<subqueue>/` `[0,max)` | **N** | scan the per-subqueue prefix plus one batched `Transact` |
| `ListTaskQueue` | across queues | **N** | `Aggregate`. Admin and UI only |
| `GetTaskQueueUserData` / `Update` | `{m}/tqud/<ns>/<tq>` | Y | |
| `ListTaskQueueUserDataEntries`, `GetTaskQueuesByBuildId` | `{m}/tqud/<ns>/`, `{m}/bid/<ns>/<buildID>/` | **P** | tagging by `{m}` puts all user data in one unit: correct but a hot spot. Alternative is tag-per-queue plus `Aggregate`. Reverse-index keys are maintained in the same tx as the user-data write, so they cannot drift |

### 4.6 MetadataStore, ClusterMetadataStore, queues, Nexus

| Method | Key | shale | Cost |
|---|---|---|---|
| `CreateNamespace` | `{m}/ns/id/<id>` + `{m}/ns/name/<name>` | **Y** | one `Transact`, both `ExpectAbsent`. The prototype needs two PUTs and can orphan one index (`metadata_store.go:53-54`) |
| `UpdateNamespace` | both index keys + `{m}/ns/version` | Y | one tx. The global notification version is genuinely global but low rate |
| `RenameNamespace` | delete old name key, write new, update id key | **Y** | 3-key atomic. **This is outright broken without transactions** |
| `DeleteNamespace` / `ByName` | both index keys | Y | |
| `ListNamespaces` | `{m}/ns/id/` | P | cursor tax, low rate |
| `ListClusterMetadata` / `Get` / `Save` / `Delete` | `{m}/cm/<name>` | Y | `SaveClusterMetadata` returns `bool` applied, which is exactly the OCC result |
| `UpsertClusterMembership` / `GetClusterMembers` | `{m}/cmem/<hostID>` | **P** | Cassandra uses a row TTL. **shale has no TTL.** Carry `expiry_ns` in the value, filter on read, physical delete in `PruneClusterMembership`: port the prototype's implementation as-is (`cluster_metadata_store.go:52,177-179,234-252`), it already does this. Dead members are scanned until pruned |
| `PruneClusterMembership` | `{m}/cmem/` | P | scan, filter, one batched `Transact`. Tombstones at R>1 |
| Queue v1 and QueueV2 (all methods) | `{m}/q1/...`, `{m}/q2/...` plus `.../meta` | P | ID from a CAS'd counter in `.../meta`, **co-committed with the message in one tx** (the prototype does two PUTs). Range delete as above |
| `NexusEndpointStore` (4 methods) | `{m}/nx/<id>` + `{m}/nx-version` | Y | table-version read-check in the same tx: the prototype's separate version object can drift |

### 4.7 VisibilityStore

Fourteen methods. Visibility must be tagged per namespace, which means a different unit from the history
shard, which means `ErrCrossShard` if you try to co-commit it. So visibility stays a **second,
non-atomic write**. That is fine and correct: Temporal's visibility is eventually consistent by
construction and has a documented "unsupported operation" precedent.

**The read path is scan-and-evaluate, not an index intersection, and that is deliberate.** The
uncommitted `common/persistence/objstore/visibility_store.go` (977 lines) already implements the whole
`visquery` converter surface as an in-memory predicate tree: `buildPredicate` (`:358`) plus
`ConvertComparisonExpr`, `ConvertRangeExpr`, `ConvertIsExpr`, `BuildAndExpr`/`BuildOrExpr`/`BuildNotExpr`,
`filterVisibilityRecords`, and `sortVisibilityRecords`. Port it verbatim onto a `ScanPrefix` of
`{v/<ns>}/e/`. An equality-index design is strictly weaker and cannot serve the query Temporal actually
sends: `converter.go:186-200` appends a `TemporalNamespaceDivision` predicate to **every** list query, and
when the caller did not filter on it the appended predicate is `IS NULL` (or, for CHASM, `= archetypeID`).
An index keyed on `<attrVal>` has no entry for an absent attribute, so the dominant predicate in the
workload is exactly the one it cannot answer.

Secondary indexes come later, as an optimization, and the first one worth building is on
`(ExecutionStatus, StartTime)` because that is the shape of the UI's default query.

| Method | Key | shale | Cost |
|---|---|---|---|
| `RecordWorkflowExecutionStarted` / `Closed` / `Upsert` | `{v/<ns>}/e/<esc runID>` | Y | one tx per record, 1 write op |
| `ListWorkflowExecutions` / `ListChasmExecutions` | `{v/<ns>}/e/` | P | scan the namespace prefix, evaluate the ported predicate tree, sort, page. O(namespace), which is the cost of correctness here |
| `CountWorkflowExecutions` / `CountChasmExecutions` | same | P | same scan. No aggregates |
| `GetWorkflowExecution` | `{v/<ns>}/e/<esc runID>` | Y | |
| `DeleteWorkflowExecution` | base plus any index keys | Y | read then delete in one tx to re-derive index keys |
| `ValidateCustomSearchAttributes` / `AddSearchAttributes` / `GetIndexName` | `{m}/sa/<esc nsID>` | Y | `AddSearchAttributes` must be idempotent per the interface doc |
| *(later)* status/time index | `{v/<ns>}/i/status/<esc status>/<be8 start><esc runID>` | Y | written in the same tx as the base record, so it cannot drift. Narrows the scan; the predicate tree still runs over the survivors |

### 4.8 The shale gaps, collected

These are prerequisites, not nice-to-haves. Seven of them, ordered by how early they bite. **G1 and G2
are the two that constrain the recommended topology**, and both are stated outright in shale's own source
comments rather than inferred.

| Gap | Bites | Severity |
|---|---|---|
| **G1. `casCommitMu` is one per-node mutex held across the durable owner-local commit** (`pkg/cluster/cas.go:190,310,318`; declared "a coarse per-node lock ... A future refinement could stripe it per shard / per partition" at `pkg/cluster/cluster.go:612-623`) | every `Transact` on the node, across all shards | **High, and it caps the whole design.** No two CAS commits on one node overlap their WAL flushes, so the node-wide strict ceiling is 1/commit-latency, not per-shard. Fix is to stripe the mutex per storage unit |
| **G2. Relaxed durability is structurally unavailable at R=1** (`backends/slate/factory.go:760-783`: "AwaitDurable is: ALWAYS true on the R=1 path, regardless of the operator's RelaxedReplicaDurability setting ... The flag is pinned here, not operator-tunable") | the entire recommended T1-prime topology | **High.** This is not a missing knob, it is a missing topology. Relaxed needs R>=2, which needs more than one shale instance. See 5.5 |
| **G3. No per-call durability knob even at R>=2** | see 5.5 | **Fatal for correctness of signals** once relaxed is in play. `RelaxedReplicaDurability` is a per-backing flag, so the choice is all-strict or all-relaxed |
| **G4. No range-bounded, limited, cursored `ScanPrefix`** | every queue poll, every history read, every list | High. Makes reads O(prefix) at the API level even though the LSM makes them O(blocks) at the storage level |
| **G5. `ScanPrefix` at R>1 returns raw LWW envelope bytes without tombstone filtering; only `Get` decodes** | transfer and timer queues, task queues, DLQ | **Fatal at R>1.** A completed-and-deleted task re-appears on the next scan. Either run the shard plane at R=1 or add a tombstone-filtering scan. Not a T1-prime blocker; it blocks the M4 relaxed configuration |
| **G6. No range delete** | `RangeCompleteHistoryTasks`, `CompleteTasksLessThan`, `DeleteHistoryBranch`, `PruneClusterMembership` | Medium. Workaround is scan plus batched `Transact` at two RTTs |
| **G7. ~4 MiB gRPC ceiling on the CAS wire, and it counts the read set twice** | remote pins only | Medium, and **not** a T1-prime problem. See below |

**On the message-size ceiling, which the first draft got wrong twice.** A `CommitCASRequest` carries, for
every read-check, the full `expected_value` the client observed (`pkg/cluster/castx.go:74`, and
`recordRead` at `:332-341` snapshots the bytes; only a `Get` *after* a `Put` of the same key is exempt).
The sketch in 4.2 does `tx.Get(msKey)` before `tx.Put(msKey, msBytes)`, so the request ships **both** the
old and the new mutable state: roughly 2x the Temporal transaction size. Against Temporal's
`system.transactionSizeLimit` default of 4 MiB (`common/dynamicconfig/constants.go:138-141`) the collision
therefore starts at ~2 MiB of Temporal transaction, not 4 MiB. Second correction: in the recommended
single-process topology the commit takes the in-process fast path (`commitCAS` resolves
`casDesignatedOwner`, and when it is local calls `CommitCASApply` with no RPC, `pkg/cluster/castx.go:47-61`),
so there is no gRPC ceiling in T1-prime at all. The clean fix, worth doing anyway, is to keep
`db_record_version` in its own small key so the OCC read-check never ships a copy of the mutable state.

Also missing and worth naming, though none block this design: no TTL, no secondary indexes, no
watch/CDC, no cross-shard transactions, no monotonic ID allocator, no anti-entropy re-replication of an
under-replicated unit, no admission control on the client surface.

---

## 5. The physics

### 5.1 Primitive costs

Assume an instance in the same region as the bucket, objects 4 to 64 KB, no client-side parallelism
(which is what the prototype does).

| Primitive | S3 Standard | S3 Express One Zone | Cassandra (local NVMe) |
|---|---:|---:|---:|
| small PUT p50 | **30 ms** (est) | **6.4 ms** (measured) | ~1 ms |
| small PUT p99 | ~150 ms (est) | **7 ms** (measured) | ~10 ms |
| small GET p50 | **18 ms** (est) | **3.8 ms** (measured) | ~0.5 ms |
| small GET p99 | 100 to 200 ms (AWS docs) | **4 ms** (measured) | ~5 ms |
| LIST, one page | ~25 ms (est) | ~10 ms (est) | n/a |
| DELETE | ~25 ms (est) | ~6 ms (est) | n/a |

Express numbers are Turso's production measurements (July 2026). The Standard estimate is bracketed:
AWS's own docs quote "roughly 100 to 200 milliseconds" for small objects (a p99-ish framing), and a
published benchmark measured a 500 KiB PUT in eu-north-1 at p50 69.75 ms / p95 101.10 ms / p99 137.23 ms.
Stripping ~470 KB of transfer puts a 4 to 64 KB PUT p50 in the 25 to 40 ms band. **30 ms is an estimate,
not a measurement, and R1 in section 8 is the experiment that replaces it.** The Cassandra column uses
3 ms per transaction rather than 1 ms because Temporal uses LWT (Paxos, 4 round trips) on the shard and
mutable-state rows.

Request pricing, us-east-1:

| | S3 Standard | S3 Express One Zone (post 2025-04-10) |
|---|---:|---:|
| PUT / COPY / POST / **LIST** per 1000 | **$0.005** | **$0.00113** |
| GET per 1000 | **$0.0004** | **$0.00003** |
| DELETE | free | free |
| storage per GB-month | $0.023 | $0.11 |
| upload per GB | $0 | $0.0032 |

Two line items matter more than people expect: **LIST is billed at the PUT rate**, and **DELETE is free**.
The prototype LISTs constantly, so its bill is LIST-shaped, not DELETE-shaped.

**Express is strictly better than Standard on both latency and cost for this workload.** 4.7x faster
PUT, 4.4x cheaper PUT, 13x cheaper GET. It buys that with 4.8x storage cost (irrelevant: this workload is
request-heavy and byte-light), one AZ, directory buckets only, and AWS only. That last one destroys the
"runs on any bucket" pitch, which is the whole motivation for the design.

### 5.1a Self-hosted MinIO, which is the substrate this actually runs on

Everything above is AWS list price against AWS latency. The prototype was developed against MinIO, the
first deployment will be MinIO, and MinIO changes the shape of the answer in one direction and leaves it
alone in the other.

- **Request cost is zero.** Every dollar figure in 5.3 collapses to hardware plus operator time, so the
  *economic* argument for an LSM does not exist here. The *latency* argument survives intact: the
  205-serial-ops fan-out in 5.2 and the O(backlog) queue poll in 2.4 are latency problems wearing a cost
  problem's clothes, and MinIO does not make a serial chain of 205 round trips shorter.
- **Latency is unmeasured, and that is the honest state of it.** The single MinIO datum in either repo is
  shale's `docs/BENCH-v0.5.md`: strict slate `Put` p50 103 ms / p99 109 ms, relaxed p50 0.39 ms at n3-r3.
  Those are **single-key `Put`/`Get`, not `Transact`**, 8 concurrent clients, against loopback MinIO inside
  colima, and the bench doc attributes the 103 ms to "MinIO's own commit fsync inside colima (virtualized
  disk on Apple Silicon)". It is a virtualized-disk artifact, not a property of MinIO and certainly not a
  proxy for S3. Nothing anywhere measures raw blob PUT/GET/LIST against MinIO.
- **So the cut line in 5.4 is an S3 cut line.** On self-hosted MinIO on real disks the request bill is
  zero and the only question left is commit latency. That is precisely R1, and it is a day or two of work.

The rest of section 5 is written against S3 because that is where the numbers can be sourced. Read every
dollar column as "if you point this at AWS", and read every latency column as an estimate until R1 lands.

### 5.2 Per-operation budgets

**Start a workflow** (7 serial PUTs):

| substrate | arithmetic | latency |
|---|---|---:|
| objstore, S3 Standard | 7 x 30 ms | **210 ms** |
| objstore, S3 Express | 7 x 6.4 ms | **45 ms** |
| LSM strict, all 7 keys in one commit | 1 WAL PUT x 30 ms | **~30 ms** |
| LSM strict, 7 separate Puts | 7 x 103 ms (single-key `Put`, loopback MinIO under colima) | ~720 ms |
| LSM relaxed, R>=2 (**not reachable in T1-prime**) | 7 x 0.39 ms (single-key `Put`, n3-r3, same caveat) | **~2.7 ms** |
| Cassandra | 1 LOGGED BATCH + LWT plus 1 unconditional history write | **~4 ms** |

The two LSM rows differ by 24x for the same substrate. **Batching is the whole game.**

Two labels apply to every LSM row in this section. **Relaxed rows are a different deployment:** at R=1
shale pins `AwaitDurable=true` (4.8, G2), so relaxed needs R>=2, which needs more than one shale instance.
**Rows quoting 103 ms are single-key `Put` on colima MinIO** (5.1a), not `Transact` on any object store.

**One workflow-task round trip** (`RecordWorkflowTaskStarted` at 6 ops plus
`RespondWorkflowTaskCompleted` at 7):

| substrate | Respond only | full pair |
|---|---:|---:|
| objstore, Standard | **210 ms** | **390 ms** |
| objstore, Express | 38 ms | 70 ms |
| LSM strict, real S3 (estimate, R1 settles it) | ~30 ms | ~60 ms |
| LSM relaxed, R>=2 (not T1-prime) | ~0.5 ms | ~1 ms |
| Cassandra | ~3 ms | ~6 ms |

**Timer fire**, on a shard holding B = 10,000 pending timer objects, `BatchSize` = 100:

| step | arithmetic | S3 Standard |
|---|---|---:|
| `getHistoryTasks` LIST, full prefix, 1000 keys/page | 10 serial LIST x 25 ms | 250 ms |
| in-memory filter and sort of 10,000 keys | CPU | ~5 ms |
| GET each task body in the page | 100 serial GET x 18 ms | 1,800 ms |
| fire one timer: `UpdateWorkflowExecution` | 7 x 30 ms | 210 ms |
| `rangeCompleteHistoryTasks`: full LIST again plus DELETEs | 10 LIST x 25 + 100 DELETE x 25 | 2,750 ms |
| **per-100 batch overhead, excluding the fires** | 250 + 5 + 1800 + 2750 | **~4.8 s** |
| **per 100 timers, all in** | 4,805 + 100 x 210 | **~25.8 s** |
| **amortized per timer** | (250 + 1800 + 2750)/100 + 210 | **~258 ms** |

| substrate | per timer |
|---|---:|
| objstore, Standard | **~258 ms**, growing linearly with B |
| objstore, Express | ~52 ms, same shape |
| LSM (`ScanPrefix`) | **~3 ms warm**, 20 to 60 ms cold |
| Cassandra | ~2 to 4 ms |

**This is the most important structural difference in the document.** An ordered range scan in an LSM
costs the number of *blocks* touched. In object-per-record it costs the number of *records*, because
every record is a separate HTTP request. A 100-item scan is 1 to 3 block reads against 100 GETs. Two
orders of magnitude, permanently, with no tuning available.

Caveat, and it is shale's fault not the LSM's: `ScanPrefix` has no start bound, no end bound, no limit,
no cursor, so it reproduces "materialize the entire backlog on every poll" at the API level even though
the storage layer is efficient. That is G4 in 4.8, and it is a small fix.

**A workflow with 100 sequential activities.** Four persistence transactions per activity (WFT started 6
ops, WFT completed 7, activity started 5, activity completed 7) = 25 serial ops per activity, plus 7 to
start and 7 to complete = 2,514 serial ops:

| substrate | arithmetic | wall clock in storage |
|---|---|---:|
| objstore, Standard | 2,514 x 30 ms | **75.4 s** |
| objstore, Express | 2,514 x 5.36 ms (60/40 PUT/GET blend) | **13.5 s** |
| LSM strict, real S3 (estimate) | 402 commits x 30 ms | **12.1 s** |
| LSM strict, at the colima-MinIO single-key figure | 402 x 103 ms | 41.4 s |
| LSM relaxed, R>=2 (not T1-prime) | 402 x 0.5 ms | **0.2 s** |
| Cassandra | 402 x 3 ms | **1.2 s** |

**The same workflow with 100 parallel activities**, scheduled from one workflow task. That single
transaction carries 100 transfer tasks and 100 activity-timeout timers:

| substrate | arithmetic | latency |
|---|---|---:|
| objstore, Standard | 205 serial ops x 30 ms (`execution_tasks.go:67-85` is a plain loop) | **6.2 s just to schedule** |
| LSM | 204 keys in one commit = 1 WAL PUT | **~30 ms strict** (0.5 ms relaxed at R>=2) |
| Cassandra | 1 LOGGED BATCH | ~3 ms |

**205x.** That is what one-object-per-record costs when the transaction is wide, and Temporal
transactions are routinely wide. This row is also the one place where the `casCommitMu` serialization in
4.8 does *not* hurt: it is a single commit, so there is nothing to overlap.

### 5.3 Cost per million workflow tasks

One workflow-task round trip = 13 ops = roughly 8 PUT-class (billed at the PUT rate, including LISTs)
and 5 GET-class, ignoring queue-processing overhead, which makes objstore look better than it is.

The objstore rows are per-WFT, so they are throughput-independent. **The LSM rows are not**: the flush
window is fixed, so the WAL PUT rate is fixed and the per-WFT cost falls as throughput rises. An LSM
number without a throughput next to it is meaningless. The headline row is at **1000 WFT/s**.

| substrate | arithmetic | $ per million WFTs |
|---|---|---:|
| objstore, S3 Standard | 8M x $0.005/1000 + 5M x $0.0004/1000 | **$42.00** |
| objstore, S3 Standard, if queue LIST amplification is fully eliminated (estimate, op mix assumed at 5 PUT-class + 5 GET-class) | 5M x $0.005/1000 + 5M x $0.0004/1000 | ~$27.00 |
| objstore, S3 Express | 8M x $0.00113/1000 + 5M x $0.00003/1000 | **$9.19** |
| LSM, Standard, 16 units, 100 ms flush window, **at 1000 WFT/s** | 160 WAL PUT/s x 1000 s x $0.005/1000 | **$0.80** |
| LSM, same, **at 3000 WFT/s** | 160 WAL PUT/s x 333 s x $0.005/1000 | $0.27 |
| LSM, GET class (SST and block reads on a cold read path) | *not counted above; see the note* | **unpriced** |

So the honest headline ratio at 1000 WFT/s is **$42.00 against $0.80, about 52x**. A sub-$0.30 LSM figure
is a 3000-WFT/s figure, not a better design.

*The GET-class row is a real hole.* A cold LSM read is an S3 GET: mutable-state reads on a cache miss,
history reads, every queue scan that misses the block cache. The rows above count WAL PUTs and compaction
and stop there. R1's harness should count total object-store requests of both classes, which turns this
from a hand-wave into a measurement.

At 1000 workflow tasks/sec, one month is 2,592M WFTs. That is **~$109k/month** on objstore Standard at
$42.00, against **~$1500/month** for the Cassandra it replaces and **~$2,070/month** for the LSM. The
prototype's cost floor is additionally set by *shard count, not workload*: 512 shards with 100k sleeping
timers each, polled on the 5-minute `TimerProcessorMaxPollInterval` backstop, costs roughly **$2,300/month
at zero throughput** purely in LISTs, and each poll takes 2.5 seconds.

On self-hosted MinIO every figure in this section is $0. See 5.1a.

### 5.4 The write-latency wall, and which workloads clear it

On S3 Standard one Temporal persistence transaction in the object-per-record design costs 5 to 7
dependent ops at ~30 ms: **150 to 210 ms p50, and 0.5 to 1.5 s p99** (the p99 compounds because the ops
are serial and each has an independent ~150 ms tail).

A single workflow is therefore capped at **1/0.210 = 4.7 persistence transactions/sec**, hence
**~1.2 activities/sec**. No hardware or concurrency changes this, because Temporal serializes
transactions per workflow behind the mutable-state lock. On Express the cap is ~26 txn/s and ~6.5
activities/sec. On Cassandra it is ~333 txn/s and ~83 activities/sec.

**Class (a): long-running, low-frequency orchestration.** Nightly ETL with 20 steps. Human-approval
workflows that sleep for days. Infrastructure provisioning sagas with 10 activities each doing 30 s of
real work. Monthly billing runs. Data-pipeline DAGs.

210 ms against a 30 s step is **0.7%**. Against a 3-day sleep it is **0.00008%**. Completely irrelevant.
This is not a niche: it is a large fraction of what Temporal is actually used for, and it is the entire
use case for a self-hosted single-node deployment.

**Class (b): high-throughput or low-latency.** Workflow-as-synchronous-request-handler (Temporal's own
"workflow as an API endpoint" pattern, target p99 under 100 ms). Fan-out of 10,000 parallel activities.
Order processing at 1000 workflows/sec. Anything user-facing.

Fatal on three independent axes: the per-transaction floor exceeds the entire end-to-end budget; the
per-workflow ceiling of 4.7 txn/s means a 10,000-activity fan-out cannot even be scheduled quickly (205
serial ops is 6.2 s before any activity starts); and on a metered bucket the request bill is ~$109k/month.

**A fourth axis applies to the LSM design too, and it is new.** Because shale's owner-side CAS holds one
per-node mutex across the durable commit (4.8, G1), a node's strict `Transact` ceiling is
**1/commit-latency across every shard it owns**, not per shard. At a 100 ms strict commit that is under 10
transactions/sec for the whole node. Class (b) is therefore out of reach on the LSM path as well until
`casCommitMu` is striped, which is why that stripe is priced as a prerequisite in section 7 rather than
filed as a future refinement.

**The cut line:** object storage as the direct record store is fine when each workflow step does more
than about 2 seconds of real work and the cluster runs fewer than about 100 workflow tasks/sec. Above
either threshold it is the wrong substrate, and the failure is not gradual. On self-hosted MinIO the cost
half of that cut line disappears and the latency half is unmeasured (5.1a); R1 decides where the line sits.

### 5.5 The durability knob, which is a correctness requirement not a tuning option

Relaxed durability (ack at memtable, flush in background) is safe for *replay-recoverable* state and
unsafe for *externally-originated* state. If the engine loses 100 ms of acknowledged writes:

| Lost write | Safe? | Why |
|---|---|---|
| Workflow-task result | **Yes** | the workflow replays deterministically from history and reproduces it |
| Activity dispatch | **Yes** | Temporal's activity contract is already at-least-once; callers must already tolerate a double execution |
| **Signal** | **No** | there is no replay that recovers it. The sender got an ack and the event never existed |
| **StartWorkflowExecution, SignalWithStart** | **No** | same |

So the correct shape is a per-call durability knob: relaxed for the internal engine loop, strict for the
API surface that accepted an external ack.

**Two separate things block that, and the first one is bigger than a missing knob.** At R=1 relaxed
durability does not exist to be tuned: `backends/slate/factory.go:760-783` pins `AwaitDurable=true` on the
R=1 path "regardless of the operator's RelaxedReplicaDurability setting", and shale's SPEC states the
reason plainly, that "R=1 + `AwaitDurable=false` has NO durability guarantee" because there is no peer
memtable to catch the un-flushed write. So **the recommended T1-prime topology is strict-only by
construction**, and every relaxed row in 5.2 belongs to a different deployment. Reaching relaxed at all
means R>=2, which means more than one shale instance. *Then* the second problem applies:
`RelaxedReplicaDurability` is a per-backing flag, so even at R>=2 the choice is all-strict or all-relaxed,
and signals need a strict path. Those are G2 and G3 in 4.8, in that order.

Framing note on R=2 that is easy to get wrong: at R=2 the second replica is **not** redundant with S3's
erasure coding. It exists to cover slatedb's acked-but-unflushed window. shale's own spec already
mandates pairing a fast-ack backend with `ReplicationFactor >= 2` in production, and that mandate is
exactly this argument.

### 5.6 The tricks, ranked

| Trick | Buys | Costs | Weakens | Verdict |
|---|---|---|---|---|
| **Wide-transaction batching** (N logical writes in 1 commit) | 210 ms to ~30 ms; 205x on wide transactions; ~52x on cost at 1000 WFT/s | you need an index, a manifest, and compaction, i.e. **you have re-invented slatedb** | nothing | **Use slatedb. Do not grow an LSM inside the persistence plugin.** This is per-transaction, needs no concurrency, and is the trick that actually pays today |
| **Group commit across *concurrent* transactions** | in principle, amortizes the fixed PUT across every in-flight commit on a node | a shale change to `casCommitMu` plus recovery demultiplexing by shard | blast-radius isolation between shards sharing a unit | **Not available on shale's CAS path today.** `casCommitMu` is one per-node mutex held across the durable owner-local commit (`pkg/cluster/cas.go:190,310,318`), so concurrent `Transact` calls cannot overlap their flushes at all. The node-wide strict ceiling is 1/commit-latency, and 512 shards over 8 units does **not** fold 64 shards' writes into one flush. Two prerequisites before this row means anything: stripe `casCommitMu` per storage unit, and confirm slatedb's WAL flush batches concurrent `CommitWithOptions` calls at all. `BENCH-v0.5` cannot answer the second: 8 concurrent clients at p50 103 ms gave 77.2 put/s, exactly 8/0.103, so batching and perfect parallelism are indistinguishable in it |
| **S3 Express One Zone** | 4.7x latency, 4.4x cheaper PUTs, 13x cheaper GETs | one AZ; directory buckets; AWS only | **durability**, and the "any bucket" pitch | **Yes, tiered:** hot WAL on Express, compacted SSTs on Standard. That is Turso's architecture |
| **A local durable log (NVMe/EBS)** | commit latency to fsync, 0.1 to 1 ms; makes class (b) possible | local state | **the architectural claim**, not durability, provided the log is replicated | Yes for production, no for the pitch. This is the trick everybody else already shipped |
| **Relaxed durability with a bounded window** | shale's bench: 77 put/s strict to 11,661 put/s relaxed at n3-r3, **151x** (single-key `Put`, 5.1a) | acked writes live in R memtables until flush; **requires R>=2, so it is not a single-instance option** | durability to "in R process memories, converging within ~100 ms" | **Yes once you are at R>=2**, plus a strict path for signals and starts (5.5). Unreachable in T1-prime |
| **Read caching of hot mutable state** | Temporal already does this; the history service keeps a per-shard mutable-state cache | none | none | **The prototype throws it away.** `readSnapshot` runs on *every* update (`execution_store.go:187`) because it needs the ETag, and Temporal's cache holds decoded state, not ETags. Switching the predicate to `DBRecordVersion` removes 2 of 7 ops for free, and on shale it also stops the CAS read-check shipping a copy of the mutable state (4.8) |

**The verdict, plainly.** Use slatedb, because `Transact` collapses a wide multi-row transaction into one
durable commit, which is worth 205x on the fan-out case with no concurrency required. Do **not** plan
around cross-transaction group commit: shale forecloses it on the CAS path today, and the fix is a shale
change, not a Temporal one.

---

## 6. Three topologies

### T1: temporal-in-a-box

**What you build:** almost nothing. Fork `temporaltest/internal.LiteServer` into a `temporalbox` package,
replace the `sqliteplugin` config block with the objstore (or shalestore) factory, keep everything else.
`temporal.Server` is already an interface with `Start`/`Stop` (`temporal/server.go:15-20`). `LiteServer`
already runs all four services in one process with `NumHistoryShards: 1` and static membership. The plugin
hooks are ServerOptions: `WithCustomDataStoreFactory` (`temporal/server_option.go:144`),
`WithCustomVisibilityStoreFactory` (`:152`), `WithStaticHosts` (`:64`).

It is already a Go library. The residual is that the SDK dials a real loopback TCP port; going fully
in-process means a `grpc.WithContextDialer` over a `bufconn` in
`client.Options.ConnectionOptions.DialOptions`. *Hypothesis, unverified: I did not confirm SDK v1.41.1
exposes that cleanly.*

**What you reuse:** everything. All four services, the whole SDK ecosystem, the UI, versioning, child
workflows, schedules, Nexus, and the `temporalio/features` corpus as a conformance oracle. This is T1's
entire argument.

**Coordination model:** none. One shard, one host, `staticResolver.Lookup` returns the only entry. The
rangeID lease exists and has no contender.

**Failure model:** no split brain is possible. On restart the process re-acquires shard 1, bumps rangeID
via `renewRangeLocked`, reloads mutable state. The exposure is **entirely** the partial-write window
inside one `UpdateWorkflowExecution` (section 2.3). Both P0 classes are fixed by the same two changes:
write history first, write tasks last, and add the read-time txn-id chain filter. That does not give
atomicity, but it converts silent corruption into retryable duplication, which is the difference between
a demo and a system.

**Which prototype gaps bite:**

| Prototype gap | Bites in T1? | Bites in T2? |
|---|---|---|
| Never reads `request.RangeID` | **No.** One process, one shard, nothing to fence | **Fatal** |
| Self-read ETag CAS instead of `DBRecordVersion` | No | **Fatal** |
| History nodes written after mutable state | **Yes, on crash** | **Yes, on crash** |
| 4 to 10 independent PUTs, no atomicity | **Yes, on crash** | **Yes, on crash** |
| `List` has no start-after cursor | Performance only | Performance only |

**Trap:** `NumHistoryShards` is immutable after first boot. `temporal/fx.go:831-838` reads the persisted
`HistoryShardCount` and overrides your config. **Pick the shard count on day one**, not later.

**Effort:**

| Item | Weeks |
|---|---:|
| `temporalbox` library wrapper over LiteServer plus objstore | 0.5 |
| Fix history/mutable-state ordering plus txn-id chain read filter | 0.5 |
| Add start-after cursor to `blob.Store.List` and thread it (S3 already has `StartAfter`) | 1 |
| Wire and run the features corpus as a CI gate | 1 |
| **Working "Temporal on a bucket, one process"** | **~3** |
| Intent log plus reconciler for crash-atomic multi-record updates | 4 to 6 |
| **Something you would trust with `kill -9`** | **~8 to 10** |

**The 4-to-6-week line is the honest one, and it is the argument for the whole recommendation.** Making
one-object-per-record crash-atomic means building a transaction log on top of blob CAS, which is
reinventing the thing you were trying to avoid. Do not build it. Put an LSM underneath instead and take
`Transact` off the shelf: the multi-key transaction stops being your problem and becomes a dependency's.
That is T1-prime in section 7.

### T2: embedded Temporal, multi-node, coordinating through shale

**What you build:** a `shalering` membership adapter, a `shalestore` persistence plugin, a shale-to-Temporal
error classifier, and a one-line seam in `temporal/fx.go`.

**The membership replacement is easy and buys almost nothing.** `membership.ServiceResolver` is 10 methods
(`common/membership/interfaces.go:69-95`), of which one matters: `Lookup(key string) (HostInfo, error)`.
shale has the exact primitive in `ring.Ring.LocateKey`. The reference implementation to copy is
`static.staticResolver`, about 110 lines. The seam is `temporal/fx.go:379-381`, which picks
`ringpop.MembershipModule` unless `StaticServiceHosts` is set. There is no `WithCustomMembership` option
today; adding one is 3 lines plus an fx module. One ring serves both routing needs, since history routes
by shard ID and matching by partition key.

The whole benefit is that you delete ringpop's separate gossip port and run one SWIM instead of two.
**It does not buy you fencing, because per 3.4 Temporal does not use membership for fencing.**

#### The shard-ownership mapping, argued through

Two candidate mappings.

**Mapping A: shard = shale key group (co-location by hash tag). Leases stay nested.** This is the layout
in section 4. Everything in Temporal shard 42 carries the tag `{h/c/42}`, hashes to one shard key,
therefore one storage unit, therefore one owner node, therefore one `Transact`.

Under mapping A there are still **two leases**:

- **shale's unit epoch** fences the *storage handle*: two processes must not both write unit U's slatedb.
- **Temporal's rangeID** fences the *engine*: two history engines must not both mutate shard 42.

These are not redundant, and this is the crux. Even when one node holds both, rangeID is still required,
because Temporal's *in-memory* state must be invalidated on ownership change: the pre-allocated task-ID
block (2^20 IDs per acquisition), the max read level, the queue ack levels, the mutable-state cache.
shale has no way to tell Temporal any of that.

**Mapping B: shard = shale unit, 1:1, collapse the leases.** Set `NumHistoryShards == UnitCount`, let unit
ownership be shard ownership and the epoch be the rangeID. Both are monotonic per-unit fencing tokens, so
it looks clean. It fails on four counts:

1. **Cardinality.** Temporal wants 512 to 4096 shards. 4096 units means 4096 slatedb instances: 4096
   manifests, WAL streams, and compaction loops, with observed mount windows of 17 to 21 seconds each on
   real object storage. That is not a system.
2. **Invert it and cap shards at a sane unit count (8 to 32), and you have capped the history service's
   parallelism and its lock granularity.** One hot shard serializes behind `ContextImpl`'s rwLock. And
   `NumHistoryShards` is immutable forever, so this is a permanent choice made at first boot.
3. **Mutability mismatch.** Temporal's shard count is fixed for the life of the cluster. shale's
   `UnitCount` is a power of two designed to change online by doubling and halving. The two sit on
   opposite sides of the mutability line. You can get exact *containment* (4096 shards over 8 units, both
   powers of two, so a unit handoff moves a clean shard set) but never equality.
4. **The epoch is not an ID allocator.** rangeID does double duty as the task-ID block allocator. shale's
   epoch is an opaque fence, so you would bolt block allocation on top anyway and be back to two tokens.

**Why the leases cannot collapse even in principle.** shale's handoff is **overlapping by design**:
pending-ranges union routing dual-writes to current and pending owners and reads the union, which is
precisely the feature that makes a 17-to-21-second mount survivable. Temporal's handoff is **exclusive by
design**: `renewRangeLocked` drains every in-flight request *before* bumping rangeID, and the code says
why.

Overlap underneath exclusive is *safe*. During shale's overlap window both A and B can physically reach
storage, but only one holds the current rangeID, so the loser's read-check fails and it gets
`ShardOwnershipLostError`, which the shard context already handles by stopping the shard
(`service/history/shard/context_impl.go:1417-1421`). But it means **shale's lease is exactly as advisory
as ringpop's**, and the temptation to say "shale's lease is authoritative, drop the rangeID check" is a
correctness bug waiting to happen.

**Conclusion on the mapping: mapping A works and is the right layout, mapping B is a trap, and under
either one you keep two nested leases. There is no collapse available.**

#### The error taxonomy is the highest-risk work in T2

Map shale's small taxonomy (`ErrAcquiring`, `ErrNotFound`, `ErrCrossShard`, `ErrClosed`, `ErrFenced`,
`ErrCASConflict`) onto Temporal's three buckets from 3.5:

- `ErrCASConflict` to bucket 1 (clean, the read-check failed).
- `ErrFenced` and `ErrAcquiring` to **bucket 3, not bucket 1.** The write may or may not have landed.
- **Committed-under-replicated is a SUCCESS that must never be retried.** If a wrapper helpfully retries
  it, you double-apply a workflow task.
- `ErrTransactRetriesExhausted` after a reshard to bucket 3.

Budget two weeks and treat it as the riskiest two weeks in the plan.

#### The one real prize, and its size

If Temporal's membership *is* shale's ring, the node owning history shard 42 is by construction the node
with the containing unit mounted. Every persistence write becomes owner-local, and shale already has the
short-circuit (`OwnsCASPin`). That removes one network hop from the hot path.

| Backend | Ops | Serial latency |
|---|---|---:|
| objstore-direct, S3 Standard | 2 GET + 5 PUT | ~140 ms |
| objstore-direct, S3 Express | 2 GET + 5 PUT | ~45 ms |
| shale relaxed, R>=2 (single-key `Put` at n3-r3, 0.39 ms, colima MinIO) | 1 local `Transact` | sub-ms, not durable at ack |
| shale strict `AwaitDurable` (single-key `Put`, 103 ms, colima MinIO) | 1 local `Transact` | **unmeasured** |

Both shale rows are single-key `Put` benchmarks, not `Transact` (5.1a). A `Transact` is strictly more
work: N routed Gets before the commit, plus the node-wide `casCommitMu` the plain `Put` path never takes.
R1 produces the real number.

Honest reading: **shale's win is not latency, it is atomicity plus wide-transaction batching.** Strict p50
is likely in the same band as the objstore serial chain, but it covers the whole multi-row update in one
durable flush. What it does *not* currently give you is amortization across shards sharing a unit: the
per-node `casCommitMu` (4.8, G1) serializes those commits, so "512 shards over 8 units folds 64 shards'
writes into one flush" is false until that mutex is striped.

#### Failure model

1. memberlist detects the departure; shale re-places units on survivors and mounts them (17 to 21 s).
2. Temporal shard controllers on survivors get a `ChangedEvent`, `verifyOwnership` points at them,
   `acquireShards` runs, `renewRangeLocked` bumps rangeID per shard.
3. Between (1) and (2), writes return shale `ErrAcquiring`, which must classify as retryable-unknown, not
   as shard-lost.
4. Recovery time is `max(shale mount ~17-21 s, config.AcquireShardInterval)`.

The genuinely interesting case is **partition, not crash**: a partitioned node still believes it owns
shard 42, its writes route to a unit owner that may have moved, and the rangeID read-check is the only
thing preventing corruption.

#### Effort

| Item | Weeks |
|---|---:|
| `shalering` membership adapter | 1 to 2 |
| fx seam plus `WithMembershipModule` | 0.5 |
| `shalestore` ExecutionStore on `Transact` | 5 to 8 |
| Other 8 DataStores on shale KV | 3 to 4 |
| Error taxonomy mapping | 2 |
| Shard/unit containment plus placement driver | 2 |
| Visibility on `ScanPrefix` plus the ported predicate evaluator | 1 |
| shale gaps: tombstone-filtering scan (G5), message-size ceiling (G7), striped `casCommitMu` (G1) | 2 to 3 |
| **Total** | **16 to 23**, plus T1's crash-ordering fixes underneath |

**Verdict on T2: skip it.** 16 to 23 weeks buys one deleted gossip port, one saved network hop, and two
nested leases you cannot collapse. Everything valuable in it (the `shalestore` plugin, the key layout, the
error classifier) is also needed by T1-prime and T3, and those need it *without* the membership adapter,
the placement driver, or the containment arithmetic.

### T3: a new engine

**The thesis.** Temporal's storage contract is complicated because Temporal has ten years of features. The
*durable execution primitive* is not complicated. Build it directly on shale's `Transact` and the entire
correctness argument compresses to one sentence:

> A workflow task is one `Transact` on the run's shard key, and everything crossing a shard key is an
> idempotent async dispatch from an intent written inside that same transaction.

That is roughly 20 lines of invariant instead of forty pages, and it is the actual payoff. It is also the
only one of the three topologies that is genuinely "similar to slatedb": a library, one durable
dependency, no separate cluster.

**Minimum viable core**, in order of how much it matters:

1. Event-sourced history plus deterministic replay.
2. Workflow-task dispatch: hand the worker the history, take back commands.
3. Activity dispatch, at-least-once, with timeouts and heartbeats.
4. Timers.

Signals are a fifth that is nearly free once you have (1). Queries are a sixth that is nearly free once
you have (2).

```
pkg/durable/
  domain/          # pure, no I/O, stdlib plus protos only
    run.go         # Run aggregate root
    event.go       # Event value object over temporal.api.history.v1
    command.go     # Command value object from the worker
    decide.go      # the Decider: the engine core
    task.go        # WorkflowTask / ActivityTask / Timer value objects
  store/
    port.go        # the Store port
    memstore/      # in-memory, for decider tests and crash-injection tests
    shalestore/    # shale KV plus Transact
  engine/          # application services (WorkflowService, TaskService)
  matcher/         # in-memory long-poll rendezvous plus persistent backlog
  server/          # optional: temporal.api.workflowservice.v1 gRPC facade
```

**The strongest datum in this document: the operator has already built most of a version of this once.**
driftwood scored 53 PASS / 1 FAIL / 1 HUNG on the 55-feature `temporalio/features` Go corpus. That is
direct evidence that a from-scratch engine speaking the Temporal wire protocol keeps the entire SDK
ecosystem, which is what makes T3 viable rather than academic.

**Why T3 is not a rewrite from zero:** it shares the key layout in the appendix with T1-prime, shares the
`shalestore` code, and shares the crash harness. The difference is what sits above the store.

**Effort: 8 to 12 weeks for the core on top of an existing `shalestore`**, assuming the gRPC facade
targets the subset of `workflowservice` the features corpus exercises. Without a working `shalestore`
underneath, add the 8 to 12 weeks from T2's persistence rows.

---

## 7. Recommendation

**One path: T1-prime, then T3. Skip T2.**

T1-prime is: **the unmodified Temporal server, single process, with a `shalestore` persistence plugin
running shale embedded in-process at R=1, slatedb backend, bucket as the only durable dependency.**

**Say the durability consequence out loud, because it is structural and permanent for this topology.** At
R=1 shale pins `AwaitDurable=true` (`backends/slate/factory.go:760-783`). T1-prime therefore pays a
synchronous WAL flush on **every** persistence transaction, with no relaxed mode available at any price,
and shale's per-node `casCommitMu` means those flushes do not overlap. That is the correct trade for the
target workload: class (a) in 5.4 does not notice a 100 ms commit, and a single-process deployment with no
peer memtable has no honest way to offer relaxed durability anyway. It does mean two things must be said
plainly rather than discovered: T1-prime's throughput ceiling is one node's 1/commit-latency, and the
relaxed numbers in section 5 belong to a different deployment (M4's, which is R>=2 and multi-instance).

Why this sequencing rather than any other:

1. **The prototype's blocker is atomicity, and shale's `Transact` is exactly that primitive.** Continuing
   on ETag-per-object means building an intent log on blob CAS, which is 4 to 6 weeks of reinventing an
   LSM badly.
2. **The full Temporal server is the only real test oracle available.** The features corpus, the SDKs, the
   replay tests, the UI. A from-scratch engine cannot borrow any of that until it is far enough along to
   answer gRPC. Validate the storage layer against the hard oracle first.
3. **T3 reuses every line of it.** Same key layout, same `Transact` shapes, same crash harness, same
   error classifier. The only thing thrown away when you move to T3 is the Temporal server itself, and
   that is a decision you can defer until the measurements in M4 are in.
4. **T2's unique work (membership adapter, placement driver, containment arithmetic, overlap-window
   reasoning) is exactly the work neither T1-prime nor T3 needs.** It is the only part that is
   throw-away, and it is 3.5 to 4.5 weeks of it out of T2's 16 to 23.

### Milestones

Each is a demonstrable capability plus the test that proves it.

---

**M0. Crash truth. (1 week)**

*Capability:* a deterministic crash-injection harness that can kill the process at any write boundary in
the persistence plugin and then assert what the system does on restart.

*Acceptance test:* a `blob.Store` decorator that fails the Nth write with a panic, driving five scenarios
(start, signal, activity complete, continue-as-new, timer fire) against every write point in
`CreateWorkflowExecution` and `UpdateWorkflowExecution`. Output is a table: injection point, restart
outcome, recoverable yes/no.

*Falsifiable prediction, to prove the harness is not vacuous:* the three P0s from 2.3 fire (orphan
current-run pointer, missing dispatch tasks, mutable state past nonexistent history), and nothing else
does. If the harness reports all-green, the harness is broken, not the prototype.

---

**M1. Order and cursor. (1 week)**

*Capability:* the objstore prototype writes history first and tasks last, and `blob.Store.List` takes a
start-after cursor threaded through every call site.

*Acceptance test:* M0 shows the "mutable state past nonexistent history" class gone. A synthetic shard
with 100k pending timers drops from 10 serial LISTs per poll to 1. The wall-clock claim is a ratio, not an
absolute: measure the before number on the target MinIO first, then require a 10x drop.

*Note:* this is work on the prototype that is thrown away when M2 lands. Do it anyway: it is 1 week, it
takes T1 from "demo" to "does not silently corrupt", and M0 needs a subject.

---

**M2. `shalestore`, single node. (11 to 16 weeks)**

*Capability:* all nine `DataStore` interfaces plus visibility, on shale embedded in-process, R=1, slatedb
backend against a bucket, using the key layout in the appendix. `ExecutionStore` mutations are one
`Transact` each.

*Priced from its own line items*, which are the persistence rows of T2's table in section 6 minus the
membership work T1-prime does not need:

| Item | Weeks |
|---|---:|
| `shalestore` ExecutionStore on `Transact` | 5 to 8 |
| Other 8 DataStores on shale KV | 3 to 4 |
| Visibility: port the existing predicate evaluator onto `ScanPrefix` (4.7) | 1 |
| Error taxonomy mapping | 2 |
| **Total** | **11 to 16** |

*Acceptance test:* three gates, all required.
- The `temporalio/features` Go corpus at 53/55 or better, run in CI with the result artifact committed.
- **M0's crash harness reports zero unrecoverable states at every injection point.** This is the gate
  objstore structurally cannot pass, and it is the whole reason for M2.
- **A visibility conformance suite**, because visibility is the one subsystem section 2.2 names as
  entirely unexercised and the features corpus does not touch it. Minimum: every operator the ported
  converter supports (`=`, `!=`, `>`/`<`/ranges, `IN`, `BETWEEN`, `STARTS_WITH`, `IS NULL`/`IS NOT NULL`,
  `AND`/`OR`/`NOT`), plus the implicit `TemporalNamespaceDivision IS NULL` predicate, plus `ORDER BY` and
  paging across a page boundary, plus `Count`.

*Prerequisites in shale (see 4.8):* range-bounded and limited `ScanPrefix`, and striping `casCommitMu` per
storage unit if M4's throughput target is anything above single-digit transactions/sec/node. Tombstone
filtering is not needed at R=1, the gRPC message ceiling does not apply on the in-process fast path, and
relaxed durability is not reachable at R=1 at all.

---

**M3. `temporalbox`. (2 weeks)**

*Capability:* `box, err := temporalbox.Open(ctx, bucketURL)` returns a running four-service Temporal in
one process, plus a `client.Client`.

*Acceptance test:* a Go test that starts a workflow, `SIGKILL`s the process mid-activity, reopens the box
against the same bucket, and observes the workflow resume and complete. Plus the same test with a fresh
empty bucket to prove bootstrap.

---

**M4. The physics gate. (2 to 3 weeks)**

*Capability:* measured numbers replacing every estimate in section 5.

*Acceptance test:* a published table with measured p50/p99 and $/million for the six operations in 5.2,
across three substrates (self-hosted MinIO, S3 Standard same-region, S3 Express same-region), with
per-node **concurrent** transaction throughput alongside single-commit p50.

**M4 cannot run on the deployment M2 builds, and that is deliberate.** The strict rows run on M2's
single-process R=1 box. The relaxed rows need R>=2, which means a three-node shale cluster with the
Temporal server pointed at it: a second configuration, not a flag. Budget the extra config inside M4's 2
to 3 weeks and treat "does the strict single-process box hold up" and "what does relaxed buy at R>=2" as
two separate questions with two separate answers. If only the first matters to you, cut M4 to the strict
half and skip the cluster.

*This is a decision point, not just a measurement.* If strict-durability commit p50 on real S3 exceeds
about 60 ms, the per-workflow ceiling is under 17 txn/s and the honest answer for class-(b) workloads is
"no", which then goes in the README rather than being discovered by a user.

*Also requires:* striped `casCommitMu` (otherwise the node-wide number is 1/commit-latency and the
concurrency column is flat), and the shale per-call durability knob, because the relaxed row is
meaningless without a strict path for signals and starts.

---

**M5. T3, the engine. (8 to 12 weeks, gated on M4)**

*Capability:* `pkg/durable`, a library implementing the four-item core from section 6, on the same
`shalestore` key layout, with an optional `workflowservice` gRPC facade.

*Acceptance test:* the same three gates as M2 (features corpus, crash harness, visibility suite) plus a
fourth: the domain package compiles with no imports outside stdlib and protos, and `decide.go` has 100%
branch coverage from pure table tests.

---

### Effort summary

| Phase | Weeks | Cumulative | Delivers |
|---|---:|---:|---|
| M0 | 1 | 1 | truth about the current prototype |
| M1 | 1 | 2 | prototype stops silently corrupting |
| M2 | 11 to 16 | 13 to 18 | crash-atomic Temporal on a bucket |
| M3 | 2 | 15 to 20 | `temporalbox` library, restart-proven |
| M4 | 2 to 3 | 17 to 23 | measured physics, honest README |
| **T1-prime complete** | | **17 to 23** | |
| M5 | 8 to 12 | 25 to 35 | the embedded engine, reading (b) answered |

Add 3 to 5 weeks of shale-side work, which can run in parallel with M0 and M1: cursored and range-bounded
`ScanPrefix`, striped `casCommitMu`, range delete, and (only if M4's relaxed half is in scope) the
per-call durability knob and the tombstone-filtering scan.

### Named out of scope

Four things this document does not cover, named so nobody assumes they are handled. None changes the key
layout, which is why they are deferrable rather than missing.

- **Archival.** A separate provider interface from persistence, and a bucket-based deployment arguably
  wants it off entirely since history already lives in the bucket. Decide at M3.
- **Retention and deletion.** `DeleteWorkflowExecution` plus `DeleteHistoryBranch` is the mechanism; 4.4
  flags bulk history delete as G6 with no range delete behind it. Scavenger rate against `Transact`-chunked
  deletes is unmeasured.
- **Key-schema versioning.** Temporal owns the value envelopes' evolution; this design owns the **key**
  layout, and nothing versions it. Write `{m}/schema` in M2, before the first bucket exists. Retrofitting a
  version marker onto populated buckets is far worse than writing it on day one.
- **Upgrade and migration.** There is no path from an objstore-layout bucket to a shalestore bucket, and
  this document does not propose building one: M1's prototype fixes are explicitly throwaway.

---

## 8. Risks and unknowns

### What would kill this

**K1. The strict-durability commit floor is too high, and it is currently unmeasured on any object
store.** Nothing in either repo measures a `Transact` with `AwaitDurable=true`: the 103 ms figure quoted
throughout section 5 is a single-key `Put` against loopback MinIO inside colima, which the bench doc itself
attributes to the guest's commit fsync on a virtualized disk. If the real number lands at 100 ms rather
than 30 ms, the per-workflow ceiling is 10 txn/s and 2.5 activities/sec, worse than the objstore prototype
on Express. Everything then depends on the relaxed path, which needs R>=2 and therefore is not available in
the recommended topology. **Settled by R1 below, in a day or two, before M2 starts.**

**K1b. The node-wide serialization makes K1 worse than per-shard reasoning suggests.** `casCommitMu` is one
per-node mutex held across the durable commit, so K1's number is not the per-shard cost, it is the whole
node's transaction period. A 100 ms commit is under 10 transactions/sec for every shard the node owns,
combined. Striping the mutex per storage unit is the fix and it is shale-side work, not Temporal-side.
**R1 must therefore measure concurrent `Transact` throughput, not only single-commit p50.**

**K2. slatedb's single-writer constraint pins this to single-node-plus-failover.** If shale mount windows
stay at 17 to 21 seconds, shard handoff takes 20+ seconds, which is worse than Temporal's normal
sub-second shard movement. That is survivable if named up front, and fatal if discovered by a user.

**K3. The features corpus is a vacuous oracle and stays vacuous.** M0 exists specifically to prevent
this. If M0's harness cannot produce a red result on the known-broken prototype, the whole acceptance
story for M2 is worthless.

**K4. `NumHistoryShards` immutability.** Picked wrong at first boot, this is unfixable for the life of a
cluster. Decide it in M2 and document it in M3. Recommendation: 512 Temporal shards over 8 shale units,
both powers of two.

**K5. Express-or-nothing.** If M4 shows the design is only viable on S3 Express, the "runs on any bucket"
pitch is dead and the honest positioning becomes "runs on any bucket for class-(a) workloads, needs
Express for anything else". That is still a good product, but it is a different one.

### The experiments, each cheap, each settling one question

| Ref | Question | Experiment | Cost |
|---|---|---|---:|
| **R1** | What is `Transact` p50/p99 with `AwaitDurable=true`, and what is a node's concurrent ceiling? | shale slate backend, one unit, one bucket. 10k commits of a 5-key write set against self-hosted MinIO, S3 Standard, and S3 Express, same region. Report p50/p95/p99 **and** aggregate commits/sec at 1, 4, and 16 concurrent `Transact` callers, so `casCommitMu`'s effect is visible as a flat line. Repeat once with the mutex striped to size the fix. **This replaces the single most load-bearing estimate in section 5, and every table quoting 103 ms is provisional until it lands.** | 1 to 2 days |
| **R2** | How big is a CommitCAS request, counting both halves? | Instrument the objstore prototype to log **read-set bytes plus write-set bytes** per `UpdateWorkflowExecution` and `ConflictResolveWorkflowExecution` across the whole features corpus. The read set matters because a read-check ships the full observed value (4.8), so the wire cost is ~2x the Temporal transaction and the ceiling bites at ~2 MiB. Take the max and the p99. Only load-bearing for remote pins, not T1-prime. | 2 hours |
| **R3** | Does `ScanPrefix` over one unit hold up with a 100k-entry timer prefix? | Synthetic: write 100k keys under one tag, scan with and without a start bound, measure wall clock and allocation. Repeat cold (fresh mount) and warm. | 1 day |
| **R4** | Is the read-then-CAS pattern in `Transact` (3 routed Gets before the commit) actually 3 RTTs, or does the owner short-circuit? | Instrument `OwnsCASPin` hit rate for a single-node embedded config. If it is 100%, the Gets are local and the "3 RTTs" line in section 4.2 is wrong in our favor. | 4 hours |
| **R5** | How bad are tombstones in practice at R=2 for a task queue? | Write and delete 1M task keys under one tag at R=2, then `ScanPrefix`. Count returned entries. If the count is 1M rather than 0, G5 in 4.8 is confirmed fatal and the shard plane must run at R=1 until shale filters. | 4 hours |
| **R6** | Does `WithCustomDataStoreFactory` survive a Temporal minor-version bump? | Rebase `s3-persistence` onto the current upstream tag and count the compile errors in `common/persistence/objstore/`. The hook is marked experimental and undocumented, so this is the maintenance-cost measurement. | 4 hours |
| **R7** | Can the SDK dial in-process over bufconn? | One test: `client.Dial` with `ConnectionOptions.DialOptions` containing a `grpc.WithContextDialer` over `bufconn`, against a running `LiteServer`. | 2 hours |

**Run R1 first and run it before reading section 5 as anything but arithmetic.** R1 plus R2 plus R5 is
about two days total and each can kill or reshape the plan.

### Known unknowns, labelled

- *Hypothesis, unverified:* SDK v1.41.1 exposes `ConnectionOptions.DialOptions` cleanly enough for a
  bufconn dialer. R7 settles it.
- *Hypothesis, unverified:* compaction and SST-read request volume for a slatedb store under Temporal's
  write pattern is small relative to WAL PUTs. The LSM rows in 5.3 count WAL PUTs and leave the GET class
  unpriced. R1's harness can also count total object-store requests by class, which turns this into a
  measurement.
- *Unknown:* whether `FairTaskStore` needs distinct behavior. The prototype returns the same
  implementation as the classic task store (`factory.go:80`), which is either correct or an unexercised
  bug. The features corpus does not distinguish them. Note that the subqueue segment restored in 4.5 and
  9.2 is the fair-task mechanism, so a correct subqueue key is a precondition for answering this at all.
- *Unknown:* how Temporal's CHASM work lands on this layout. It generalizes rather than changes the
  contract (an `ArchetypeID` on every execution-scoped request, a `ChasmNodes` path-keyed map alongside
  the existing activity and timer maps), so the expectation is "no change to the key schema, one more map
  in the value". Not verified against a recent upstream tag.

---

## 9. Appendix: object key schema

### 9.1 Existing (objstore prototype, HEAD `899fca7`)

`esc()` is `url.PathEscape` (`common/persistence/objstore/keys.go:21`). All keys bucket-relative. Thirteen
top-level prefixes. Every value is a JSON envelope.

```
shards/{cluster}/{shardID}/info
    shardEnvelope{encoding, data, range_id}          [cluster is NOT escaped]

executions/{esc(nsID)}/{esc(wfID)}/current_run
    currentRunPtr{rid, es, lwv, ts}
executions/{esc(nsID)}/{esc(wfID)}/runs/{esc(runID)}/snapshot
    workflowEnv = THE ENTIRE MUTABLE STATE IN ONE OBJECT:
    ExecutionInfo, ExecutionState, Checksum, and map-of-blob
    ActivityInfos / TimerInfos / ChildExecutionInfos / RequestCancelInfos /
    SignalInfos / ChasmNodes / SignalRequestedIDs / BufferedEvents

history/nodes/{esc(treeID)}/{esc(branchID)}/{nodeID:020d}-{fixedWidthTxn}
    historyNodeEnv{nid, txn, ptxn, ev}
    fixedWidthTxn: "0"+%020d for txn >= 0, "n"+%020d(abs) for txn < 0.
    Negatives sort AFTER positives, which is wrong and currently unreachable.
    Highest txnID per nodeID wins; the list scan shadows lower ones.
history/trees/{esc(treeID)}/branches/{esc(branchID)}
    historyBranchEnv{TreeInfo blob}

history-tasks/{shardID}/{categoryID}/{fireNs:020d}-{taskID:020d}
dlq/replication/{esc(srcCluster)}/{shardID}/{fireNs:020d}-{taskID:020d}

tasks/{ns}/{taskQueueType}/{esc(tq)}/meta
tasks/{ns}/{taskQueueType}/{esc(tq)}/items/{subqueue}/{pass:020d}-{taskID:020d}
tasks/{ns}/user-data/{esc(tq)}
tasks/{ns}/build-ids/{esc(buildID)}/{esc(tq)}

namespaces/by-id/{esc(id)}
namespaces/by-name/{esc(name)}
cluster-metadata/{esc(name)}
cluster-members/by-id/{hex(hostID)}
nexus/endpoints/{esc(id)}

queues-v1/{queueType}/metadata
queues-v1/{queueType}/dlq-metadata
queues-v1/{queueType}/messages/{id:020d}
queues-v1/{queueType}/dlq-messages/{id:020d}
queues-v2/{queueV2Type}/{name}/meta
queues-v2/{queueV2Type}/{name}/messages/{id:020d}

visibility/{esc(nsID)}/runs/{esc(runID)}
```

Properties worth naming:

- No key carries a hash tag, so nothing co-locates. Every record is independently addressable and
  independently written.
- `history-tasks` and `dlq` are shard-scoped; `executions` and `history` are not, which is why the
  prototype could not co-commit them even if it had a transaction.
- All integers are 20-digit zero-padded decimal, which is 20 bytes for 8 bytes of information and breaks
  on negatives.

### 9.2 Proposed (`shalestore`)

Four hash-tag planes. `be8(x)` = 8-byte big-endian of `x ^ (1<<63)`. `be8i(x)` = `be8(^x)`, giving
descending order. All values are the same `DataBlob` envelopes Temporal already hands the plugin, so the
codec is unchanged from 9.1.

**`esc` is `url.PathEscape`, the prototype's own helper (`common/persistence/objstore/keys.go:21`), and it
is applied to every caller-supplied component including the ones inside a hash tag.** Section 4
gives the two reasons: `/` in a workflow ID makes two different executions collide on one key, and `{`
or `}` in a task-queue name silently truncates or shifts the hash tag, which is the invariant everything
else rests on. Components that are not caller-supplied (`<cluster>`, `<shardID>`, `<categoryID>`,
`<type>`) are left raw.

```
### history plane: tag = Temporal history shard. One Transact covers all of it.
{h/<cluster>/<shardID>}/shard
{h/<cluster>/<shardID>}/cur/<esc nsID>/<esc wfID>
{h/<cluster>/<shardID>}/ms/<esc nsID>/<esc wfID>/<esc runID>
{h/<cluster>/<shardID>}/q/<categoryID>/<be8 fireNs><be8 taskID>
{h/<cluster>/<shardID>}/dlq/<esc srcCluster>/<be8 taskID>
{h/<cluster>/<shardID>}/hn/<esc treeID>/<esc branchID>/<be8 nodeID><be8i txnID>
{h/<cluster>/<shardID>}/ht/<esc treeID>/<esc branchID>

### matching plane: tag = one task-queue partition. Tag contents are escaped.
{q/<esc nsID>/<type>/<esc tqName>}/meta
{q/<esc nsID>/<type>/<esc tqName>}/t/<subqueue>/<be8 pass><be8 taskID>

### global metadata. Low rate, one unit, acceptable hot spot.
{m}/schema                                key-layout version; write it before the first bucket exists
{m}/ns/id/<esc nsID>
{m}/ns/name/<esc name>
{m}/ns/version
{m}/cm/<esc clusterName>
{m}/cmem/<hex hostID>                     value carries expiry_ns; no TTL in shale
{m}/nx/<esc endpointID>
{m}/nx-version
{m}/sa/<esc nsID>
{m}/tqud/<esc nsID>/<esc tqName>
{m}/bid/<esc nsID>/<esc buildID>/<esc tqName>
{m}/q1/<queueType>/meta
{m}/q1/<queueType>/<be8 msgID>
{m}/q2/<queueV2Type>/<esc name>/meta
{m}/q2/<queueV2Type>/<esc name>/<be8 msgID>

### visibility plane: tag = namespace. Written outside the history transaction.
### The read path scans e/ and evaluates the ported predicate tree; see 4.7.
{v/<esc nsID>}/e/<esc runID>
{v/<esc nsID>}/i/status/<esc status>/<be8 startTime><esc runID>    (later, optional)
```

The five changes from 9.1 that matter:

1. **`hn` and `ht` move under the shard tag.** This is what makes history nodes co-committable with
   mutable state, and it is what fixes cliff 2 structurally rather than by reordering. It follows SQL's
   `history_node` primary key, which already leads with `shard_id`.
2. **`be8` replaces `%020d`.** 8 bytes instead of 20, correct across the full int64 range.
3. **`be8i` on `txnID`** reproduces Cassandra's `CLUSTERING ORDER BY (... txn_id DESC)`, so the winning
   node for a `nodeID` is the first key encountered and a bounded scan can stop. The prototype must read
   every txnID for a node and take the max.
4. **Reverse indexes (`bid`, and any visibility index) are written in the same transaction as their base
   record**, so they cannot drift. In the prototype they are separate PUTs.
5. **`{m}/schema` is new.** The values are Temporal's own envelopes and Temporal versions those; the key
   layout is this design's, and nothing versions it. Write the marker on bucket creation.

Two things deliberately carried over from 9.1 rather than changed: **escaping on every caller-supplied
component**, and **the `<subqueue>` segment in the matching key**. Both are load-bearing, both are easy to
drop by accident when transcribing the layout, and the prototype gets both right today
(`task_store.go:85-86,331,404`).

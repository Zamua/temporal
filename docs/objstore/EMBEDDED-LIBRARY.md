# Temporal as an importable library: can it be done, and what would it cost

**The question.** Is there anything Temporal does that is incompatible with (1) a Temporal-API-compatible embedded service on any backend, (2) no extra process, daemon, or job anywhere, (3) working on object storage, and (4) trivially scaling horizontally by adding nodes? If we forked Temporal, could we get to a library you import, like SQLite, that lets you define and execute workflows inside your own service? Or does it fundamentally require a separate process?

**Provenance.** This checkout is a **shallow fork**, not upstream: remote `Zamua/temporal`, `.git/shallow` present, `git rev-list --count HEAD` = **17**, of which 16 are local `objstore` work on top of one upstream commit (`a662b77`). Every `file:line` citation with no repository prefix refers to this checkout, and those citations are valid because the Temporal source here is unmodified upstream outside `common/persistence/objstore/`, `cmd/server/main.go` and `config/development-objstore*.yaml`. **Line counts and file counts are counted here. Commit-rate counts cannot be, and are not**: every commits-per-year figure in this document comes from the GitHub commits API against `temporalio/temporal` with `since=2025-08-14` (`gh api "repos/temporalio/temporal/commits?path=<p>&since=...&per_page=1"`, reading the last page number from the `Link` header). Storage-layer citations prefixed `shale/` refer to that repository's `docs/SPEC.md`. SDK citations are `go.temporal.io/sdk@v1.44.0`; API citations are `go.temporal.io/api@v1.62.13`. Storage figures are labelled measured or estimated at the point of use.

---

## TL;DR

- **No. Temporal does not fundamentally require a separate process.** Running all four server roles inside one Go binary is already a supported, in-tree mode, and `Server.Start()` already returns instead of blocking (`temporal/server_impl.go:44-53`, `temporal/fx.go:308-322`).
- **The persistence plugin seam is public API, not a fork.** `WithCustomDataStoreFactory` is a supported `ServerOption` whose interface doc says it exists "to implement custom datastore support outside of the Temporal core" (`temporal/server_option.go:146`, `common/persistence/client/abstract_data_store_factory.go:14-15`). It is marked experimental. It took 1 commit in the last 12 months.
- **Membership *discovery* is a persistence concern; membership *transport* is not.** Discovery bootstraps from `GetClusterMembers` / `UpsertClusterMembership` / `PruneClusterMembership` on the shared store (`common/persistence/persistence_interface.go:109-112`, consumed at `common/membership/ringpop/monitor.go:321-356`), so there is **no seed list and no static host map**. But the store is only ringpop's seed source and heartbeat sink: ringpop still opens a tchannel listener per service on `RPCConfig.MembershipPort` (`common/membership/ringpop/factory.go:105, 150-163`), and there is **no `ServerOption` for a custom `membership.Monitor`** (21 options enumerated in `temporal/server_option.go:34-203`; the module is hard-selected at `temporal/fx.go:379-381`). **A replica opens 8 Temporal listeners, plus 2 more for the embedded store, and the store needs a seed list of its own (`shale/docs/SPEC.md:400, 963`).**
- **Temporal's background jobs are not daemons. They are Temporal workflows deduped by workflow ID.** Every replica races to start the same fixed workflow ID and all losers treat `WorkflowExecutionAlreadyStarted` as success (`service/worker/scanner/scanner.go:268-277`, options at `service/worker/scanner/workflow.go:57-75`). Requirement 2 is satisfied by Temporal's existing design, not in spite of it.
- **Three things are genuinely hard.** First, `NumHistoryShards` is immutable once data exists, enforced by a silent no-op (`common/persistence/cluster_metadata_store.go:167-169, 204-218`) with the configured value overwritten from the database after a single `Warn` (`temporal/fx.go:831-838`). Second, a raw bucket cannot host Temporal at any price, because one `UpdateWorkflowExecution` is an atomic conditional multi-key commit and a bucket's atomic unit is one object. Third, that transaction layer must be **linearizable per partition**, and the leading candidate is not.
- **The store's per-shard CAS must be linearizable, and shale as specified is not.** shale declares AP over CP: on a partition "both sides accept writes" and conflicts resolve by last-write-wins (`shale/docs/SPEC.md:928`), with strong consistency across partitions an explicit non-goal (`:938`). The ownership gate is evaluated against the *local* ring view (`:792`) and the commit lock is *per node* (`:805`), so a minority side computes `W=1` (`:531`) and commits. Two history nodes can then both pass `IF range_id = ?`. Temporal detects nothing. **This is a P0 on the backend, not on the architecture** (section 7 item 0).
- **The physics you cannot delete: two dependent durable writes per workflow-task round trip.** On S3 Standard at an estimated 30 ms per commit that is 60 ms; on S3 Express One Zone at a measured 6.4 ms it is 12.8 ms; on a self-hosted LAN object store at an estimated 1 ms it is 2 ms. **The only *measured* per-commit number on the recommended substrate is 103 ms p50, giving 206 ms per round trip, which section 2.3 states is not viable.** Getting off that number is what R>=2 with relaxed replica durability is for, and that path is unmeasured. This is the real scope statement.
- **"Just add more nodes" works up to a ceiling you choose once, and that ceiling is throughput as well as node count.** Adding a replica needs zero configuration beyond the same bucket, and a shard move copies **zero bytes** because `shard_id` already leads every primary key. But Temporal serializes persistence *per shard*: `ShardIOConcurrency` defaults to **1** (`common/dynamicconfig/constants.go:1810-1814`) and sizes the shard's semaphore (`service/history/shard/context_impl.go:2105`), which every write path acquires around its persistence call (`:489, 543, 601, 679, 741`). A shard sustains **1/commit-latency**, so **shards must be sized from the throughput target, not the node target.**
- **Recommendation: build the library as a thin facade plus a storage plugin, on top of unmodified Temporal.** Estimated **62 engineer-weeks** upfront and **1 to 2 engineer-weeks per year** of upstream tracking, against 410,135 lines of Temporal you reuse and **1,861** upstream commits per year you never read.
- **The alternative of writing a new engine costs 82 engineer-weeks and buys 45% to 58% of the compatibility.** Dropping API compatibility entirely saves 26 weeks on the server and costs 16 weeks writing your own SDK, so **the Temporal API compatibility tax is negative**. Compatibility is close to free; the engine is what is expensive, and the plugin approach gets the engine for nothing.
- **Three P0 storage questions must be answered before committing**: linearizability of the per-shard CAS, and two about acked-write durability. Details in section 7. A wrong answer to any of them changes the backend, not the architecture.
- **A partial implementation already exists in this checkout and it fails the hardest row.** `common/persistence/objstore` is 22 non-test files and 6,655 lines covering ten of the nineteen line items below, and `execution_store.go` contains **zero** references to `RangeID`. Section 4.9.

---

## 1. The answer in one page

### Does Temporal fundamentally require a separate process?

**No.**

The public constructor is `temporal.NewServer(opts ...ServerOption) (Server, error)` (`temporal/server.go:44`) returning an interface with exactly `Start() error` and `Stop() error` (`temporal/server.go:16-19`). `Start()` returns immediately unless the `InterruptOn` option is supplied (`temporal/fx.go:308-322`); Temporal's own test helper depends on this and documents it as "Start does not block as long as InterruptOn is unset" (`temporaltest/server.go:163`). Running matching, history, frontend and worker in one process is explicitly supported, with an in-tree comment naming the required ordering: "When starting multiple services in one process (typically a development server), start them in this order... the worker depends on the frontend, which depends on matching and history" (`temporal/server_impl.go:44-53`, verified).

No configuration file is required either. `WithConfig(cfg *config.Config)` supplies configuration as a Go struct and `loadConfig` runs only when the struct is nil (`temporal/server_option.go:34`, `temporal/server_options.go:83-88`). The in-tree lite server builds a complete config in code with nothing on disk (`temporaltest/internal/lite_server.go:74-171`).

So the target shape already exists in outline. `engine := lib.New(...); engine.Start()` inside the app's own binary is not a new capability; it is an existing one behind an unpleasant configuration surface.

### The four things that make it hard

**1. Object storage alone cannot host it.** One `ExecutionStore.UpdateWorkflowExecution` must apply atomically, conditional on **two** simultaneous conditions (`RangeID` and `Condition`/`DBRecordVersion`), a set that includes the execution info blob, the execution state blob, upserts and deletes across six sub-maps plus CHASM nodes, buffered events, task rows across up to six categories, M history nodes, the current-execution pointer, and optionally a second complete snapshot for a brand-new run (`common/persistence/persistence_interface.go:357-369, 435-472`). A bucket's atomic unit is one object. You need a transaction layer over the bucket: an embedded distributed KV with a partition-scoped `Transact`, or an LSM with conditional commit. This is arithmetic, not a Temporal defect.

**1b. That transaction layer must be linearizable per partition, and an AP store is not.** Temporal's entire safety story is the per-shard `IF range_id = ?` compare-and-set. It is only a fence if two writers cannot both pass it. shale, the candidate, explicitly chooses availability: "Both sides accept writes. On heal, conflicts resolve via Last-Write-Wins" (`shale/docs/SPEC.md:928`) and lists "Strong consistency across partitions" as a non-goal (`:938`). The mechanism confirms it, twice over: `CommitCAS` gates ownership on the **local** ring view (`:792`) and serializes commits under a **per-node** lock (`:805`), and the write quorum tracks live membership rather than configured R, so "a configured R=5 cluster running on 2 live members yields W=2" (`:531`) and a 1-node minority yields W=1. Two history nodes on opposite sides of a partition therefore both bump `range_id`, both allocate task IDs from overlapping ranges, and LWW discards one side's history on heal. Temporal cannot see it: `shard_id` is a pure hash (`common/util.go:389-397`) and `renewRangeLocked` treats a successful `UpdateShard` as proof of ownership (`service/history/shard/context_impl.go:1152-1185`). Same hazard at founding boot: a per-process `ClusterId` is generated (`temporal/fx.go:725-735`) and saved with `UseClusterIdMembership: true` (`:737-750`), and the ringpop app name is derived from it (`common/membership/ringpop/factory.go:100-104`), so two replicas that both win the founding write form two permanently disjoint rings on one bucket. **This is a backend requirement, not an architecture defect**, and section 7 item 0 states the three ways to close it.

**2. The history shard count is immutable while data exists, and Temporal hides that from you.** The count is written to `cluster_metadata` on the first boot of the first process (`temporal/fx.go:741`, verified). Any later change is rejected by `immutableFieldsChanged`, which returns `(false, nil)`: not applied, and **not an error** (`common/persistence/cluster_metadata_store.go:167-169, 204-218`, verified). Every later boot then overwrites the configured value with the persisted one after one `logger.Warn` (`temporal/fx.go:831-838`, verified). Deleting those checks does not fix anything: routing is `farm.Fingerprint32(namespaceID + "_" + workflowID) % numberOfShards + 1` (`common/util.go:389-397`, verified) and `shard_id` is the leading component of the primary key of every execution, task and history table, so changing the count remaps every workflow to a different physical partition. Two nodes running different counts would produce two distinct primary-key rows for the same workflow and **both writers would succeed**: silent per-workflow split brain, not a detected conflict. The only in-tree remap path is cross-cluster replication, and it panics unless one count divides the other (`common/util.go:399-406`, verified).

**3. Two dependent durable writes sit on the critical path of every workflow-task round trip.** `RecordWorkflowTaskStarted` persists (`service/history/api/recordworkflowtaskstarted/api.go:166-182`), then `RespondWorkflowTaskCompleted` persists (`service/history/workflow/context.go:544,641`, `service/history/workflow/transaction_impl.go:163-196`). Neither is removable; that pair **is** durable execution. On a bucket, each one costs an object-store commit. Section 2.3 does the arithmetic.

### What is *not* hard, and is widely assumed to be

- **The worker role.** It is a role in the same binary, not a daemon, and its cluster-wide jobs deduplicate by workflow ID with no election and no lock.
- **Retention and cleanup.** A per-workflow `DeleteHistoryEventTask` timer task, not a sweeper (`service/history/tasks/workflow_cleanup_timer.go:15`).
- **Membership discovery.** Database-backed, so the bucket is the discovery mechanism and Temporal needs no seed list. The gossip *port* stays, and the storage layer brings a seed list of its own; see 2.2.
- **Adding a node.** Zero configuration, zero byte movement, automatic rate-limit redivision.
- **Elasticsearch.** Optional. SQL-backed standard visibility exists and ships with a SQLite dev config.

### The recommendation

Build **an out-of-tree Go module** containing (a) a storage plugin implementing Temporal's persistence interfaces over a **linearizable** partition-scoped store, and (b) a thin facade that synthesizes Temporal's config in Go, allocates loopback ports, manages lifecycle, guards the shard count loudly, and exposes `RegisterWorkflow` / `Start` / `Client`. Import unmodified Temporal. Do not fork it.

- **Upfront: 62 engineer-weeks.**
- **Upstream tracking: 1 to 2 engineer-weeks per year**, because the tracked surface is 5 files totalling **17 commits** in the last 12 months, against **1,861** repo-wide.
- **You reuse 410,135 lines of non-test, non-generated Go**, including every one of the 60 history event types, 17 command types, timers, server-side activity retry, buffered signals, sticky queues, resets, updates, schedules, child workflows and Nexus, plus all five official SDKs, the CLI and the Web UI.

The reason this is the right answer, stated once: **Temporal's expensive part is the engine, and the two extension points the target shape needs are already public API.** Writing a new engine costs more and delivers less compatibility. Forking the coordination layer costs more still and buys a knob you can set correctly on day one for the price of an idle floor.

**What that answer is contingent on, stated in the same breath:** a store whose per-partition conditional commit is linearizable. Every safety claim below inherits from that one property, so it is priced (item 17 of the P1 table) and gated (section 7 item 0) rather than assumed.

---

## 2. Requirement by requirement

### 2.1 "i want a temporal api compatible embedded service implemented on any backend"

**Achievable, with a precise definition of "compatible" and a precise definition of "any backend".**

**Compatible with what, exactly.** "Temporal API" is four nested surfaces and the outer ones are internal:

| Surface | RPC count | Who calls it |
| --- | ---: | --- |
| `temporal.api.workflowservice.v1.WorkflowService` | 121 | every SDK, the CLI, the Web UI |
| `temporal.api.operatorservice.v1.OperatorService` | 12 | CLI, admin tooling |
| `grpc.health.v1.Health` | 1 | SDK `CheckHealth` |
| `adminservice` / `historyservice` / `matchingservice` | 46 / 77 / 40 | **server-internal only** |

The internal three exist only inside the server repo. Neither the CLI nor the Web UI references `adminservice`. A single-process deployment could in principle delete all three from the wire, though under the recommended path they stay as loopback traffic and cost nothing to keep.

The Go SDK calls 82 of the 121; 39 are never called by it. A worker running one workflow with one activity end to end needs **6 RPCs** that the happy path cannot complete without: `StartWorkflowExecution`, `PollWorkflowTaskQueue`, `RespondWorkflowTaskCompleted`, `PollActivityTaskQueue`, `RespondActivityTaskCompleted`, `GetWorkflowExecutionHistory`. Seven more make it uncrippled: `RespondWorkflowTaskFailed`, `RespondActivityTaskFailed`, `RespondActivityTaskCanceled`, `RecordActivityTaskHeartbeat`, `GetSystemInfo`, `DescribeNamespace`, `ShutdownWorker`. That is 13 of 121, or 11%. A practical target that real users accept is roughly 23 to 25.

**This matters only if you are writing the engine.** Under the recommended path you get all 121 for free, because you are running Temporal's own frontend. The census is here so the alternative can be priced honestly in section 5.

**Any backend, precisely.** The backend has to supply nine primitives, and two of them eliminate candidates:

| # | Primitive | Why |
| --- | --- | --- |
| 1 | Get by key | mutable state reads |
| 2 | Put by key | writes |
| 3 | Delete by key | cleanup, retention |
| 4 | Ordered prefix scan | task queues, timers, history nodes |
| 5 | **Partition-scoped atomic conditional multi-key commit** | the `UpdateWorkflowExecution` contract |
| 6 | **Partition-scoped single-writer fencing, linearizable** | the `range_id` lease is a fence only if two partitioned writers cannot both pass it |
| 7 | Durable ack with a knob | `Strict` vs `Relaxed` durability |
| 8 | Membership / placement readback | so `GetClusterMembers` has something to read |
| 9 | Bounded-size values or chunking | history blobs |

| Candidate | Verdict |
| --- | --- |
| Raw bucket, no transaction layer | **Fails 5 outright.** One object is the atomic unit; one mutation touches 3 + N + M records. |
| **AP** embedded distributed KV with partition-pinned `Transact` (shale as specified) | **Passes 1-5 and 7-9. Fails 6.** The read-set carries the `RangeID` row and the `Condition`, and that is enough in steady state. Under partition it is not: both sides accept writes, the ownership gate reads the local ring, and LWW resolves the conflict (`shale/docs/SPEC.md:928, 938, 792, 531`). Section 7 item 0 |
| **CP** embedded distributed KV with partition-pinned `Transact` | **Passes all nine.** This is the shape the design requires. Reaching it from the AP candidate is item 17 of the P1 estimate |
| Postgres | **Passes all nine cleanly**, and is the only candidate with a genuine per-call durability knob. But it is a separate service, which fails requirement 2, and it is not object storage, which fails requirement 3. **Any fallback in this document that reaches for Postgres is therefore a scope change, not a fix**, and is labelled as one |
| SQLite | **Passes single-node.** Fails multi-node structurally: a local file cannot be reopened by a peer, so every clustering story reintroduces a daemon. |

So "any backend" resolves to: **anything that can supply a linearizable partition-scoped transaction.** Temporal already treats that as a plugin point, so supporting a second backend later is a second `AbstractDataStoreFactory` implementation, not a second architecture. Primitive 6 is the one that eliminates candidates people expect to qualify.

**Surface to implement.** 108 methods across 9 interfaces, plus 13 for visibility:

| Interface | Methods |
| --- | ---: |
| `DataStoreFactory` | 10 |
| `ShardStore` | 5 |
| `TaskStore` | 14 |
| `MetadataStore` | 9 |
| `ClusterMetadataStore` | 8 (includes the 3 membership methods) |
| `ExecutionStore` | 27 |
| `Queue` | 12 |
| `QueueV2` | 5 |
| `NexusEndpointStore` | 5 |
| `VisibilityStore` | 13 |

**Cost, and the thing that makes it tractable:** Temporal ships a shared conformance suite at `common/persistence/tests/`, **21 files, 13,622 lines, 110 test methods** (counted in this checkout), already parameterized per backend. A new backend adds one `_test.go` file and then grinds the suite to green. That is a falsifiable acceptance test for the single largest work item in this project, and it exists already.

### 2.2 "i want no extra process/daemon/job anywhere"

**Achievable today. This is the requirement Temporal satisfies best, and it is the one people expect it to fail.**

Take the requirement at its word: no extra deployable unit. No `temporal-server`, no matching daemon, no worker daemon, no scheduler, no sidecar, no cron entry, no operator.

**All four roles in one process.** Supported and in-tree (`temporal/server_impl.go:44-53`, verified).

**The cluster-wide jobs are workflows, and they self-deduplicate.** Each replica's scanner calls `client.ExecuteWorkflow` with a **fixed workflow ID**, `WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE` and `CronSchedule: "0 */12 * * *"` (`service/worker/scanner/workflow.go:57-75`, verified), and `startWorkflow` swallows `WorkflowExecutionAlreadyStarted` as `nil` (`service/worker/scanner/scanner.go:268-277`, verified):

```go
_, err := client.ExecuteWorkflow(ctx, options, workflowType, workflowArgs...)
if err != nil {
    if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok {
        return nil
    }
    ...
}
```

N replicas racing produce exactly one running instance. There is no election, no lock, no split brain, and no new mechanism to build. The mutual exclusion is workflow-ID uniqueness, which is the same conditional write that protects every user workflow, which means it is the strongest fence in the system.

**Per-namespace workers shard by the same consistent hash that shards history.** `getLocallyDesiredWorkers` does `serviceResolver.LookupN(namespaceID, count)` and counts how many desired slots land on this host (`service/worker/pernamespaceworker.go:268-281`), so each namespace's worker runs on exactly `count` hosts and rebalances on membership change. Schedules and the batcher sit behind namespace-scoped dynamic-config booleans (`common/dynamicconfig/constants.go:3248-3251, 3198-3201`) and can be turned off entirely.

**Retention needs no sweeper.** It is a per-workflow `DeleteHistoryEventTask` timer task executed by the timer queue (`service/history/tasks/workflow_cleanup_timer.go:15`, `service/history/timer_queue_active_task_executor.go:119-120`).

**What it actually costs, stated plainly.**

- **Loopback sockets, counted honestly.** `RPCFactory.createGRPCListener` does `net.Listen("tcp", hostAddress)` and calls `logger.Fatal` on failure (`common/rpc/rpc.go:161-172`, verified). There is no in-process transport: `grep -rn "bufconn" --include="*.go" .` returns **0** in this checkout (verified). Temporal's all-in-one lite server opens **10** listeners for one node: 4 gRPC, 4 ringpop, 1 prometheus, 1 pprof (`temporaltest/internal/lite_server.go:94-112, 353-375`). pprof is skipped at port 0 (`common/pprof/pprof.go:41-45`), prometheus when the metrics config is nil (`common/metrics/config.go:463`), and the frontend's HTTP/Nexus listener at `HTTPPort: 0` (`service/frontend/service.go:492-501`). **The four ringpop listeners do not disappear.** Store-backed membership replaces the *seed list*, not the *transport*: `fetchCurrentBootstrapHostports` reads `GetClusterMembers` only to build ringpop's bootstrap set (`common/membership/ringpop/monitor.go:321-356`), then ringpop is constructed over a tchannel (`factory.go:105`) which listens on `RPCConfig.MembershipPort` per service (`factory.go:150-163`, allocated per service at `temporaltest/internal/lite_server.go:353-357`). There is no `ServerOption` to replace the monitor: the module is selected at `temporal/fx.go:379-381` between `ringpop.MembershipModule` and `static.MembershipModule`, and 3.2 rules out static. **So: 8 listeners per replica, 4 gRPC (one client-facing) plus 4 ringpop, before the storage layer adds its own.** The worker role's gRPC listener registers only health and reflection (`service/worker/service.go:272-282`).
- **The storage layer adds two more.** shale runs its own memberlist gossip instance with *configured seed addresses* (`shale/docs/SPEC.md:400, 411`) on a bind address plus a gRPC address (`:963`). Embedded in-process that is **10 listeners per replica and a seed list after all**. The seed list is the part that costs deployment complexity; the ports are the part that costs nothing.
- **The system worker dials the frontend over gRPC, so embedded, the process dials itself.** `workerManager.Start` builds an SDK worker on `sdkClientFactory.GetSystemClient()`, which is `sdkclient.Dial` (`service/worker/worker.go:60-61`, `common/sdk/factory.go:91-118`). A system workflow task takes three hops inside one process. This is latency on background work, not a topology cost.
- **Once N > 1, history and matching bind peer-routable addresses.** Unavoidable: an SDK client is a plain gRPC client and will not route by workflow ID, so any replica can receive a call for any workflow and must forward it to the owner. Mitigate with a private network and mTLS, not with configuration.
- **The scanner cron workflows execute in the operator's own cluster** and will appear in their workflow list.

**Verdict: one binary, one process per replica, ten listeners of which one is client-facing. Requirement 2 is about deployable units, and this is one.** The listener count is a red herring: sockets are not deployable units, and no number of them reintroduces a daemon. What *does* cost something is the storage layer's seed list, which is real configuration a new replica needs beyond "the same bucket".

### 2.3 "it should work on object storage"

**Achievable through a transaction layer, and the honest constraint is latency, not correctness.**

**Counting the writes.** A workflow-task round trip is: poll returns a task, the worker executes, the worker responds.

| Write | Where | Notes |
| --- | --- | --- |
| W1 `RecordWorkflowTaskStarted` | `service/history/api/recordworkflowtaskstarted/api.go:166-182` | one `UpdateWorkflowExecution`. `Noop = true` only for speculative tasks |
| W2 `RespondWorkflowTaskCompleted` | `service/history/workflow/context.go:544,641` into `transaction_impl.go:163-196` | one `UpdateWorkflowExecution` |

**Two dependent durable writes. That is the floor.**

W2 is genuinely one round trip and not several, which is the good news buried in the atomicity contract: history events ride inside the same request as `UpdateWorkflowNewEvents []*InternalAppendHistoryNodesRequest` (`common/persistence/persistence_interface.go:366`), and the mutation carries mutable state, all sub-records, buffered events and per-category task rows in one struct (`:435-472`). **The persistence contract is already wide-transaction shaped**, which is exactly what makes an LSM over a bucket viable at all.

Two writes that are *not* on the critical path: shard ack-level updates are amortized to at most once per 5 minutes (`common/dynamicconfig/constants.go:2434-2438`), and visibility is its own task category polled at 1 minute (`:2248-2250`). One write that is conditional: if no poller is already parked on the target partition, the task spools to storage for +1 write; if one is, `TrySyncMatch` succeeds with **no persistence write** (`service/matching/task_queue_partition_manager.go:551-553`, `service/matching/physical_task_queue_manager.go:692-713`).

A whole one-activity workflow is `CreateWorkflowExecution`, `RecordWorkflowTaskStarted`, `RespondWorkflowTaskCompleted`, `RecordActivityTaskStarted`, `RespondActivityTaskCompleted`, `RecordWorkflowTaskStarted`, `RespondWorkflowTaskCompleted`: **7 dependent writes with sync match everywhere, 10 without.**

**Multiplying by a per-commit latency.** Provenance for each figure is stated; do not treat the estimates as measurements.

| Substrate | Per durable commit | 1 WFT round trip (2 writes) | One-activity workflow (7) | Same, no sync match (10) |
| --- | ---: | ---: | ---: | ---: |
| S3 Standard, LSM strict | 30 ms (estimate) | **60 ms** | **210 ms** | 300 ms |
| S3 Express One Zone, LSM strict | 6.4 ms (published measurement) | **12.8 ms** | **45 ms** | 64 ms |
| Self-hosted object store on a LAN | ~1 ms (estimate) | **2 ms** | **7 ms** | 10 ms |
| Embedded KV, R=1, strict, on a real object store | 103 ms p50 (measured, 8 concurrent clients) | **206 ms** | **721 ms** | 1,030 ms |
| Embedded KV, R>=2, relaxed replica durability | ~1 ms LAN (hypothesis, unverified as a per-write p50) | ~2 ms | ~7 ms | ~10 ms |
| Cassandra / local NVMe, for reference | ~3 ms LWT | 6 ms | 21 ms | 30 ms |

**Read this as a scope statement.** At 12.8 ms or 2 ms per round trip, an embedded engine on object storage is a perfectly good orchestrator for human-scale and batch-scale work. At 206 ms it is not: a workflow with 20 sequential activities burns 4 seconds of pure commit latency before any user code runs.

**There are two throughput ceilings, one per layer, and they must not be conflated.**

**Ceiling A, the store's, is per node.** For the in-house KV at R=1 the commit path serializes validate-and-apply under a **per-node** lock (`shale/docs/SPEC.md:805`) across a durable owner-local commit, and R=1 pins await-durable on. At 103 ms per commit that is **9.7 commits/sec per node, so 4.85 workflow-task round trips per second per node, across every shard it owns**, and adding shards does not improve it. At R>=2 with relaxed replica durability the lock window collapses to a memtable insert, a same-unit burst coalesces from O(writes) flushes to O(flush-windows), and the measured production-shaped number is **11,661 puts/sec**. **R>=2 is not a tuning choice for this design. It is the only configuration in which it works**, and it is independently required for durability (section 7).

**Ceiling B, Temporal's, is per shard and survives every storage fix.** `ShardIOConcurrency` defaults to **1** (`common/dynamicconfig/constants.go:1810-1814`), is read once at shard-context construction and used to size `ioSemaphore` (`service/history/shard/context_impl.go:2066-2072, 2105`), and every write path acquires it *around* the persistence call: `AddTasks` (`:480`, acquire `:489`), `CreateWorkflowExecution` (`:531`, acquire `:543`), `UpdateWorkflowExecution` (`:590`, acquire `:601`), `ConflictResolveWorkflowExecution` (`:668`, acquire `:679`), `SetWorkflowExecution` (`:730`, acquire `:741`). One in-flight persistence op per shard, full stop.

| Per-commit latency L | Ops/sec per shard (1/L) | WFT round trips/sec per shard | Shards needed for 1,000 WFT/sec |
| ---: | ---: | ---: | ---: |
| 6.4 ms (S3 Express, measured) | 156 | 78 | **13** |
| 30 ms (S3 Standard, estimate) | 33.3 | 16.7 | **60** |
| 103 ms (KV R=1, measured) | 9.7 | 4.85 | 207 |

**Per-node throughput is `min(ceiling A, shards_on_node / L)`.** At R=1 ceiling A binds and the two numbers coincide at 4.85, which is why that figure must not be attributed to the storage lock alone: at 103 ms per commit a single shard produces it independently. Fix the storage lock and ceiling B is what you are left with, which is why the shard count is a throughput parameter (2.4).

`ShardIOConcurrency` is a global dynamic-config int, so it can be raised above 1 on a non-Cassandra backend (the forced-to-1 branch at `:2066-2072` is Cassandra-only). That does not delete the ceiling, it relocates it: concurrent commits pinned to the same shard become OCC read-set conflicts in the store instead of semaphore waits, and the retry costs another round trip. Treat raising it as an experiment with a measured before and after, not a default.

**The idle floor, which is the cost of over-provisioning the shard count.** With a single cluster, no cross-region replication and no archival, a shard runs four queue processors: transfer, visibility and outbound at a 1-minute max poll interval (`common/dynamicconfig/constants.go:2076, 2151, 2250`) and timer at 5 minutes (`:2009`), plus at most one shard-row write per 5 minutes (`:2436`, gated at `service/history/shard/context_impl.go:1231-1238`).

Per shard per second: `1/60 + 1/60 + 1/60 + 1/300 = 0.0533` polls and `1/300 = 0.00333` writes.

| Shards | Idle polls/sec | Idle writes/sec | S3 Standard list price per month, upper bound |
| ---: | ---: | ---: | ---: |
| 1 | 0.05 | 0.003 | ~$0.10 |
| 4 | 0.21 | 0.013 | **~$0.39** |
| 64 | 3.41 | 0.213 | ~$6.30 |
| 512 | 27.3 | 1.71 | **~$50** |

Working for the 512 row: `27.3 x 86,400 = 2,358,720` GET/day at $0.0004 per 1,000 = $0.94/day = **$28.3/month**; `1.71 x 86,400 = 147,744` PUT/day at $0.005 per 1,000 = $0.74/day = **$22.1/month**.

**Why the dollar column is an upper bound:** with an LSM in front, a queue poll is a range scan that a warm block cache or memtable can serve with **zero** object GETs. The CPU, goroutine and wakeup cost does not go away, and neither does the shard-row write. The point survives either way: **the idle cost is a function of a number you must choose on day one and can never change.**

### 2.4 "it should trivially scale horizontally. just add more nodes"

**Achievable as literally stated, up to a ceiling chosen once at cluster init. Adding a node is free. Changing the ceiling is not possible.**

Split the requirement, because Temporal answers the halves very differently.

**Adding a node works today, with zero configuration and zero byte movement.**

- A new replica upserts itself into `cluster_membership` **in the shared store** and reads the live member list back out to bootstrap (`common/membership/ringpop/monitor.go:237, 290, 328`, over `common/persistence/persistence_interface.go:109-112`, verified). **For Temporal: no seed list, no join token, no static host map.** Point it at the same bucket and it joins.
- Shard ownership is a pure consistent-hash lookup on the shard id (`service/history/shard/ownership.go:135-147`).
- **A shard move copies zero bytes**, because `shard_id` is already the leading component of every primary key.
- Per-node rate limits are computed as cluster limit divided by live member count, so quotas redistribute with no operator action.

That is precisely the property requested, and Temporal already has it. **The one asterisk is the storage layer**, which gossips over memberlist and does need configured seed addresses (`shale/docs/SPEC.md:400, 411`). Those seeds only have to be reachable, not exhaustive, so two stable pod names cover any replica count (4.3). **Net: adding a node is one integer, and the seed list is written once.**

**Changing the shard count later is impossible, and configuration cannot reach it.** See section 1. The consequence is that the shard count is **both a node-count ceiling and a throughput ceiling**. N nodes split N_shards between them until node count exceeds shard count, *and* each shard sustains only 1/commit-latency because `ShardIOConcurrency` defaults to 1 (2.3, ceiling B). **Size the shard count from the throughput target, then check it also covers the node target.** The throughput target almost always demands more shards.

| Shards | Node ceiling | WFT/sec ceiling at 30 ms | WFT/sec ceiling at 6.4 ms | Idle polls/sec | Idle shard writes/sec |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 32 | 534 | 2,496 | 1.7 | 0.11 |
| 128 | 128 | 2,138 | 9,984 | 6.8 | 0.43 |
| 512 | 512 | 8,550 | 39,936 | 27.3 | 1.71 |

Those throughput columns are `shards x (1 / (2L))` and assume enough nodes that ceiling A never binds. They are upper bounds on the persistence path, not end-to-end predictions.

**The library's job is to make that choice loud instead of silent.** Temporal accepts your value, discards it, and logs a `Warn` into your application's log (`temporal/fx.go:831-838`). The facade must read the persisted count during `New()` and **hard-error on mismatch**. That is roughly 50 lines in code you own, not a patch to Temporal, and it converts a silent lie into a startup failure. Because the count now sets throughput as well as node count, the facade should also record the commit-latency assumption used to pick it, so a later operator can tell an intentional 512 from a copied one.

**Two scale-out costs to be honest about.**

1. **Shard handoff is a steal, not a handoff, and the obvious remedy is gated.** The new owner conditionally bumps `range_id`; the old owner learns it lost only when its next write fails. The linger window that would soften this defaults to `0` (`common/dynamicconfig/constants.go:1782-1788`; `service/history/shard/context_impl.go:1152, 1958`). So every scale event and every rolling restart produces a brief per-shard write gap. **You may not simply enable linger.** The setting's own doc says "Do NOT use non-zero value with persistence layers that are missing `AssertShardOwnership` support" (`common/dynamicconfig/constants.go:1786-1788`), and that method is a real `ShardStore` obligation (`common/persistence/persistence_interface.go:60`, dispatched at `common/persistence/shard_manager.go:91-95`). The prototype in this checkout returns `nil` unconditionally (`common/persistence/objstore/shard_store.go:179-184`), which is exactly the stub the warning is about: linger on top of it would extend the window during which a shard the node no longer owns keeps writing. Order of operations: implement `AssertShardOwnership` as a real read-and-compare of the persisted `range_id`, *then* enable linger. A storage layer whose own handoff keeps the outgoing owner serving until the successor is durably ready removes the storage half of the window, never Temporal's `range_id` half.
2. **Two rings converge independently, and unifying them is not a facade decision.** The engine's ownership ring and the storage layer's placement ring are separate systems. A node can own a Temporal shard while not owning the storage unit holding its rows, turning every write into a cross-node hop. The obvious fix, feeding storage placement into `ServiceResolver`, **is not reachable from configuration**: no `ServerOption` accepts a `membership.Monitor` or `membership.ServiceResolver` (21 options, `temporal/server_option.go:34-203`; `WithPersistenceServiceResolver` at `:124` is a `resolver.ServiceResolver` for persistence *endpoints*, a different type), and the module is hard-selected at `temporal/fx.go:379-381`. Doing it means carrying a patch on `temporal/fx.go`, a file that took **26 commits** in the last 12 months, which roughly doubles the tracked surface for one optimization. **Do not take it.** Accept the hop, measure its rate (section 7 item 8), and revisit only if the measurement says it dominates.

**Rolling upgrades.** The inter-role wire version check is permissive (`common/headers/version_checker.go:26-29`, `ServerVersion = "1.32.0"`, inter-role range `>=1.0.0 <2.0.0`), so it will not stop a bad deploy. Temporal's supported mixed-version window is one minor version (current N against the highest patch of N-1); I did not find a mixed-version test in this checkout, so treat the exact window as unverified here. The practical constraint: pin the fleet's Temporal version and roll one minor at a time. That is a constraint on the application's release process, not on the library.

---

## 3. The incompatibility ledger

Ranked by consequence. **FUNDAMENTAL** = cannot be fixed without abandoning Temporal's model or its wire API. **HARD** = fixable only by changing internals; configuration and plugins cannot reach it. **WORK** = known engineering, no design risk. **NON-ISSUE** = looks like a conflict, is not.

Of 27 rows: **3 FUNDAMENTAL, 5 HARD, 11 WORK, 8 NON-ISSUE.** Three are P0 (rows 0, 5, 6) and all three live in the storage layer.

| # | Temporal behaviour | Collides with | Class | Evidence | Fix, and its cost |
| ---: | --- | --- | --- | --- | --- |
| 0 | Every safety property rests on the per-shard `range_id` CAS being **linearizable**, and the candidate store is AP with LWW | 3, 4 | **FUNDAMENTAL for an AP store**, NON-ISSUE for a CP one | `shale/docs/SPEC.md:928` (both sides accept writes, LWW on heal), `:938` (strong consistency across partitions is a non-goal), `:792` (ownership checked against the local ring), `:805` (per-node commit lock), `:531` (W tracks live membership, so a 1-node minority yields W=1); Temporal side `common/util.go:389-397`, `service/history/shard/context_impl.go:1152-1185` | Two partitioned nodes both pass `IF range_id = ?`, both allocate overlapping task-ID ranges, LWW drops one side's history on heal, and Temporal never sees a conflict. **Three closes, priced as item 17: (a) make the store CP for the fenced keys, (b) refuse writes on the minority side rather than accept at W=1, or (c) route the shard row and the current-execution row to a CP store.** Section 7 item 0. **Until this closes, rows 19 and 20 and the whole of 4.5 are unsupported** |
| 1 | Two dependent durable writes per workflow-task round trip | 3 | **FUNDAMENTAL** | `service/history/api/recordworkflowtaskstarted/api.go:166-182`; `service/history/workflow/context.go:544,641`; `transaction_impl.go:163-196` | None. This is durable execution. You choose which workloads are in scope. Section 2.3 |
| 2 | Persistence is an atomic conditional multi-key commit with two simultaneous conditions | 3 | **FUNDAMENTAL for a raw bucket**, WORK with a transaction layer | `common/persistence/persistence_interface.go:357-369, 435-472` | A bucket's atomic unit is one object; one mutation touches 3 + N + M records. Requires a KV with a partition-pinned `Transact` (linearizable, per row 0), or Postgres |
| 2b | Persistence I/O is serialized **per shard**: `ShardIOConcurrency` defaults to 1 and gates every shard write path | 4 | **HARD** (tunable, but the tuning moves the contention rather than removing it) | `common/dynamicconfig/constants.go:1810-1814`; `service/history/shard/context_impl.go:2066-2072, 2105`, acquired at `:489, 543, 601, 679, 741` | A shard sustains 1/commit-latency. Size the immutable shard count from throughput, not node count (2.3, 2.4). Raising the knob converts semaphore waits into store-side OCC conflicts |
| 3 | `NumHistoryShards` immutable after first boot, enforced by a silent no-op, configured value overwritten | 4, 1 | **HARD** | `common/persistence/cluster_metadata_store.go:167-169, 204-218`; `temporal/fx.go:741, 831-838`; `common/util.go:389-397` | Choose once from expected peak concurrency. Facade hard-errors on mismatch. The real fix is a reshard protocol: months, and section 5 argues it is not worth buying |
| 4 | An LSM-over-bucket writer is single-writer per database and typically does not enforce it | 3, 4 | **WORK** | storage-layer behaviour, see section 7 | One database per (generation, unit, replica) at its own prefix, with fencing at open. Existing embedded KVs already solve this |
| 5 | Cannot prove from Go that the LSM fences **before** taking its WAL-recovery snapshot | 3 | **HARD, and it is a P0** | opaque FFI boundary in the current KV backend | A lost acked write in a workflow history is unrecoverable by retry. Must be closed before anything ships. Section 7 |
| 6 | No anti-entropy re-replication in the candidate KV: a unit written while the cluster was undersized is never backfilled | 4 | **HARD** (in the storage layer, not Temporal) | a repo test records a steady state of 93/107 of 200 keys across a grown pair | Backfill on membership change. This is a data-loss path for histories, not a tidiness issue |
| 7 | Temporal's `range_id` lease and the storage layer's unit epoch are two independent nested leases, **and the storage fence can land at `Commit`** | 3, 4 | **WORK**, with a silent-correctness hazard | `common/persistence/persistence_interface.go:359`; `service/history/workflow/transaction_impl.go:197`; `common/persistence/error_type.go:5-21`; `shale/docs/SPEC.md:799, 801, 1320, 1322` | Putting the `IF range_id = ?` check inside one `Transact` pinned to the shard key is necessary and **not sufficient**: `ErrFenced` is recoded to the transient acquiring error at *any* owner-local op site including `Commit` (`SPEC.md:801`), and "a backend's failed `Commit` is not guaranteed to have finalized the transaction" (`:799`), so the outcome is genuinely **ambiguous**. The wire encoding then destroys the signal: shale maps that transient code to `codes.ResourceExhausted` on every forwarded leg (`:1320, 1322`), and `serviceerror.ResourceExhausted` is in Temporal's **definitely-not-committed** list (`common/persistence/error_type.go:9-16`, returning `false`). So the natural mapping files an ambiguous write as never-applied, `NotifyOnExecutionMutation` is skipped (`transaction_impl.go:197`) and the caller retries a write that may have landed. **Forbid the plugin from mapping the acquiring or fenced code to `ResourceExhausted` or to any `ConditionFailedError` variant; require a class that falls into the default `true` branch.** Acceptance test in M7 |
| 8 | Idle persistence floor is set by shard count, not load | 3 | **WORK** | `common/dynamicconfig/constants.go:2009, 2076, 2151, 2250, 2436` | 27.3 polls/sec and 1.71 writes/sec at 512 shards and zero traffic. Since the shard count is immutable, **you pick your idle bill on day one** |
| 9 | Shard handoff is a steal; the linger window is off by default **and unsafe to enable without `AssertShardOwnership`** | 4 | **WORK**, ordered | `common/dynamicconfig/constants.go:1782-1788` ("Do NOT use non-zero value with persistence layers that are missing AssertShardOwnership support"); `service/history/shard/context_impl.go:1152, 1958`; obligation at `common/persistence/persistence_interface.go:60`; stub at `common/persistence/objstore/shard_store.go:179-184` (returns `nil` unconditionally) | Per-shard write gap on every scale event and rolling restart. **Implement `AssertShardOwnership` as a real persisted-`range_id` compare first, then enable linger.** Enabling it over a stub extends the window in which a dispossessed node keeps writing |
| 10 | Two consistent-hash rings converge independently, **and no `ServerOption` can unify them** | 4 | **HARD** (needs a patch, not configuration) | `common/membership/ringpop/monitor.go:290-353`; `temporal/fx.go:379-381`; 21 options at `temporal/server_option.go:34-203`, none taking a `membership.Monitor` | Deriving engine ownership from storage placement means patching `temporal/fx.go` (**26 commits/yr**), roughly doubling the tracked surface. **Recommendation: do not.** Accept the cross-node hop, measure its rate (section 7 item 8), revisit only on evidence |
| 10b | Ringpop opens a tchannel gossip listener per service; the store supplies only the bootstrap seed set | 2 | **NON-ISSUE** (a port is not a deployable unit) | `common/membership/ringpop/monitor.go:321-356`; `common/membership/ringpop/factory.go:105, 150-163`; `temporaltest/internal/lite_server.go:353-357` | 8 listeners per replica, plus 2 for an embedded shale (`shale/docs/SPEC.md:963`). No seed list for Temporal; a seed list *is* required for shale (`SPEC.md:400, 411`), and that is the part that is real configuration |
| 11 | Engine version skew across replicas during a rolling deploy | 1, 4 | **WORK** | `common/headers/version_checker.go:26-29` | The wire check will not stop you. Pin the fleet version, roll one minor at a time. Exact supported window unverified in this checkout |
| 12 | The library would accept `Shards: N`, silently discard it, and log a `Warn` into the app's log | 1 | **WORK** | `temporal/fx.go:831-838` | Read the persisted count in `New()` and error. Hours of work, but this lie must not survive into the public API |
| 13 | ~60 config fields must be populated in Go; `Persistence.VisibilityStore` must be non-empty or startup fails | 1 | **WORK** | `temporal/server_option.go:34`; `temporal/server_options.go:83-88, 124-135`; `common/config/persistence.go:59-61` | `WithConfig` already takes a struct with no file on disk. Hide it behind defaults. Days |
| 14 | Multi-replica means history and matching bind peer-routable addresses | 2 | **WORK** | `common/config/config.go:75-89` | Unavoidable once N > 1. Mitigate with mTLS and a private network, plus a shared cluster secret on the internal services |
| 15 | Visibility has only `sql` and `elasticsearch` implementations | 3 | **WORK, and the largest new storage design** | `common/persistence/visibility/store/` contains exactly `sql/` and `elasticsearch/` | On a KV you must build a secondary index supporting predicates, sort and pagination. Weeks to months depending on how much of the query language you honour |
| 16 | No in-process transport; `net.Listen` is `Fatal` on failure; `bufconn` appears **0** times | 2 | **WORK** | `common/rpc/rpc.go:161-172` (verified); `grep -rn bufconn --include="*.go" .` returns 0 (verified) | Reducible from 11 listeners to 8 by disabling pprof, prometheus and the Nexus HTTP port. An in-process transport at the `RPCFactory` seam would remove 4 more and is optional; it cannot touch the 4 ringpop listeners (row 10b) |
| 17 | The system worker dials the frontend over gRPC; embedded, the process dials itself | 2 | **WORK, arguably NON-ISSUE** | `service/worker/worker.go:60-61`; `common/sdk/factory.go:91-118` | Three hops inside one process per system workflow task. Latency on background work only |
| 18 | The frontend's client-facing listener | 2 | **NON-ISSUE** | `common/config/config.go:79-86` (`BindOnLocalHost`) | Embedding does **not** force you to serve gRPC to the outside. Bind to loopback; its only mandatory consumer is the process's own system worker. External SDK access is opt-in |
| 19 | The worker role and its "singletons": scanner, scavengers, batcher, scheduler, archival, DLQ | 2 | **NON-ISSUE, conditional on row 0** | `service/worker/scanner/scanner.go:268-277` and `workflow.go:57-75` (both verified) | No extra process and no leader election. "No split brain" holds only because workflow-ID uniqueness is the same conditional write as `range_id`, so it is exactly as linearizable as the store. Section 4.5 |
| 20 | Adding a node | 4 | **NON-ISSUE, conditional on row 0** | `common/membership/ringpop/monitor.go:237, 290, 328`; `common/util.go:389-397` | Temporal discovery is via the shared store, so a new replica needs no Temporal config beyond the same bucket; the store needs a seed list (row 10b). Shard moves copy zero bytes. Rate limits redivide automatically. Acceptance test in M6 |
| 21 | Retention | 2 | **NON-ISSUE** | `service/history/tasks/workflow_cleanup_timer.go:15`; `service/history/timer_queue_active_task_executor.go:119-120` | A per-workflow timer task, not a sweeper daemon |
| 22 | Cross-region replication, archival, Nexus, schedules, batcher | 2 | **NON-ISSUE when off, but two default ON** | `service/history/tasks/category.go:59, 71, 83`; `service/frontend/service.go:492-501`; archival's zero value is valid-disabled at `common/config/archival.go:29-40`; **`WorkerEnableScheduler` defaults `true` (`common/dynamicconfig/constants.go:3248-3251`)** and **`EnableBatcherNamespace` defaults `true` (`:3198-3201`)**, with `WorkerPerNamespaceWorkerCount` defaulting to 1 (`:3233-3237`) | Replication, archival and Nexus default off or empty. **Schedules and the batcher start a per-namespace worker per namespace unless explicitly disabled**, so `New()` must set both booleans false for the idle-floor arithmetic in 2.3 to hold. Config synthesis step, hours |
| 23 | Workflow-code determinism across versions | 1, 4 | **NON-ISSUE** | `GetVersion`/`Patch`, worker versioning, sticky-queue fallback | Identical to any Temporal deployment. Embedding makes it slightly easier: workflow code and engine ship in one artifact, so "which build produced this history" is answerable from one version string |
| 24 | Running all roles in one process | 1, 2 | **NON-ISSUE** | `temporal/server_impl.go:44-53`; `temporal/server.go:16-19, 44`; `temporal/fx.go:308-322`; `temporaltest/server.go:163` | Already supported, already shipped. The "typically a development server" comment is a statement about Temporal's confidence, not about the mechanism |

### 3.1 The non-issues, called out

Eight of the twenty-seven rows look like blockers and are not. Four deserve naming because they are the ones people reject the whole idea over:

1. **The worker role is not a daemon** (row 19). It is a role in the same binary, its cluster-wide jobs are workflows deduped by workflow ID, and its per-namespace workers shard by the same hash ring as history.
2. **Embedding does not expose your app to the internet** (row 18). `BindOnLocalHost` binds the frontend to loopback and the only mandatory consumer is the process itself.
3. **Adding a node is already trivial** (row 20). One integer, zero byte movement.
4. **Elasticsearch is optional** (row 22 and row 15's fine print). SQL-backed standard visibility exists, supports custom search attributes through a JSON column and a per-dialect query converter (`common/persistence/visibility/store/sql/visibility_store.go:781-800`), and ships in a SQLite dev config. The real conflict is that there is no **KV** visibility implementation, which is work, not a wall.

**And one that looks like a non-issue and is not:** the port count (row 10b). Ten open listeners per replica sounds like a topology, and it is not one, but the accompanying claim of "no seed list" is only true for Temporal. The store brings its own.

### 3.2 One landmine

Do not reach for `WithStaticHosts` as the multi-node answer. `staticResolver.Lookup` is `hash % len(hostInfos)` (`common/membership/static/service_resolver.go:56-63`), which is **not** consistent hashing: adding one host remaps nearly every shard. And `LookupN` ignores `n` and returns exactly one host (`:66-72`), so every per-namespace worker piles onto a single node. It is a development-mode path. Use the store-backed membership described in section 2.4.

---

## 4. What the library looks like

Placeholder name `dex`. Module path `example.com/dex`. **The library owns the server side only.** Workflow code is written against the unmodified Temporal Go SDK. The library never reimplements determinism, replay, or the command protocol, because it is running Temporal's engine.

### 4.1 The public API

```go
package dex // example.com/dex

// Config is the whole configuration surface. Everything except Store has a
// working default.
type Config struct {
	// --- required ---
	Store store.Store // the durable substrate. See 4.6.

	// --- identity: per replica, must be unique and stable ---
	NodeID    string // default: hostname + pid
	Advertise string // "host:port" peers dial. default: derived from Listen

	// --- topology, chosen once per cluster ---
	Shards int // history shard count. IMMUTABLE after first boot.
	        //   New() reads the persisted value and ERRORS on mismatch.

	// --- transport ---
	Listen string // ":7233" opens the client-facing gRPC listener.
	       //       "" binds it to loopback only.
	TLS           *tls.Config
	ClusterSecret []byte // gates internal services when they share Listen

	// --- the default worker (optional convenience) ---
	Namespace     string // default "default"
	TaskQueue     string // if set, Start() runs a worker on it
	WorkerOptions worker.Options

	// --- cluster policy: must agree across replicas ---
	Durability    store.Durability // Strict (default) | Relaxed
	DataConverter converter.DataConverter

	// --- observability ---
	Logger  log.Logger
	Metrics metrics.Handler
}

type Engine struct{ /* unexported */ }

// New validates cfg and constructs. It performs no network I/O except the
// single cluster-metadata read that verifies Shards.
func New(cfg Config) (*Engine, error)

// Registration, on the default worker. Same argument shapes as the SDK.
func (e *Engine) RegisterWorkflow(w any)
func (e *Engine) RegisterWorkflowWithOptions(w any, o workflow.RegisterOptions)
func (e *Engine) RegisterActivity(a any)
func (e *Engine) RegisterActivityWithOptions(a any, o activity.RegisterOptions)

// NewWorker is the general form: one worker per task queue, returning the
// SDK's own worker.Worker.
func (e *Engine) NewWorker(taskQueue string, o worker.Options) worker.Worker

// RegisterSingleton runs w exactly once cluster-wide. See 4.5. The mutual
// exclusion is workflow-ID uniqueness, not a lease.
func (e *Engine) RegisterSingleton(name string, w any, o SingletonOptions)

// Start brings the node up and RETURNS. It does not block.
func (e *Engine) Start(ctx context.Context) error

// WaitReady blocks until this node owns at least one shard and its storage is
// mounted. Wire this to a readiness probe, not Start.
func (e *Engine) WaitReady(ctx context.Context) error

// Stop drains gracefully. Honors ctx as the drain deadline.
func (e *Engine) Stop(ctx context.Context) error

// Client returns the standard SDK client. StartWorkflow / Signal / Query /
// Update / Schedules are the SDK's own methods, unchanged, so this code moves
// to a hosted Temporal cluster by swapping the constructor.
func (e *Engine) Client() client.Client

func (e *Engine) Addr() string
func (e *Engine) Health() Health

type Health struct {
	Ready          bool
	OwnedShards    int
	MountedFraction float64
	LostShards     int    // shards lost to a range_id steal since Start
	LastStoreError string
}
```

There is deliberately **no** `dex.StartWorkflow`, `dex.Signal`, `dex.Query`. Those are `client.Client` methods, and reinventing them would break the compatibility claim. `Engine.Client()` is the whole client story, and it is what makes "move to Temporal Cloud later" a one-line change.

### 4.2 Hello world

```go
package main

import (
	"context"; "log"; "time"

	"example.com/dex"
	"example.com/dex/store/kvstore"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

func Greet(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})
	var out string
	return out, workflow.ExecuteActivity(ctx, SayHello, name).Get(ctx, &out)
}

func SayHello(ctx context.Context, name string) (string, error) {
	return "hello " + name, nil
}

func main() {
	st, err := kvstore.Open(kvstore.Config{
		Endpoint: "objects.example.com", Bucket: "acme-wf", Prefix: "prod/",
	})
	if err != nil { log.Fatal(err) }
	defer st.Close()

	eng, err := dex.New(dex.Config{Store: st, Shards: 4, TaskQueue: "greet"})
	if err != nil { log.Fatal(err) }
	eng.RegisterWorkflow(Greet)
	eng.RegisterActivity(SayHello)

	ctx := context.Background()
	if err := eng.Start(ctx); err != nil { log.Fatal(err) }
	defer eng.Stop(ctx)

	run, err := eng.Client().ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{ID: "greet-1", TaskQueue: "greet"}, Greet, "world")
	if err != nil { log.Fatal(err) }
	var out string
	if err := run.Get(ctx, &out); err != nil { log.Fatal(err) }
	log.Println(out) // hello world
}
```

30 lines of body. No server to run, no `temporal server start-dev`, no sidecar.

### 4.3 The same program as three replicas

The workflow code, the registrations and the client calls are byte-identical. Only `Config` and the deployment change.

```go
	st, err := kvstore.Open(kvstore.Config{
		Endpoint: os.Getenv("OBJ_ENDPOINT"), Bucket: "acme-wf", Prefix: "prod/",
		ReplicationFactor: 2,                               // required, see 4.4 and section 7
		Seeds:             strings.Split(os.Getenv("KV_SEEDS"), ","), // the store gossips; the engine does not
		GossipAddr:        ":7946",
		Linearizable:      true,                            // section 7 item 0. New() refuses false at N>1
	})
	...
	eng, err := dex.New(dex.Config{
		Store:         st,
		Shards:        64,                      // chosen once. New() errors on mismatch.
		TaskQueue:     "greet",
		NodeID:        os.Getenv("POD_NAME"),   // stable per replica
		Advertise:     net.JoinHostPort(os.Getenv("POD_IP"), "7233"),
		Listen:        ":7233",
		ClusterSecret: []byte(os.Getenv("DEX_CLUSTER_SECRET")),
	})
	...
	http.HandleFunc("/greet", func(w http.ResponseWriter, r *http.Request) {
		// Any replica may serve this. The engine routes to the shard owner.
		run, err := eng.Client().ExecuteWorkflow(r.Context(),
			client.StartWorkflowOptions{ID: "greet-" + name, TaskQueue: "greet"}, Greet, name)
		...
	})
	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !eng.Health().Ready { http.Error(w, "not ready", 503) }
	})
```

```yaml
kind: StatefulSet                  # not Deployment: seeds need stable pod DNS names
spec:
  serviceName: app-peers
  replicas: 3                      # <- this is "add more nodes"
  template:
    spec:
      containers:
      - name: app
        # 7233 client+peer gRPC; 7946/7947 the store's gossip and data ports.
        # The 4 ringpop tchannel ports are pod-local and need no Service entry.
        ports:
        - {containerPort: 8080}
        - {containerPort: 7233, name: peer}
        - {containerPort: 7946, name: kv-gossip}
        - {containerPort: 7947, name: kv-grpc}
        env:
        - {name: POD_NAME, valueFrom: {fieldRef: {fieldPath: metadata.name}}}
        - {name: POD_IP,   valueFrom: {fieldRef: {fieldPath: status.podIP}}}
        - {name: KV_SEEDS, value: "app-0.app-peers:7946,app-1.app-peers:7946"}
        readinessProbe: {httpGet: {path: /readyz, port: 8080}}
---
kind: Service                      # headless: peers need pod-to-pod addressability
metadata: {name: app-peers}
spec:
  clusterIP: None
  selector: {app: app}
  ports: [{port: 7233}, {port: 7946}, {port: 7947}]
```

Scaling out is `replicas`, and nothing else. The one piece of real configuration is `KV_SEEDS`, which is stable across scale events because seeds only have to be *reachable*, not exhaustive: a StatefulSet's first two ordinals serve for any replica count. Note the workload must be a StatefulSet rather than a Deployment for those DNS names to exist.

### 4.4 The node lifecycle

**`New()`** validates, synthesizes the full Temporal `config.Config` in Go, and performs exactly one store read: the cluster-metadata row, to verify `Shards`. `New()` failing means your configuration is wrong. It never means the cluster is down.

Four synthesis steps are load-bearing rather than cosmetic, because each one is a default that is wrong for this shape: set `worker.enableScheduler` and `worker.enableNamespaceBatcher` to **false** (both default `true`, row 22), leave `history.shardLingerTimeLimit` at **0** until `AssertShardOwnership` is real (row 9), and refuse `R=1` when more than one replica is configured (4.4, below).

**`Start()`** returns as soon as the node can accept traffic. Readiness is separate.

| # | Step | I/O | Typical |
| ---: | --- | --- | --- |
| 1 | Join membership: upsert this node's row | 1 conditional write | one object round trip |
| 2 | Read cluster metadata; create-if-absent on an empty bucket | 1 read (+1 write if founding) | one to two round trips |
| 3 | Storage mount: open the databases this node owns | many | **17 to 21 s observed** on a 3-node cluster on object storage (measured, storage layer) |
| 4 | Open the 8 Temporal listeners (4 gRPC, 4 ringpop), register services, serve | none | ms |
| 5 | Acquire shards: conditional `range_id` bump per owned shard | 1 write per shard | one round trip each, concurrent |
| 6 | Start per-shard queue processors | scans | ms once mounted |
| 7 | Start in-process SDK workers, begin long polls | none | ms |

Steps 1, 2 and 4 complete before `Start` returns. Steps 3, 5, 6, 7 run in the background, and `WaitReady` gates on them.

**One ordering constraint is load-bearing:** step 5 must never run ahead of step 3 for a given shard. Taking the `range_id` fences the incumbent out (`service/history/shard/context_impl.go:1152-1185`), and the incumbent only discovers it at its next write. Acquiring before your storage is mounted converts a one-write cutover into a 17-to-21-second outage.

**A second replica boots.** Membership visible to peers within one poll interval (about 1 s with a store-backed coordinator), a rebalance debounce (about 5 s, reset by each further membership event), then storage ownership moves. At R>=2 the outgoing owner keeps serving until the successor's serving marker is durable, so reads and writes stay available across the mount. Temporal then sees the ring change and moves shards. **Worst case for a single workflow: one in-flight drain plus one conditional write, order 100 ms to 1 s.**

At **R=1 this is materially worse**: there is no union, so a handoff is a genuine per-unit write outage bounded by the storage write timeout (about 5 s) against a mount that takes 17 to 21 s. The budget expires first. **R=1 multi-node is not a supported configuration, and `New()` should refuse it.**

**SIGTERM during a rolling deploy.**

```
1. Mark Draining in membership (1 conditional write). Peers stop routing here
   within one poll (about 1 s).
2. Stop accepting new external RPCs; in-flight ones run to completion.
3. Stop the SDK pollers. Long polls return EMPTY, not an error. Wait for
   in-flight ACTIVITIES up to worker.Options.WorkerStopTimeout.
4. Let in-flight workflow tasks complete: bounded by the workflow task
   timeout, default 10 s (common/primitives/constants.go:16).
5. Release shards so a successor does not wait for a lease timeout.
6. Flush storage so the successor's mount recovers a minimal WAL tail.
7. Leave membership. Close.
```

`Stop(ctx)` honors `ctx` as the total drain deadline. A sensible `terminationGracePeriodSeconds` is `WorkerStopTimeout + 10 s + flush`, call it 45 s. With `maxUnavailable: 1` and a readiness gate on `WaitReady`, worst-case unavailability for a single workflow is the same 100 ms to 1 s as scale-out. **Without the readiness gate you get the hard-kill number below on every rollout**, which is the single most common way to make this design look bad.

**A replica dies hard.** No graceful leave, so detection is observer-side lease expiry.

| Phase | Cost |
| --- | ---: |
| Membership expiry: ringpop SWIM failure detection, not a store poll | ~5 s (unverified in this checkout; the store-membership cutoff of 20 s at `common/membership/ringpop/monitor.go:35` gates only the bootstrap seed set, not liveness) |
| Rebalance settle | 5 s |
| Storage mount of the promoted position | 17 to 21 s |
| `range_id` acquire | 1 round trip |
| **Total** | **~27 to 31 s** (composed estimate, not measured end to end) |

During that window, calls for those shards fail fast with `Unavailable`; they do not hang. **Nothing is lost**, provided durability is `Strict` or R>=2: in-flight workflow tasks were recorded Started with a `StartToCloseTimeout` (default 10 s) and are redelivered by the new owner's timer queue; in-flight activities are covered by their own timeouts and retry policies. Activities must be idempotent, which is already the Temporal contract.

**Two numbers to judge this design on: about 1 second for a planned event, about 30 seconds for an unplanned one.** The 30 decomposes into 10 s of policy (detection and settle, both tunable down at the cost of false positives) and 17 to 21 s of physics (opening an LSM from object storage). Only the physics half is hard.

### 4.5 The singleton mechanism

**Cluster-singleton work runs as a workflow with a fixed workflow ID, started idempotently by every replica.** No election, no lease, no well-known-shard assignment, no new mechanism of any kind.

**The mechanism is exactly as strong as the store, and no stronger.** The "election" is the current-execution-row conditional write, fenced by the shard's `range_id`. That is the same CAS as row 0, so on an AP store two partitioned replicas can each start "the" singleton and LWW picks a winner on heal. Every claim in this section is conditional on section 7 item 0 closing. It is not an independent guarantee, and treating it as one is the failure mode this section exists to prevent.

This is not an invention; it is exactly what Temporal already does for its own scavengers (`service/worker/scanner/workflow.go:57-75` and `scanner.go:268-277`, both verified in section 2.2). The library exposes it directly as `RegisterSingleton`.

**Why not the alternatives.**

- **Leader election over the store.** Rejected because it introduces a *second* fence chain (a lease epoch) that has to stay consistent with the *first* (the shard `range_id`). Every bug in that class is a split-brain bug, and the two chains disagree at exactly the moment you need them not to. The smell is visible in Temporal today: the Nexus endpoints table uses a ring lookup for reads but for writes says "let persistence verify table ownership" (`service/matching/matching_engine.go:2683-2684, 2695-2704, 2746`). Two mechanisms, one advisory and one authoritative. Keep only the authoritative one.
- **Assignment to the owner of a well-known shard.** The same objection in weaker form: membership is not a fence. Temporal's own ownership pre-check is advisory; the actual fence is always the `range_id` conditional write.
- **Partitioning the work per shard.** Correct for queue processors and timer scanning, and Temporal already does it there. It is not an answer for genuinely global work.

**The strongest form of the argument: there is no cluster-singleton at the engine layer at all.** Everything a Temporal deployment normally runs a worker process for is either per-shard (one owner, one fence) or a workflow. That is why "no extra process" is achievable rather than a slogan.

**What breaks when a replica dies:** nothing named "leader", because there is no leader. A shard moves, and a cron workflow's next fire time is workflow state on the new owner's timer queue.

**The failure mode is a delayed run when the outage is shorter than one cron period, and a skipped one otherwise.** Temporal's cron scheduler does not queue missed fires. `GetBackoffForNextSchedule` computes the next instant after the last scheduled time and then advances past everything already in the past: `for !nextScheduleTime.IsZero() && nextScheduleTime.Before(nowUTC) { nextScheduleTime = schedule.Next(nextScheduleTime) }` (`common/backoff/cron.go:49-54`). The hard-death recovery in 4.4 is 27 to 31 seconds, so a singleton on a sub-minute cadence loses runs at exactly the failure this section describes.

**`RegisterSingleton` therefore does not promise "no skipped runs", and must not claim to.** Callers that need every tick to execute get one of two shapes instead: a workflow that reads its own last-completed instant from durable state, computes the fire times it owes, and continues-as-new; or Temporal Schedules with an explicit catchup window. Both are ordinary workflow code and neither needs a new engine mechanism, but both are opt-in and neither is what a bare `CronSchedule` string gives you.

### 4.6 The transport design

**Ten listeners inside the process, of which exactly one is client-facing.** Four gRPC (`RPCConfig.GRPCPort` per service), four ringpop tchannel (`RPCConfig.MembershipPort` per service, `common/membership/ringpop/factory.go:150-163`), and two for an embedded shale (memberlist plus gRPC, `shale/docs/SPEC.md:963`). When `Listen` is empty, the four gRPC listeners bind loopback and the process is closed to external clients: the only consumers of the frontend are the process's own system worker and the in-process SDK client. When `Listen` is set, the client-facing frontend binds it, and internal services are registered on the **same** `grpc.Server` (gRPC multiplexes by fully-qualified service name, so one `net.Listener` carries all of them).

**Authentication is mandatory once `Listen` is public.** A unary interceptor rejects any internal-service method whose metadata lacks a MAC over `(method, deadline, nonce)` keyed by `Config.ClusterSecret`. `Listen` on a public interface with no `ClusterSecret` is a `New()` error.

**Discovery needs no new mechanism *for Temporal*; the storage layer is a different story.** Temporal's membership rows carry every node's advertise address and live in the same store as the data, so a Temporal replica needs **no seed list** (`common/membership/ringpop/monitor.go:321-356`). It still opens a ringpop tchannel port per service, because the store is ringpop's seed source, not its transport. shale is the opposite: it gossips over memberlist and **does** need configured seed addresses (`shale/docs/SPEC.md:400, 411, 963`). **Net: no seed list for the engine, one seed list for the store, and 10 open ports.** The 4.3 manifest carries both.

**Node-to-node RPC is unavoidable and should not be routed through the bucket.** An SDK client is a plain gRPC client and will not route by workflow ID, so any replica can receive any call. Forwarding via the bucket would add one object-store round trip per hop (about 30 ms on S3 Standard, estimated), turning a 60 ms workflow-task round trip into 120 ms plus a poll interval. The port is cheaper than the physics.

**An in-process transport is a later optimization, not a requirement.** Temporal has none today (`bufconn` count: 0), and adding one at the `RPCFactory` seam removes loopback sockets but changes nothing about the deployable-unit count. Do it only if socket count becomes an operational problem.

**What this honestly costs beyond "one binary":** a StatefulSet with a headless Service so every replica has a stable advertise address and the store's seeds have stable DNS names, one shared cluster secret to distribute, the store's seed list, and a second class of traffic on the application's own port that shows up in its metrics and access logs.

### 4.7 The backend port

The library defines a narrow store port; each backend is one adapter file plus one wiring line. No business code names a provider.

```go
package store

type Store interface {
	// Partition-pinned transaction. fn builds a read-set of conditions and a
	// write-set of mutations; the whole thing applies atomically or not at all.
	// pin co-locates every key touched, so a Temporal shard maps to one pin.
	Transact(ctx context.Context, pin Key, fn func(Tx) error) error

	Get(ctx context.Context, k Key) ([]byte, error)
	Put(ctx context.Context, k Key, v []byte) error
	Delete(ctx context.Context, k Key) error
	ScanPrefix(ctx context.Context, p Key, opt ScanOptions) (Iter, error)

	// Placement feeds the engine's ownership so both rings are one fact.
	Placement() <-chan Placement

	Flush(ctx context.Context) error
	Close() error
}

type Durability int
const (
	Strict  Durability = iota // ack means durable in the object store
	Relaxed                   // ack means replicated to R nodes, flush is async
)
```

Adapters: `kvstore` (embedded distributed KV over object storage), `pgstore` (Postgres, for teams that already run one and are willing to give up requirements 2 and 3), `memstore` (tests).

**The error taxonomy is part of the port, it is not optional, and it fails dangerously in one direction and merely noisily in the other.** The authority is `common/persistence/error_type.go:5-21`: `OperationPossiblySucceeded` returns `false` for `CurrentWorkflowConditionFailedError`, `WorkflowConditionFailedError`, `ConditionFailedError`, `ShardOwnershipLostError`, `InvalidPersistenceRequestError`, `TransactionSizeLimitError`, `AppendHistoryTimeoutError`, `serviceerror.ResourceExhausted`, `serviceerror.NotFound` and `serviceerror.NamespaceNotFound`, under the comment "Persistence failure that means that write was definitely not committed", and `true` for everything else.

- **Too-ambiguous is noise.** An adapter that returns an unclassified error for every hiccup lands in the `true` branch, and the shard unloads. Annoying, self-healing, visible in metrics.
- **Too-certain is silent corruption, and it is the default you fall into.** shale recodes `ErrFenced` at *any* owner-local op site including `Commit` to the transient acquiring error (`shale/docs/SPEC.md:801`) while conceding "a backend's failed `Commit` is not guaranteed to have finalized the transaction" (`:799`), and puts that code on the wire as `codes.ResourceExhausted` (`:1320, 1322`). Map it through naively and an ambiguous outcome is filed as definitely-not-committed: `NotifyOnExecutionMutation` is skipped (`service/history/workflow/transaction_impl.go:197`) and the caller retries a write that may have landed.

**Rule for the adapter: the acquiring and fenced classes must surface as an error type that falls into the default `true` branch, never as `ResourceExhausted` and never as any `ConditionFailedError` variant.** Only outcomes the store can prove did not apply (a read-set conflict evaluated before any write, an ownership refusal taken before opening a transaction, `shale/docs/SPEC.md:792`) may be mapped to the `false` list. M7 pins this with a fence injected at `Commit`.

### 4.8 Where two design passes disagreed, and what was chosen

Two independent design passes were run on this question and they diverged on one point: whether to build the engine or to run Temporal's.

- One pass designed a from-scratch engine with its own ownership vocabulary (leaves and partitions), its own matching layer, and an in-process transport.
- The other found the two public extension points and concluded the engine should be Temporal's.

**Chosen: run Temporal's engine.** The reason is that the from-scratch pass, priced honestly, costs 82 engineer-weeks and reaches 45% to 58% of the compatibility corpus, while the plugin pass costs 62 and reaches 100% of what Temporal supports. The from-scratch pass also does not escape partitioning: without a shard there is nowhere for timer scanning to live, so it reintroduces a fixed count of timer partitions, which is Temporal's shard concept rediscovered with a cheaper migration.

**What survives from the from-scratch pass** is everything above the engine: the public API in 4.1, the lifecycle contract in 4.4, the singleton mechanism in 4.5 (which both passes independently chose), and the store port in 4.7. What is dropped is the leaf/partition model and the custom matching engine.

### 4.9 The prototype already in this checkout, and what it proves

`common/persistence/objstore` is a working partial implementation of exactly the plugin described above: **22 non-test files, 6,655 lines**, wired through the public seam at `cmd/server/main.go:239-240` (`WithCustomDataStoreFactory` plus `WithCustomVisibilityStoreFactory`) with two YAML profiles (`config/development-objstore.yaml`, `config/development-objstore-minio.yaml`) and four blob backends (`filefs`, `memfs`, `sharedmem`, `s3`). It implements `ShardStore`, `ExecutionStore`, `TaskStore`, `MetadataStore`, `ClusterMetadataStore`, `NexusEndpointStore`, Queue v1, Queue v2, history v2 and `VisibilityStore`: line items 1 through 10 of the estimate below, 36 of its 62 weeks. It boots `temporal-server` end to end.

**It is nonetheless not a 36-week credit, and the reason is the row that matters most.**

- **It fences nothing on the execution write path.** `grep -c RangeID common/persistence/objstore/execution_store.go` returns **0**. `RangeID` appears only in `shard_store.go` and `task_store.go`. A history host that has lost its shard lease can still write mutable state, history nodes and history tasks. That is row 7's hazard, already realized.
- **It has no transaction.** `UpdateWorkflowExecution` decomposes the atomic multi-key commit into a read plus an ETag-conditional `Put` of the snapshot (`execution_store.go:187, 219-228`), then independent unconditioned writes of tasks (`:236-241`) and history nodes (`:243-256`), with no rollback. The current-run pointer update swallows its own error by design (`:264-278`: `_, _ = e.blob.Put(...)` under a comment accepting the lost write). Any failure between those steps leaves a state no retry restores.
- **`AssertShardOwnership` is a stub** returning `nil` unconditionally (`shard_store.go:179-184`), which is why row 9 forbids enabling linger.
- **No conformance runner references it.** Grepping `objstore` under `common/persistence/tests/`, `tests/` and the `Makefile` returns nothing. The "53/53 conformance on MinIO" in commit `899fca7` has no artifact in the repo; the sibling analysis in `docs/objstore/EMBEDDED-DESIGN.md:148-175` traces the same number to an earlier project's result file and concludes it is unrecorded either way.

**What it does prove, which is worth a great deal:** the public seam works. `WithCustomDataStoreFactory` is sufficient to run a whole Temporal server on a wholly foreign store with no fork, which is the single load-bearing assumption of P1, and it is now demonstrated rather than argued. **What it does not prove is any correctness property**, because the one oracle it was run against cannot fail on torn writes.

**Effect on the estimate: 6 weeks of credit against items 1, 3, 8 and 9** (keyspace design, namespaces, queues, Nexus endpoints, where the absence of a transaction costs least), and **zero against items 2, 4, 5, 6 and 12**, which have to be rewritten around a real `Transact` rather than extended. That credit is the explicit negative row in the P1 table, and the 62-week total is net of it.

---

## 5. The paths

Counting basis: this checkout contains **410,135 lines** of Go excluding `_test.go`, `*.pb.go` and `*_mock.go` (counted here). `service/history` alone is **100,432**, `chasm` 25,228, `service/matching` 22,819, `service/worker` 21,628, `service/frontend` 17,906, `common/persistence` 94,154. Commit rates come from the GitHub API against upstream `temporalio/temporal`, not from this shallow fork (see Provenance): **1,861 commits** in the 12 months to 2026-08-14, of which `service/history` took 471, `common/persistence` 133, `temporal/fx.go` 26 and `common/membership` 10.

### P1. Embed and extend (recommended)

**Build:** a storage plugin implementing the 108 + 13 persistence methods over a **linearizable** transactional KV, plus the facade from section 4.

**Reuse:** all 410,135 lines. Every event type, command type, timer, retry, buffered signal, sticky queue, reset, update, schedule, child workflow and Nexus. All five official SDKs, the CLI, the Web UI.

**Throw away:** nothing structural. Configured off: Elasticsearch, cross-region replication, archival, pprof, prometheus, schedules, the batcher.

**Failure model, and the precondition it rests on.** A node dies, surviving nodes re-point its shards, the new owner conditionally bumps `range_id`, the old owner discovers the loss at its next write. **Temporal's safety does not depend on membership being correct.** It depends entirely on the per-shard `range_id` compare-and-set being **linearizable**. Given that, two nodes that both believe they own a shard both attempt the bump, the store serializes, and one gets `ShardOwnershipLost`. This is the single most important safety property of P1 and it is **inherited, which means it is only as good as what it is inherited from**.

**A partition-pinned `Transact` alone does not supply it.** shale's is atomic and, in steady state, correctly serialized under a per-node lock (`shale/docs/SPEC.md:805`), but under partition both sides accept writes with LWW on heal (`:928`) by explicit design (`:938`), the ownership gate reads the local ring (`:792`), and a minority of one computes `W=1` (`:531`). Both nodes then pass the CAS, both allocate task IDs, and one side's history is discarded silently on heal. **P1's failure model is correct on a CP store and unsound on an AP one**, which is why item 17 exists and why section 7 item 0 is a P0. This does not change the path: it changes which store you may ship on, exactly like items 1 and 2.

**Upstream tracking surface, in commits over the last 12 months:**

| File you track | Commits/yr |
| --- | ---: |
| `common/persistence/persistence_interface.go` | 3 |
| `common/persistence/client/abstract_data_store_factory.go` | 1 |
| `temporal/server_option.go` | 6 |
| `common/persistence/visibility/store/visibility_store.go` | 6 |
| `common/membership/interfaces.go` | 1 |
| **Total tracked** | **17** |
| (whole repo, which you do **not** track) | 1,861 |

**1 to 2 engineer-weeks per year** to rebase across quarterly releases. Named risk: `WithCustomDataStoreFactory` carries "this option is experimental and may be changed or removed in future release" (`temporal/server_option.go:145`, verified). It changed once in 12 months. **This budget holds only while you carry no patch.** Two temptations would break it, and both are declined above: patching `temporal/fx.go:379-381` for a custom membership monitor or for ring co-location adds a file with **26 commits/yr** and roughly triples the tracked surface. Take the ringpop ports and the cross-node hop instead.

**Effort: 62 engineer-weeks.** Items 1 through 16 are the plugin and facade; 17 through 19 are the corrections this document's ledger forces.

| # | Line item | Weeks |
| ---: | --- | ---: |
| 1 | Keyspace design and written spec (executions, sub-maps, 6 task categories, history nodes and branches, task queues, namespaces, membership rows) | 3 |
| 2 | ShardStore (5) + ClusterMetadataStore (8, including the 3 membership methods) | 3 |
| 3 | MetadataStore (9, namespaces) | 2 |
| 4 | ExecutionStore A, mutable state, versus the conformance suite | 8 |
| 5 | ExecutionStore B, tasks, including the 5 replication-DLQ methods | 4 |
| 6 | ExecutionStore C, history v2 (Append/Read/Fork/Delete/GetTree) | 4 |
| 7 | TaskStore (14, including fair task queues) | 4 |
| 8 | Queue (12) + QueueV2 (5) | 2 |
| 9 | NexusEndpointStore (5) | 1 |
| 10 | VisibilityStore (13), standard visibility, restricted query subset | 5 |
| 11 | Facade: config synthesis, ports, lifecycle, shard-count guard, registration passthrough | 3 |
| 12 | Wire the shared conformance suite and get **110 test methods** green | 4 |
| 13 | `temporalio/features` run against the embedded engine, triage, document the pass set | 3 |
| 14 | Multi-node: N replicas on one bucket, R>=2, prove shard steal and ownership churn under `kill -9` | 5 |
| 15 | Close the storage-layer durability questions from section 7, or ship a mitigation | 3 |
| 16 | Buffer (history branch forking and the reset paths are genuinely subtle) | 5 |
| 17 | **Linearizable per-shard fence** (section 7 item 0): make the store CP for the fenced keys, or refuse minority writes rather than accept at W=1, or route the shard row and current-execution row to a CP store. Includes a partition test showing two writers cannot both pass the CAS. Range 4 to 8 depending on which close is taken | 6 |
| 18 | **Error-taxonomy adapter and its fence-at-`Commit` test** (row 7, 4.7): classify every store outcome against `common/persistence/error_type.go:5-21` and prove an ambiguous commit is not filed as definitely-not-committed | 1 |
| 19 | **Operations**: backup and restore of the bucket prefix, bucket-unavailable behaviour, a stuck-workflow runbook, tracing across the loopback hops, and the alert set (8a) | 2 |
| | *Credit for the existing `objstore` prototype (4.9), against items 1, 3, 8 and 9 only* | **-6** |
| | **Total** | **62** |

### P2. Fork and replace the coordination layer

**Build:** P1, plus rip out Temporal's shard and replace it with the storage layer's unit, so there is exactly one coordination system and the unit count can change online.

**Argue it, then refute it.** The tempting argument is elasticity: a modern embedded KV can reshard online and Temporal cannot, so unifying buys the ability to move the ceiling. **The argument is stronger than it first looks**, because the shard count is a throughput ceiling as well as a node ceiling (2.3, ceiling B): getting it wrong costs headroom, not just fleet size, and there is no in-place fix.

**It still loses, and the corrected arithmetic is why.** The ceiling is bought, not built. At 30 ms per commit a shard carries 16.7 workflow-task round trips/sec, so **512 shards buy 8,550 WFT/sec** and 512 nodes, for an idle floor of 27.3 polls/sec and 1.71 writes/sec, roughly $50/month at S3 list price as an upper bound (2.3). At S3 Express latency the same 512 shards buy 39,936. **The elasticity P2 sells costs 35 extra weeks and 8 weeks a year to avoid a $50/month over-provisioning bill**, on a number you can compute on day one from a latency measurement you have to take anyway (section 7 item 3). Buy the headroom instead: pick the shard count from `target_WFT_per_sec x 2 x measured_commit_latency`, double it, and stop thinking about it.

**The price of buying that knob.** The Temporal shard is not merely a placement key. It is (a) the leading PK component of every table, (b) the `range_id` lease that fences task-ID allocation, and (c) the container for per-category queue ack levels in `ShardInfo`. Replacing it means forking `service/history` (100,432 lines) and rewriting its shard package, its task-ID allocator and its ack bookkeeping. **471 commits per year in `service/history` become yours, forever.**

**Effort: 97 engineer-weeks upfront, plus about 8 per year forever.**

| # | Line item | Weeks |
| ---: | --- | ---: |
| 0 | All of P1 | 62 |
| 1 | Rewrite `service/history/shard` (context, controller, ownership, task key manager) | 10 |
| 2 | Rewrite queue-processor ack bookkeeping to be per-unit | 8 |
| 3 | Rewrite task-ID allocation without a shard range lease | 6 |
| 4 | `ShardInfo` proto change plus a migration | 3 |
| 5 | Re-verify the history test corpus against the new shard model | 8 |

**Verdict: no.** 35 extra weeks and a permanent 8 weeks per year to buy a knob you can set correctly once for the price of an idle floor.

### P3. New engine, Temporal-API compatible

**Build:** the whole event-sourced core on the store port, speaking the Temporal wire protocol. Domain layer free of I/O (aggregate root `Execution`, transactional boundary = one execution's history plus its emitted tasks, pinned by `hash(namespaceID + workflowID)`), application services above it, adapters below, and roughly 25 of the 121 RPCs at the edge.

**The genuine win:** no shard-count ceiling at all, because the pin is the workflow, not a shard. That removes both ceilings from 2.3 at once, which is P3's strongest argument and is worth more than it first appears.

**What P3 does not win:** the linearizability requirement. Pinning per workflow rather than per shard changes the granularity of the fence, not its nature: two partitioned nodes writing the same execution still need exactly one to win. Item 17's 6 weeks is owed under P3 too, and P3 pays it without inheriting Temporal's `range_id` machinery to hang it on. **Row 0 is not a reason to prefer P3.**

**The cost of that win, stated so nobody is surprised:** without a shard you lose batched shard-scoped task-ID allocation and shard-level queue ack levels, so timer scanning has nowhere to live. The answer is a fixed count of **timer partitions** used only as an ordered index, keyed `t/<part>/<fire_ts>/<seq>`. That is Temporal's shard concept rediscovered, with a cheaper migration (reindex, not a data move). P3 does not escape partitioning; it makes the partition count a property of an index rather than a component of every primary key.

**What you must reimplement:** 60 history event types and 17 command types (`go.temporal.io/api/enums/v1` declares 61 and 18 including UNSPECIFIED), the workflow-task lifecycle including transient tasks at attempt > 1 that are never persisted and the sticky-queue fallback with its partial-versus-full-history decision, server-side activity retry, all six timeout families, buffered signals, matching from scratch (backlog, poller registry, sync match, the 70-second long-poll contract with an empty response before the deadline and rejection of deadlines under 2 seconds), child workflows, continue-as-new, cancellation.

**Compatibility you could honestly claim.** `temporalio/features` declares 104 scenarios, 77 implemented in at least one language, of which 24 stay inside the one-workflow-one-activity bucket. Realistic v1: **35 to 45 of 77, so 45% to 58%.** The honest sentence is *"runs unmodified Temporal SDK workers for workflows using activities, timers, signals, queries, child workflows, continue-as-new and cancellation; does not implement updates, schedules, worker versioning, deployments, Nexus, resets, cross-region replication, search attributes beyond the defaults, or archival."* You can say "runs the Temporal SDK". You cannot say "Temporal compatible".

**Upstream tracking cost: zero.** This is P3's only structural advantage.

**Effort: 82 engineer-weeks.** Largest items: the event and command model 9, workflow-task lifecycle 7, activity lifecycle 7, matching 7, multi-node ownership and poller migration 7, features triage 6, child/CAN/cancellation 6, timers 5, signals and queries 5, store port and three adapters 5, gRPC edge 5, visibility 3, buffer 10.

### P4. New engine, your own API

Included only to price the compatibility tax.

**Saves** against P3: a smaller event model (9 to 4), no sticky/partial-history contract (7 to 3), your own dispatch protocol instead of the long-poll contract (7 to 4), a simpler signal and query surface (5 to 3), no `serviceerror` mapping (5 to 2), no features corpus (6 to 0). **Total saving: 26 weeks.**

**Costs:** you write the client SDK, in every language you want, including deterministic replay. `go.temporal.io/sdk@v1.44.0/internal` is **51,961 lines** of non-test Go, of which the three replay-core files are **8,844**. Call it 12 weeks for a first Go-only SDK and 4 weeks to build a conformance corpus from nothing.

**Effort: 82 - 26 + 16 = 72 engineer-weeks**, and you lose every Temporal SDK, the CLI, the UI, every line of existing workflow code, and the hiring pool.

**The number that matters:** the Temporal API compatibility tax is **10 weeks out of 82, about 12%**, and it is **negative** once you count the SDK you have to write instead. **Compatibility is nearly free.** That kills P4, and it also removes any claim that P3's ambition is what makes P3 expensive. What makes P3 expensive is the engine, and P1 gets the engine for nothing.

### The comparison

| | **P1 embed** | P2 fork coordination | P3 new engine, Temporal API | P4 new engine, own API |
| --- | ---: | ---: | ---: | ---: |
| Upfront engineer-weeks | **62** | 97 | 82 | 72 |
| Upstream tracking, weeks/yr | **1 to 2** | 8+ | 0 | 0 |
| Compatibility corpus reachable | **100% of what Temporal supports** | same | 45 to 58% | n/a |
| Lines of Temporal reused | **410,135** | ~310,000 | 0 | 0 |
| Node ceiling | chosen once at init | elastic | none (but timer partitions return) | none |
| Throughput ceiling | shards x 1/(2L), fixed at init | elastic | per-workflow pin, no fixed ceiling | same |
| Needs a linearizable per-partition CAS | **yes** | yes | yes | yes |
| SDKs / CLI / Web UI | **all** | all | all | none |
| Risk concentrated in | storage plugin correctness | forked `service/history` | the whole engine | the engine and the SDK |

### Recommendation: P1, decisively

P1 is simultaneously the **cheapest** and the **most compatible**. That is an unusual combination and it is the whole finding. It happens for three reasons, each verified:

1. The two extension points the target shape needs are already public API: a persistence factory (`temporal/server_option.go:146`) and store-backed membership bootstrap (`common/persistence/persistence_interface.go:109-112`). The prototype in 4.9 demonstrates the first one end to end rather than assuming it.
2. Temporal's background work is already in-process workflows deduped by workflow ID (`service/worker/scanner/scanner.go:268-277`), so requirement 2 needs no new mechanism.
3. Temporal's per-shard safety reduces to **one** property, a linearizable `range_id` compare-and-set. That is worth having even though the candidate store does not yet supply it: it is a single, testable, backend-local requirement (item 17, section 7 item 0) rather than a correctness argument spread across an engine you wrote. P3 does not avoid this property; it has to build the same fence *and* the engine around it.

**What would change the recommendation.** Only one thing: if no store can be made to supply primitive 6 within the object-storage and no-extra-process constraints, then requirements 2, 3 and 4 are jointly unsatisfiable and the right move is to relax one of them explicitly, not to switch paths. P3 and P4 fail the same test for the same reason. Nothing in the ledger moves the decision from P1 to P2, P3 or P4.

---

## 6. Milestones

Each milestone is a demonstrable capability with a falsifiable acceptance test. Early ones are useful on their own. **Every acceptance test below must be shown to go red as well as green**: a check that cannot fail is not evidence.

**The milestones and the 62-week estimate are the same plan counted two ways**, so they reconcile explicitly:

| Milestone | Weeks | Covers estimate items |
| --- | ---: | --- |
| M0 linearizable fence | 6 | 17 |
| M1 one process, one bucket | 6 | 1, 2, 3 (less prototype credit), 11 |
| M2 mutable state conformance | 9 | 4, 18 |
| M3 hello world embedded | 2 | part of 11 |
| M4 full persistence conformance | 15 | 5, 6, 7, 8, 9, 12 |
| M5 compatibility corpus | 3 | 13 |
| M6 three replicas, and a fourth | 5 | 14 |
| M7 durability and taxonomy proof | 3 | 15 |
| M8 idle floor and latency | 2 | part of 15 |
| M9 visibility | 5 | 10 |
| Buffer | 6 | 16, 19 |
| **Total** | **62** | |

### M0. The fence is linearizable (6 weeks)

**This is first because everything after it inherits from it**, and because a wrong answer changes the backend rather than the plan (section 7 item 0). Whichever close is taken, the deliverable is the same.

**Accept:** partition the store into a majority and a minority side under continuous load, drive a `range_id` bump from a node on each side, and show **at most one** succeeds. Then heal and show no history was discarded, by an acked-write ledger that survives the heal. **Red check:** run the identical test against the unmodified AP configuration and show that **both** bumps succeed and the ledger records a loss. If the red run does not fail, the harness cannot partition the store and the milestone is not met.

### M1. One process, one bucket, no config file (6 weeks)

Storage plugin covers ShardStore, ClusterMetadataStore and MetadataStore. Single replica boots against a bucket from a Go struct.

**Accept:** `temporal operator cluster health` returns SERVING against the embedded frontend; `ps` shows exactly **1** process for the application; `lsof -p <pid> -i` shows **10** listening TCP sockets, of which 8 are loopback-bound Temporal listeners (4 gRPC, 4 ringpop) and 2 belong to the embedded store; no configuration file exists on disk. **Red check:** delete the cluster-metadata object and confirm the next boot recreates it rather than silently continuing.

### M2. Mutable state green on the conformance suite (9 weeks)

**Accept:** `go test ./common/persistence/tests -run 'ExecutionMutableState'` with the new backend wired in: report the exact `passed/total` count of the 110 methods. **Red check:** deliberately drop the `RangeID` condition from the transaction and confirm the shard-ownership tests fail. If they still pass, the fixture cannot distinguish correct from buggy and the milestone is not met. **This red check is not hypothetical:** the prototype in 4.9 has zero `RangeID` references on its execution write path and was nonetheless reported green, which is precisely the outcome an undischarged red check produces.

### M3. Hello world, embedded, end to end (2 weeks after M2)

The 30-line program in 4.2 runs.

**Accept:** the workflow completes and returns "hello world". Then `kill -9` the process mid-activity, restart it, and confirm (a) the workflow completes, and (b) the activity's side-effect counter, stored as an object in the bucket, shows the activity ran **at least once and no more than `RetryPolicy.MaximumAttempts` times**. **Red check:** disable durability (`Relaxed`, R=1) and confirm the same test can be made to lose the workflow, proving the test observes durability rather than luck.

### M4. Full persistence conformance (15 weeks)

**Accept:** **110/110** conformance test methods green, including the task, history-v2, queue and Nexus suites, run by a driver checked into the repo with its result artifact committed. Publish the number, not an adjective. **Red check:** a conformance claim with no runner in the repo is not a claim (4.9); the artifact and the `go test` invocation that produced it are part of the deliverable.

### M5. Compatibility corpus (3 weeks)

Run `temporalio/features` against the embedded engine using its external-server address mode.

**Accept:** publish `passed/77` with the per-scenario list, and confirm **24/24** of the one-workflow-one-activity scenarios pass. **Red check:** confirm the harness reports a failure when a deliberately broken command handler is introduced. **Stated limit:** this corpus never crash-kills a history host mid-write, so it is a **vacuous oracle for torn-write and fencing defects** and must not be cited as evidence for M0, M6 or M7.

### M6. Three replicas on one bucket, and then a fourth (5 weeks)

This is the acceptance test for requirement 4, which otherwise has none.

**Accept, node death:** 3 replicas, R>=2, under continuous load of at least 100 concurrent workflows. `kill -9` one replica. Measure and publish: (a) time to full shard reassignment, target under 35 s; (b) **zero** workflows lost; (c) activity executions beyond retry policy: **zero**; (d) `ShardOwnershipLost` occurrences counted as a metric, not surfaced as user-visible errors. **Red check:** run the same test at R=1 and show it fails one of (b) or (c).

**Accept, node addition (requirement 4 as literally stated):** with the same load running, scale 3 to 5 by changing `replicas` and **nothing else**, then publish: (e) the diff of every configuration input, which must be exactly one integer; (f) time from pod-ready to first shard owned; (g) measured bytes moved between nodes as a consequence of the shard remap, which must be **0**; (h) sustained WFT/sec before and after, which must rise; (i) per-node rate limits after settle, which must equal cluster-limit over 5. Then scale 5 back to 3 and repeat (b) and (c). **Red check:** pin the storage layer's placement so it cannot rebalance and confirm (g) or (h) fails, proving the harness measures redistribution rather than assuming it.

### M7. Durability and error-taxonomy proof (3 weeks)

**Accept, durability:** an acked-write ledger across 1,000 forced kills at R>=2 shows **zero** acked writes lost. Separately, a test demonstrates the storage layer fences a stale writer **before** that writer can take a WAL-recovery snapshot; if the backend cannot be instrumented to prove this, say so explicitly and treat it as unclosed. **Red check:** run against a deliberately unfenced writer and show the ledger detects the loss.

**Accept, taxonomy (row 7):** inject a fence at the store's `Commit` step, on a path where the commit may or may not have finalized, and assert that the error reaching `service/history/workflow/transaction_impl.go:197` makes `OperationPossiblySucceeded` return **true**. **Red check:** map the fence to `serviceerror.ResourceExhausted`, the mapping the store's own wire encoding invites (`shale/docs/SPEC.md:1320`), and confirm the test fails. A taxonomy test that passes under both mappings is testing nothing.

### M8. Idle floor and latency, measured (2 weeks)

**Accept:** publish measured p50/p99 for a workflow-task round trip and for a one-activity workflow at the chosen shard count and backend, plus measured idle GET/PUT rates at zero traffic with schedules and the batcher disabled. Additionally publish **per-shard** sustained throughput and compare it against `1/(2L)` from 2.3, since that is the ceiling the shard count was sized against. Explain any deviation over 2x.

### M9. Visibility (5 weeks)

**Accept:** publish the exact List and Count query subset supported, as a table of predicate forms with pass or fail, against the query set enumerated in section 7 item 7. **Red check:** a query outside the supported subset must return a typed unsupported error, never a silently wrong result set.

**Useful on its own:** M3 is a working single-node embedded durable-execution library, which already beats running a separate dev server. M6 is the product.

---

## 7. What to measure before committing, and what flips the decision

Every item below is a measurement, not an opinion. Each states the result that changes the plan.

**Three of these are P0. They are stated first because none of them is answered today and all three are backend questions, so a wrong answer changes the store and leaves P1 standing.**

| # | Measure | Method | Result that flips the decision |
| ---: | --- | --- | --- |
| 0 | **Is the per-shard CAS linearizable?** The store as specified says no (`shale/docs/SPEC.md:928, 938`) | M0: partition the store, drive a `range_id` bump from each side, count successes; heal and diff an acked-write ledger | Two successes means Temporal's only fence is not a fence. **Flip: change the store, not the architecture.** Three closes, in increasing order of cost: **(a)** refuse writes on the minority side, which means the write path must compare live members against configured R and fail rather than compute W=1 (`SPEC.md:531`), and is the smallest change that makes the existing store usable; **(b)** route just the shard row and the current-execution row to a CP store, which is cheap in code and **abandons requirement 2** if that store is a separate process, so it is only acceptable if the CP store is also embedded; **(c)** make the store CP for fenced keys generally, which is a consensus protocol and is the 8-week end of item 17's range. **Take (a) unless it measurably fails.** P1 survives all three |
| 1 | **Does the storage layer fence a stale writer before it takes a WAL-recovery snapshot?** | Two writers, forced epoch inversion, acked-write ledger across 1,000 kills | If acked writes can be lost, this backend is unusable for workflow histories, because a lost history write is not recoverable by retry. **Flip: change backend, not architecture.** If the replacement is Postgres you have **given up requirements 2 and 3**, so that is a scope decision for the requirement owner, not an engineering substitution. A different embedded LSM keeps both. P1 survives either way |
| 2 | **Is there anti-entropy re-replication?** A unit written while the cluster was undersized must be backfilled | Write 200 keys at N=1, grow to N=2, count replicas per key after settle | A prior observation records a steady state of 93 of 107 keys replicated after growth. If backfill does not exist, **build it before M6** or run a backend that has it. Same flip as #1 |
| 3 | **Per-commit p50 on the actual target bucket** | The M8 harness, 8 concurrent clients, realistic value sizes | Over ~50 ms means a workflow-task round trip over 100 ms and a one-activity workflow over 350 ms. **Flip: move to a low-latency object tier or a LAN object store, or restrict scope to batch-shaped workloads.** Does not flip the path |
| 4 | **Throughput at R>=2 with three replicas, and per-shard throughput separately** | M6 and M8 under load. Report both cluster WFT/sec and WFT/sec **per shard** | Two distinct ceilings can produce the same number (2.3). If per-shard throughput sits at `1/(2L)` the binding constraint is `ShardIOConcurrency=1` and the answer is more shards, which is unchangeable after first boot. If cluster throughput sits near the R=1 figure of ~4.85 round trips/sec/node while per-shard throughput is well below `1/(2L)`, the storage node-wide lock binds and the relaxed-durability path is not doing what the design assumes. **Flip: measure this before choosing the shard count, because the choice is permanent** |
| 5 | **Plugin velocity on the conformance suite** | Do M2 as a timed spike; measure engineer-days per suite method on the first 20 | Over 2 days per method average means the 62-week estimate is wrong by roughly 2x, which moves P1 to ~105 weeks and makes P3 competitive on cost while still losing on compatibility. **Flip: renegotiate scope, not path** |
| 6 | **Stability of `WithCustomDataStoreFactory`** | Track the option across the next two quarterly releases | It is explicitly marked experimental (`temporal/server_option.go:145`). 1 commit in 12 months so far. If it is removed or reshaped, the fallback is a one-line in-tree patch carried in a fork of `common/persistence/client/fx.go`, which raises tracking from 17 commits/yr to about 20. **Does not flip the path** |
| 7 | **Visibility query coverage actually needed** | Enumerate the List/Count queries the application will run, as the M9 predicate table | If arbitrary predicates with sort and pagination are required on a KV backend, item 10 of the P1 estimate grows from 5 weeks to 10 or more. **Flip: either narrow the query set, or run visibility on a SQL store alongside the KV.** Temporal supports the split natively, but a Postgres visibility store is **a second deployable unit and therefore abandons requirement 2**. SQLite keeps requirement 2 and fails requirement 4, since a local file cannot be read by a peer. State which requirement is being given up before taking this |
| 8 | **The two-ring hop rate** | Instrument the fraction of persistence calls served by the local storage owner | If a large fraction of writes are cross-node hops, every affected write costs one extra network round trip. **The co-location fix is NOT available from configuration** (row 10): it needs a patch to `temporal/fx.go`, 26 commits/yr. **Flip: only if the measured hop cost exceeds the tracking cost.** Default is to accept the hop |

**One thing NOT to measure, because the answer is already known:** whether Temporal can run all roles in one process without a config file. It can (`temporal/server_impl.go:44-53`, `temporal/server_option.go:34`, `temporaltest/internal/lite_server.go:74-171`).

---

## 8. What this gives up

Stated plainly, so nobody is surprised in month six.

1. **The shard count is a one-way door, and it gates throughput as well as fleet size.** You choose it on the day the cluster is created and can never change it while data exists. Because `ShardIOConcurrency` defaults to 1, each shard carries only `1/(2L)` workflow-task round trips per second, so under-provisioning caps throughput, not just node count. Over-provision and you pay an idle floor forever (27.3 polls/sec and 1.71 writes/sec at 512 shards, roughly $50/month at S3 list price). There is no in-place migration and no seam for one.
2. **Object storage sets a latency floor you cannot engineer away, and the only measured number on the recommended substrate is bad.** Two dependent durable commits per workflow-task round trip. On S3 Standard that is roughly 60 ms per round trip and 210 ms for a one-activity workflow, both **estimated**. The one **measured** per-commit figure on this store is 103 ms, giving 206 ms per round trip and 721 ms for a one-activity workflow, which 2.3 states is not viable. Every viable row in that table is either a published third-party measurement or a hypothesis. **The design is a fine orchestrator for human-scale and batch-scale work if section 7 item 3 lands under 50 ms, and a poor one otherwise.** Measure before building past M2.
3. **R>=2 is mandatory.** R=1 is both a durability gap and a node-wide throughput ceiling of about 4.85 workflow-task round trips per second. That is a storage cost you must budget for, not a tuning option. Note that R>=2 removes only the storage lock, never the per-shard semaphore.
4. **You own a storage plugin, and Temporal will not support it.** 108 + 13 methods, and any bug in the transaction boundary is a workflow-correctness bug rather than a performance bug. The conformance suite is a strong net (110 methods) but it is not a proof: it never crash-kills a writer, so it cannot fail on a torn write or a missing fence. **You also own the linearizability of the fence** (row 0). Temporal assumes it and tests nothing about it, so M0 is a test only you will ever run.
5. **The extension point is marked experimental.** `WithCustomDataStoreFactory` says so in its own doc comment. It moved once in 12 months. The fallback is a small in-tree patch, which raises the tracked surface from about 17 commits/yr to about 20.
6. **Advanced visibility needs Elasticsearch, which you are not running.** You get standard visibility with a restricted query subset, published as the M9 predicate table. If the application needs rich search, the escape hatch is visibility on a SQL store alongside the KV, and that hatch **costs requirement 2**: a Postgres visibility store is a second deployable unit. It is not a free fallback, and the choice belongs to whoever owns the requirement.
7. **Rolling upgrades are constrained.** Pin the fleet's Temporal version and roll one minor at a time. The wire version check will not stop a bad deploy.
8. **The application process now listens for peer traffic on ten ports and needs a seed list.** Once N > 1 you need pod-to-pod reachability, a stable advertise address per replica (a StatefulSet with a headless Service, not a plain ClusterIP), a shared cluster secret, and the storage layer's seed addresses. Ports are cheap; the seed list is the part that is genuinely extra configuration beyond "the same bucket".
9. **Temporal's own scanner cron workflows will appear in the application's workflow list**, and every replica polls the default system task queue.
10. **A backend built on a native LSM brings cgo into the application binary**, and such backends often write process-global object-store environment variables, which means one object-store configuration for the process lifetime.
11. **Cross-region replication is off.** No cross-region failover story, and turning it on would mean a second cluster, which violates requirement 2.
12. **A hard node failure costs about 27 to 31 seconds** of unavailability for the shards that node owned, of which 17 to 21 seconds is the physics of opening an LSM from object storage. Calls fail fast during that window rather than hanging, and nothing durable is lost, but it is not invisible.
13. **Cron singletons can skip runs, not merely delay them.** Temporal's cron scheduler advances past every fire instant already in the past (`common/backoff/cron.go:49-54`), so an outage longer than one cron period drops those runs. At the 27-to-31-second hard-death number, any cadence under a minute is exposed. 4.5 names the two workflow shapes that fix it.

None of these is a surprise waiting to happen if it is written down now. That is why they are here.

### 8a. Running it, which is the half a feasibility study omits

Everything above is about whether the thing can be built. This is about the first bad night. Item 19 of the estimate buys these, and the answers are design decisions, not documentation tasks.

**What happens when the bucket is unavailable.** Every durable write fails, so: shard renewal fails and shards unload (`service/history/shard/context_impl.go:1152-1185`), in-flight workflow tasks time out at their `StartToCloseTimeout` (default 10 s, `common/primitives/constants.go:16`) and are retried, activities are covered by their own timeouts, and `Start`/`Signal`/`Query` return `Unavailable` rather than hanging. **Nothing durable is lost and nothing durable advances.** Two consequences worth stating before an incident rather than during one: workflow-task retry counts climb for the whole outage and can exhaust a user's `RetryPolicy` on activities whose policy is finite, and membership heartbeats also fail, so a long enough outage makes every node consider every other node dead. The design decision to take now: **a bucket-unavailable circuit breaker that stops shard reacquisition attempts rather than thrashing the ring**, plus a health state that fails readiness without failing liveness so the orchestrator does not restart pods into a store that is down.

**Backup and restore.** The unit of backup is the bucket prefix, and it is not point-in-time consistent by default: a naive copy taken during writes can capture a half-applied multi-key commit. Object versioning plus a quiesce, or the store's own consistent-snapshot primitive if it has one, is the requirement. **Restore is the harder half**: restoring an older prefix rewinds `range_id` and every task ID, so any node still holding a lease from the newer state can write through the restore. **The restore runbook must therefore stop every replica first, and `New()` should refuse to start against a prefix whose recorded generation is older than one this node has already seen.** That last guard is cheap and prevents the worst class of restore accident.

**Disaster recovery is single-region, full stop.** Cross-region replication is off (item 11) and turning it on means a second cluster, which violates requirement 2. **Say the RPO and RTO out loud: RPO is whatever the backup cadence is, RTO is a full restore plus a cold mount, and the cold mount alone is 17 to 21 seconds per node.** If the application needs better, requirement 2 is the thing to renegotiate.

**The stuck-workflow runbook.** The single most common operational question is "why is this workflow not progressing", and embedding does not change the answer, it changes the tools. `temporal workflow describe` and the Web UI work unchanged because you are running Temporal's frontend, which is a real payoff of P1 worth naming. What embedding does change: the server logs are now the application's logs, and Temporal is verbose. Route them through the injected `log.Logger` at a level the application chooses, and keep `ShardOwnershipLost` at debug, since it is normal (M6 acceptance (d)).

**Tracing across the loopback hops.** A single workflow task crosses frontend, history and matching in-process over gRPC (2.2), so without trace propagation a latency regression is invisible. Temporal emits OpenTelemetry spans; the facade must accept a `TracerProvider` and pass it through, or the three-hop cost stays unattributable.

**The alert set, which is short.** Shard acquisition failure rate above zero for more than one minute. `OperationPossiblySucceeded` returning true at any sustained rate, since that is the ambiguous-write path (row 7). Persistence p99 above the value the shard count was sized against, because that ceiling is permanent. Membership size not equal to replica count for more than one rebalance interval. Under-replicated write count above zero at R>=2. **Five alerts, each mapped to a number that already appears in this document**, which is the point of having measured them.

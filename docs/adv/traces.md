<!--
template: docs.html
-->

# Traces

The SimKube tracer collects timeseries data about the events happening in a live Kubernetes cluster and exports that
data to a trace file for future replay and analysis.  These trace files can then be stored in a cloud provider or
downloaded locally.  We describe configuration options for each of these use cases.

## Cloud storage

We support exporting traces to Amazon S3, Google Cloud Storage, and Microsoft Azure Storage through the
[object\_store](https://docs.rs/object_store/latest/object_store/) crate.  The `sk-tracer` and `sk-driver` pods need to
be configured with the correct permissions to write and read data to your chosen cloud storage.  One option is to inject
environment variables into the pod that object\_store understands.

- Amazon S3: use the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
- Google Cloud Storage: use the `GOOGLE_SERVICE_ACCOUNT` environment variable and inject your service account JSON file
  into the `sk-tracer` pod
- Microsoft Azure: use the `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` environment variables

The object\_store crate will try other authentication/authorization methods if these environment variables are not set
(for example, it will try to get credentials from the instance metadata endpoint for AWS), so these are not the only
ways to grant permissions to the tracer and the driver.  Configuring these permissions is beyond the scope of this
documentation, and we encourage you to consult the IAM documentation for your chosen cloud provider(s).

## Local storage

If you do not have access to (or do not want to use) cloud storage, you can also save a trace file to local storage
using, for example, `skctl export -o file:///path/to/trace`.  However, using this trace file in the simulator is a bit
more complicated; it will need to be injected into the node(s) where your Simulation driver pods will run, and then
volume-mounted into the driver pod.  If you are running locally via `kind`, you can add the following block to your
`kind` config to mount the trace file directory on your laptop into the kind nodes:

```yaml
  - role: worker
    extraMounts:
      - hostPath: /tmp/kind-node-data
        containerPath: /data
```

From there, when you run a simulation, you need to specify the trace data using `skctl run --trace-path
file:///data/trace`.  This location is the location _inside the Kind node docker container_, not inside the driver pod.
SimKube will automatically construct the appropriate volume mounts so that the driver pod can reference the trace.

## Seed state

The standard recording path captures a stream of events: which objects were created, modified, and
deleted between two timestamps.  Replaying that stream into a fresh cluster reconstructs whatever
state the original cluster had at the trace's end time, but the path it takes is to re-run the
events.

Sometimes you want to start a simulation from a specific cluster state without replaying any
history first - for example, to A/B test how two builds of a controller respond to an
identical starting configuration.  For this, use `skctl snapshot --seed`:

```bash
skctl snapshot --config tracker-config.yaml --seed -o seed.trace
```

A `--seed` snapshot writes the captured objects verbatim into the trace's `initial_state` field.
Unlike a normal export, it does not deduplicate against tracked owners: a Pod whose Deployment is
also captured is preserved with its `spec.nodeName`, status, and finalizers intact.  This matters
when pod placement, controller-managed status conditions, or finalizers are load-bearing for the
behavior you are testing.

When the driver runs a trace with a non-empty `initial_state`, it materializes those objects
(rewriting namespaces for namespace-scoped objects, preserving cluster-scoped objects verbatim,
and applying captured `.status` via a follow-up `patch_status` pass) before stepping into the
event loop.  Set `sim.spec.duration` to control how long the driver holds the simulated cluster
open after applying the seed.  A seed-only trace without a duration is a hard error: there is no
sensible default for "how long should the driver hold the cluster open."

### Caveats

A few assumptions baked into the current design.  Each is fine for the use case the feature was
built for; some will surface as failure modes in adjacent use cases.

**Owner reference UIDs are not threaded.**  The Kubernetes apiserver mints fresh UIDs on object
create, so any ownerRef UIDs in seed objects are stale by the time they reach the replay
cluster.  We make no attempt to rewrite them.  This is fine for controllers that associate
objects via labels (e.g. `karpenter.sh/nodepool`) or via providerID matching, and for the
Kubernetes garbage collector if you do not delete owner objects mid-simulation.  Controllers
that strictly UID-match dependents will treat seed objects as orphaned and may detach them.

**Captured `.status` is replayed verbatim.**  If the captured cluster had a NodeClaim in
`Ready=False` state, the replay cluster gets the same condition.  If you want a clean replay,
capture a clean cluster - the seed-state path does not heuristically curate status fields.

**DaemonSet pods are filtered even in verbatim mode.**  The export filter that drops
DaemonSet-owned pods applies to seed exports too.  If your DaemonSet is part of the seed,
re-applying its pods with stale ownerRefs would not work cleanly anyway; the DaemonSet
controller will recreate them in the replay cluster.

**Status patch races against running controllers.**  `apply_seed_state` applies specs in pass
one and statuses in pass two.  If a controller is already reconciling between the two passes,
it can mutate status between our spec apply and our status patch, and we will overwrite its
work.  In practice this means: scale relevant controllers to zero before starting the driver,
then scale them up after the seed is fully applied.  The driver does not enforce this.

**`sim.spec.duration` is overloaded.**  For traces with events, it caps the event stream and
governs replay length.  For seed-only traces, it governs how long the driver holds the cluster
open after the seed is applied.  This is two semantics on one knob; we did not add a separate
`holdAfterReplay` field for now.

**Replay path still assumes namespace-scoped events.**  Cluster-scoped objects work in
`initial_state` (seed); they do not work in `events` (replay).  If you record a trace that
tracks cluster-scoped GVKs, the replay loop will panic on the namespace unwrap.  Pre-existing
limitation, unrelated to seed state, called out here for completeness.

**Snapshot captures race against watcher readiness.**  `skctl snapshot --seed` calls
`TraceManager::wait_ready` before exporting, but watchers that are still syncing their initial
list against the apiserver can miss their first observation of an object.  In one end-to-end
run, a `karpenter.sh/v1.NodePool` that was present in the cluster did not appear in the
resulting trace.  Workaround today: re-snapshot, or add your own settling time.  A
`--wait-ms` or `--require-gvks` flag on `skctl snapshot` would surface this cleanly; both are
follow-up work.

### Trace format version

`initial_state` was added in trace format version 3.  v3 traces are not loadable by SimKube
binaries built before version 2.5.0.  v2 traces remain loadable by current binaries (the seed
state is treated as empty).

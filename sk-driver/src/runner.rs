use std::time::Duration;

use anyhow::{
    anyhow,
    bail,
};
use clockabilly::prelude::*;
use either::Either;
use json_patch_ext::prelude::*;
use kube::api::{
    DeleteParams,
    DynamicObject,
    Patch,
    PatchParams,
    PropagationPolicy,
};
use kube::discovery::Scope;
use serde_json::json;
use sk_core::errors::*;
use sk_core::k8s::{
    DynamicApiSet,
    GVK,
    add_common_metadata,
    build_global_object_meta,
    build_simulation_root,
    dyn_obj_type_str,
    try_update_lease,
};
use sk_core::macros::*;
use sk_core::prelude::*;
use sk_core::time::duration_to_ts_from;
use tokio::time::sleep;
use tracing::*;

use super::*;
use crate::util::{
    compute_step_size,
    wait_if_paused,
};

pub(crate) const DRIVER_CLEANUP_TIMEOUT_SECONDS: i64 = 300;

err_impl! {SkDriverError,
    #[error("could not delete simulation root {0}")]
    CleanupFailed(String),

    #[error("timed out deleting simulation root {0}")]
    CleanupTimeout(String),
}

pub fn build_virtual_ns(ctx: &DriverContext, root: &SimulationRoot, namespace: &str) -> corev1::Namespace {
    let owner = root;
    let mut ns = corev1::Namespace {
        metadata: build_global_object_meta(namespace, &ctx.name, owner),
        ..Default::default()
    };
    klabel_insert!(ns, VIRTUAL_LABEL_KEY => "true");

    ns
}

pub fn build_virtual_obj(
    ctx: &DriverContext,
    root: &SimulationRoot,
    original_ns: &str,
    virtual_ns: &str,
    obj: &DynamicObject,
    maybe_pod_spec_template_paths: Option<&[String]>,
) -> anyhow::Result<DynamicObject> {
    let owner = root;
    let mut vobj = obj.clone();
    add_common_metadata(&ctx.name, owner, &mut vobj.metadata);
    vobj.metadata.namespace = Some(virtual_ns.into());
    klabel_insert!(vobj, VIRTUAL_LABEL_KEY => "true");
    patch_ext(&mut vobj.data, remove_operation(format_ptr!("/status")))?;

    if let Some(pod_spec_template_paths) = maybe_pod_spec_template_paths {
        for pod_spec_template_path in pod_spec_template_paths {
            patch_ext(
                &mut vobj.data,
                add_operation(
                    format_ptr!(
                        "{pod_spec_template_path}/metadata/annotations/{}",
                        escape(ORIG_NAMESPACE_ANNOTATION_KEY)
                    ),
                    json!(original_ns),
                ),
            )?;

            // This is semi-duplicated from the pod mutation webhook; we need to set the tolerations
            // and nodeSelectors _here_ so that, e.g., things like DaemonSets will schedule properly
            // in the simulation.  We _also_ set them in the mutation webhook, to catch pods that
            // are owned by something that doesn't use the standard sort of podTemplateSpec pattern.
            // Maybe this is overkill and we shouldn't worry about it, but *shrug*.
            //
            // I also kindof wish we could not have the code duplication, but it's _just_ different
            // enough that it felt hard to refactor/combine.  Maybe something to revisit in the
            // future.
            if vobj
                .data
                .pointer(&format!("{pod_spec_template_path}/spec/nodeSelector"))
                .is_none()
            {
                // N.B. add_operation will replace existing fields, which already bit me once
                patch_ext(
                    &mut vobj.data,
                    add_operation(format_ptr!("{pod_spec_template_path}/spec/nodeSelector",), json!({})),
                )?;
            }
            if vobj
                .data
                .pointer(&format!("{pod_spec_template_path}/spec/tolerations"))
                .is_none()
            {
                patch_ext(
                    &mut vobj.data,
                    add_operation(format_ptr!("{pod_spec_template_path}/spec/tolerations"), json!([])),
                )?;
            }
            patch_ext(
                &mut vobj.data,
                add_operation(format_ptr!("{pod_spec_template_path}/spec/nodeSelector/type"), json!("virtual")),
            )?;
            patch_ext(
                &mut vobj.data,
                add_operation(
                    format_ptr!("{pod_spec_template_path}/spec/tolerations/-"),
                    json!({"key": VIRTUAL_NODE_TOLERATION_KEY, "operator": "Exists", "effect": "NoSchedule"}),
                ),
            )?;

            // We remove all container ports from the pod specification just before applying, because it is
            // _possible_ to create a pod with duplicate container ports, but the apiserver will _reject_ a
            // patch containing duplicate container ports.  Since pods are mocked out _anyways_ there's no
            // reason to expose the ports.  We do this here because we still want the ports to be a part of
            // the podspec when we're computing its hash, i.e., changes to the container ports will still
            // result in changes to the pod in the trace/simulation
            patch_ext(
                &mut vobj.data,
                remove_operation(format_ptr!("{pod_spec_template_path}/spec/containers/*/ports")),
            )?;
        }
    }

    Ok(vobj)
}

#[instrument(parent=None, skip_all, fields(simulation=ctx.name))]
pub async fn run_trace(ctx: DriverContext, client: kube::Client, sim: Simulation) -> EmptyResult {
    let roots_api: kube::Api<SimulationRoot> = kube::Api::all(client.clone());
    let root = if let Some(root) = roots_api.get_opt(&ctx.root_name).await? {
        warn!("Driver root {} already exists; continuing...", ctx.root_name);
        root
    } else {
        let root_obj = build_simulation_root(&ctx.root_name, &sim);
        roots_api.create(&Default::default(), &root_obj).await?
    };

    let clock = UtcClock::boxed();
    let (sim_ts, sim_end_ts) = compute_sim_window(&ctx, &sim, clock.now_ts())?;
    let sim_duration = compute_step_size(sim.speed(), sim_ts, sim_end_ts);
    info!(
        "trace start time: {sim_ts}; trace end time: {sim_end_ts}; simulation speed: {}; computed simulation duration: {sim_duration}",
        sim.speed()
    );

    try_update_lease(client.clone(), &sim, &ctx.ctrl_ns, sim_duration as u64).await?;
    run_trace_internal(&ctx, client, sim.speed(), root, sim_ts, sim_end_ts, clock.clone()).await?;

    let timeout = clock.now_ts() + DRIVER_CLEANUP_TIMEOUT_SECONDS;
    cleanup_trace(&ctx, roots_api, clock, timeout).await
}

// Compute the (start, end) timestamps for this simulation run.  Three cases:
//
// - Trace has events: use the first/last event timestamps.  sim.spec.duration is honored at
//   import time by capping the event stream; we do not re-pad here.
// - Trace has no events but does carry initial_state (seed-only): require sim.spec.duration so
//   the driver knows how long to hold the seeded cluster open.  Window is (now, now + duration).
// - Trace has no events and no initial_state: nothing to simulate.
fn compute_sim_window(ctx: &DriverContext, sim: &Simulation, now_ts: i64) -> anyhow::Result<(i64, i64)> {
    if let Some(start) = ctx.trace.start_ts() {
        let end = ctx.trace.end_ts().expect("end_ts is Some whenever start_ts is Some");
        return Ok((start, end));
    }
    if ctx.trace.initial_state.is_empty() {
        bail!("trace contains no events and no seed state; nothing to do");
    }
    let duration = sim.spec.duration.as_ref().ok_or_else(|| {
        anyhow!(
            "seed-only traces (initial_state populated, events empty) require sim.spec.duration \
             to control how long the driver holds the simulated cluster open after the seed is applied"
        )
    })?;
    let end = duration_to_ts_from(now_ts, duration)?;
    Ok((now_ts, end))
}

// Transform a captured seed object for application into the simulated cluster.  Cluster-scoped
// objects (Node, NodeClaim, NodePool, ...) keep their original name and identity; namespace-scoped
// objects get rewritten into the per-simulation virtual namespace.  Both retain their original
// ownerReferences (load-bearing for controller adoption) and have the SimulationRoot ref appended
// for cleanup.  Status is intentionally left in place here; the driver applies it via patch_status
// in a follow-up pass.
pub fn build_seed_obj(
    sim_name: &str,
    root: &SimulationRoot,
    virtual_ns_prefix: &str,
    obj: &DynamicObject,
    scope: &Scope,
) -> DynamicObject {
    let mut sobj = obj.clone();
    add_common_metadata(sim_name, root, &mut sobj.metadata);
    klabel_insert!(sobj, VIRTUAL_LABEL_KEY => "true");

    if matches!(scope, Scope::Namespaced) {
        let original_ns = sobj.namespace().unwrap_or_default();
        sobj.metadata.namespace = Some(format!("{virtual_ns_prefix}-{original_ns}"));
    }

    sobj
}

pub(crate) async fn apply_seed_state(
    ctx: &DriverContext,
    root: &SimulationRoot,
    apiset: &mut DynamicApiSet,
    ns_api: &kube::Api<corev1::Namespace>,
) -> EmptyResult {
    if ctx.trace.initial_state.is_empty() {
        return Ok(());
    }
    info!("Applying {} seed objects", ctx.trace.initial_state.len());

    let mut to_patch_status: Vec<DynamicObject> = vec![];

    // First pass: apply specs.  Server-side apply does not write subresources, so any captured
    // .status field is stashed for the second pass.
    for obj in &ctx.trace.initial_state {
        let gvk = GVK::from_dynamic_obj(obj)?;
        let scope = apiset.scope_for(&gvk).await?;
        let mut sobj = build_seed_obj(&ctx.name, root, &ctx.virtual_ns_prefix, obj, &scope);

        if matches!(scope, Scope::Namespaced) {
            let virtual_ns = sobj.namespace().unwrap();
            if ns_api.get_opt(&virtual_ns).await?.is_none() {
                info!("creating virtual namespace: {virtual_ns}");
                let vns = build_virtual_ns(ctx, root, &virtual_ns);
                ns_api.create(&Default::default(), &vns).await?;
            }
        }

        let stashed_status = sobj.data.as_object_mut().and_then(|m| m.remove("status"));

        info!("seed-applying {} {}", dyn_obj_type_str(&sobj), sobj.namespaced_name());
        apiset
            .api_for_obj(&sobj)
            .await?
            .patch(&sobj.name_any(), &PatchParams::apply("simkube"), &Patch::Apply(&sobj))
            .await?;

        if let Some(status) = stashed_status {
            sobj.data["status"] = status;
            to_patch_status.push(sobj);
        }
    }

    // Second pass: status subresources.  Done after all specs are present so controllers do not
    // observe half-populated objects mid-seed (e.g. a NodeClaim reading as Ready before its
    // companion Node exists).
    for sobj in &to_patch_status {
        info!("seed-patching status for {} {}", dyn_obj_type_str(sobj), sobj.namespaced_name());
        let status_patch = json!({"status": sobj.data["status"]});
        apiset
            .api_for_obj(sobj)
            .await?
            .patch_status(&sobj.name_any(), &PatchParams::apply("simkube"), &Patch::Apply(&status_patch))
            .await?;
    }

    Ok(())
}

pub(crate) async fn run_trace_internal(
    ctx: &DriverContext,
    client: kube::Client,
    sim_speed: f64,
    root: SimulationRoot,
    mut current_ts: i64,
    sim_end_ts: i64,
    clock: Box<dyn Clockable + Send>,
) -> EmptyResult {
    let ns_api: kube::Api<corev1::Namespace> = kube::Api::all(client.clone());
    let mut apiset = DynamicApiSet::new(client.clone());

    apply_seed_state(ctx, &root, &mut apiset, &ns_api).await?;

    for (evt, maybe_next_ts) in ctx.trace.iter() {
        current_ts += wait_if_paused(client.clone(), &ctx.sim_name, clock.clone()).await?;

        // We're currently assuming that all tracked objects are namespace-scoped,
        // this will panic/fail if that is not true.
        for obj in &evt.applied_objs {
            let gvk = GVK::from_dynamic_obj(obj)?;
            let original_ns = obj.namespace().unwrap();
            let virtual_ns = format!("{}-{}", ctx.virtual_ns_prefix, original_ns);

            if ns_api.get_opt(&virtual_ns).await?.is_none() {
                info!("creating virtual namespace: {virtual_ns}");
                let vns = build_virtual_ns(ctx, &root, &virtual_ns);
                ns_api.create(&Default::default(), &vns).await?;
            }

            let pod_spec_template_path = ctx.trace.config.pod_spec_template_paths(&gvk);
            let vobj = build_virtual_obj(ctx, &root, &original_ns, &virtual_ns, obj, pod_spec_template_path)?;

            info!("applying {} {}", dyn_obj_type_str(&vobj), vobj.namespaced_name());
            apiset
                .api_for_obj(&vobj)
                .await?
                .patch(&vobj.name_any(), &PatchParams::apply("simkube"), &Patch::Apply(&vobj))
                .await?;
        }

        for obj in &evt.deleted_objs {
            info!("deleting {} {}", dyn_obj_type_str(obj), obj.namespaced_name());
            let virtual_ns = format!("{}-{}", ctx.virtual_ns_prefix, obj.namespace().unwrap());
            let mut vobj = obj.clone();
            vobj.metadata.namespace = Some(virtual_ns);
            apiset
                .api_for_obj(&vobj)
                .await?
                .delete(&obj.name_any(), &Default::default())
                .await?;
        }

        if let Some(next_ts) = maybe_next_ts {
            let sleep_duration = compute_step_size(sim_speed, current_ts, next_ts);
            info!("next event happens in {sleep_duration} seconds, sleeping");
            debug!("current sim ts = {current_ts}, next sim ts = {next_ts}");

            current_ts = next_ts;
            clock.sleep(sleep_duration).await;
        }
    }

    // Hold the cluster open until the configured simulation end if we have not reached it.
    // For event-only traces this is usually a no-op (last event ts == sim_end_ts); for
    // seed-only traces with a duration set this is what makes the driver wait long enough
    // for controllers to react before cleanup runs.
    let remaining = compute_step_size(sim_speed, current_ts, sim_end_ts);
    if remaining > 0 {
        info!("holding simulated cluster open for {remaining} seconds");
        clock.sleep(remaining).await;
    }
    Ok(())
}

pub(crate) async fn cleanup_trace(
    ctx: &DriverContext,
    roots_api: kube::Api<SimulationRoot>,
    clock: Box<dyn Clockable + Send>,
    timeout: i64,
) -> EmptyResult {
    info!("Cleaning up simulation objects...");

    let mut cleanup_done = false;
    while clock.now_ts() < timeout {
        // delete returns an "either" object; left contains the object being deleted,
        // and right contains a status code indicating the delete is finished.
        match roots_api
            .delete(
                &ctx.root_name,
                &DeleteParams {
                    propagation_policy: Some(PropagationPolicy::Foreground),
                    ..Default::default()
                },
            )
            .await
        {
            // In the situation where we delete, wait five seconds, and then everything's cleaned
            // up, the second delete call will return not found, which is not an error in this case
            Ok(Either::Right(_)) | Err(kube::Error::Api(kube::core::ErrorResponse { code: 404, .. })) => {
                cleanup_done = true;
                break;
            },
            Err(e) => {
                error!("delete failed: {e}");
                bail!(SkDriverError::cleanup_failed(&ctx.root_name));
            },
            _ => sleep(Duration::from_secs(RETRY_DELAY_SECONDS)).await,
        }
    }
    if !cleanup_done {
        bail!(SkDriverError::cleanup_timeout(&ctx.root_name));
    }
    info!("All objects deleted!");
    Ok(())
}

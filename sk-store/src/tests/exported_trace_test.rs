use std::collections::HashMap;

use assertables::*;
use serde::Serialize;
use sk_core::k8s::{
    GVK,
    PodLifecycleData,
};

use super::*;
use crate::config::TracerConfig;
use crate::index::TraceIndex;
use crate::pod_owners_map::PodLifecyclesMap;

#[fixture]
fn trace() -> ExportedTrace {
    ExportedTrace::default()
}

#[rstest]
fn test_lookup_pod_lifecycle_no_owner(trace: ExportedTrace) {
    let res = trace.lookup_pod_lifecycle(&DEPL_GVK, TEST_DEPLOYMENT, EMPTY_POD_SPEC_HASH, 0);
    assert_eq!(res, PodLifecycleData::Empty);
}

#[rstest]
fn test_lookup_pod_lifecycle_no_hash(mut trace: ExportedTrace) {
    trace.index.insert(DEPL_GVK.clone(), TEST_DEPLOYMENT.into(), 1234);
    let res = trace.lookup_pod_lifecycle(&DEPL_GVK, TEST_DEPLOYMENT, EMPTY_POD_SPEC_HASH, 0);
    assert_eq!(res, PodLifecycleData::Empty);
}

#[rstest]
fn test_lookup_pod_lifecycle(mut trace: ExportedTrace) {
    let owner_ns_name = format!("{TEST_NAMESPACE}/{TEST_DEPLOYMENT}");
    let pod_lifecycle = PodLifecycleData::Finished(1, 2);

    trace.index.insert(DEPL_GVK.clone(), owner_ns_name.clone(), 1234);
    trace.pod_lifecycles = HashMap::from([(
        (DEPL_GVK.clone(), owner_ns_name.clone()),
        HashMap::from([(EMPTY_POD_SPEC_HASH, vec![pod_lifecycle.clone()])]),
    )]);

    let res = trace.lookup_pod_lifecycle(&DEPL_GVK, &owner_ns_name, EMPTY_POD_SPEC_HASH, 0);
    assert_eq!(res, pod_lifecycle);
}

#[rstest]
fn test_trace_start_end_ts(mut trace: ExportedTrace) {
    trace.append_event(TraceEvent { ts: 0, ..Default::default() });
    trace.append_event(TraceEvent { ts: 1, ..Default::default() });

    assert_some_eq_x!(trace.start_ts(), 0);
    assert_some_eq_x!(trace.end_ts(), 1);
}

// Mirror of the v2 ExportedTrace shape (no initial_state field).  Used to
// confirm that v2 trace bytes still decode against the v3 struct via
// #[serde(default)].
#[derive(Serialize)]
struct ExportedTraceV2 {
    version: u16,
    config: TracerConfig,
    events: Vec<TraceEvent>,
    index: TraceIndex,
    pod_lifecycles: HashMap<(GVK, String), PodLifecyclesMap>,
}

#[rstest]
fn test_v2_trace_imports_with_empty_initial_state() {
    let v2 = ExportedTraceV2 {
        version: 2,
        config: TracerConfig::default(),
        events: vec![TraceEvent { ts: 100, ..Default::default() }],
        index: TraceIndex::default(),
        pod_lifecycles: HashMap::default(),
    };
    let bytes = rmp_serde::to_vec_named(&v2).unwrap();

    let trace = ExportedTrace::import(bytes, None).unwrap();
    assert_eq!(trace.version, 2);
    assert!(trace.initial_state.is_empty());
    assert_eq!(trace.events.len(), 1);
}

#[rstest]
fn test_unsupported_old_trace_version_rejected() {
    let v1 = ExportedTraceV2 {
        version: 1,
        config: TracerConfig::default(),
        events: vec![],
        index: TraceIndex::default(),
        pod_lifecycles: HashMap::default(),
    };
    let bytes = rmp_serde::to_vec_named(&v1).unwrap();
    assert!(ExportedTrace::import(bytes, None).is_err());
}

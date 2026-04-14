// The annotated trace code is actually fairly complex; we need to take a trace (which already has
// several layers of nesting) and also append annotations about which events and which objects
// within those events are failing validation.
//
// The structure of an annotated trace is as follows:
//    - path (read-only): the location of the original trace
//    - base (read-only): the imported/parsed trace located at `path`; we store the base trace data
//      so that we can recompute a fixed/patched version by iteratively applying all the patches in
//      order on-demand
//    - patches (read-write): the list of patches applied to the original trace value; this will
//      allow us in the future to "undo" patches, by walking back up the list and recomputing the
//      events
//    - events (read-write): the computed list of (annotated) events after all the patches have been
//      applied
//
// In addition to the event data (timestamp, applied objects, and deleted objects), an annotated
// event contains a set of annotations indicating which objects failed some validation check.
// Throughout this code, we use the convention that the annotation index can be between 0 and
// applied_objs.len() + deleted_objs.len(), where if the index is larger than applied_objs.len(),
// you must subtract applied_objs.len() and use the resulting value to index into deleted_objs.
//
// Because each object can fail multiple different validation checks, the "value" for a particular
// object index is a vector of annotations, where an annotation contains a ValidatorCode (which can
// be used to look up more information about the failed check), together with a list of _possible_
// (and probably mutually-exclusive) patches that we can apply to the object that will fix the
// validation issue.  The first patch in this list is the "recommended" fix, in that this is the
// one that will be applied by `skctl validate check --fix`.  The others may also solve the
// problem, but are probably not what most users want; we may show these in skctl xray eventually.
//
// Lastly, an individual patch applies in one or more locations in the trace (for example, all
// deployments with a particular name), and has one or more operations (that is, json-patch
// operations) that need to be applied at that location.

use std::collections::BTreeMap; // BTreeMap sorts by key, HashMap doesn't
use std::iter::once;
use std::slice;

use json_patch_ext::prelude::*;
use serde_json::json;
use sk_core::external_storage::{
    ObjectStoreWrapper,
    SkObjectStore,
};
use sk_core::prelude::*;
use sk_store::{
    ExportedTrace,
    TraceAction,
    TraceEvent,
};

use super::validator::{
    Validator,
    ValidatorCode,
};


type Annotation = BTreeMap<usize, Vec<ValidatorCode>>;

#[derive(Default)]
pub struct AnnotatedTrace {
    path: String,
    trace: ExportedTrace,
    annotations: BTreeMap<usize, Annotation>,
}

impl AnnotatedTrace {
    pub async fn new(trace_path: &str) -> anyhow::Result<AnnotatedTrace> {
        Ok(AnnotatedTrace {
            trace,
            path: trace_path.into(),
            ..Default::default()
        })
    }

    pub fn validate(&mut self, validators: &BTreeMap<ValidatorCode, Validator>) -> EmptyResult {
        let mut summary = BTreeMap::new();
        for event in self.trace.iter() {
            for (code, validator) in validators.iter() {
                let event_patches = validator.check_next_event(event, &self.base.config)?;
                let count = event_patches.len();
                summary.entry(*code).and_modify(|e| *e += count).or_insert(count);
            }
        }
        Ok(())
    }

    pub fn get_event(&self, idx: usize) -> Option<&TraceEvent> {
        self.base.events.get(idx)
    }

    pub fn get_object(&self, event_idx: usize, obj_idx: usize) -> Option<&DynamicObject> {
        let event = self.get_event(event_idx)?;
        let applied_len = event.applied_objs.len();
        if obj_idx >= applied_len {
            event.deleted_objs.get(obj_idx - applied_len)
        } else {
            event.applied_objs.get(obj_idx)
        }
    }

    pub fn is_empty_at(&self, idx: usize) -> bool {
        self.get_event(idx)
            .map(|evt| evt.applied_objs.is_empty() && evt.deleted_objs.is_empty())
            .unwrap_or(true)
    }

    pub fn iter(&self) -> TraceIterator {
        self.base.iter()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn path(&self) -> String {
        self.path.clone()
    }

    pub fn start_ts(&self) -> Option<i64> {
        self.events.first().map(|evt| evt.data.ts)
    }

    fn object_iter_mut(&mut self) -> impl Iterator<Item = &mut DynamicObject> {
        self.events
            .iter_mut()
            .flat_map(|e| e.data.applied_objs.iter_mut().chain(e.data.deleted_objs.iter_mut()))
    }
}

impl<'a> AnnotatedTrace {
    fn matched_objects(
        &'a mut self,
        locations: &'a PatchLocations,
    ) -> Box<dyn Iterator<Item = &'a mut DynamicObject> + 'a> {
        match locations {
            PatchLocations::Everywhere => Box::new(self.object_iter_mut()),
            PatchLocations::ObjectReference(type_, ns_name) => Box::new(self.object_iter_mut().filter(move |obj| {
                obj.types.as_ref().is_some_and(|t| t == type_) && &obj.namespaced_name() == ns_name
            })),
            PatchLocations::InsertAt(relative_ts, action, type_meta, object_meta) => {
                let insert_ts = self.start_ts().unwrap_or_default() + relative_ts;
                let insert_idx = find_or_create_event_at_ts(&mut self.events, insert_ts);

                let new_obj = DynamicObject {
                    types: Some(type_meta.clone()),
                    metadata: *object_meta.clone(),
                    data: json!({}),
                };
                let obj = match action {
                    TraceAction::ObjectApplied => {
                        self.events[insert_idx].data.applied_objs.push(new_obj);
                        self.events[insert_idx].data.applied_objs.iter_mut().last().unwrap()
                    },
                    TraceAction::ObjectDeleted => {
                        self.events[insert_idx].data.deleted_objs.push(new_obj);
                        self.events[insert_idx].data.deleted_objs.iter_mut().last().unwrap()
                    },
                };
                Box::new(once(obj))
            },
        }
    }
}

pub(super) fn find_or_create_event_at_ts(events: &mut Vec<AnnotatedTraceEvent>, ts: i64) -> usize {
    let new_event = AnnotatedTraceEvent {
        data: TraceEvent { ts, ..Default::default() },
        ..Default::default()
    };
    // Walk through the events list backwards until we find the first one less than the given ts
    match events.iter().rposition(|e| e.data.ts <= ts) {
        Some(i) => {
            // If we found one, and the ts isn't equal, create an event with the specified
            // timestamp; this goes at index i+1 since it needs to go after the lower (found) ts
            if events[i].data.ts < ts {
                events.insert(i + 1, new_event);
                i + 1
            } else {
                i // otherwise the timestamp is equal so return this index
            }
        },
        None => {
            // In this case there are no events in the trace, so we add one at the beginning
            events.push(new_event);
            0
        },
    }
}

#[cfg(test)]
#[cfg_attr(coverage, coverage(off))]
impl AnnotatedTrace {
    pub fn new_with_events(events: Vec<AnnotatedTraceEvent>) -> AnnotatedTrace {
        AnnotatedTrace { events, ..Default::default() }
    }

    pub fn new_from_test_json(trace_type: &str) -> AnnotatedTrace {
        let exported_trace = sk_testutils::exported_trace_from_json(trace_type);
        let annotated_events = exported_trace
            .events()
            .iter()
            .cloned()
            .map(|e| AnnotatedTraceEvent::new(e))
            .collect();

        AnnotatedTrace {
            base: exported_trace,
            events: annotated_events,
            ..Default::default()
        }
    }
}

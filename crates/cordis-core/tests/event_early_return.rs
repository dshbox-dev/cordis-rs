//! Public Event regression for destruction of unclaimed listener snapshots.

use cordis_core::event::{
    DispatchError, DispatchOutcomeKind, EventOperation, ListenerOptions, mapper_sync,
    observer_sync, responder_sync,
};
use cordis_core::observation::RuntimeObservation;
use cordis_core::{Context, Event, Level, QueryOutcome, Routing, logger::BufferExporter};
use futures::FutureExt;
use parking_lot::Mutex;
use std::convert::Infallible;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Notify;
impl Event for Notify {
    const NAME: &'static str = "early-return/notify";
    type Args = ();
    type Output = usize;
}

struct Ask;
impl Event for Ask {
    const NAME: &'static str = "early-return/ask";
    type Args = ();
    type Output = usize;
}

struct Flow;
impl Event for Flow {
    const NAME: &'static str = "early-return/flow";
    type Args = usize;
    type Output = usize;
}

struct PanicOnDrop(Arc<AtomicUsize>);
impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("unclaimed listener capture dropped");
    }
}

fn completions(
    ctx: &Context,
    event: &'static str,
    operation: EventOperation,
) -> tokio::sync::mpsc::UnboundedReceiver<DispatchOutcomeKind> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    ctx.observe_runtime(observer_sync(move |_, record: RuntimeObservation| {
        if let RuntimeObservation::DispatchCompleted {
            event: completed_event,
            operation: completed_operation,
            outcome,
            ..
        } = record
            && completed_event == event
            && completed_operation == operation
        {
            tx.send(outcome).unwrap();
        }
        Ok::<(), Infallible>(())
    }))
    .unwrap();
    rx
}

#[tokio::test]
async fn emit_failure_survives_unclaimed_removed_listener_drop_and_reports_completion() {
    let ctx = Context::new();
    let reports = Arc::new(BufferExporter::new(4, Level::Warn).unwrap());
    let _exporter = ctx.add_exporter(reports.clone()).unwrap();
    let mut observed = completions(&ctx, Notify::NAME, EventOperation::Emit);
    let drops = Arc::new(AtomicUsize::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let mut later = Vec::new();
    for _ in 0..2 {
        let capture = PanicOnDrop(drops.clone());
        let b_hits = hits.clone();
        later.push(
            ctx.on::<Notify, _>(observer_sync(move |_, ()| {
                let _ = &capture;
                b_hits.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Infallible>(())
            }))
            .unwrap(),
        );
    }
    let b_slot = Arc::new(Mutex::new(later));
    let a_slot = b_slot.clone();
    let a_drops = drops.clone();
    ctx.on_with::<Notify, _>(
        observer_sync(move |_, ()| {
            for registration in std::mem::take(&mut *a_slot.lock()) {
                assert!(registration.remove());
                assert_eq!(a_drops.load(Ordering::SeqCst), 0, "snapshots retain B");
            }
            Err::<(), _>(io::Error::other("A failed"))
        }),
        ListenerOptions::default().prepend(),
    )
    .unwrap();

    let result = AssertUnwindSafe(ctx.emit::<Notify>(Routing::Unscoped, ()))
        .catch_unwind()
        .await;
    let Err(DispatchError::Invocation(failure)) = result.expect("snapshot Drop escaped emit")
    else {
        panic!("A's original failure must survive");
    };
    assert_eq!(failure.diagnostic(), "A failed");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observed.recv())
            .await
            .unwrap(),
        Some(DispatchOutcomeKind::Failed)
    );
    let reports = reports.snapshot();
    assert_eq!(reports.len(), 2, "each destructor panic is reported");
    for report in reports {
        assert_eq!(report.level(), Level::Warn);
        assert_eq!(
            report.text(),
            "cordis: unclaimed event listener snapshot destruction panicked: unclaimed listener capture dropped"
        );
    }
}

#[tokio::test]
async fn query_answer_survives_unclaimed_removed_listener_drop_and_reports_completion() {
    let ctx = Context::new();
    let mut observed = completions(&ctx, Ask::NAME, EventOperation::Query);
    let drops = Arc::new(AtomicUsize::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let capture = PanicOnDrop(drops.clone());
    let b_hits = hits.clone();
    let b = ctx
        .on::<Ask, _>(responder_sync(move |_, ()| {
            let _ = &capture;
            b_hits.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Infallible>(Some(99))
        }))
        .unwrap();
    let b_slot = Arc::new(Mutex::new(Some(b)));
    let a_slot = b_slot.clone();
    let a_drops = drops.clone();
    ctx.on_with::<Ask, _>(
        responder_sync(move |_, ()| {
            assert!(a_slot.lock().take().unwrap().remove());
            assert_eq!(a_drops.load(Ordering::SeqCst), 0, "snapshot retains B");
            Ok::<_, Infallible>(Some(42))
        }),
        ListenerOptions::default().prepend(),
    )
    .unwrap();

    let result = AssertUnwindSafe(ctx.query::<Ask>(Routing::Unscoped, ()))
        .catch_unwind()
        .await;
    assert!(matches!(
        result.expect("snapshot Drop escaped query"),
        Ok(QueryOutcome::Answer(42))
    ));
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observed.recv())
            .await
            .unwrap(),
        Some(DispatchOutcomeKind::Answered)
    );
}

#[tokio::test]
async fn waterfall_skipped_mapper_drop_preserves_tail_result_and_completion() {
    let ctx = Context::new();
    let mut observed = completions(&ctx, Flow::NAME, EventOperation::Waterfall);
    let drops = Arc::new(AtomicUsize::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let capture = PanicOnDrop(drops.clone());
    let b_hits = hits.clone();
    let b = ctx
        .on::<Flow, _>(mapper_sync(move |_, value| {
            let _ = &capture;
            b_hits.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Infallible>(value + 10)
        }))
        .unwrap();
    let b_slot = Arc::new(Mutex::new(Some(b)));
    let a_slot = b_slot.clone();
    let a_drops = drops.clone();
    ctx.on_with::<Flow, _>(
        mapper_sync(move |_, value| {
            assert!(a_slot.lock().take().unwrap().remove());
            assert_eq!(a_drops.load(Ordering::SeqCst), 0, "snapshot retains B");
            Ok::<_, Infallible>(value + 1)
        }),
        ListenerOptions::default().prepend(),
    )
    .unwrap();

    let result = AssertUnwindSafe(ctx.waterfall::<Flow, _, _, Infallible>(
        Routing::Unscoped,
        1,
        |value| async move { Ok(value + 1) },
    ))
    .catch_unwind()
    .await;
    assert_eq!(result.expect("snapshot Drop escaped waterfall").unwrap(), 3);
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observed.recv())
            .await
            .unwrap(),
        Some(DispatchOutcomeKind::Completed)
    );
}

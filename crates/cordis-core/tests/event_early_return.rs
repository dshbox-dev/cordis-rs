//! Public Event regression for destruction of unclaimed listener snapshots.

use cordis_core::event::{
    DispatchError, DispatchOutcomeKind, EventOperation, ListenerOptions, observer_sync,
    responder_sync,
};
use cordis_core::observation::RuntimeObservation;
use cordis_core::{Context, Event, QueryOutcome, Routing};
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
    let mut observed = completions(&ctx, Notify::NAME, EventOperation::Emit);
    let drops = Arc::new(AtomicUsize::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let capture = PanicOnDrop(drops.clone());
    let b_hits = hits.clone();
    let b = ctx
        .on::<Notify, _>(observer_sync(move |_, ()| {
            let _ = &capture;
            b_hits.fetch_add(1, Ordering::SeqCst);
            Ok::<(), Infallible>(())
        }))
        .unwrap();
    let b_slot = Arc::new(Mutex::new(Some(b)));
    let a_slot = b_slot.clone();
    let a_drops = drops.clone();
    ctx.on_with::<Notify, _>(
        observer_sync(move |_, ()| {
            assert!(a_slot.lock().take().unwrap().remove());
            assert_eq!(a_drops.load(Ordering::SeqCst), 0, "snapshot retains B");
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
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observed.recv())
            .await
            .unwrap(),
        Some(DispatchOutcomeKind::Failed)
    );
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

//! Issue 34 contract: owned waterfall and derived waterfall_query.

use cordis_core::event::{
    DispatchError, DispatchOutcomeKind, EventOperation, InvocationFailureKind, ListenerOptions,
    Next, around, mapper_sync, observer_sync, responder_sync,
};
use cordis_core::observation::RuntimeObservation;
use cordis_core::{Context, Event, Level, QueryOutcome, Routing, logger::BufferExporter};
use futures::FutureExt;
use std::convert::Infallible;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Flow;
impl Event for Flow {
    const NAME: &'static str = "issue34/flow";
    type Args = String;
    type Output = String;
}

struct Route;
impl Event for Route {
    const NAME: &'static str = "issue34/route";
    type Args = String;
    type Output = String;
}

struct PanicOnTailDrop(Arc<AtomicUsize>);

impl Drop for PanicOnTailDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("uncalled tail capture dropped");
    }
}

fn waterfall_completions(
    ctx: &Context,
) -> tokio::sync::mpsc::UnboundedReceiver<DispatchOutcomeKind> {
    let (completion_tx, completion_rx) = tokio::sync::mpsc::unbounded_channel();
    ctx.observe_runtime(observer_sync(move |_, record: RuntimeObservation| {
        if let RuntimeObservation::DispatchCompleted {
            operation: EventOperation::Waterfall,
            event: Flow::NAME,
            outcome,
            ..
        } = record
        {
            completion_tx.send(outcome).unwrap();
        }
        Ok::<(), Infallible>(())
    }))
    .unwrap();
    completion_rx
}

#[tokio::test]
async fn waterfall_is_an_owned_outer_to_inner_onion_and_veto_skips_tail() {
    let ctx = Context::new();
    let tail_hits = Arc::new(AtomicUsize::new(0));

    ctx.on::<Flow, _>(around(|_, value, next: Next<Flow>| async move {
        Ok::<_, cordis_core::event::InvocationFailure>(format!(
            "outer({})",
            next.call(value).await?
        ))
    }))
    .unwrap();
    ctx.on::<Flow, _>(mapper_sync(|_, value: String| {
        Ok::<_, Infallible>(format!("{value}!"))
    }))
    .unwrap();

    let hits = tail_hits.clone();
    let output = ctx
        .waterfall::<Flow, _, _, Infallible>(Routing::Unscoped, "x".into(), move |value| {
            let hits = hits.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(format!("{value}>"))
            }
        })
        .await
        .unwrap();
    assert_eq!(output, "outer(x!>)");
    assert_eq!(tail_hits.load(Ordering::SeqCst), 1);

    let veto = Context::new();
    veto.on::<Flow, _>(around(|_, _value, _next| async move {
        Ok::<_, io::Error>("veto".to_owned())
    }))
    .unwrap();
    let hits = tail_hits.clone();
    let output = veto
        .waterfall::<Flow, _, _, Infallible>(Routing::Unscoped, "x".into(), move |value| {
            let hits = hits.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(value)
            }
        })
        .await
        .unwrap();
    assert_eq!(output, "veto");
    assert_eq!(
        tail_hits.load(Ordering::SeqCst),
        1,
        "veto does not consume Next or run tail"
    );
}

#[tokio::test]
async fn waterfall_correlates_listener_failure_but_framework_tail_has_no_registration() {
    let mapper_ctx = Context::new();
    mapper_ctx
        .on::<Flow, _>(mapper_sync(|_, _| {
            Err::<String, _>(io::Error::other("mapper"))
        }))
        .unwrap();
    let DispatchError::Invocation(mapper_failure) = mapper_ctx
        .waterfall::<Flow, _, _, Infallible>(Routing::Unscoped, "x".into(), |value| async move {
            Ok(value)
        })
        .await
        .unwrap_err()
    else {
        panic!("expected invocation failure")
    };
    assert_eq!(mapper_failure.kind(), InvocationFailureKind::ReturnedError);
    assert_eq!(mapper_failure.diagnostic(), "mapper");
    assert!(mapper_failure.registration_id().is_some());

    let tail_ctx = Context::new();
    let DispatchError::Invocation(tail_failure) = tail_ctx
        .waterfall::<Flow, _, _, io::Error>(Routing::Unscoped, "x".into(), |_| async move {
            Err(io::Error::other("tail"))
        })
        .await
        .unwrap_err()
    else {
        panic!("expected invocation failure")
    };
    assert_eq!(tail_failure.diagnostic(), "tail");
    assert!(tail_failure.registration_id().is_none());
}

#[tokio::test]
async fn mapper_failure_survives_uncalled_tail_destructor_and_publishes_completion() {
    let ctx = Context::new();
    let mut completion_rx = waterfall_completions(&ctx);
    ctx.on::<Flow, _>(mapper_sync(|_, _: String| {
        Err::<String, _>(io::Error::other("mapper-original"))
    }))
    .unwrap();

    let drops = Arc::new(AtomicUsize::new(0));
    let tail_hits = Arc::new(AtomicUsize::new(0));
    let capture = PanicOnTailDrop(drops.clone());
    let hits = tail_hits.clone();
    let dispatch = AssertUnwindSafe(ctx.waterfall::<Flow, _, _, Infallible>(
        Routing::Unscoped,
        "x".into(),
        move |value| {
            let _ = &capture;
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(value)
            }
        },
    ))
    .catch_unwind()
    .await;
    assert!(
        dispatch.is_ok(),
        "uncalled tail Drop replaced mapper failure"
    );
    let Err(DispatchError::Invocation(failure)) = dispatch.unwrap() else {
        panic!("the mapper's returned error must remain the primary failure");
    };
    assert_eq!(failure.kind(), InvocationFailureKind::ReturnedError);
    assert_eq!(
        failure.diagnostic(),
        "mapper-original; uncalled waterfall continuation destruction panicked: uncalled tail capture dropped"
    );
    assert!(failure.registration_id().is_some());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(tail_hits.load(Ordering::SeqCst), 0);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), completion_rx.recv())
            .await
            .expect("failed waterfall completion was not published"),
        Some(DispatchOutcomeKind::Failed)
    );
}

#[tokio::test]
async fn preflight_failure_survives_unused_tail_destructor_and_publishes_completion() {
    let ctx = Context::new();
    let reports = Arc::new(BufferExporter::new(4, Level::Warn).unwrap());
    let _exporter = ctx.add_exporter(reports.clone()).unwrap();
    let mut completion_rx = waterfall_completions(&ctx);
    ctx.on::<Flow, _>(responder_sync(|_, value| Ok::<_, Infallible>(Some(value))))
        .unwrap();

    let drops = Arc::new(AtomicUsize::new(0));
    let tail_hits = Arc::new(AtomicUsize::new(0));
    let capture = PanicOnTailDrop(drops.clone());
    let observed_tail_hits = tail_hits.clone();
    let dispatch = AssertUnwindSafe(ctx.waterfall::<Flow, _, _, Infallible>(
        Routing::Unscoped,
        "x".into(),
        move |value| {
            let _ = &capture;
            observed_tail_hits.fetch_add(1, Ordering::SeqCst);
            async move { Ok(value) }
        },
    ))
    .catch_unwind()
    .await;
    assert!(
        dispatch.is_ok(),
        "unused tail Drop replaced preflight failure"
    );
    assert!(matches!(
        dispatch.unwrap(),
        Err(DispatchError::IncompatibleRole {
            operation: EventOperation::Waterfall,
            role: cordis_core::event::ListenerRole::Responder,
        })
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(tail_hits.load(Ordering::SeqCst), 0);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), completion_rx.recv())
            .await
            .expect("failed waterfall completion was not published"),
        Some(DispatchOutcomeKind::Failed)
    );
    let reports = reports.snapshot();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].level(), Level::Warn);
    assert_eq!(
        reports[0].text(),
        "cordis: uncalled waterfall tail destruction panicked: uncalled tail capture dropped"
    );
}

#[tokio::test]
async fn waterfall_contains_tail_panic_and_outer_around_can_recover_downstream_failure() {
    let ctx = Context::new();
    ctx.on::<Flow, _>(around(|_, value, next: Next<Flow>| async move {
        match next.call(value).await {
            Ok(output) => Ok::<_, io::Error>(output),
            Err(error) => Ok::<_, io::Error>(format!("recovered:{}", error.diagnostic())),
        }
    }))
    .unwrap();
    let output = ctx
        .waterfall::<Flow, _, _, io::Error>(Routing::Unscoped, "x".into(), |_| async move {
            panic!("tail panic");
            #[allow(unreachable_code)]
            Ok(String::new())
        })
        .await
        .unwrap();
    assert_eq!(output, "recovered:tail panic");
}

#[tokio::test]
async fn waterfall_query_uses_same_routing_and_resolver_sees_answer_miss_and_failure() {
    let root = Context::new();
    let selected = root.with_child_scope();
    let sibling = root.with_child_scope();

    selected
        .on::<Route, _>(responder_sync(|_, value: String| {
            Ok::<_, Infallible>(Some(format!("selected:{value}")))
        }))
        .unwrap();
    sibling
        .on::<Route, _>(responder_sync(|_, value: String| {
            Ok::<_, Infallible>(Some(format!("sibling:{value}")))
        }))
        .unwrap();
    root.on::<Flow, _>(mapper_sync(|_, value: String| {
        Ok::<_, Infallible>(value.to_uppercase())
    }))
    .unwrap();

    let routing = Routing::Scoped(selected.scope());
    let output = root
        .waterfall_query::<Flow, Route, _, _, Infallible>(
            routing,
            "ab".into(),
            |query| async move {
                match query {
                    Ok(QueryOutcome::Answer(answer)) => Ok(answer),
                    Ok(QueryOutcome::Miss) => Ok("miss".into()),
                    Err(error) => Ok(format!("failure:{error}")),
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(output, "selected:AB");

    let miss_ctx = Context::new();
    let miss = miss_ctx
        .waterfall_query::<Flow, Route, _, _, Infallible>(
            Routing::Unscoped,
            "q".into(),
            |query| async move {
                assert!(matches!(query, Ok(QueryOutcome::Miss)));
                Ok("resolved-miss".into())
            },
        )
        .await
        .unwrap();
    assert_eq!(miss, "resolved-miss");

    let fail_ctx = Context::new();
    fail_ctx
        .on::<Route, _>(responder_sync(|_, _| {
            Err::<Option<String>, _>(io::Error::other("route down"))
        }))
        .unwrap();
    let failure = fail_ctx
        .waterfall_query::<Flow, Route, _, _, Infallible>(
            Routing::Unscoped,
            "q".into(),
            |query| async move {
                let Err(DispatchError::Invocation(failure)) = query else {
                    panic!("resolver must see query failure")
                };
                assert!(failure.registration_id().is_some());
                Ok(format!("resolved:{}", failure.diagnostic()))
            },
        )
        .await
        .unwrap();
    assert_eq!(failure, "resolved:route down");
}

#[tokio::test]
async fn waterfall_query_resolver_failure_enters_tail_without_listener_identity_and_is_recoverable()
{
    let ctx = Context::new();
    ctx.on::<Flow, _>(around(|_, value, next: Next<Flow>| async move {
        match next.call(value).await {
            Ok(output) => Ok::<_, io::Error>(output),
            Err(error) => {
                assert!(error.registration_id().is_none());
                Ok::<_, io::Error>(format!("caught:{}", error.diagnostic()))
            }
        }
    }))
    .unwrap();
    let output = ctx
        .waterfall_query::<Flow, Route, _, _, io::Error>(
            Routing::Unscoped,
            "q".into(),
            |_query| async move { Err(io::Error::other("resolver")) },
        )
        .await
        .unwrap();
    assert_eq!(output, "caught:resolver");
}

#[tokio::test]
async fn prepend_is_outermost_for_waterfall() {
    let ctx = Context::new();
    ctx.on::<Flow, _>(around(|_, value, next: Next<Flow>| async move {
        Ok::<_, cordis_core::event::InvocationFailure>(format!("a({})", next.call(value).await?))
    }))
    .unwrap();
    ctx.on_with::<Flow, _>(
        around(|_, value, next: Next<Flow>| async move {
            Ok::<_, cordis_core::event::InvocationFailure>(format!(
                "p({})",
                next.call(value).await?
            ))
        }),
        ListenerOptions::default().prepend(),
    )
    .unwrap();
    let output = ctx
        .waterfall::<Flow, _, _, Infallible>(Routing::Unscoped, "x".into(), |value| async move {
            Ok(value)
        })
        .await
        .unwrap();
    assert_eq!(output, "p(a(x))");
}

#[tokio::test]
async fn waterfall_transports_move_only_args_without_clone() {
    struct MoveFlow;
    struct MoveOnly(String);
    impl Event for MoveFlow {
        const NAME: &'static str = "issue34/move-only";
        type Args = MoveOnly;
        type Output = usize;
    }

    let ctx = Context::new();
    ctx.on::<MoveFlow, _>(mapper_sync(|_, mut value: MoveOnly| {
        value.0.push_str("-mapped");
        Ok::<_, Infallible>(value)
    }))
    .unwrap();

    let output = ctx
        .waterfall::<MoveFlow, _, _, Infallible>(
            Routing::Unscoped,
            MoveOnly("owned".into()),
            |value| async move { Ok(value.0.len()) },
        )
        .await
        .unwrap();
    assert_eq!(output, "owned-mapped".len());
}

#[tokio::test]
async fn outer_passthrough_preserves_the_inner_listener_failure_identity() {
    let ctx = Context::new();
    ctx.on::<Flow, _>(around(|_, value, next: Next<Flow>| async move {
        next.call(value).await
    }))
    .unwrap();
    ctx.on::<Flow, _>(around(|_, _value, _next: Next<Flow>| async move {
        Err::<String, _>(io::Error::other("inner"))
    }))
    .unwrap();

    let DispatchError::Invocation(failure) = ctx
        .waterfall::<Flow, _, _, Infallible>(Routing::Unscoped, "x".into(), |value| async move {
            Ok(value)
        })
        .await
        .unwrap_err()
    else {
        panic!("expected invocation failure")
    };
    assert_eq!(failure.diagnostic(), "inner");
    assert!(failure.registration_id().is_some());
}

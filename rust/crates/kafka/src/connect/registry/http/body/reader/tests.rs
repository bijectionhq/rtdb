use std::{sync::Arc, task::Poll, time::Duration};
use futures::stream;
use rtdb_columnar::MemoryBudget;
use rtdb_storage::IoRegion;
use super::*;

#[tokio::test]
async fn a_completed_consumer_closes_without_polling_a_later_body_failure() {
    let budget = MemoryBudget::new(1 << 20);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let charge = context.reserve(256).unwrap();
    let region = IoRegion::new();
    let mut frame = Some(Bytes::from_static(b"data"));
    let source = stream::poll_fn(move |_| Poll::Ready(Some(frame.take().ok_or(Error("undemanded trailing frame failed")))));
    let input = BodyInput::new(source, 64);
    let got = decode_body(input, charge, &region, &mut context, |reader, parser| {
        assert_eq!(reader.available(), 0);
        let mut bytes = [0; 2];
        assert_eq!(reader.read(&mut bytes).unwrap(), 2);
        assert_eq!(reader.available(), 2);
        reader.close();
        assert_eq!(reader.available(), 0);
        assert_eq!(reader.read(&mut [0]).unwrap(), 0);
        Ok((bytes, parser.reserve(128)?))
    }).await.unwrap();
    assert_eq!(&got.0, b"da");
    assert!(context.retains(&got.1));
    region.close_and_wait().await;
    drop((context, got));
    assert_eq!(budget.used_bytes(), 0);
}

#[tokio::test]
async fn demand_preserves_source_and_byte_limit_errors_after_the_first_window() {
    for too_many_bytes in [false, true] {
        let budget = MemoryBudget::new(1 << 20);
        let mut context = ConversionContext::new(budget);
        let charge = context.reserve(256).unwrap();
        let region = IoRegion::new();
        let second = if too_many_bytes { Ok(Bytes::from_static(b"56789")) } else { Err(Error("failed refill")) };
        let source = stream::iter([Ok(Bytes::new()), Ok(Bytes::from_static(b"1234")), second]);
        let result: Result<()> = decode_body(BodyInput::new(source, 8), charge, &region, &mut context, move |reader, parser| {
            let mut bytes = [0; 8];
            assert_eq!(reader.read(&mut bytes).unwrap(), 4);
            assert_eq!(&bytes[..4], b"1234");
            let cause = reader.read(&mut bytes).unwrap_err();
            assert_eq!(matches!(&cause, ReadFailure::Native(_)), too_many_bytes);
            parser.reserve(32)?;
            Err(cause.into_anomaly())
        }).await;
        assert!(result.is_err());
        assert!(context.into_charges().iter().any(|charge| charge.bytes() == 32));
        region.close_and_wait().await;
    }
}

#[tokio::test]
async fn cancelling_a_pending_read_unblocks_the_assignment_drain() {
    let budget = MemoryBudget::new(1 << 20);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let charge = context.reserve(256).unwrap();
    let region = IoRegion::new();
    let (sender, mut read_started) = tokio::sync::oneshot::channel();
    let mut sender = Some(sender);
    let source = stream::poll_fn(move |_| {
        if let Some(sender) = sender.take() { sender.send(()).unwrap(); }
        Poll::Pending
    });
    let mut decode = Box::pin(decode_body(BodyInput::new(source, 64), Arc::clone(&charge), &region, &mut context, |reader, _| {
        reader.read(&mut [0]).map_err(ReadFailure::into_anomaly)
    }));
    tokio::select! {
        start = &mut read_started => start.unwrap(),
        _ = &mut decode => panic!("read should remain pending"),
    }
    drop(decode);
    tokio::time::timeout(Duration::from_secs(5), region.close_and_wait()).await.unwrap();
    assert!(context.retains(&charge));
    drop((context, charge));
    assert_eq!(budget.used_bytes(), 0);
}

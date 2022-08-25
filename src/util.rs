//! Helper structs that facilitate setting up a `rework` system.

use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, Sink};

use crate::WorkFn;

/// A no-op `Sink` type that is used as the default `tx` and `panic_tx` of a `Builder`.
#[derive(Clone)]
pub struct NoopSink<T> {
    _pd: PhantomData<T>,
}

impl<T> NoopSink<T> {
    pub(crate) fn new() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T> Sink<T> for NoopSink<T> {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _item: T) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// A no-op `WorkFn<(), ()>` type that is used by the default `make_init_fn` of a `Builder`.
#[derive(Clone)]
pub struct NoopInitFn;

impl WorkFn<(), ()> for NoopInitFn {
    fn work(&self, _req: ()) -> Pin<Box<dyn Future<Output = ()>>> {
        async {}.boxed_local()
    }
}

/// A wrapper struct that makes a function that returns a future into a proper `make_init_fn`.
pub struct InitFn<Fut> {
    pub(crate) inner: Box<dyn Fn() -> Fut + Send + 'static>,
}

impl<Fut> WorkFn<(), (), Fut> for InitFn<Fut>
where
    Fut: Future<Output = ()>,
{
    fn work(&self, _req: ()) -> Fut {
        (self.inner)()
    }
}

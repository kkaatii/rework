//! Multi-threaded task processing in the Master-Worker pattern.
//!
//! # Features
//! * Multi-threaded workers, each with a single-thread async runtime, that can work on `!Send` futures.
//! * Async as an option--you don't necessarily need an async runtime at all.
//! * Load balanced, as long as `Workload` is properly defined.
//!
//! # Usage
//! First define the [`Request`] type and the [`WorkFn`].
//! ```rust
//! use futures::future::FutureExt;
//! use rework::{Request, WorkFn, Workload};
//!
//! // Debug is a supertrait of Request
//! #[derive(Debug)]
//! enum HelloOrEcho {
//!     Hello,
//!     Echo(String),
//! }
//!
//! impl Request for HelloOrEcho {
//!     fn workload(&self) -> Workload {
//!         1.into()    
//!     }
//! }
//!
//! // WorkFn is implemented for async fns that take a Request type parameter and return a Send + 'static
//! async fn work(req: HelloOrEcho) -> String {
//!     match req {
//!         HelloOrEcho::Hello => "Hello".to_owned(),
//!         HelloOrEcho::Echo(s) => s,
//!     }
//! }
//! ```
//! Then use [`builder()`] to get the system up and running:
//! ```rust
//! # use futures::future::FutureExt;
//! # use rework::{Request, WorkFn, Workload};
//! # #[derive(Debug)]
//! # enum HelloOrEcho {
//! #     Hello,
//! #     Echo(String),
//! # }
//! # impl Request for HelloOrEcho {
//! #     fn workload(&self) -> Workload {
//! #         1.into()    
//! #     }
//! # }
//! # async fn work(req: HelloOrEcho) -> String {
//! #     match req {
//! #         HelloOrEcho::Hello => "Hello".to_owned(),
//! #         HelloOrEcho::Echo(s) => s,
//! #     }
//! # }
//! use futures::channel::mpsc;
//! use futures::stream::StreamExt;
//! use tokio::runtime::Builder;
//!
//! let msg = "Heeey".to_owned();
//! let (tx, mut rx) = mpsc::unbounded();
//!
//! let handle = rework::builder(|| work).sink(tx).build();
//! handle.request(HelloOrEcho::Echo(msg.clone()));
//!
//! let rt = Builder::new_current_thread().build().unwrap();
//! rt.block_on(async {
//!     let recv = rx.next().await;
//!     assert_eq!(recv, Some(msg));
//! });
//! ```

use std::{
    fmt::Debug, future::Future, marker::PhantomData, num::NonZeroUsize, pin::Pin, sync::Arc,
    time::Duration,
};

mod dispatcher;
pub mod panic;
pub mod scheduler;
pub mod util;
mod worker;
mod workload;

pub use dispatcher::{CommandAgent, Handle, RequestAgent};
use futures::Sink;
use panic::PanicInfo;
use scheduler::{P2cScheduler, Scheduler};
use util::{InitFn, NoopInitFn, NoopSink};
pub use workload::Workload;

/// Shorthand for `Pin<Box<dyn Future<Output = T>>>`.
pub type Async<T> = Pin<Box<dyn Future<Output = T>>>;

/// Defines how workers should respond to a request.
///
/// This trait is blanketly implemented for most `async fn`s.
pub trait WorkFn<Req, Resp, Fut = Async<Resp>>
where
    Fut: Future<Output = Resp>,
{
    fn work(&self, req: Req) -> Fut;
}

impl<Req, Resp, F, Fut> WorkFn<Req, Resp, Fut> for F
where
    Req: Request,
    Resp: Send + 'static,
    Fut: Future<Output = Resp> + 'static,
    F: Fn(Req) -> Fut + Clone + 'static,
{
    fn work(&self, req: Req) -> Fut {
        self(req)
    }
}

/// A trait that the actual request type must implement.
///
/// A request must implement `Debug` so as to improve log ergonomics.
pub trait Request: Debug + Send + 'static {
    /// Workload of a request.
    ///
    /// Workload is used by the [`scheduler::Scheduler`] when dispatching incoming requests to make the system load balanced.
    fn workload(&self) -> Workload {
        1.into()
    }
}

/// Commands that can be sent to workers to control how they behave.
#[derive(Debug, Clone)]
pub enum Command {
    Shutdown,
}

/// Returns a pre-configured builder that can be used to set up a `rework` system.
pub fn builder<Req, Resp, Fut, WFn>(
    make_work_fn: impl Fn() -> WFn + Send + Sync + 'static,
) -> Builder<Req, Resp, Fut, WFn, NoopInitFn, NoopSink<Resp>, NoopSink<PanicInfo>, P2cScheduler>
where
    Req: Request,
    Resp: Send + 'static,
    Fut: Future<Output = Resp> + 'static,
    WFn: WorkFn<Req, Resp, Fut>,
{
    Builder::new(0, make_work_fn)
}

/// Builder of a `rework` system.
///
/// The following parameters control how the system works:
/// * `num_workers` - number of worker threads
/// * `make_work_fn` - function that deploys the `WorkFn` for each worker thread
/// * `make_init_fn` - function that returns a future that will be run once at the very beginning of each worker thread; useful for setting up database connections etc.
/// * `sink` - `Sink` used by the dispatcher to send back responses
/// * `panic_sink` - `Sink` used by the dispatcher to send back `PanicInfo`s
/// * `sched` - `Scheduler` that decides how incoming requests are dispatched to workers based on their workloads
/// * `shutdown_grace_period` - seconds to wait since the last request before
pub struct Builder<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched> {
    num_workers: NonZeroUsize,
    make_work_fn: Box<dyn Fn() -> WFn + Send + Sync + 'static>,
    make_init_fn: Box<dyn Fn() -> IFn + Send + Sync + 'static>,
    sink: Si,
    panic_sink: PSi,
    sched: Sched,
    shutdown_grace_period: Duration,
    _pd: PhantomData<(Req, Resp, Fut)>,
}

impl<Req, Resp, Fut, WFn>
    Builder<Req, Resp, Fut, WFn, NoopInitFn, NoopSink<Resp>, NoopSink<PanicInfo>, P2cScheduler>
where
    Req: Request,
    Resp: Send + 'static,
    Fut: Future<Output = Resp> + 'static,
    WFn: WorkFn<Req, Resp, Fut>,
{
    /// Creates a builder struct for setting up the dispatcher and the workers.
    ///
    /// If `num_workers == 0`, a number determined by the CPU cores will be set instead.
    pub fn new(
        mut num_workers: usize,
        make_work_fn: impl Fn() -> WFn + Send + Sync + 'static,
    ) -> Self {
        if num_workers == 0 {
            let n = num_cpus::get();
            num_workers = if n >= 3 { n - 2 } else { 1 };
        }
        let num_workers = NonZeroUsize::new(num_workers).unwrap();
        Self {
            num_workers,
            make_work_fn: Box::new(make_work_fn),
            make_init_fn: Box::new(|| NoopInitFn),
            sink: NoopSink::new(),
            panic_sink: NoopSink::new(),
            sched: P2cScheduler::new(num_workers),
            shutdown_grace_period: Duration::from_secs(30),
            _pd: PhantomData,
        }
    }
}

impl<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched> Builder<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched> {
    /// Sets `shutdown_grace_period`.
    pub fn shutdown_grace_period(mut self, period: Duration) -> Self {
        self.shutdown_grace_period = period;
        self
    }
}

impl<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched> Builder<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched>
where
    Sched: Scheduler,
{
    /// Sets `num_workers`.
    ///
    /// If `n == 0`, a number determined by the CPU cores will be set instead.
    pub fn num_workers(mut self, mut n: usize) -> Self {
        if n == 0 {
            n = num_cpus::get();
            n = if n >= 3 { n - 2 } else { 1 };
        }
        self.num_workers = NonZeroUsize::new(n).unwrap();
        self.sched.init(self.num_workers);
        self
    }
}

impl<Req, Resp, Fut, WFn, Si, PSi, Sched> Builder<Req, Resp, Fut, WFn, NoopInitFn, Si, PSi, Sched> {
    /// Sets `make_init_fn` from a `raw_fn`.
    pub fn make_init_fn_from<IFn, IFut>(
        self,
        raw_fn: IFn,
    ) -> Builder<Req, Resp, Fut, WFn, InitFn<IFut>, Si, PSi, Sched>
    where
        IFn: Fn() -> IFut + Send + Sync + 'static,
        IFut: Future<Output = ()> + 'static,
    {
        let raw_fn = Arc::new(raw_fn);
        let raw_fn_cp = Arc::clone(&raw_fn);
        Builder {
            num_workers: self.num_workers,
            make_work_fn: self.make_work_fn,
            make_init_fn: Box::new(move || {
                let raw_fn = raw_fn_cp.clone();
                InitFn {
                    inner: Box::new(move || raw_fn()),
                }
            }),
            sink: self.sink,
            panic_sink: self.panic_sink,
            sched: self.sched,
            shutdown_grace_period: self.shutdown_grace_period,
            _pd: PhantomData,
        }
    }
}

impl<Req, Resp, Fut, WFn, IFn, PSi, Sched>
    Builder<Req, Resp, Fut, WFn, IFn, NoopSink<Resp>, PSi, Sched>
{
    /// Sets `sink`.
    pub fn sink<Si>(self, sink: Si) -> Builder<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched>
    where
        Si: Sink<Resp> + Clone + Send + 'static,
        Si::Error: Debug,
    {
        Builder {
            num_workers: self.num_workers,
            make_work_fn: self.make_work_fn,
            make_init_fn: self.make_init_fn,
            sink,
            panic_sink: self.panic_sink,
            sched: self.sched,
            shutdown_grace_period: self.shutdown_grace_period,
            _pd: PhantomData,
        }
    }
}

impl<Req, Resp, Fut, WFn, IFn, Si, Sched>
    Builder<Req, Resp, Fut, WFn, IFn, Si, NoopSink<PanicInfo>, Sched>
{
    /// Sets `panic_sink`.
    pub fn panic_sink<PSi>(
        self,
        panic_sink: PSi,
    ) -> Builder<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched>
    where
        PSi: Sink<PanicInfo> + Clone + Send + 'static,
        PSi::Error: Debug,
    {
        Builder {
            num_workers: self.num_workers,
            make_work_fn: self.make_work_fn,
            make_init_fn: self.make_init_fn,
            sink: self.sink,
            panic_sink,
            sched: self.sched,
            shutdown_grace_period: self.shutdown_grace_period,
            _pd: PhantomData,
        }
    }
}

impl<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched> Builder<Req, Resp, Fut, WFn, IFn, Si, PSi, Sched>
where
    Req: Request,
    Resp: Send + 'static,
    Fut: Future<Output = Resp> + 'static,
    WFn: WorkFn<Req, Resp, Fut> + 'static,
    IFn: WorkFn<(), ()> + 'static,
    Si: Sink<Resp> + Clone + Send + 'static,
    Si::Error: Debug,
    PSi: Sink<PanicInfo> + Clone + Send + 'static,
    PSi::Error: Debug,
    Sched: Scheduler + Send + 'static,
{
    /// Builds a `rework` system by setting up the dispatcher thread and the worker threads, and returns a `Handle` to the dispatcher.
    pub fn build(self) -> Handle<Req> {
        dispatcher::setup(
            self.num_workers,
            self.sink,
            self.panic_sink,
            self.make_work_fn,
            self.make_init_fn,
            Box::new(self.sched),
            self.shutdown_grace_period,
        )
    }
}

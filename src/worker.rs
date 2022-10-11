use std::{
    cell::RefCell,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use futures::{
    channel::mpsc::{channel, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender},
    FutureExt, StreamExt,
};
use pandet::{PanicMonitor, UnsendOnPanic};
use tokio::{
    runtime::Builder,
    task,
    time::{self, Duration, Instant, Sleep},
};
use tracing::{error, info, span, warn, Instrument, Level};

use crate::PanicInfo;

use super::{Command, Request, WorkFn, Workload};

thread_local! {
    static REQ_COUNTER: RefCell<usize> = RefCell::new(0);
}

/// Convenient function for sending a message of type `T` through an `UnboundedSender<T>`.
#[inline]
fn send<T>(tx: &UnboundedSender<T>, payload: T, err_msg: &str) -> Result<(), T> {
    if let Err(e) = tx.unbounded_send(payload) {
        warn!("{}: {:?}", err_msg, e);
        Err(e.into_inner())
    } else {
        Ok(())
    }
}

struct ConnTimeout {
    thres: Duration,
    last: Instant,
    sleep: Pin<Box<Sleep>>,
}

impl ConnTimeout {
    pub fn new(thres: Duration) -> Self {
        ConnTimeout {
            thres,
            last: Instant::now(),
            sleep: Box::pin(time::sleep(thres)),
        }
    }

    pub fn reset(&mut self) {
        self.last = Instant::now();
        self.sleep
            .as_mut()
            .reset(Instant::now().checked_add(self.thres).unwrap());
    }
}

impl Future for ConnTimeout {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.sleep.poll_unpin(cx)
    }
}

struct InnerWorker<Req, Resp, Fut, WFn> {
    resp_tx: UnboundedSender<(Workload, Resp)>,
    task_fut: WFn,
    _pd: PhantomData<(Req, Fut)>,
}

impl<Req, Resp, Fut, WFn> InnerWorker<Req, Resp, Fut, WFn>
where
    Req: Request,
    Resp: Send + 'static,
    WFn: WorkFn<Req, Resp, Fut> + 'static,
    Fut: Future<Output = Resp>,
{
    async fn work(&self, req: Req) {
        info!("New request {:?}", &req);

        let wl = req.workload();
        let resp = self.task_fut.work(req).await;
        let _ = send(
            &self.resp_tx,
            (wl, resp),
            "Error sending response to Dispatcher",
        );
    }
}

//                   ┌─────────────────┐
//         cmd_rx -->│                 │
//                   │  worker-thread  │--> resp_tx
//         req_rx -->│                 │
//                   └─────────────────┘
pub(crate) struct Worker<Req, Resp> {
    id: usize,
    agent: WorkerAgent<Req>,
    req_rx: UnboundedReceiver<Req>,
    resp_tx: UnboundedSender<(Workload, Resp)>,
    panic_tx: UnboundedSender<(Workload, PanicInfo)>,
    cmd_rx: Receiver<Command>,
    shutdown_grace_period: Duration,
}

impl<Req, Resp> Worker<Req, Resp> {
    pub(crate) fn new(
        id: usize,
        resp_tx: UnboundedSender<(Workload, Resp)>,
        panic_tx: UnboundedSender<(Workload, PanicInfo)>,
        shutdown_grace_period: Duration,
    ) -> Self {
        let (req_tx, req_rx) = unbounded();
        let (cmd_tx, cmd_rx) = channel(0);

        Worker {
            id,
            agent: WorkerAgent { id, req_tx, cmd_tx },
            req_rx,
            resp_tx,
            panic_tx,
            cmd_rx,
            shutdown_grace_period,
        }
    }

    pub(crate) fn new_agent(&self) -> WorkerAgent<Req> {
        self.agent.clone()
    }

    /// Deploys the `Worker` onto a spawned `Thread`. Returns `Worker.id` when the thread exits.
    pub(crate) fn deploy<Fut, WFn, IFn>(
        mut self,
        make_work_fn: impl Fn() -> WFn + Clone + Send + 'static,
        make_init_fn: impl Fn() -> IFn + Clone + Send + 'static,
    ) -> thread::JoinHandle<usize>
    where
        Req: Request,
        Resp: Send + 'static,
        Fut: Future<Output = Resp> + 'static,
        WFn: WorkFn<Req, Resp, Fut> + 'static,
        Fut: Future<Output = Resp> + 'static,
        IFn: WorkFn<(), ()> + 'static,
    {
        let work = move || {
            let work_fn = make_work_fn();
            let init_fn = make_init_fn();
            let id = self.id;
            let span = span!(Level::ERROR, "worker", id = id);
            let _enter = span.enter();
            info!("Worker thread deployed");

            let rt = match Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => {
                    error!("Failed to build thread-local tokio runtime: {:?}", e);
                    return self.id;
                }
            };
            let local = task::LocalSet::new();
            local.block_on(&rt, async move {
                init_fn.work(()).await;

                let inner_worker = Box::new(InnerWorker {
                    resp_tx: self.resp_tx,
                    task_fut: work_fn,
                    _pd: PhantomData,
                });

                // FIXME: memory leak if worker thread exited prematurely
                let inner_worker: &'static InnerWorker<_, _, _, _> = &*Box::leak(inner_worker);

                let (ref mut monitor, det) = PanicMonitor::<(usize, String, Workload)>::new();

                let fut = async move {
                    let shutting_down = &mut false;
                    let conn_timeout = ConnTimeout::new(self.shutdown_grace_period);
                    tokio::pin!(conn_timeout);
                    loop {
                        tokio::select! {
                            Some(cmd) = self.cmd_rx.next(), if !*shutting_down => {
                                info!("Received command {cmd:?}");
                                match cmd {
                                    Command::Shutdown => {
                                        *shutting_down = true;
                                    },
                                }
                            },
                            Some(req) = self.req_rx.next(), if !*shutting_down => {
                                let req_id = REQ_COUNTER.with(|c| {
                                    *c.borrow_mut() += 1;
                                    *c.borrow()
                                });

                                let kind = format!("{req:?}");
                                let wl = req.workload();
                                let work_req_span = span!(Level::ERROR, "work-req", id=req_id);
                                task::spawn_local(
                                    async move {
                                        inner_worker.work(req).await;
                                    }
                                    .instrument(work_req_span)
                                    .unsend_on_panic_info(&det, (req_id, kind, wl))
                                );
                                conn_timeout.reset();
                            },
                            _ = &mut conn_timeout, if *shutting_down => {
                                info!("Connections timed out");
                                break;
                            },
                            else => {
                                warn!("WorkerAgent dropped");
                                break;
                            }
                        }
                    }
                };
                task::spawn_local(fut);

                while let Some(e) = monitor.next().await {
                    let (req_id, kind, wl) = e.0;
                    warn!("Panic detected when working on work-req{{id={req_id}}} {kind}");
                    let _ = send(
                        &self.panic_tx,
                        (wl, PanicInfo { request: kind }),
                        "Error reporting panick to Dispatcher",
                    );
                }
            });

            info!("Worker thread exited");
            id
        };

        thread::Builder::new()
            .name(format!("worker-thread-{}", self.id))
            .spawn(work)
            .expect("failed to deploy worker {self.id}")
    }
}

pub(crate) struct WorkerAgent<Req> {
    id: usize,
    req_tx: UnboundedSender<Req>,
    cmd_tx: Sender<Command>,
}

impl<Req> WorkerAgent<Req> {
    pub(crate) fn request(&self, req: Req) {
        let _ = send(
            &self.req_tx,
            req,
            &format!("Error sending work request to worker {}", self.id),
        );
    }

    pub(crate) fn command(&mut self, cmd: Command) {
        if let Err(e) = self.cmd_tx.try_send(cmd) {
            error!("Error sending command to worker {}: {:?}", self.id, e);
        }
    }
}

impl<Req> Clone for WorkerAgent<Req> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            req_tx: self.req_tx.clone(),
            cmd_tx: self.cmd_tx.clone(),
        }
    }
}

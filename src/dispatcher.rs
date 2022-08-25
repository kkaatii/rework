use std::{cell::RefCell, fmt::Debug, num::NonZeroUsize, rc::Rc, sync::Arc, thread};

use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    pin_mut, stream, Future, Sink, SinkExt, StreamExt,
};
use tokio::{runtime::Builder, task};
use tracing::{debug, error, info, span, warn, Level};

use crate::scheduler::Scheduler;

use super::{
    panic::PanicInfo,
    worker::{Worker, WorkerAgent},
    Command, Request, WorkFn, Workload,
};

const CONCURRENT_RESP_RX_FUTURE_LIMIT: usize = 0;

fn dispatch_request<Req>(
    sched: &mut dyn Scheduler,
    workloads: &mut [Workload],
    req: Req,
    worker_agents: &[WorkerAgent<Req>],
) where
    Req: Request,
{
    let i = sched.select(workloads);
    workloads[i] += req.workload();
    worker_agents[i].request(req);
    debug!(
        "Dispatched request to worker id={}, workload={}",
        i, workloads[i]
    );
}

pub(crate) fn setup<Req, Resp, WFn, Fut, IFn, Si, PSi, Sched>(
    num_workers: NonZeroUsize,
    outbound_tx: Si,
    outbound_panic_tx: PSi,
    make_work_fn: impl Fn() -> WFn + Send + Sync + 'static,
    make_init_fn: impl Fn() -> IFn + Send + Sync + 'static,
    sched: Box<Sched>,
) -> Handle<Req>
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
    Sched: Scheduler,
{
    let num_workers = num_workers.get();

    // "inbound" means from server to dispatcher
    let (inbound_tx, mut inbound_rx) = unbounded();
    let (cmd_tx, mut cmd_rx) = unbounded();

    let mut agents = vec![];
    let mut resp_rxs = vec![];
    let mut worker_handles = vec![];

    let make_task_fut = Arc::new(make_work_fn);
    let make_init_fn = Arc::new(make_init_fn);

    for i in 1..=num_workers {
        // Creating new pairs of tx/rx because we need to tell which worker abruptly exited
        let (resp_tx, resp_rx) = unbounded();
        let (panic_tx, panic_rx) = unbounded();
        let worker = Worker::new(i, resp_tx, panic_tx);
        agents.push(worker.new_agent());
        resp_rxs.push((resp_rx, panic_rx));
        let make_task_fut_cp = Arc::clone(&make_task_fut);
        let make_init_fn_cp = Arc::clone(&make_init_fn);
        worker_handles.push(worker.deploy(move || make_task_fut_cp(), move || make_init_fn_cp()));
    }

    let dispatch = move || {
        let span = span!(Level::ERROR, "dispatcher");
        let _enter = span.enter();

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        let local = task::LocalSet::new();

        local.block_on(&rt, async move {
            let is_shutting_down = Rc::new(RefCell::new(false));
            let is_shutting_down_cp = Rc::clone(&is_shutting_down);

            let agents = Rc::new(RefCell::new(agents));
            let agents_cp = Rc::clone(&agents);

            let workloads = Rc::new(RefCell::new(
                (0..num_workers)
                    .map(|_| 0usize.into())
                    .collect::<Vec<Workload>>(),
            ));
            let workloads_cp = Rc::clone(&workloads);

            let make_task_fut_cp = Arc::clone(&make_task_fut);
            let make_init_fn_cp = Arc::clone(&make_init_fn);

            #[allow(clippy::type_complexity)]
            let handle_worker_resp = move |(i, (resp_rx, panic_rx)): (
                usize,
                (
                    UnboundedReceiver<(Workload, Resp)>,
                    UnboundedReceiver<(Workload, PanicInfo)>,
                ),
            )| {
                let is_shutting_down = Rc::clone(&is_shutting_down_cp);
                let agents = Rc::clone(&agents_cp);
                let workloads = Rc::clone(&workloads_cp);
                let make_task_fut = Arc::clone(&make_task_fut_cp);
                let make_init_fn = Arc::clone(&make_init_fn_cp);

                let tx = outbound_tx.clone();
                let p_tx = outbound_panic_tx.clone();
                // rx might be re-assigned when a worker thread panics
                let mut rx = resp_rx;
                let mut p_rx = panic_rx;

                async move {
                    pin_mut!(tx);
                    pin_mut!(p_tx);
                    loop {
                        'inner: loop {
                            tokio::select! {
                                Some((wl, resp)) = rx.next() => {
                                    workloads.borrow_mut()[i] -= wl;
                                    if let Err(e) = tx.send(resp).await {
                                        error!("Error sending work response: {:?}", e);
                                        break 'inner;
                                    }
                                },
                                Some((wl, panic)) = p_rx.next() => {
                                    workloads.borrow_mut()[i] -= wl;
                                    if let Err(e) = p_tx.send(panic).await {
                                        error!("Error sending panic info: {:?}", e);
                                        break 'inner;
                                    }
                                },
                                else => {
                                    break 'inner;
                                }
                            }
                        }

                        // tries to spawn a new worker
                        if !*is_shutting_down.borrow() {
                            let (resp_tx, resp_rx) = unbounded();
                            let (panic_tx, panic_rx) = unbounded();
                            let worker = Worker::new(i, resp_tx, panic_tx);
                            agents.borrow_mut()[i] = worker.new_agent();
                            workloads.borrow_mut()[i] = 0usize.into();

                            let make_task_fut_cp = Arc::clone(&make_task_fut);
                            let make_init_fn_cp = Arc::clone(&make_init_fn);
                            worker.deploy(move || make_task_fut_cp(), move || make_init_fn_cp());
                            rx = resp_rx;
                            p_rx = panic_rx;
                        } else {
                            break;
                        }
                    }
                }
            };
            task::spawn_local(
                stream::iter(resp_rxs)
                    .enumerate()
                    .for_each_concurrent(CONCURRENT_RESP_RX_FUTURE_LIMIT, handle_worker_resp),
            );

            info!("Dispatcher thread deployed");
            let mut sched = *sched;
            loop {
                tokio::select! {
                    Some(cmd) = cmd_rx.next() => {
                        match cmd {
                            Command::Shutdown => {
                                info!("Received Shutdown command");
                                *is_shutting_down.borrow_mut() = true;
                                break;
                            },
                        }
                    },
                    Some(req) = inbound_rx.next() => {
                        dispatch_request(
                            &mut sched,
                            &mut workloads.borrow_mut(),
                            req,
                            &agents.borrow()
                        );
                    },
                    else => {
                        warn!("Handle dropped");
                        break;
                    },
                }
            }

            agents
                .borrow_mut()
                .iter_mut()
                .for_each(|agent| agent.command(Command::Shutdown));
        });
        worker_handles.into_iter().for_each(|handle| {
            let _ = handle.join();
        });
        info!("Dispatcher thread exited");
    };

    let thread_handle = thread::Builder::new()
        .name("dispatcher-thread".to_owned())
        .spawn(dispatch)
        .expect("failed to spawn the dispatcher thread");

    Handle {
        inbound_tx,
        cmd_tx,
        thread_handle,
    }
}

/// Handle to a dispatcher.
///
/// A dispatcher in a `rework` system is responsible for dispatching incoming requests among workers with the help of a `Scheduler`.
/// The dispatcher runs on a dedicated thread and its `Handle` allows sending `Request`s and `Command`s to it.
pub struct Handle<Req> {
    inbound_tx: UnboundedSender<Req>,
    cmd_tx: UnboundedSender<Command>,
    thread_handle: thread::JoinHandle<()>,
}

impl<Req> Handle<Req> {
    /// Waits for the dispatcher thread to finish.
    ///
    /// The dispatcher thread will finish after all the worker threads have finished.
    pub fn join(self) -> Result<(), Box<dyn std::any::Any + Send>> {
        self.thread_handle.join()
    }

    /// Creates a `RequestAgent` for the dispatcher.
    pub fn req_agent(&self) -> RequestAgent<Req> {
        RequestAgent {
            inbound_tx: self.inbound_tx.clone(),
        }
    }

    /// Dispatches a request among the workers.
    pub fn request(&self, req: Req) {
        self.inbound_tx
            .unbounded_send(req)
            .expect("error sending work request to the dispatcher thread");
    }

    /// Creates a `CommandAgent` for the dispatcher.
    pub fn cmd_agent(&self) -> CommandAgent {
        CommandAgent {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    /// Sends a command to all workers.
    pub fn command(&self, cmd: Command) {
        self.cmd_tx
            .unbounded_send(cmd)
            .expect("error sending worker command to the dispatcher thread");
    }
}

/// A helper struct that allows sending `Request`s to the workers.
pub struct RequestAgent<Req> {
    inbound_tx: UnboundedSender<Req>,
}

impl<Req> RequestAgent<Req> {
    /// Dispatches a request to a worker.
    pub fn request(&self, req: Req) {
        self.inbound_tx
            .unbounded_send(req)
            .expect("error sending work request to the dispatcher thread");
    }
}

impl<Req> Clone for RequestAgent<Req> {
    fn clone(&self) -> Self {
        Self {
            inbound_tx: self.inbound_tx.clone(),
        }
    }
}

/// A helper struct that allows sending `Command`s to the workers.
#[derive(Clone)]
pub struct CommandAgent {
    cmd_tx: UnboundedSender<Command>,
}

impl CommandAgent {
    /// Sends a command to all workers.
    pub fn command(&self, cmd: Command) {
        self.cmd_tx
            .unbounded_send(cmd)
            .expect("error sending worker command to the dispatcher thread");
    }
}

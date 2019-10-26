// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::configpb;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::Ordering;
// use std::sync::mpsc::{self, Sender};
use crate::raftstore::store::fsm::HandlerBuilder;
use crate::raftstore::store::transport::Transport;
use pd_client::PdClient;
use std::sync::Arc;
use std::thread;

use crate::raftstore::store::fsm::{
    ApplyControlFsm, ApplyFsm, PeerFsm, PoolHandlerBuilder, StoreFsm,
};
use crate::raftstore::store::{BatchRouter, Fsm, FsmTypes, Poller, PoolState};
use tikv_util::worker::Runnable;

pub enum Task {
    Update { cfg: configpb::ConfigEntry },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            Task::Update { cfg } => write!(f, "Update config: {:?}", cfg),
            _ => unreachable!(),
        }
    }
}

struct PoolControl<N: Fsm, C: Fsm, PT, PC> {
    pub router: BatchRouter<N, C>,
    pub state: PoolState<N, C, PT, PC>,
}

impl<N: Fsm, C: Fsm, PT, PC> PoolControl<N, C, PT, PC> {
    fn new(router: BatchRouter<N, C>, state: PoolState<N, C, PT, PC>) -> Self {
        PoolControl { router, state }
    }
}
impl<N: Fsm, C: Fsm, PT, PC> PoolControl<N, C, PT, PC>
where
    PT: Transport + 'static,
    PC: PdClient + 'static,
{
    fn resize_to(&mut self, size: usize) {}

    fn decrease_by(&mut self, size: usize) {
        for _ in 0..size {
            self.state.fsm_sender.send(FsmTypes::Empty);
        }
    }
}

impl<PT, PC> PoolControl<PeerFsm, StoreFsm, PT, PC>
where
    PT: Transport + 'static,
    PC: PdClient + 'static,
{
    fn increase_raft_by(&mut self, size: usize) {
        let name_prefix = self.state.name_prefix.clone();
        let workers = self.state.workers.lock().unwrap();
        let id_base = workers.len();
        for i in 0..size {
            let handler = match &self.state.handler_builder {
                PoolHandlerBuilder::Raft(r) => r.build(),
                _ => unreachable!(),
            };
            let pool_size = Arc::clone(&self.state.pool_size);
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.state.fsm_receiver.clone(),
                handler,
                max_batch_size: self.state.max_batch_size,
                pool_size,
            };
            let t = thread::Builder::new()
                .name(thd_name!(format!("{}-{}", name_prefix, i + id_base)))
                .spawn(move || poller.poll())
                .unwrap();
            self.state.workers.lock().unwrap().push(t);
        }
        let _ = self.state.pool_size.fetch_add(size, Ordering::AcqRel);
    }
}

impl<PT, PC> PoolControl<ApplyFsm, ApplyControlFsm, PT, PC>
where
    PT: Transport + 'static,
    PC: PdClient + 'static,
{
    fn increase_apply_by(&mut self, size: usize) {
        let name_prefix = self.state.name_prefix.clone();
        let workers = self.state.workers.lock().unwrap();
        let id_base = workers.len();
        for i in 0..size {
            let handler = match &self.state.handler_builder {
                PoolHandlerBuilder::Apply(a) => a.build(),
                _ => unreachable!(),
            };
            let pool_size = Arc::clone(&self.state.pool_size);
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.state.fsm_receiver.clone(),
                handler,
                max_batch_size: self.state.max_batch_size,
                pool_size,
            };
            let t = thread::Builder::new()
                .name(thd_name!(format!("{}-{}", name_prefix, i + id_base)))
                .spawn(move || poller.poll())
                .unwrap();
            self.state.workers.lock().unwrap().push(t);
        }
        let _ = self.state.pool_size.fetch_add(size, Ordering::AcqRel);
    }
}

pub struct Runner<T, C> {
    apply_pool: PoolControl<ApplyFsm, ApplyControlFsm, T, C>,
    raft_pool: PoolControl<PeerFsm, StoreFsm, T, C>,
}

impl<T, C> Runner<T, C> {
    pub fn new(
        apply_router: BatchRouter<ApplyFsm, ApplyControlFsm>,
        apply_state: PoolState<ApplyFsm, ApplyControlFsm, T, C>,
        raft_router: BatchRouter<PeerFsm, StoreFsm>,
        raft_state: PoolState<PeerFsm, StoreFsm, T, C>,
    ) -> Self {
        let apply_pool = PoolControl::new(apply_router, apply_state);
        let raft_pool = PoolControl::new(raft_router, raft_state);
        Runner {
            apply_pool,
            raft_pool,
        }
    }
}

impl<T, C> Runnable<Task> for Runner<T, C> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Update { cfg } => {
                debug!("received config change request: {:?}", cfg);
            }
            _ => {}
        }
    }
}

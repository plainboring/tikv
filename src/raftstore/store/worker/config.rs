// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::raftstore::store::fsm::HandlerBuilder;
use crate::raftstore::store::transport::Transport;
use pd_client::PdClient;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use crate::raftstore::store::fsm::{
    ApplyControlFsm, ApplyFsm, DynamicConfig, PeerFsm, PoolHandlerBuilder, StoreFsm,
};
use crate::raftstore::store::{
    BatchRouter, Config as RaftStoreConfig, Fsm, FsmTypes, Poller, PoolState,
};
use tikv_util::worker::Runnable;

pub enum Task {
    Update { cfg: String },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            Task::Update { cfg } => write!(f, "Update config: {:?}", cfg),
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
    fn resize_to(&mut self, size: usize) -> (bool, usize) {
        let pool_size = self.state.pool_size.load(Ordering::AcqRel);
        if pool_size > size {
            return (true, pool_size - size);
        }
        (false, size - pool_size)
    }

    fn decrease_by(&mut self, size: usize) {
        let s = self.state.pool_size.load(Ordering::AcqRel);
        for _ in 0..size {
            if let Err(e) = self.state.fsm_sender.send(FsmTypes::Empty) {
                error!(
                    "failed to decrese threah pool";
                    "decrease to" => size,
                    "err" => %e,
                );
                return;
            }
        }
        info!(
            "decrese threah pool";
            "decrese to" => s+size,
            "pool" => ?self.state.name_prefix
        );
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
        let s = self.state.pool_size.fetch_add(size, Ordering::AcqRel);
        info!("increase thread pool"; "size" => s + size, "pool" => ?self.state.name_prefix);
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
        let s = self.state.pool_size.fetch_add(size, Ordering::AcqRel);
        info!("increase thread pool"; "size" => s + size, "pool" => ?self.state.name_prefix);
    }
}

pub struct Runner<T, C> {
    cfg: DynamicConfig,
    last_raft_store_cfg: RaftStoreConfig,
    apply_pool: PoolControl<ApplyFsm, ApplyControlFsm, T, C>,
    raft_pool: PoolControl<PeerFsm, StoreFsm, T, C>,
}

impl<T, C> Runner<T, C>
where
    T: Transport + 'static,
    C: PdClient + 'static,
{
    pub fn new(
        cfg: DynamicConfig,
        apply_router: BatchRouter<ApplyFsm, ApplyControlFsm>,
        apply_state: PoolState<ApplyFsm, ApplyControlFsm, T, C>,
        raft_router: BatchRouter<PeerFsm, StoreFsm>,
        raft_state: PoolState<PeerFsm, StoreFsm, T, C>,
    ) -> Self {
        let apply_pool = PoolControl::new(apply_router, apply_state);
        let raft_pool = PoolControl::new(raft_router, raft_state);
        let last_raft_store_cfg = cfg.get().clone();
        Runner {
            cfg,
            last_raft_store_cfg,
            apply_pool,
            raft_pool,
        }
    }

    fn resize_raft(&mut self, size: usize) {
        match self.raft_pool.resize_to(size) {
            (_, 0) => return,
            (true, s) => self.raft_pool.decrease_by(s),
            (false, s) => self.raft_pool.increase_raft_by(s),
        }
    }

    fn resize_apply(&mut self, size: usize) {
        match self.apply_pool.resize_to(size) {
            (_, 0) => return,
            (true, s) => self.apply_pool.decrease_by(s),
            (false, s) => self.apply_pool.increase_apply_by(s),
        }
    }

    fn update_raft_store_config(&mut self, new: RaftStoreConfig) {
        if self.last_raft_store_cfg.apply_pool_size != new.apply_pool_size {
            self.last_raft_store_cfg.apply_pool_size = new.apply_pool_size;
            self.resize_apply(new.apply_pool_size);
        }
        if self.last_raft_store_cfg.store_pool_size != new.store_pool_size {
            self.last_raft_store_cfg.store_pool_size = new.store_pool_size;
            self.resize_raft(new.store_pool_size);
        }

        let mut c = self.cfg.0.write().unwrap();
        if self.last_raft_store_cfg.sync_log != new.sync_log {
            self.last_raft_store_cfg.sync_log = new.sync_log;
            c.sync_log = new.sync_log;
        }

        if self.last_raft_store_cfg.raft_base_tick_interval != new.raft_base_tick_interval {
            self.last_raft_store_cfg.raft_base_tick_interval = new.raft_base_tick_interval;
            c.raft_base_tick_interval = new.raft_base_tick_interval;
        }

        if self.last_raft_store_cfg.raft_max_size_per_msg != new.raft_max_size_per_msg {
            self.last_raft_store_cfg.raft_max_size_per_msg = new.raft_max_size_per_msg;
            c.raft_max_size_per_msg = new.raft_max_size_per_msg;
        }

        if self.last_raft_store_cfg.raft_max_inflight_msgs != new.raft_max_inflight_msgs {
            self.last_raft_store_cfg.raft_max_inflight_msgs = new.raft_max_inflight_msgs;
            c.raft_max_inflight_msgs = new.raft_max_inflight_msgs;
        }

        if self.last_raft_store_cfg.raft_entry_max_size != new.raft_entry_max_size {
            self.last_raft_store_cfg.raft_entry_max_size = new.raft_entry_max_size;
            c.raft_entry_max_size = new.raft_entry_max_size;
        }

        if self.last_raft_store_cfg.raft_log_gc_tick_interval != new.raft_log_gc_tick_interval {
            self.last_raft_store_cfg.raft_log_gc_tick_interval = new.raft_log_gc_tick_interval;
            c.raft_log_gc_tick_interval = new.raft_log_gc_tick_interval;
        }

        if self.last_raft_store_cfg.raft_log_gc_threshold != new.raft_log_gc_threshold {
            self.last_raft_store_cfg.raft_log_gc_threshold = new.raft_log_gc_threshold;
            c.raft_log_gc_threshold = new.raft_log_gc_threshold;
        }

        if self.last_raft_store_cfg.raft_log_gc_count_limit != new.raft_log_gc_count_limit {
            self.last_raft_store_cfg.raft_log_gc_count_limit = new.raft_log_gc_count_limit;
            c.raft_log_gc_count_limit = new.raft_log_gc_count_limit;
        }

        if self.last_raft_store_cfg.raft_log_gc_size_limit != new.raft_log_gc_size_limit {
            self.last_raft_store_cfg.raft_log_gc_size_limit = new.raft_log_gc_size_limit;
            c.raft_log_gc_size_limit = new.raft_log_gc_size_limit;
        }

        if self.last_raft_store_cfg.raft_entry_cache_life_time != new.raft_entry_cache_life_time {
            self.last_raft_store_cfg.raft_entry_cache_life_time = new.raft_entry_cache_life_time;
            c.raft_entry_cache_life_time = new.raft_entry_cache_life_time;
        }

        if self
            .last_raft_store_cfg
            .raft_reject_transfer_leader_duration
            != new.raft_reject_transfer_leader_duration
        {
            self.last_raft_store_cfg
                .raft_reject_transfer_leader_duration = new.raft_reject_transfer_leader_duration;
            c.raft_reject_transfer_leader_duration = new.raft_reject_transfer_leader_duration;
        }

        if self.last_raft_store_cfg.split_region_check_tick_interval
            != new.split_region_check_tick_interval
        {
            self.last_raft_store_cfg.split_region_check_tick_interval =
                new.split_region_check_tick_interval;
            c.split_region_check_tick_interval = new.split_region_check_tick_interval;
        }

        if self.last_raft_store_cfg.region_split_check_diff != new.region_split_check_diff {
            self.last_raft_store_cfg.region_split_check_diff = new.region_split_check_diff;
            c.region_split_check_diff = new.region_split_check_diff;
        }

        if self.last_raft_store_cfg.region_compact_check_interval
            != new.region_compact_check_interval
        {
            self.last_raft_store_cfg.region_compact_check_interval =
                new.region_compact_check_interval;
            c.region_compact_check_interval = new.region_compact_check_interval;
        }

        if self.last_raft_store_cfg.clean_stale_peer_delay != new.clean_stale_peer_delay {
            self.last_raft_store_cfg.clean_stale_peer_delay = new.clean_stale_peer_delay;
            c.clean_stale_peer_delay = new.clean_stale_peer_delay;
        }

        if self.last_raft_store_cfg.region_compact_check_step != new.region_compact_check_step {
            self.last_raft_store_cfg.region_compact_check_step = new.region_compact_check_step;
            c.region_compact_check_step = new.region_compact_check_step;
        }

        if self.last_raft_store_cfg.region_compact_min_tombstones
            != new.region_compact_min_tombstones
        {
            self.last_raft_store_cfg.region_compact_min_tombstones =
                new.region_compact_min_tombstones;
            c.region_compact_min_tombstones = new.region_compact_min_tombstones;
        }

        if self.last_raft_store_cfg.region_compact_tombstones_percent
            != new.region_compact_tombstones_percent
        {
            self.last_raft_store_cfg.region_compact_tombstones_percent =
                new.region_compact_tombstones_percent;
            c.region_compact_tombstones_percent = new.region_compact_tombstones_percent;
        }

        if self.last_raft_store_cfg.pd_heartbeat_tick_interval != new.pd_heartbeat_tick_interval {
            self.last_raft_store_cfg.pd_heartbeat_tick_interval = new.pd_heartbeat_tick_interval;
            c.pd_heartbeat_tick_interval = new.pd_heartbeat_tick_interval;
        }

        if self.last_raft_store_cfg.pd_store_heartbeat_tick_interval
            != new.pd_store_heartbeat_tick_interval
        {
            self.last_raft_store_cfg.pd_store_heartbeat_tick_interval =
                new.pd_store_heartbeat_tick_interval;
            c.pd_store_heartbeat_tick_interval = new.pd_store_heartbeat_tick_interval;
        }

        if self.last_raft_store_cfg.snap_mgr_gc_tick_interval != new.snap_mgr_gc_tick_interval {
            self.last_raft_store_cfg.snap_mgr_gc_tick_interval = new.snap_mgr_gc_tick_interval;
            c.snap_mgr_gc_tick_interval = new.snap_mgr_gc_tick_interval;
        }

        if self.last_raft_store_cfg.snap_gc_timeout != new.snap_gc_timeout {
            self.last_raft_store_cfg.snap_gc_timeout = new.snap_gc_timeout;
            c.snap_gc_timeout = new.snap_gc_timeout;
        }

        if self.last_raft_store_cfg.lock_cf_compact_interval != new.lock_cf_compact_interval {
            self.last_raft_store_cfg.lock_cf_compact_interval = new.lock_cf_compact_interval;
            c.lock_cf_compact_interval = new.lock_cf_compact_interval;
        }

        if self.last_raft_store_cfg.lock_cf_compact_bytes_threshold
            != new.lock_cf_compact_bytes_threshold
        {
            self.last_raft_store_cfg.lock_cf_compact_bytes_threshold =
                new.lock_cf_compact_bytes_threshold;
            c.lock_cf_compact_bytes_threshold = new.lock_cf_compact_bytes_threshold;
        }

        if self.last_raft_store_cfg.notify_capacity != new.notify_capacity {
            self.last_raft_store_cfg.notify_capacity = new.notify_capacity;
            c.notify_capacity = new.notify_capacity;
        }

        if self.last_raft_store_cfg.messages_per_tick != new.messages_per_tick {
            self.last_raft_store_cfg.messages_per_tick = new.messages_per_tick;
            c.messages_per_tick = new.messages_per_tick;
        }

        if self.last_raft_store_cfg.max_peer_down_duration != new.max_peer_down_duration {
            self.last_raft_store_cfg.max_peer_down_duration = new.max_peer_down_duration;
            c.max_peer_down_duration = new.max_peer_down_duration;
        }

        if self.last_raft_store_cfg.max_leader_missing_duration != new.max_leader_missing_duration {
            self.last_raft_store_cfg.max_leader_missing_duration = new.max_leader_missing_duration;
            c.max_leader_missing_duration = new.max_leader_missing_duration;
        }

        if self.last_raft_store_cfg.abnormal_leader_missing_duration
            != new.abnormal_leader_missing_duration
        {
            self.last_raft_store_cfg.abnormal_leader_missing_duration =
                new.abnormal_leader_missing_duration;
            c.abnormal_leader_missing_duration = new.abnormal_leader_missing_duration;
        }

        if self.last_raft_store_cfg.peer_stale_state_check_interval
            != new.peer_stale_state_check_interval
        {
            self.last_raft_store_cfg.peer_stale_state_check_interval =
                new.peer_stale_state_check_interval;
            c.peer_stale_state_check_interval = new.peer_stale_state_check_interval;
        }

        if self.last_raft_store_cfg.leader_transfer_max_log_lag != new.leader_transfer_max_log_lag {
            self.last_raft_store_cfg.leader_transfer_max_log_lag = new.leader_transfer_max_log_lag;
            c.leader_transfer_max_log_lag = new.leader_transfer_max_log_lag;
        }

        if self.last_raft_store_cfg.snap_apply_batch_size != new.snap_apply_batch_size {
            self.last_raft_store_cfg.snap_apply_batch_size = new.snap_apply_batch_size;
            c.snap_apply_batch_size = new.snap_apply_batch_size;
        }

        if self.last_raft_store_cfg.consistency_check_interval != new.consistency_check_interval {
            self.last_raft_store_cfg.consistency_check_interval = new.consistency_check_interval;
            c.consistency_check_interval = new.consistency_check_interval;
        }

        if self.last_raft_store_cfg.report_region_flow_interval != new.report_region_flow_interval {
            self.last_raft_store_cfg.report_region_flow_interval = new.report_region_flow_interval;
            c.report_region_flow_interval = new.report_region_flow_interval;
        }

        if self.last_raft_store_cfg.raft_store_max_leader_lease != new.raft_store_max_leader_lease {
            self.last_raft_store_cfg.raft_store_max_leader_lease = new.raft_store_max_leader_lease;
            c.raft_store_max_leader_lease = new.raft_store_max_leader_lease;
        }

        if self.last_raft_store_cfg.right_derive_when_split != new.right_derive_when_split {
            self.last_raft_store_cfg.right_derive_when_split = new.right_derive_when_split;
            c.right_derive_when_split = new.right_derive_when_split;
        }

        if self.last_raft_store_cfg.allow_remove_leader != new.allow_remove_leader {
            self.last_raft_store_cfg.allow_remove_leader = new.allow_remove_leader;
            c.allow_remove_leader = new.allow_remove_leader;
        }

        if self.last_raft_store_cfg.merge_max_log_gap != new.merge_max_log_gap {
            self.last_raft_store_cfg.merge_max_log_gap = new.merge_max_log_gap;
            c.merge_max_log_gap = new.merge_max_log_gap;
        }

        if self.last_raft_store_cfg.merge_check_tick_interval != new.merge_check_tick_interval {
            self.last_raft_store_cfg.merge_check_tick_interval = new.merge_check_tick_interval;
            c.merge_check_tick_interval = new.merge_check_tick_interval;
        }

        if self.last_raft_store_cfg.use_delete_range != new.use_delete_range {
            self.last_raft_store_cfg.use_delete_range = new.use_delete_range;
            c.use_delete_range = new.use_delete_range;
        }

        if self.last_raft_store_cfg.cleanup_import_sst_interval != new.cleanup_import_sst_interval {
            self.last_raft_store_cfg.cleanup_import_sst_interval = new.cleanup_import_sst_interval;
            c.cleanup_import_sst_interval = new.cleanup_import_sst_interval;
        }

        if self.last_raft_store_cfg.local_read_batch_size != new.local_read_batch_size {
            self.last_raft_store_cfg.local_read_batch_size = new.local_read_batch_size;
            c.local_read_batch_size = new.local_read_batch_size;
        }

        if self.last_raft_store_cfg.apply_max_batch_size != new.apply_max_batch_size {
            self.last_raft_store_cfg.apply_max_batch_size = new.apply_max_batch_size;
            c.apply_max_batch_size = new.apply_max_batch_size;
        }

        if self.last_raft_store_cfg.store_max_batch_size != new.store_max_batch_size {
            self.last_raft_store_cfg.store_max_batch_size = new.store_max_batch_size;
            c.store_max_batch_size = new.store_max_batch_size;
        }

        if self.last_raft_store_cfg.future_poll_size != new.future_poll_size {
            self.last_raft_store_cfg.future_poll_size = new.future_poll_size;
            c.future_poll_size = new.future_poll_size;
        }

        if self.last_raft_store_cfg.hibernate_regions != new.hibernate_regions {
            self.last_raft_store_cfg.hibernate_regions = new.hibernate_regions;
            c.hibernate_regions = new.hibernate_regions;
        }
    }
}

impl<T, C> Runnable<Task> for Runner<T, C>
where
    T: Transport + 'static,
    C: PdClient + 'static,
{
    fn run(&mut self, task: Task) {
        match task {
            Task::Update { cfg } => {
                debug!("received config change request: {:?}", cfg);
                match toml::from_str::<crate::config::TiKvConfig>(&cfg) {
                    Ok(tikvcfg) => self.update_raft_store_config(tikvcfg.raft_store),
                    Err(e) => {
                        error!("update config failed"; "error" => ?e);
                    }
                }
            }
        }
    }
}

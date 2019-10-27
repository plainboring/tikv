// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::config::TiKvConfig;
use crate::coprocessor::EndpointConfig;
use crate::raftstore::store::fsm::HandlerBuilder;
use crate::raftstore::store::transport::Transport;
use engine::Engines;
use pd_client::PdClient;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use crate::raftstore::store::fsm::{
    ApplyControlFsm, ApplyFsm, DynamicConfig, PeerFsm, PoolHandlerBuilder, StoreFsm,
};
use crate::raftstore::store::{BatchRouter, Fsm, FsmTypes, Poller, PoolState};
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
        let pool_size = self.state.pool_size.load(Ordering::Relaxed);
        if pool_size > size {
            return (true, pool_size - size);
        }
        (false, size - pool_size)
    }

    fn decrease_by(&mut self, size: usize) {
        let s = self.state.pool_size.load(Ordering::Relaxed);
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
            "decrese to" => s - size,
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
        let mut workers = self.state.workers.lock().unwrap();
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
            workers.push(t);
        }
        let s = self.state.pool_size.fetch_add(size, Ordering::Relaxed);
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
        let mut workers = self.state.workers.lock().unwrap();
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
            workers.push(t);
        }
        let s = self.state.pool_size.fetch_add(size, Ordering::Relaxed);
        info!("increase thread pool"; "size" => s + size, "pool" => ?self.state.name_prefix);
    }
}

pub struct Runner<T, C> {
    cfg: DynamicConfig,
    // last_raft_store_cfg: RaftStoreConfig,
    last_cfg: TiKvConfig,
    cop_cfg: Arc<EndpointConfig>,
    apply_pool: PoolControl<ApplyFsm, ApplyControlFsm, T, C>,
    raft_pool: PoolControl<PeerFsm, StoreFsm, T, C>,
    engines: Engines,
}

impl<T, C> Runner<T, C>
where
    T: Transport + 'static,
    C: PdClient + 'static,
{
    pub fn new(
        last_cfg: TiKvConfig,
        cfg: DynamicConfig,
        cop_cfg: Arc<EndpointConfig>,
        apply_router: BatchRouter<ApplyFsm, ApplyControlFsm>,
        apply_state: PoolState<ApplyFsm, ApplyControlFsm, T, C>,
        raft_router: BatchRouter<PeerFsm, StoreFsm>,
        raft_state: PoolState<PeerFsm, StoreFsm, T, C>,
        engines: Engines,
    ) -> Self {
        let apply_pool = PoolControl::new(apply_router, apply_state);
        let raft_pool = PoolControl::new(raft_router, raft_state);
        // let last_raft_store_cfg = cfg.get().clone();
        Runner {
            cfg,
            last_cfg,
            // last_raft_store_cfg,
            cop_cfg,
            apply_pool,
            raft_pool,
            engines,
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

    fn update_config(&mut self, new: TiKvConfig) {
        if self.last_cfg.raft_store.apply_pool_size != new.raft_store.apply_pool_size {
            self.last_cfg.raft_store.apply_pool_size = new.raft_store.apply_pool_size;
            self.resize_apply(new.raft_store.apply_pool_size);
        }
        if self.last_cfg.raft_store.store_pool_size != new.raft_store.store_pool_size {
            self.last_cfg.raft_store.store_pool_size = new.raft_store.store_pool_size;
            self.resize_raft(new.raft_store.store_pool_size);
        }

        let mut c = self.cfg.0.write().unwrap();
        if self.last_cfg.raft_store.sync_log != new.raft_store.sync_log {
            self.last_cfg.raft_store.sync_log = new.raft_store.sync_log;
            c.sync_log = new.raft_store.sync_log;
        }

        if self.last_cfg.raft_store.raft_base_tick_interval
            != new.raft_store.raft_base_tick_interval
        {
            self.last_cfg.raft_store.raft_base_tick_interval =
                new.raft_store.raft_base_tick_interval;
            c.raft_base_tick_interval = new.raft_store.raft_base_tick_interval;
        }

        if self.last_cfg.raft_store.raft_max_size_per_msg != new.raft_store.raft_max_size_per_msg {
            self.last_cfg.raft_store.raft_max_size_per_msg = new.raft_store.raft_max_size_per_msg;
            c.raft_max_size_per_msg = new.raft_store.raft_max_size_per_msg;
        }

        if self.last_cfg.raft_store.raft_max_inflight_msgs != new.raft_store.raft_max_inflight_msgs
        {
            self.last_cfg.raft_store.raft_max_inflight_msgs = new.raft_store.raft_max_inflight_msgs;
            c.raft_max_inflight_msgs = new.raft_store.raft_max_inflight_msgs;
        }

        if self.last_cfg.raft_store.raft_entry_max_size != new.raft_store.raft_entry_max_size {
            self.last_cfg.raft_store.raft_entry_max_size = new.raft_store.raft_entry_max_size;
            c.raft_entry_max_size = new.raft_store.raft_entry_max_size;
        }

        if self.last_cfg.raft_store.raft_log_gc_tick_interval
            != new.raft_store.raft_log_gc_tick_interval
        {
            self.last_cfg.raft_store.raft_log_gc_tick_interval =
                new.raft_store.raft_log_gc_tick_interval;
            c.raft_log_gc_tick_interval = new.raft_store.raft_log_gc_tick_interval;
        }

        if self.last_cfg.raft_store.raft_log_gc_threshold != new.raft_store.raft_log_gc_threshold {
            self.last_cfg.raft_store.raft_log_gc_threshold = new.raft_store.raft_log_gc_threshold;
            c.raft_log_gc_threshold = new.raft_store.raft_log_gc_threshold;
        }

        if self.last_cfg.raft_store.raft_log_gc_count_limit
            != new.raft_store.raft_log_gc_count_limit
        {
            self.last_cfg.raft_store.raft_log_gc_count_limit =
                new.raft_store.raft_log_gc_count_limit;
            c.raft_log_gc_count_limit = new.raft_store.raft_log_gc_count_limit;
        }

        if self.last_cfg.raft_store.raft_log_gc_size_limit != new.raft_store.raft_log_gc_size_limit
        {
            self.last_cfg.raft_store.raft_log_gc_size_limit = new.raft_store.raft_log_gc_size_limit;
            c.raft_log_gc_size_limit = new.raft_store.raft_log_gc_size_limit;
        }

        if self.last_cfg.raft_store.raft_entry_cache_life_time
            != new.raft_store.raft_entry_cache_life_time
        {
            self.last_cfg.raft_store.raft_entry_cache_life_time =
                new.raft_store.raft_entry_cache_life_time;
            c.raft_entry_cache_life_time = new.raft_store.raft_entry_cache_life_time;
        }

        if self
            .last_cfg
            .raft_store
            .raft_reject_transfer_leader_duration
            != new.raft_store.raft_reject_transfer_leader_duration
        {
            self.last_cfg
                .raft_store
                .raft_reject_transfer_leader_duration =
                new.raft_store.raft_reject_transfer_leader_duration;
            c.raft_reject_transfer_leader_duration =
                new.raft_store.raft_reject_transfer_leader_duration;
        }

        if self.last_cfg.raft_store.split_region_check_tick_interval
            != new.raft_store.split_region_check_tick_interval
        {
            self.last_cfg.raft_store.split_region_check_tick_interval =
                new.raft_store.split_region_check_tick_interval;
            c.split_region_check_tick_interval = new.raft_store.split_region_check_tick_interval;
        }

        if self.last_cfg.raft_store.region_split_check_diff
            != new.raft_store.region_split_check_diff
        {
            self.last_cfg.raft_store.region_split_check_diff =
                new.raft_store.region_split_check_diff;
            c.region_split_check_diff = new.raft_store.region_split_check_diff;
        }

        if self.last_cfg.raft_store.region_compact_check_interval
            != new.raft_store.region_compact_check_interval
        {
            self.last_cfg.raft_store.region_compact_check_interval =
                new.raft_store.region_compact_check_interval;
            c.region_compact_check_interval = new.raft_store.region_compact_check_interval;
        }

        if self.last_cfg.raft_store.clean_stale_peer_delay != new.raft_store.clean_stale_peer_delay
        {
            self.last_cfg.raft_store.clean_stale_peer_delay = new.raft_store.clean_stale_peer_delay;
            c.clean_stale_peer_delay = new.raft_store.clean_stale_peer_delay;
        }

        if self.last_cfg.raft_store.region_compact_check_step
            != new.raft_store.region_compact_check_step
        {
            self.last_cfg.raft_store.region_compact_check_step =
                new.raft_store.region_compact_check_step;
            c.region_compact_check_step = new.raft_store.region_compact_check_step;
        }

        if self.last_cfg.raft_store.region_compact_min_tombstones
            != new.raft_store.region_compact_min_tombstones
        {
            self.last_cfg.raft_store.region_compact_min_tombstones =
                new.raft_store.region_compact_min_tombstones;
            c.region_compact_min_tombstones = new.raft_store.region_compact_min_tombstones;
        }

        if self.last_cfg.raft_store.region_compact_tombstones_percent
            != new.raft_store.region_compact_tombstones_percent
        {
            self.last_cfg.raft_store.region_compact_tombstones_percent =
                new.raft_store.region_compact_tombstones_percent;
            c.region_compact_tombstones_percent = new.raft_store.region_compact_tombstones_percent;
        }

        if self.last_cfg.raft_store.pd_heartbeat_tick_interval
            != new.raft_store.pd_heartbeat_tick_interval
        {
            self.last_cfg.raft_store.pd_heartbeat_tick_interval =
                new.raft_store.pd_heartbeat_tick_interval;
            c.pd_heartbeat_tick_interval = new.raft_store.pd_heartbeat_tick_interval;
        }

        if self.last_cfg.raft_store.pd_store_heartbeat_tick_interval
            != new.raft_store.pd_store_heartbeat_tick_interval
        {
            self.last_cfg.raft_store.pd_store_heartbeat_tick_interval =
                new.raft_store.pd_store_heartbeat_tick_interval;
            c.pd_store_heartbeat_tick_interval = new.raft_store.pd_store_heartbeat_tick_interval;
        }

        if self.last_cfg.raft_store.snap_mgr_gc_tick_interval
            != new.raft_store.snap_mgr_gc_tick_interval
        {
            self.last_cfg.raft_store.snap_mgr_gc_tick_interval =
                new.raft_store.snap_mgr_gc_tick_interval;
            c.snap_mgr_gc_tick_interval = new.raft_store.snap_mgr_gc_tick_interval;
        }

        if self.last_cfg.raft_store.snap_gc_timeout != new.raft_store.snap_gc_timeout {
            self.last_cfg.raft_store.snap_gc_timeout = new.raft_store.snap_gc_timeout;
            c.snap_gc_timeout = new.raft_store.snap_gc_timeout;
        }

        if self.last_cfg.raft_store.lock_cf_compact_interval
            != new.raft_store.lock_cf_compact_interval
        {
            self.last_cfg.raft_store.lock_cf_compact_interval =
                new.raft_store.lock_cf_compact_interval;
            c.lock_cf_compact_interval = new.raft_store.lock_cf_compact_interval;
        }

        if self.last_cfg.raft_store.lock_cf_compact_bytes_threshold
            != new.raft_store.lock_cf_compact_bytes_threshold
        {
            self.last_cfg.raft_store.lock_cf_compact_bytes_threshold =
                new.raft_store.lock_cf_compact_bytes_threshold;
            c.lock_cf_compact_bytes_threshold = new.raft_store.lock_cf_compact_bytes_threshold;
        }

        if self.last_cfg.raft_store.notify_capacity != new.raft_store.notify_capacity {
            self.last_cfg.raft_store.notify_capacity = new.raft_store.notify_capacity;
            c.notify_capacity = new.raft_store.notify_capacity;
        }

        if self.last_cfg.raft_store.messages_per_tick != new.raft_store.messages_per_tick {
            self.last_cfg.raft_store.messages_per_tick = new.raft_store.messages_per_tick;
            c.messages_per_tick = new.raft_store.messages_per_tick;
        }

        if self.last_cfg.raft_store.max_peer_down_duration != new.raft_store.max_peer_down_duration
        {
            self.last_cfg.raft_store.max_peer_down_duration = new.raft_store.max_peer_down_duration;
            c.max_peer_down_duration = new.raft_store.max_peer_down_duration;
        }

        if self.last_cfg.raft_store.max_leader_missing_duration
            != new.raft_store.max_leader_missing_duration
        {
            self.last_cfg.raft_store.max_leader_missing_duration =
                new.raft_store.max_leader_missing_duration;
            c.max_leader_missing_duration = new.raft_store.max_leader_missing_duration;
        }

        if self.last_cfg.raft_store.abnormal_leader_missing_duration
            != new.raft_store.abnormal_leader_missing_duration
        {
            self.last_cfg.raft_store.abnormal_leader_missing_duration =
                new.raft_store.abnormal_leader_missing_duration;
            c.abnormal_leader_missing_duration = new.raft_store.abnormal_leader_missing_duration;
        }

        if self.last_cfg.raft_store.peer_stale_state_check_interval
            != new.raft_store.peer_stale_state_check_interval
        {
            self.last_cfg.raft_store.peer_stale_state_check_interval =
                new.raft_store.peer_stale_state_check_interval;
            c.peer_stale_state_check_interval = new.raft_store.peer_stale_state_check_interval;
        }

        if self.last_cfg.raft_store.leader_transfer_max_log_lag
            != new.raft_store.leader_transfer_max_log_lag
        {
            self.last_cfg.raft_store.leader_transfer_max_log_lag =
                new.raft_store.leader_transfer_max_log_lag;
            c.leader_transfer_max_log_lag = new.raft_store.leader_transfer_max_log_lag;
        }

        if self.last_cfg.raft_store.snap_apply_batch_size != new.raft_store.snap_apply_batch_size {
            self.last_cfg.raft_store.snap_apply_batch_size = new.raft_store.snap_apply_batch_size;
            c.snap_apply_batch_size = new.raft_store.snap_apply_batch_size;
        }

        if self.last_cfg.raft_store.consistency_check_interval
            != new.raft_store.consistency_check_interval
        {
            self.last_cfg.raft_store.consistency_check_interval =
                new.raft_store.consistency_check_interval;
            c.consistency_check_interval = new.raft_store.consistency_check_interval;
        }

        if self.last_cfg.raft_store.report_region_flow_interval
            != new.raft_store.report_region_flow_interval
        {
            self.last_cfg.raft_store.report_region_flow_interval =
                new.raft_store.report_region_flow_interval;
            c.report_region_flow_interval = new.raft_store.report_region_flow_interval;
        }

        if self.last_cfg.raft_store.raft_store_max_leader_lease
            != new.raft_store.raft_store_max_leader_lease
        {
            self.last_cfg.raft_store.raft_store_max_leader_lease =
                new.raft_store.raft_store_max_leader_lease;
            c.raft_store_max_leader_lease = new.raft_store.raft_store_max_leader_lease;
        }

        if self.last_cfg.raft_store.right_derive_when_split
            != new.raft_store.right_derive_when_split
        {
            self.last_cfg.raft_store.right_derive_when_split =
                new.raft_store.right_derive_when_split;
            c.right_derive_when_split = new.raft_store.right_derive_when_split;
        }

        if self.last_cfg.raft_store.allow_remove_leader != new.raft_store.allow_remove_leader {
            self.last_cfg.raft_store.allow_remove_leader = new.raft_store.allow_remove_leader;
            c.allow_remove_leader = new.raft_store.allow_remove_leader;
        }

        if self.last_cfg.raft_store.merge_max_log_gap != new.raft_store.merge_max_log_gap {
            self.last_cfg.raft_store.merge_max_log_gap = new.raft_store.merge_max_log_gap;
            c.merge_max_log_gap = new.raft_store.merge_max_log_gap;
        }

        if self.last_cfg.raft_store.merge_check_tick_interval
            != new.raft_store.merge_check_tick_interval
        {
            self.last_cfg.raft_store.merge_check_tick_interval =
                new.raft_store.merge_check_tick_interval;
            c.merge_check_tick_interval = new.raft_store.merge_check_tick_interval;
        }

        if self.last_cfg.raft_store.use_delete_range != new.raft_store.use_delete_range {
            self.last_cfg.raft_store.use_delete_range = new.raft_store.use_delete_range;
            c.use_delete_range = new.raft_store.use_delete_range;
        }

        if self.last_cfg.raft_store.cleanup_import_sst_interval
            != new.raft_store.cleanup_import_sst_interval
        {
            self.last_cfg.raft_store.cleanup_import_sst_interval =
                new.raft_store.cleanup_import_sst_interval;
            c.cleanup_import_sst_interval = new.raft_store.cleanup_import_sst_interval;
        }

        if self.last_cfg.raft_store.local_read_batch_size != new.raft_store.local_read_batch_size {
            self.last_cfg.raft_store.local_read_batch_size = new.raft_store.local_read_batch_size;
            c.local_read_batch_size = new.raft_store.local_read_batch_size;
        }

        if self.last_cfg.raft_store.apply_max_batch_size != new.raft_store.apply_max_batch_size {
            self.last_cfg.raft_store.apply_max_batch_size = new.raft_store.apply_max_batch_size;
            c.apply_max_batch_size = new.raft_store.apply_max_batch_size;
        }

        if self.last_cfg.raft_store.store_max_batch_size != new.raft_store.store_max_batch_size {
            self.last_cfg.raft_store.store_max_batch_size = new.raft_store.store_max_batch_size;
            c.store_max_batch_size = new.raft_store.store_max_batch_size;
        }

        if self.last_cfg.raft_store.future_poll_size != new.raft_store.future_poll_size {
            self.last_cfg.raft_store.future_poll_size = new.raft_store.future_poll_size;
            c.future_poll_size = new.raft_store.future_poll_size;
        }

        if self.last_cfg.raft_store.hibernate_regions != new.raft_store.hibernate_regions {
            self.last_cfg.raft_store.hibernate_regions = new.raft_store.hibernate_regions;
            c.hibernate_regions = new.raft_store.hibernate_regions;
        }

        macro_rules! modi_db {
            ($db:ident, $self:ident, $newc:ident, $name:ident) => {
                if $self.last_cfg.$db.$name != $newc.$db.$name {
                    $self.last_cfg.$db.$name = $newc.$db.$name.clone();
                    if let Err(e) = $self.modify_rocksdb_config(
                        &String::from(stringify!($db)),
                        None,
                        &String::from(stringify!($name)),
                        &format!("{:?}", $newc.rocksdb.$name),
                    ) {
                        info!("fail to modify db config {:}", e);
                    }
                }
            };
        }

        modi_db!(rocksdb, self, new, wal_recovery_mode);
        modi_db!(rocksdb, self, new, wal_dir);
        modi_db!(rocksdb, self, new, wal_ttl_seconds);
        modi_db!(rocksdb, self, new, wal_size_limit);
        modi_db!(rocksdb, self, new, max_total_wal_size);
        modi_db!(rocksdb, self, new, max_background_jobs);
        modi_db!(rocksdb, self, new, max_manifest_file_size);
        modi_db!(rocksdb, self, new, create_if_missing);
        modi_db!(rocksdb, self, new, max_open_files);
        modi_db!(rocksdb, self, new, enable_statistics);
        modi_db!(rocksdb, self, new, stats_dump_period);
        modi_db!(rocksdb, self, new, compaction_readahead_size);
        modi_db!(rocksdb, self, new, info_log_max_size);
        modi_db!(rocksdb, self, new, info_log_roll_time);
        modi_db!(rocksdb, self, new, info_log_keep_log_file_num);
        modi_db!(rocksdb, self, new, info_log_dir);
        modi_db!(rocksdb, self, new, rate_bytes_per_sec);
        modi_db!(rocksdb, self, new, rate_limiter_mode);
        modi_db!(rocksdb, self, new, auto_tuned);
        modi_db!(rocksdb, self, new, bytes_per_sync);
        modi_db!(rocksdb, self, new, max_sub_compactions);
        modi_db!(rocksdb, self, new, writable_file_max_buffer_size);
        modi_db!(rocksdb, self, new, use_direct_io_for_flush_and_compaction);
        modi_db!(rocksdb, self, new, enable_pipelined_write);

        modi_db!(raftdb, self, new, wal_recovery_mode);
        modi_db!(raftdb, self, new, wal_dir);
        modi_db!(raftdb, self, new, wal_ttl_seconds);
        modi_db!(raftdb, self, new, wal_size_limit);
        modi_db!(raftdb, self, new, max_total_wal_size);
        modi_db!(raftdb, self, new, max_background_jobs);
        modi_db!(raftdb, self, new, max_manifest_file_size);
        modi_db!(raftdb, self, new, create_if_missing);
        modi_db!(raftdb, self, new, max_open_files);
        modi_db!(raftdb, self, new, enable_statistics);
        modi_db!(raftdb, self, new, stats_dump_period);
        modi_db!(raftdb, self, new, compaction_readahead_size);
        modi_db!(raftdb, self, new, info_log_max_size);
        modi_db!(raftdb, self, new, info_log_roll_time);
        modi_db!(raftdb, self, new, info_log_keep_log_file_num);
        modi_db!(raftdb, self, new, info_log_dir);
        modi_db!(raftdb, self, new, max_sub_compactions);
        modi_db!(raftdb, self, new, writable_file_max_buffer_size);
        modi_db!(raftdb, self, new, use_direct_io_for_flush_and_compaction);
        modi_db!(raftdb, self, new, enable_pipelined_write);
        // modi_db!(raftdb, self, new, allow_concurrent_memtable_write);
        modi_db!(raftdb, self, new, bytes_per_sync);
        modi_db!(raftdb, self, new, wal_bytes_per_sync);

        macro_rules! modi_db_cf {
            ($db:ident, $cf:ident, $cf_name:ident, $self:ident, $newc:ident, $name:ident) => {
                if $self.last_cfg.$db.$cf_name.$name != $newc.$db.$cf_name.$name {
                    $self.last_cfg.$db.$cf_name.$name = $newc.$db.$cf_name.$name.clone();
                    if let Err(e) = $self.modify_rocksdb_config(
                        &String::from(stringify!($db)),
                        Some(&String::from(stringify!($cf))),
                        &String::from(stringify!($name)),
                        &format!("{:?}", $newc.$db.$cf_name.$name),
                    ) {
                        info!("fail to modify db config {:}", e);
                    }
                }
            };
        }

        modi_db_cf!(rocksdb, default, defaultcf, self, new, block_size);
        modi_db_cf!(rocksdb, default, defaultcf, self, new, block_cache_size);
        modi_db_cf!(rocksdb, default, defaultcf, self, new, disable_block_cache);
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            cache_index_and_filter_blocks
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            pin_l0_filter_and_index_blocks
        );
        modi_db_cf!(rocksdb, default, defaultcf, self, new, use_bloom_filter);
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            optimize_filters_for_hits
        );
        modi_db_cf!(rocksdb, default, defaultcf, self, new, whole_key_filtering);
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            bloom_filter_bits_per_key
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            block_based_bloom_filter
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            read_amp_bytes_per_bit
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            compression_per_level
        );
        modi_db_cf!(rocksdb, default, defaultcf, self, new, write_buffer_size);
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            max_write_buffer_number
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            min_write_buffer_number_to_merge
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            max_bytes_for_level_base
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            target_file_size_base
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            level0_file_num_compaction_trigger
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            level0_slowdown_writes_trigger
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            level0_stop_writes_trigger
        );
        modi_db_cf!(rocksdb, default, defaultcf, self, new, max_compaction_bytes);
        modi_db_cf!(rocksdb, default, defaultcf, self, new, compaction_pri);
        modi_db_cf!(rocksdb, default, defaultcf, self, new, dynamic_level_bytes);
        modi_db_cf!(rocksdb, default, defaultcf, self, new, num_levels);
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            max_bytes_for_level_multiplier
        );
        modi_db_cf!(rocksdb, default, defaultcf, self, new, compaction_style);
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            disable_auto_compactions
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            soft_pending_compaction_bytes_limit
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            hard_pending_compaction_bytes_limit
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            force_consistency_checks
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            prop_size_index_distance
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            prop_keys_index_distance
        );
        modi_db_cf!(
            rocksdb,
            default,
            defaultcf,
            self,
            new,
            enable_doubly_skiplist
        );

        modi_db_cf!(rocksdb, write, writecf, self, new, block_size);
        modi_db_cf!(rocksdb, write, writecf, self, new, block_cache_size);
        modi_db_cf!(rocksdb, write, writecf, self, new, disable_block_cache);
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            cache_index_and_filter_blocks
        );
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            pin_l0_filter_and_index_blocks
        );
        modi_db_cf!(rocksdb, write, writecf, self, new, use_bloom_filter);
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            optimize_filters_for_hits
        );
        modi_db_cf!(rocksdb, write, writecf, self, new, whole_key_filtering);
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            bloom_filter_bits_per_key
        );
        modi_db_cf!(rocksdb, write, writecf, self, new, block_based_bloom_filter);
        modi_db_cf!(rocksdb, write, writecf, self, new, read_amp_bytes_per_bit);
        modi_db_cf!(rocksdb, write, writecf, self, new, compression_per_level);
        modi_db_cf!(rocksdb, write, writecf, self, new, write_buffer_size);
        modi_db_cf!(rocksdb, write, writecf, self, new, max_write_buffer_number);
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            min_write_buffer_number_to_merge
        );
        modi_db_cf!(rocksdb, write, writecf, self, new, max_bytes_for_level_base);
        modi_db_cf!(rocksdb, write, writecf, self, new, target_file_size_base);
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            level0_file_num_compaction_trigger
        );
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            level0_slowdown_writes_trigger
        );
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            level0_stop_writes_trigger
        );
        modi_db_cf!(rocksdb, write, writecf, self, new, max_compaction_bytes);
        modi_db_cf!(rocksdb, write, writecf, self, new, compaction_pri);
        modi_db_cf!(rocksdb, write, writecf, self, new, dynamic_level_bytes);
        modi_db_cf!(rocksdb, write, writecf, self, new, num_levels);
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            max_bytes_for_level_multiplier
        );
        modi_db_cf!(rocksdb, write, writecf, self, new, compaction_style);
        modi_db_cf!(rocksdb, write, writecf, self, new, disable_auto_compactions);
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            soft_pending_compaction_bytes_limit
        );
        modi_db_cf!(
            rocksdb,
            write,
            writecf,
            self,
            new,
            hard_pending_compaction_bytes_limit
        );
        modi_db_cf!(rocksdb, write, writecf, self, new, force_consistency_checks);
        modi_db_cf!(rocksdb, write, writecf, self, new, prop_size_index_distance);
        modi_db_cf!(rocksdb, write, writecf, self, new, prop_keys_index_distance);
        modi_db_cf!(rocksdb, write, writecf, self, new, enable_doubly_skiplist);

        modi_db_cf!(rocksdb, lock, lockcf, self, new, block_size);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, block_cache_size);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, disable_block_cache);
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            cache_index_and_filter_blocks
        );
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            pin_l0_filter_and_index_blocks
        );
        modi_db_cf!(rocksdb, lock, lockcf, self, new, use_bloom_filter);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, optimize_filters_for_hits);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, whole_key_filtering);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, bloom_filter_bits_per_key);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, block_based_bloom_filter);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, read_amp_bytes_per_bit);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, compression_per_level);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, write_buffer_size);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, max_write_buffer_number);
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            min_write_buffer_number_to_merge
        );
        modi_db_cf!(rocksdb, lock, lockcf, self, new, max_bytes_for_level_base);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, target_file_size_base);
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            level0_file_num_compaction_trigger
        );
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            level0_slowdown_writes_trigger
        );
        modi_db_cf!(rocksdb, lock, lockcf, self, new, level0_stop_writes_trigger);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, max_compaction_bytes);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, compaction_pri);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, dynamic_level_bytes);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, num_levels);
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            max_bytes_for_level_multiplier
        );
        modi_db_cf!(rocksdb, lock, lockcf, self, new, compaction_style);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, disable_auto_compactions);
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            soft_pending_compaction_bytes_limit
        );
        modi_db_cf!(
            rocksdb,
            lock,
            lockcf,
            self,
            new,
            hard_pending_compaction_bytes_limit
        );
        modi_db_cf!(rocksdb, lock, lockcf, self, new, force_consistency_checks);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, prop_size_index_distance);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, prop_keys_index_distance);
        modi_db_cf!(rocksdb, lock, lockcf, self, new, enable_doubly_skiplist);

        modi_db_cf!(rocksdb, raft, raftcf, self, new, block_size);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, block_cache_size);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, disable_block_cache);
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            cache_index_and_filter_blocks
        );
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            pin_l0_filter_and_index_blocks
        );
        modi_db_cf!(rocksdb, raft, raftcf, self, new, use_bloom_filter);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, optimize_filters_for_hits);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, whole_key_filtering);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, bloom_filter_bits_per_key);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, block_based_bloom_filter);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, read_amp_bytes_per_bit);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, compression_per_level);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, write_buffer_size);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, max_write_buffer_number);
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            min_write_buffer_number_to_merge
        );
        modi_db_cf!(rocksdb, raft, raftcf, self, new, max_bytes_for_level_base);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, target_file_size_base);
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            level0_file_num_compaction_trigger
        );
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            level0_slowdown_writes_trigger
        );
        modi_db_cf!(rocksdb, raft, raftcf, self, new, level0_stop_writes_trigger);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, max_compaction_bytes);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, compaction_pri);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, dynamic_level_bytes);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, num_levels);
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            max_bytes_for_level_multiplier
        );
        modi_db_cf!(rocksdb, raft, raftcf, self, new, compaction_style);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, disable_auto_compactions);
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            soft_pending_compaction_bytes_limit
        );
        modi_db_cf!(
            rocksdb,
            raft,
            raftcf,
            self,
            new,
            hard_pending_compaction_bytes_limit
        );
        modi_db_cf!(rocksdb, raft, raftcf, self, new, force_consistency_checks);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, prop_size_index_distance);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, prop_keys_index_distance);
        modi_db_cf!(rocksdb, raft, raftcf, self, new, enable_doubly_skiplist);
    }

    fn modify_rocksdb_config(
        &self,
        db_name: &str,
        cf_name: Option<&str>,
        config_name: &str,
        config_value: &str,
    ) -> Result<(), String> {
        use crate::server::CONFIG_ROCKSDB_GAUGE;
        let db = if db_name == "raftdb" {
            &self.engines.raft
        } else {
            &self.engines.kv
        };
        if cf_name.is_none() {
            // [raftdb] name = value or [rokcsdb] name = value
            db.set_db_options(&[(config_name, config_value)])
                .map_err(|e| {
                    format!(
                        "set_db_options({:?} = {:?}) err: {:?}",
                        config_name, config_value, e
                    )
                })?;
        } else {
            let cf_name = cf_name.unwrap();
            // [rocksdb.defaultcf] name = value
            // currently we can't modify block_cache_size via set_options_cf
            if config_name == "block_cache_size" {
                return Err("shared block cache is enabled, change cache size through \
                            //      block_cache.capacity in storage module instead"
                    .to_owned());
            }
            let handle = db
                .cf_handle(cf_name)
                .map_or(Err(format!("cf {} not found", cf_name)), Result::Ok)?;
            let mut opt = Vec::new();
            opt.push((config_name, config_value));
            db.set_options_cf(handle, &opt[..])
                .map_err(|e| format!("{:?}", e))?;
            if let Ok(v) = config_value.parse::<f64>() {
                CONFIG_ROCKSDB_GAUGE
                    .with_label_values(&[&cf_name, &config_name])
                    .set(v);
            }
        }
        Ok(())
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
                if cfg.is_empty() {
                    info!("ignore empty config");
                    return;
                }
                debug!("received config change request: {:?}", cfg);
                match toml::from_str::<crate::config::TiKvConfig>(&cfg) {
                    Ok(tikvcfg) => self.update_config(tikvcfg),
                    Err(e) => {
                        error!("update config failed"; "error" => ?e);
                    }
                }
            }
        }
    }
}

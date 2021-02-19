// Copyright 2019 Intel Corporation. All Rights Reserved.
// Copyright 2019-2021 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::error;
use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::sync::{Arc, RwLock};
use std::thread;

use vhost::vhost_user::message::{
    VhostUserConfigFlags, VhostUserMemoryRegion, VhostUserProtocolFeatures,
    VhostUserVirtioFeatures, VhostUserVringAddrFlags, VhostUserVringState,
};
use vhost::vhost_user::{Error as VhostUserError, Result as VhostUserResult, SlaveFsCacheReq};
use virtio_bindings::bindings::virtio_ring::VIRTIO_RING_F_EVENT_IDX;
use vm_memory::{FileOffset, GuestAddress, GuestMemoryAtomic, GuestMemoryMmap};
use vmm_sys_util::eventfd::EventFd;

use super::*;

const MAX_MEM_SLOTS: u64 = 32;

#[derive(Debug)]
/// Errors related to vhost-user handler.
pub enum VhostUserHandlerError {
    /// Failed to create epoll file descriptor.
    EpollCreateFd(io::Error),
    /// Failed to spawn vring worker.
    SpawnVringWorker(io::Error),
    /// Could not find the mapping from memory regions.
    MissingMemoryMapping,
    /// Could not register exit event
    RegisterExitEvent(io::Error),
}

impl std::fmt::Display for VhostUserHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            VhostUserHandlerError::EpollCreateFd(e) => write!(f, "failed creating epoll fd: {}", e),
            VhostUserHandlerError::SpawnVringWorker(e) => {
                write!(f, "failed spawning the vring worker: {}", e)
            }
            VhostUserHandlerError::MissingMemoryMapping => write!(f, "Missing memory mapping"),
            VhostUserHandlerError::RegisterExitEvent(e) => {
                write!(f, "Failed to register exit event: {}", e)
            }
        }
    }
}

impl error::Error for VhostUserHandlerError {}

/// Result of vhost-user handler operations.
type VhostUserHandlerResult<T> = std::result::Result<T, VhostUserHandlerError>;

struct AddrMapping {
    vmm_addr: u64,
    size: u64,
    gpa_base: u64,
}

pub struct VhostUserHandler<S: VhostUserBackend> {
    backend: S,
    workers: Vec<Arc<VringWorker>>,
    owned: bool,
    features_acked: bool,
    acked_features: u64,
    acked_protocol_features: u64,
    num_queues: usize,
    max_queue_size: usize,
    queues_per_thread: Vec<u64>,
    mappings: Vec<AddrMapping>,
    atomic_mem: GuestMemoryAtomic<GuestMemoryMmap>,
    vrings: Vec<Arc<RwLock<Vring>>>,
    worker_threads: Vec<thread::JoinHandle<VringWorkerResult<()>>>,
}

impl<S: VhostUserBackend + Clone> VhostUserHandler<S> {
    pub fn new(backend: S) -> VhostUserHandlerResult<Self> {
        let num_queues = backend.num_queues();
        let max_queue_size = backend.max_queue_size();
        let queues_per_thread = backend.queues_per_thread();

        let atomic_mem = GuestMemoryAtomic::new(GuestMemoryMmap::new());

        let mut vrings: Vec<Arc<RwLock<Vring>>> = Vec::new();
        for _ in 0..num_queues {
            let vring = Arc::new(RwLock::new(Vring::new(
                atomic_mem.clone(),
                max_queue_size as u16,
            )));
            vrings.push(vring);
        }

        let mut workers = Vec::new();
        let mut worker_threads = Vec::new();
        for (thread_id, queues_mask) in queues_per_thread.iter().enumerate() {
            // Create the epoll file descriptor
            let epoll_fd = epoll::create(true).map_err(VhostUserHandlerError::EpollCreateFd)?;
            // Use 'File' to enforce closing on 'epoll_fd'
            let epoll_file = unsafe { File::from_raw_fd(epoll_fd) };

            let vring_worker = Arc::new(VringWorker { epoll_file });
            let worker = vring_worker.clone();

            let exit_event_id =
                if let Some((exit_event_fd, exit_event_id)) = backend.exit_event(thread_id) {
                    worker
                        .register_listener(
                            exit_event_fd.as_raw_fd(),
                            epoll::Events::EPOLLIN,
                            u64::from(exit_event_id),
                        )
                        .map_err(VhostUserHandlerError::RegisterExitEvent)?;
                    Some(exit_event_id)
                } else {
                    None
                };

            let mut thread_vrings: Vec<Arc<RwLock<Vring>>> = Vec::new();
            for (index, vring) in vrings.iter().enumerate() {
                if (queues_mask >> index) & 1u64 == 1u64 {
                    thread_vrings.push(vring.clone());
                }
            }

            let vring_handler = VringEpollHandler {
                backend: backend.clone(),
                vrings: thread_vrings,
                exit_event_id,
                thread_id,
            };

            let worker_thread = thread::Builder::new()
                .name("vring_worker".to_string())
                .spawn(move || vring_worker.run(vring_handler))
                .map_err(VhostUserHandlerError::SpawnVringWorker)?;

            workers.push(worker);
            worker_threads.push(worker_thread);
        }

        Ok(VhostUserHandler {
            backend,
            workers,
            owned: false,
            features_acked: false,
            acked_features: 0,
            acked_protocol_features: 0,
            num_queues,
            max_queue_size,
            queues_per_thread,
            mappings: Vec::new(),
            atomic_mem,
            vrings,
            worker_threads,
        })
    }

    pub fn get_vring_workers(&self) -> Vec<Arc<VringWorker>> {
        self.workers.clone()
    }

    fn vmm_va_to_gpa(&self, vmm_va: u64) -> VhostUserHandlerResult<u64> {
        for mapping in self.mappings.iter() {
            if vmm_va >= mapping.vmm_addr && vmm_va < mapping.vmm_addr + mapping.size {
                return Ok(vmm_va - mapping.vmm_addr + mapping.gpa_base);
            }
        }

        Err(VhostUserHandlerError::MissingMemoryMapping)
    }
}

impl<S: VhostUserBackend + Clone> VhostUserSlaveReqHandlerMut for VhostUserHandler<S> {
    fn set_owner(&mut self) -> VhostUserResult<()> {
        if self.owned {
            return Err(VhostUserError::InvalidOperation);
        }
        self.owned = true;
        Ok(())
    }

    fn reset_owner(&mut self) -> VhostUserResult<()> {
        self.owned = false;
        self.features_acked = false;
        self.acked_features = 0;
        self.acked_protocol_features = 0;
        Ok(())
    }

    fn get_features(&mut self) -> VhostUserResult<u64> {
        Ok(self.backend.features())
    }

    fn set_features(&mut self, features: u64) -> VhostUserResult<()> {
        if (features & !self.backend.features()) != 0 {
            return Err(VhostUserError::InvalidParam);
        }

        self.acked_features = features;
        self.features_acked = true;

        // If VHOST_USER_F_PROTOCOL_FEATURES has not been negotiated,
        // the ring is initialized in an enabled state.
        // If VHOST_USER_F_PROTOCOL_FEATURES has been negotiated,
        // the ring is initialized in a disabled state. Client must not
        // pass data to/from the backend until ring is enabled by
        // VHOST_USER_SET_VRING_ENABLE with parameter 1, or after it has
        // been disabled by VHOST_USER_SET_VRING_ENABLE with parameter 0.
        let vring_enabled =
            self.acked_features & VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits() == 0;
        for vring in self.vrings.iter_mut() {
            vring.write().unwrap().enabled = vring_enabled;
        }

        self.backend.acked_features(self.acked_features);

        Ok(())
    }

    fn get_protocol_features(&mut self) -> VhostUserResult<VhostUserProtocolFeatures> {
        Ok(self.backend.protocol_features())
    }

    fn set_protocol_features(&mut self, features: u64) -> VhostUserResult<()> {
        // Note: slave that reported VHOST_USER_F_PROTOCOL_FEATURES must
        // support this message even before VHOST_USER_SET_FEATURES was
        // called.
        self.acked_protocol_features = features;
        Ok(())
    }

    fn set_mem_table(
        &mut self,
        ctx: &[VhostUserMemoryRegion],
        files: Vec<File>,
    ) -> VhostUserResult<()> {
        // We need to create tuple of ranges from the list of VhostUserMemoryRegion
        // that we get from the caller.
        let mut regions: Vec<(GuestAddress, usize, Option<FileOffset>)> = Vec::new();
        let mut mappings: Vec<AddrMapping> = Vec::new();

        for (region, file) in ctx.iter().zip(files) {
            let g_addr = GuestAddress(region.guest_phys_addr);
            let len = region.memory_size as usize;
            let f_off = FileOffset::new(file, region.mmap_offset);

            regions.push((g_addr, len, Some(f_off)));
            mappings.push(AddrMapping {
                vmm_addr: region.user_addr,
                size: region.memory_size,
                gpa_base: region.guest_phys_addr,
            });
        }

        let mem = GuestMemoryMmap::from_ranges_with_files(regions).map_err(|e| {
            VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
        })?;

        // Updating the inner GuestMemory object here will cause all our vrings to
        // see the new one the next time they call to `atomic_mem.memory()`.
        self.atomic_mem.lock().unwrap().replace(mem);

        self.backend
            .update_memory(self.atomic_mem.clone())
            .map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;
        self.mappings = mappings;

        Ok(())
    }

    fn get_queue_num(&mut self) -> VhostUserResult<u64> {
        Ok(self.num_queues as u64)
    }

    fn set_vring_num(&mut self, index: u32, num: u32) -> VhostUserResult<()> {
        if index as usize >= self.num_queues || num == 0 || num as usize > self.max_queue_size {
            return Err(VhostUserError::InvalidParam);
        }
        self.vrings[index as usize].write().unwrap().queue.size = num as u16;
        Ok(())
    }

    fn set_vring_addr(
        &mut self,
        index: u32,
        _flags: VhostUserVringAddrFlags,
        descriptor: u64,
        used: u64,
        available: u64,
        _log: u64,
    ) -> VhostUserResult<()> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        if !self.mappings.is_empty() {
            let desc_table = self.vmm_va_to_gpa(descriptor).map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;
            let avail_ring = self.vmm_va_to_gpa(available).map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;
            let used_ring = self.vmm_va_to_gpa(used).map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;
            self.vrings[index as usize]
                .write()
                .unwrap()
                .queue
                .desc_table = GuestAddress(desc_table);
            self.vrings[index as usize]
                .write()
                .unwrap()
                .queue
                .avail_ring = GuestAddress(avail_ring);
            self.vrings[index as usize].write().unwrap().queue.used_ring = GuestAddress(used_ring);
            Ok(())
        } else {
            Err(VhostUserError::InvalidParam)
        }
    }

    fn set_vring_base(&mut self, index: u32, base: u32) -> VhostUserResult<()> {
        self.vrings[index as usize]
            .write()
            .unwrap()
            .queue
            .set_next_avail(base as u16);

        let event_idx: bool = (self.acked_features & (1 << VIRTIO_RING_F_EVENT_IDX)) != 0;
        self.vrings[index as usize]
            .write()
            .unwrap()
            .mut_queue()
            .set_event_idx(event_idx);
        self.backend.set_event_idx(event_idx);
        Ok(())
    }

    fn get_vring_base(&mut self, index: u32) -> VhostUserResult<VhostUserVringState> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }
        // Quote from vhost-user specification:
        // Client must start ring upon receiving a kick (that is, detecting
        // that file descriptor is readable) on the descriptor specified by
        // VHOST_USER_SET_VRING_KICK, and stop ring upon receiving
        // VHOST_USER_GET_VRING_BASE.
        self.vrings[index as usize].write().unwrap().queue.ready = false;
        if let Some(fd) = self.vrings[index as usize].read().unwrap().kick.as_ref() {
            for (thread_index, queues_mask) in self.queues_per_thread.iter().enumerate() {
                let shifted_queues_mask = queues_mask >> index;
                if shifted_queues_mask & 1u64 == 1u64 {
                    let evt_idx = queues_mask.count_ones() - shifted_queues_mask.count_ones();
                    self.workers[thread_index]
                        .unregister_listener(
                            fd.as_raw_fd(),
                            epoll::Events::EPOLLIN,
                            u64::from(evt_idx),
                        )
                        .map_err(VhostUserError::ReqHandlerError)?;
                    break;
                }
            }
        }

        let next_avail = self.vrings[index as usize]
            .read()
            .unwrap()
            .queue
            .next_avail();

        Ok(VhostUserVringState::new(index, u32::from(next_avail)))
    }

    fn set_vring_kick(&mut self, index: u8, file: Option<File>) -> VhostUserResult<()> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        // SAFETY: EventFd requires that it has sole ownership of its fd. So
        // does File, so this is safe.
        // Ideally, we'd have a generic way to refer to a uniquely-owned fd,
        // such as that proposed by Rust RFC #3128.
        self.vrings[index as usize].write().unwrap().kick =
            file.map(|f| unsafe { EventFd::from_raw_fd(f.into_raw_fd()) });

        // Quote from vhost-user specification:
        // Client must start ring upon receiving a kick (that is, detecting
        // that file descriptor is readable) on the descriptor specified by
        // VHOST_USER_SET_VRING_KICK, and stop ring upon receiving
        // VHOST_USER_GET_VRING_BASE.
        self.vrings[index as usize].write().unwrap().queue.ready = true;
        if let Some(fd) = self.vrings[index as usize].read().unwrap().kick.as_ref() {
            for (thread_index, queues_mask) in self.queues_per_thread.iter().enumerate() {
                let shifted_queues_mask = queues_mask >> index;
                if shifted_queues_mask & 1u64 == 1u64 {
                    let evt_idx = queues_mask.count_ones() - shifted_queues_mask.count_ones();
                    self.workers[thread_index]
                        .register_listener(
                            fd.as_raw_fd(),
                            epoll::Events::EPOLLIN,
                            u64::from(evt_idx),
                        )
                        .map_err(VhostUserError::ReqHandlerError)?;
                    break;
                }
            }
        }

        Ok(())
    }

    fn set_vring_call(&mut self, index: u8, file: Option<File>) -> VhostUserResult<()> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        // SAFETY: see comment in set_vring_kick()
        self.vrings[index as usize].write().unwrap().call =
            file.map(|f| unsafe { EventFd::from_raw_fd(f.into_raw_fd()) });

        Ok(())
    }

    fn set_vring_err(&mut self, index: u8, file: Option<File>) -> VhostUserResult<()> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        // SAFETY: see comment in set_vring_kick()
        self.vrings[index as usize].write().unwrap().err =
            file.map(|f| unsafe { EventFd::from_raw_fd(f.into_raw_fd()) });

        Ok(())
    }

    fn set_vring_enable(&mut self, index: u32, enable: bool) -> VhostUserResult<()> {
        // This request should be handled only when VHOST_USER_F_PROTOCOL_FEATURES
        // has been negotiated.
        if self.acked_features & VhostUserVirtioFeatures::PROTOCOL_FEATURES.bits() == 0 {
            return Err(VhostUserError::InvalidOperation);
        } else if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        // Slave must not pass data to/from the backend until ring is
        // enabled by VHOST_USER_SET_VRING_ENABLE with parameter 1,
        // or after it has been disabled by VHOST_USER_SET_VRING_ENABLE
        // with parameter 0.
        self.vrings[index as usize].write().unwrap().enabled = enable;

        Ok(())
    }

    fn get_config(
        &mut self,
        offset: u32,
        size: u32,
        _flags: VhostUserConfigFlags,
    ) -> VhostUserResult<Vec<u8>> {
        Ok(self.backend.get_config(offset, size))
    }

    fn set_config(
        &mut self,
        offset: u32,
        buf: &[u8],
        _flags: VhostUserConfigFlags,
    ) -> VhostUserResult<()> {
        self.backend
            .set_config(offset, buf)
            .map_err(VhostUserError::ReqHandlerError)
    }

    fn set_slave_req_fd(&mut self, vu_req: SlaveFsCacheReq) {
        if self.acked_protocol_features & VhostUserProtocolFeatures::REPLY_ACK.bits() != 0 {
            vu_req.set_reply_ack_flag(true);
        }

        self.backend.set_slave_req_fd(vu_req);
    }

    fn get_max_mem_slots(&mut self) -> VhostUserResult<u64> {
        Ok(MAX_MEM_SLOTS)
    }

    fn add_mem_region(
        &mut self,
        region: &VhostUserSingleMemoryRegion,
        file: File,
    ) -> VhostUserResult<()> {
        let mmap_region = MmapRegion::from_file(
            FileOffset::new(file, region.mmap_offset),
            region.memory_size as usize,
        )
        .map_err(|e| VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e)))?;
        let guest_region = Arc::new(
            GuestRegionMmap::new(mmap_region, GuestAddress(region.guest_phys_addr)).map_err(
                |e| VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e)),
            )?,
        );

        let mem = self
            .atomic_mem
            .memory()
            .insert_region(guest_region)
            .map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;

        self.atomic_mem.lock().unwrap().replace(mem);

        self.backend
            .update_memory(self.atomic_mem.clone())
            .map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;

        self.mappings.push(AddrMapping {
            vmm_addr: region.user_addr,
            size: region.memory_size,
            gpa_base: region.guest_phys_addr,
        });

        Ok(())
    }

    fn remove_mem_region(&mut self, region: &VhostUserSingleMemoryRegion) -> VhostUserResult<()> {
        let (mem, _) = self
            .atomic_mem
            .memory()
            .remove_region(GuestAddress(region.guest_phys_addr), region.memory_size)
            .map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;

        self.atomic_mem.lock().unwrap().replace(mem);

        self.backend
            .update_memory(self.atomic_mem.clone())
            .map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;

        self.mappings
            .retain(|mapping| mapping.gpa_base != region.guest_phys_addr);

        Ok(())
    }

    fn get_inflight_fd(
        &mut self,
        _inflight: &vhost::vhost_user::message::VhostUserInflight,
    ) -> VhostUserResult<(vhost::vhost_user::message::VhostUserInflight, File)> {
        // Assume the backend hasn't negotiated the inflight feature; it
        // wouldn't be correct for the backend to do so, as we don't (yet)
        // provide a way for it to handle such requests.
        Err(VhostUserError::InvalidOperation)
    }

    fn set_inflight_fd(
        &mut self,
        _inflight: &vhost::vhost_user::message::VhostUserInflight,
        _file: File,
    ) -> VhostUserResult<()> {
        Err(VhostUserError::InvalidOperation)
    }
}

impl<S: VhostUserBackend> Drop for VhostUserHandler<S> {
    fn drop(&mut self) {
        for thread in self.worker_threads.drain(..) {
            if let Err(e) = thread.join() {
                error!("Error in vring worker: {:?}", e);
            }
        }
    }
}

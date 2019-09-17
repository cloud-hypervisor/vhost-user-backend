// Copyright 2019 Intel Corporation. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2019 Alibaba Cloud Computing. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::error;
use std::fs::File;
use std::io;
use std::num::Wrapping;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::result;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use vhost_rs::vhost_user::message::{
    VhostUserConfigFlags, VhostUserMemoryRegion, VhostUserProtocolFeatures,
    VhostUserVirtioFeatures, VhostUserVringAddrFlags, VhostUserVringState,
    VHOST_USER_CONFIG_OFFSET, VHOST_USER_CONFIG_SIZE,
};
use vhost_rs::vhost_user::{
    Error as VhostUserError, Result as VhostUserResult, SlaveReqHandler, VhostUserSlaveReqHandler,
};
use vm_memory::guest_memory::FileOffset;
use vm_memory::{GuestAddress, GuestMemoryMmap};
use vm_virtio::{DescriptorChain, Queue};
use vmm_sys_util::eventfd::EventFd;

#[derive(Debug)]
/// Errors related to vhost-user daemon.
pub enum Error {
    /// Failed to create a new vhost-user handler.
    NewVhostUserHandler(VhostUserHandlerError),
    /// Failed creating vhost-user slave handler.
    CreateSlaveReqHandler(VhostUserError),
    /// Failed starting daemon thread.
    StartDaemon(io::Error),
    /// Failed waiting for daemon thread.
    WaitDaemon(std::boxed::Box<dyn std::any::Any + std::marker::Send>),
    /// Failed handling a vhost-user request.
    HandleRequest(VhostUserError),
    /// Failed to handle the event.
    HandleEvent(io::Error),
    /// Failed to process queue.
    ProcessQueue(VringEpollHandlerError),
    /// Failed to register listener.
    RegisterListener(io::Error),
    /// Failed to unregister listener.
    UnregisterListener(io::Error),
}

/// Result of vhost-user daemon operations.
pub type Result<T> = result::Result<T, Error>;

/// This trait must be implemented by the caller in order to provide backend
/// specific implementation.
pub trait VhostUserBackend: Send + Sync + 'static {
    /// Number of queues.
    fn num_queues(&self) -> usize;

    /// Depth of each queue.
    fn max_queue_size(&self) -> usize;

    /// Virtio features.
    fn features(&self) -> u64;

    /// This function gets called if the backend registered some additional
    /// listeners onto specific file descriptors. The library can handle
    /// virtqueues on its own, but does not know what to do with events
    /// happening on custom listeners.
    fn handle_event(
        &mut self,
        device_event: u16,
        evset: epoll::Events,
    ) -> result::Result<bool, io::Error>;

    /// This function is responsible for the actual processing that needs to
    /// happen when one of the virtqueues is available.
    fn process_queue(
        &mut self,
        q_idx: u16,
        avail_desc: &DescriptorChain,
        mem: &GuestMemoryMmap,
    ) -> result::Result<u32, io::Error>;

    /// Get virtio device configuration.
    fn get_config(&self, offset: u32, size: u32) -> Vec<u8>;

    /// Set virtio device configuration.
    fn set_config(&mut self, offset: u32, buf: &[u8]) -> result::Result<(), io::Error>;
}

/// This structure is the public API the backend is allowed to interact with
/// in order to run a fully functional vhost-user daemon.
pub struct VhostUserDaemon<S: VhostUserBackend> {
    name: String,
    sock_path: String,
    handler: Arc<Mutex<VhostUserHandler<S>>>,
    vring_handler: Arc<RwLock<VringEpollHandler<S>>>,
    main_thread: Option<thread::JoinHandle<Result<()>>>,
}

impl<S: VhostUserBackend> VhostUserDaemon<S> {
    /// Create the daemon instance, providing the backend implementation of
    /// VhostUserBackend.
    /// Under the hood, this will start a dedicated thread responsible for
    /// listening onto registered event. Those events can be vring events or
    /// custom events from the backend, but they get to be registered later
    /// during the sequence.
    pub fn new(name: String, sock_path: String, backend: S) -> Result<Self> {
        let handler = Arc::new(Mutex::new(
            VhostUserHandler::new(backend).map_err(Error::NewVhostUserHandler)?,
        ));
        let vring_handler = handler.lock().unwrap().get_vring_handler();

        Ok(VhostUserDaemon {
            name,
            sock_path,
            handler,
            vring_handler,
            main_thread: None,
        })
    }

    /// Connect to the vhost-user socket and run a dedicated thread handling
    /// all requests coming through this socket. This runs in an infinite loop
    /// that should be terminating once the other end of the socket (the VMM)
    /// disconnects.
    pub fn start(&mut self) -> Result<()> {
        let mut slave_handler =
            SlaveReqHandler::connect(self.sock_path.as_str(), self.handler.clone())
                .map_err(Error::CreateSlaveReqHandler)?;
        let handle = thread::Builder::new()
            .name(self.name.clone())
            .spawn(move || loop {
                slave_handler
                    .handle_request()
                    .map_err(Error::HandleRequest)?;
            })
            .map_err(Error::StartDaemon)?;

        self.main_thread = Some(handle);

        Ok(())
    }

    /// Wait for the thread handling the vhost-user socket connection to
    /// terminate.
    pub fn wait(&mut self) -> Result<()> {
        if let Some(handle) = self.main_thread.take() {
            let _ = handle.join().map_err(Error::WaitDaemon)?;
        }
        Ok(())
    }

    /// Register a custom event only meaningful to the caller. When this event
    /// is later triggered, and because only the caller knows what to do about
    /// it, the backend implementation of `handle_event` will be called.
    /// This lets entire control to the caller about what needs to be done for
    /// this special event, without forcing it to run its own dedicated epoll
    /// loop for it.
    pub fn register_listener(&self, fd: RawFd, ev_type: epoll::Events, data: u64) -> Result<()> {
        self.vring_handler
            .read()
            .unwrap()
            .register_listener(fd, ev_type, data)
            .map_err(Error::RegisterListener)
    }

    /// Unregister a custom event. If the custom event is triggered after this
    /// function has been called, nothing will happen as it will be removed
    /// from the list of file descriptors the epoll loop is listening to.
    pub fn unregister_listener(&self, fd: RawFd, ev_type: epoll::Events, data: u64) -> Result<()> {
        self.vring_handler
            .read()
            .unwrap()
            .unregister_listener(fd, ev_type, data)
            .map_err(Error::RegisterListener)
    }

    /// Trigger the processing of a virtqueue. This function is meant to be
    /// used by the caller whenever it might need some available queues to
    /// send data back to the guest.
    /// A concrete example is a backend registering one extra listener for
    /// data that needs to be sent to the guest. When the associated event
    /// is triggered, the backend will be invoked through its `handle_event`
    /// implementation. And in this case, the way to handle the event is to
    /// call into `process_queue` to let it invoke the backend implementation
    /// of `process_queue`. With this twisted trick, all common parts related
    /// to the virtqueues can remain part of the library.
    pub fn process_queue(&self, q_idx: u16) -> Result<()> {
        self.vring_handler
            .write()
            .unwrap()
            .process_queue(q_idx)
            .map_err(Error::ProcessQueue)
    }
}

struct AddrMapping {
    vmm_addr: u64,
    size: u64,
}

struct Memory {
    mappings: Vec<AddrMapping>,
}

struct Vring {
    queue: Queue,
    kick: Option<EventFd>,
    call: Option<EventFd>,
    err: Option<EventFd>,
    enabled: bool,
}

impl Vring {
    fn new(max_queue_size: u16) -> Self {
        Vring {
            queue: Queue::new(max_queue_size),
            kick: None,
            call: None,
            err: None,
            enabled: false,
        }
    }
}

#[derive(Debug)]
/// Errors related to vring epoll handler.
pub enum VringEpollHandlerError {
    /// Failed to process the queue from the backend.
    ProcessQueueBackendProcessing(io::Error),
    /// Failed to signal used queue.
    SignalUsedQueue(io::Error),
    /// Failed to read the event from kick EventFd.
    HandleEventReadKick(io::Error),
    /// Failed to handle the event from the backend.
    HandleEventBackendHandling(io::Error),
    /// Failed to register vring listener.
    RegisterVringListener(io::Error),
    /// Failed to unregister vring listener.
    UnregisterVringListener(io::Error),
}

impl std::fmt::Display for VringEpollHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            VringEpollHandlerError::ProcessQueueBackendProcessing(e) => {
                write!(f, "failed processing queue from backend: {}", e)
            }
            VringEpollHandlerError::SignalUsedQueue(e) => {
                write!(f, "failed signalling used queue: {}", e)
            }
            VringEpollHandlerError::HandleEventReadKick(e) => {
                write!(f, "failed reading from kick eventfd: {}", e)
            }
            VringEpollHandlerError::HandleEventBackendHandling(e) => {
                write!(f, "failed handling event from backend: {}", e)
            }
            VringEpollHandlerError::RegisterVringListener(e) => {
                write!(f, "failed registering vring listener: {}", e)
            }
            VringEpollHandlerError::UnregisterVringListener(e) => {
                write!(f, "failed unregistering vring listener: {}", e)
            }
        }
    }
}

impl error::Error for VringEpollHandlerError {}

/// Result of vring epoll handler operations.
type VringEpollHandlerResult<T> = std::result::Result<T, VringEpollHandlerError>;

struct VringEpollHandler<S: VhostUserBackend> {
    backend: Arc<RwLock<S>>,
    vrings: Vec<Arc<RwLock<Vring>>>,
    mem: Option<GuestMemoryMmap>,
    epoll_fd: RawFd,
}

impl<S: VhostUserBackend> VringEpollHandler<S> {
    fn update_memory(&mut self, mem: Option<GuestMemoryMmap>) {
        self.mem = mem;
    }

    fn process_queue(&mut self, q_idx: u16) -> VringEpollHandlerResult<()> {
        let vring = &mut self.vrings[q_idx as usize].write().unwrap();
        let mut used_desc_heads = vec![(0, 0); vring.queue.size as usize];
        let mut used_count = 0;
        if let Some(mem) = &self.mem {
            for avail_desc in vring.queue.iter(&mem) {
                let used_len = self
                    .backend
                    .write()
                    .unwrap()
                    .process_queue(q_idx, &avail_desc, &mem)
                    .map_err(VringEpollHandlerError::ProcessQueueBackendProcessing)?;

                used_desc_heads[used_count] = (avail_desc.index, used_len);
                used_count += 1;
            }

            for &(desc_index, len) in &used_desc_heads[..used_count] {
                vring.queue.add_used(&mem, desc_index, len);
            }
        }

        if used_count > 0 {
            if let Some(call) = &vring.call {
                call.write(1)
                    .map_err(VringEpollHandlerError::SignalUsedQueue)?;
            }
        }

        Ok(())
    }

    fn handle_event(
        &mut self,
        device_event: u16,
        evset: epoll::Events,
    ) -> VringEpollHandlerResult<bool> {
        let num_queues = self.vrings.len();
        match device_event as usize {
            x if x < num_queues => {
                if let Some(kick) = &self.vrings[device_event as usize].read().unwrap().kick {
                    kick.read()
                        .map_err(VringEpollHandlerError::HandleEventReadKick)?;
                }

                // If the vring is not enabled, it should not be processed.
                // The event is only read to be discarded.
                if !self.vrings[device_event as usize].read().unwrap().enabled {
                    return Ok(false);
                }

                self.process_queue(device_event)?;
                Ok(false)
            }
            _ => self
                .backend
                .write()
                .unwrap()
                .handle_event(device_event, evset)
                .map_err(VringEpollHandlerError::HandleEventBackendHandling),
        }
    }

    fn register_vring_listener(&self, q_idx: usize) -> VringEpollHandlerResult<()> {
        if let Some(fd) = &self.vrings[q_idx].read().unwrap().kick {
            self.register_listener(fd.as_raw_fd(), epoll::Events::EPOLLIN, q_idx as u64)
                .map_err(VringEpollHandlerError::RegisterVringListener)
        } else {
            Ok(())
        }
    }

    fn unregister_vring_listener(&self, q_idx: usize) -> VringEpollHandlerResult<()> {
        if let Some(fd) = &self.vrings[q_idx].read().unwrap().kick {
            self.unregister_listener(fd.as_raw_fd(), epoll::Events::EPOLLIN, q_idx as u64)
                .map_err(VringEpollHandlerError::UnregisterVringListener)
        } else {
            Ok(())
        }
    }

    fn register_listener(
        &self,
        fd: RawFd,
        ev_type: epoll::Events,
        data: u64,
    ) -> result::Result<(), io::Error> {
        epoll::ctl(
            self.epoll_fd,
            epoll::ControlOptions::EPOLL_CTL_ADD,
            fd,
            epoll::Event::new(ev_type, data),
        )
    }

    fn unregister_listener(
        &self,
        fd: RawFd,
        ev_type: epoll::Events,
        data: u64,
    ) -> result::Result<(), io::Error> {
        epoll::ctl(
            self.epoll_fd,
            epoll::ControlOptions::EPOLL_CTL_DEL,
            fd,
            epoll::Event::new(ev_type, data),
        )
    }
}

#[derive(Debug)]
/// Errors related to vring worker.
enum VringWorkerError {
    /// Failed while waiting for events.
    EpollWait(io::Error),
    /// Failed to handle event.
    HandleEvent(VringEpollHandlerError),
}

/// Result of vring worker operations.
type VringWorkerResult<T> = std::result::Result<T, VringWorkerError>;

struct VringWorker<S: VhostUserBackend> {
    handler: Arc<RwLock<VringEpollHandler<S>>>,
}

impl<S: VhostUserBackend> VringWorker<S> {
    fn run(&self, epoll_fd: RawFd) -> VringWorkerResult<()> {
        const EPOLL_EVENTS_LEN: usize = 100;
        let mut events = vec![epoll::Event::new(epoll::Events::empty(), 0); EPOLL_EVENTS_LEN];

        'epoll: loop {
            let num_events = match epoll::wait(epoll_fd, -1, &mut events[..]) {
                Ok(res) => res,
                Err(e) => {
                    if e.kind() == io::ErrorKind::Interrupted {
                        // It's well defined from the epoll_wait() syscall
                        // documentation that the epoll loop can be interrupted
                        // before any of the requested events occurred or the
                        // timeout expired. In both those cases, epoll_wait()
                        // returns an error of type EINTR, but this should not
                        // be considered as a regular error. Instead it is more
                        // appropriate to retry, by calling into epoll_wait().
                        continue;
                    }
                    return Err(VringWorkerError::EpollWait(e));
                }
            };

            for event in events.iter().take(num_events) {
                let evset = match epoll::Events::from_bits(event.events) {
                    Some(evset) => evset,
                    None => {
                        let evbits = event.events;
                        println!("epoll: ignoring unknown event set: 0x{:x}", evbits);
                        continue;
                    }
                };

                let ev_type = event.data as u16;

                if self
                    .handler
                    .write()
                    .unwrap()
                    .handle_event(ev_type, evset)
                    .map_err(VringWorkerError::HandleEvent)?
                {
                    break 'epoll;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
/// Errors related to vhost-user handler.
pub enum VhostUserHandlerError {
    /// Failed to create epoll file descriptor.
    EpollCreateFd(io::Error),
    /// Failed to spawn vring worker.
    SpawnVringWorker(io::Error),
    /// Could not find the mapping from memory regions.
    MissingMemoryMapping,
}

impl std::fmt::Display for VhostUserHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            VhostUserHandlerError::EpollCreateFd(e) => write!(f, "failed creating epoll fd: {}", e),
            VhostUserHandlerError::SpawnVringWorker(e) => {
                write!(f, "failed spawning the vring worker: {}", e)
            }
            VhostUserHandlerError::MissingMemoryMapping => write!(f, "Missing memory mapping"),
        }
    }
}

impl error::Error for VhostUserHandlerError {}

/// Result of vhost-user handler operations.
type VhostUserHandlerResult<T> = std::result::Result<T, VhostUserHandlerError>;

struct VhostUserHandler<S: VhostUserBackend> {
    backend: Arc<RwLock<S>>,
    vring_handler: Arc<RwLock<VringEpollHandler<S>>>,
    owned: bool,
    features_acked: bool,
    acked_features: u64,
    acked_protocol_features: u64,
    num_queues: usize,
    max_queue_size: usize,
    memory: Option<Memory>,
    vrings: Vec<Arc<RwLock<Vring>>>,
}

impl<S: VhostUserBackend> VhostUserHandler<S> {
    fn new(backend: S) -> VhostUserHandlerResult<Self> {
        let num_queues = backend.num_queues();
        let max_queue_size = backend.max_queue_size();

        let arc_backend = Arc::new(RwLock::new(backend));
        let vrings = vec![Arc::new(RwLock::new(Vring::new(max_queue_size as u16))); num_queues];
        // Create the epoll file descriptor
        let epoll_fd = epoll::create(true).map_err(VhostUserHandlerError::EpollCreateFd)?;

        let vring_handler = Arc::new(RwLock::new(VringEpollHandler {
            backend: arc_backend.clone(),
            vrings: vrings.clone(),
            mem: None,
            epoll_fd,
        }));
        let worker = VringWorker {
            handler: vring_handler.clone(),
        };

        thread::Builder::new()
            .name("vring_worker".to_string())
            .spawn(move || worker.run(epoll_fd))
            .map_err(VhostUserHandlerError::SpawnVringWorker)?;

        Ok(VhostUserHandler {
            backend: arc_backend,
            vring_handler,
            owned: false,
            features_acked: false,
            acked_features: 0,
            acked_protocol_features: 0,
            num_queues,
            max_queue_size,
            memory: None,
            vrings,
        })
    }

    fn get_vring_handler(&self) -> Arc<RwLock<VringEpollHandler<S>>> {
        self.vring_handler.clone()
    }

    fn vmm_va_to_gpa(&self, vmm_va: u64) -> VhostUserHandlerResult<u64> {
        if let Some(memory) = &self.memory {
            for mapping in memory.mappings.iter() {
                if vmm_va >= mapping.vmm_addr && vmm_va < mapping.vmm_addr + mapping.size {
                    return Ok(vmm_va - mapping.vmm_addr);
                }
            }
        }

        Err(VhostUserHandlerError::MissingMemoryMapping)
    }
}

impl<S: VhostUserBackend> VhostUserSlaveReqHandler for VhostUserHandler<S> {
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
        Ok(self.backend.read().unwrap().features())
    }

    fn set_features(&mut self, features: u64) -> VhostUserResult<()> {
        if !self.owned || self.features_acked {
            return Err(VhostUserError::InvalidOperation);
        } else if (features & !self.backend.read().unwrap().features()) != 0 {
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

        Ok(())
    }

    fn get_protocol_features(&mut self) -> VhostUserResult<VhostUserProtocolFeatures> {
        Ok(VhostUserProtocolFeatures::all())
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
        fds: &[RawFd],
    ) -> VhostUserResult<()> {
        // We need to create tuple of ranges from the list of VhostUserMemoryRegion
        // that we get from the caller.
        let mut regions: Vec<(GuestAddress, usize, Option<FileOffset>)> = Vec::new();
        let mut mappings: Vec<AddrMapping> = Vec::new();

        for (idx, region) in ctx.iter().enumerate() {
            let g_addr = GuestAddress(region.guest_phys_addr);
            let len = (region.memory_size + region.mmap_offset) as usize;
            let file = unsafe { File::from_raw_fd(fds[idx]) };
            let f_off = FileOffset::new(file, 0);

            regions.push((g_addr, len, Some(f_off)));
            mappings.push(AddrMapping {
                vmm_addr: region.user_addr,
                size: region.memory_size + region.mmap_offset,
            });
        }

        let mem = GuestMemoryMmap::from_ranges_with_files(regions).map_err(|e| {
            VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
        })?;
        self.vring_handler.write().unwrap().update_memory(Some(mem));
        self.memory = Some(Memory { mappings });

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

        if self.memory.is_some() {
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
            .next_avail = Wrapping(base as u16);
        self.vrings[index as usize].write().unwrap().queue.next_used = Wrapping(base as u16);
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
        self.vring_handler
            .read()
            .unwrap()
            .unregister_vring_listener(index as usize)
            .map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;

        let next_avail = self.vrings[index as usize]
            .read()
            .unwrap()
            .queue
            .next_avail
            .0 as u16;

        Ok(VhostUserVringState::new(index, u32::from(next_avail)))
    }

    fn set_vring_kick(&mut self, index: u8, fd: Option<RawFd>) -> VhostUserResult<()> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        if let Some(kick) = self.vrings[index as usize].write().unwrap().kick.take() {
            // Close file descriptor set by previous operations.
            let _ = unsafe { libc::close(kick.as_raw_fd()) };
        }
        self.vrings[index as usize].write().unwrap().kick =
            fd.map(|x| unsafe { EventFd::from_raw_fd(x) });

        // Quote from vhost-user specification:
        // Client must start ring upon receiving a kick (that is, detecting
        // that file descriptor is readable) on the descriptor specified by
        // VHOST_USER_SET_VRING_KICK, and stop ring upon receiving
        // VHOST_USER_GET_VRING_BASE.
        self.vrings[index as usize].write().unwrap().queue.ready = true;
        self.vring_handler
            .read()
            .unwrap()
            .register_vring_listener(index as usize)
            .map_err(|e| {
                VhostUserError::ReqHandlerError(io::Error::new(io::ErrorKind::Other, e))
            })?;

        Ok(())
    }

    fn set_vring_call(&mut self, index: u8, fd: Option<RawFd>) -> VhostUserResult<()> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        if let Some(call) = self.vrings[index as usize].write().unwrap().call.take() {
            // Close file descriptor set by previous operations.
            let _ = unsafe { libc::close(call.as_raw_fd()) };
        }
        self.vrings[index as usize].write().unwrap().call =
            fd.map(|x| unsafe { EventFd::from_raw_fd(x) });

        Ok(())
    }

    fn set_vring_err(&mut self, index: u8, fd: Option<RawFd>) -> VhostUserResult<()> {
        if index as usize >= self.num_queues {
            return Err(VhostUserError::InvalidParam);
        }

        if let Some(err) = self.vrings[index as usize].write().unwrap().err.take() {
            // Close file descriptor set by previous operations.
            let _ = unsafe { libc::close(err.as_raw_fd()) };
        }
        self.vrings[index as usize].write().unwrap().err =
            fd.map(|x| unsafe { EventFd::from_raw_fd(x) });

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
        if self.acked_features & VhostUserProtocolFeatures::CONFIG.bits() == 0 {
            return Err(VhostUserError::InvalidOperation);
        } else if offset < VHOST_USER_CONFIG_OFFSET
            || offset >= VHOST_USER_CONFIG_SIZE
            || size > VHOST_USER_CONFIG_SIZE - VHOST_USER_CONFIG_OFFSET
            || size + offset > VHOST_USER_CONFIG_SIZE
        {
            return Err(VhostUserError::InvalidParam);
        }

        Ok(self.backend.read().unwrap().get_config(offset, size))
    }

    fn set_config(
        &mut self,
        offset: u32,
        buf: &[u8],
        _flags: VhostUserConfigFlags,
    ) -> VhostUserResult<()> {
        let size = buf.len() as u32;
        if self.acked_features & VhostUserProtocolFeatures::CONFIG.bits() == 0 {
            return Err(VhostUserError::InvalidOperation);
        } else if offset < VHOST_USER_CONFIG_OFFSET
            || offset >= VHOST_USER_CONFIG_SIZE
            || size > VHOST_USER_CONFIG_SIZE - VHOST_USER_CONFIG_OFFSET
            || size + offset > VHOST_USER_CONFIG_SIZE
        {
            return Err(VhostUserError::InvalidParam);
        }

        self.backend
            .write()
            .unwrap()
            .set_config(offset, buf)
            .map_err(VhostUserError::ReqHandlerError)
    }
}

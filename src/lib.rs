// Copyright 2019 Intel Corporation. All Rights Reserved.
// Copyright 2019 Alibaba Cloud Computing. All rights reserved.
// Copyright 2020 Ant Group. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

#[macro_use]
extern crate log;

use std::error;
use std::io;
use std::result;
use std::sync::{Arc, RwLock};

use vhost_rs::vhost_user::message::VhostUserProtocolFeatures;
use vhost_rs::vhost_user::{Error as VhostUserError, SlaveFsCacheReq};
use vm_memory::GuestMemoryMmap;
use vmm_sys_util::eventfd::EventFd;

mod backend;
pub use backend::*;

#[derive(Debug)]
/// Errors related to vhost-user daemon.
pub enum Error {
    /// Failed to create a new vhost-user handler.
    NewVhostUserHandler(VhostUserHandlerError),
    /// Failed creating vhost-user slave listener.
    CreateSlaveListener(VhostUserError),
    /// Failed creating vhost-user slave handler.
    CreateSlaveReqHandler(VhostUserError),
    /// Failed starting daemon thread.
    StartDaemon(io::Error),
    /// Failed waiting for daemon thread.
    WaitDaemon(std::boxed::Box<dyn std::any::Any + std::marker::Send>),
    /// Failed handling a vhost-user request.
    HandleRequest(VhostUserError),
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

    /// Available virtio features.
    fn features(&self) -> u64;

    /// Acked virtio features.
    fn acked_features(&mut self, _features: u64) {}

    /// Virtio protocol features.
    fn protocol_features(&self) -> VhostUserProtocolFeatures;

    /// Tell the backend if EVENT_IDX has been negotiated.
    fn set_event_idx(&mut self, enabled: bool);

    /// Update guest memory regions.
    fn update_memory(&mut self, mem: GuestMemoryMmap) -> result::Result<(), io::Error>;

    /// This function gets called if the backend registered some additional
    /// listeners onto specific file descriptors. The library can handle
    /// virtqueues on its own, but does not know what to do with events
    /// happening on custom listeners.
    fn handle_event(
        &self,
        device_event: u16,
        evset: epoll::Events,
        vrings: &[Arc<RwLock<Vring>>],
        thread_id: usize,
    ) -> result::Result<bool, io::Error>;

    /// Get virtio device configuration.
    /// A default implementation is provided as we cannot expect all backends
    /// to implement this function.
    fn get_config(&self, _offset: u32, _size: u32) -> Vec<u8> {
        Vec::new()
    }

    /// Set virtio device configuration.
    /// A default implementation is provided as we cannot expect all backends
    /// to implement this function.
    fn set_config(&mut self, _offset: u32, _buf: &[u8]) -> result::Result<(), io::Error> {
        Ok(())
    }

    /// Provide an exit EventFd
    /// When this EventFd is written to the worker thread will exit. An optional id may
    /// also be provided, if it not provided then the exit event will be first event id
    /// after the last queue
    fn exit_event(&self, _thread_index: usize) -> Option<(EventFd, Option<u16>)> {
        None
    }

    /// Set slave fd.
    /// A default implementation is provided as we cannot expect all backends
    /// to implement this function.
    fn set_slave_req_fd(&mut self, _vu_req: SlaveFsCacheReq) {}

    fn queues_per_thread(&self) -> Vec<u64> {
        vec![0xffff_ffff]
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
}

/// Result of vring epoll handler operations.
type VringEpollHandlerResult<T> = std::result::Result<T, VringEpollHandlerError>;

#[derive(Debug)]
/// Errors related to vring worker.
enum VringWorkerError {
    /// Failed while waiting for events.
    EpollWait(io::Error),
    /// Failed to handle the event.
    HandleEvent(VringEpollHandlerError),
}

/// Result of vring worker operations.
type VringWorkerResult<T> = std::result::Result<T, VringWorkerError>;

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

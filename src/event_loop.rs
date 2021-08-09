// Copyright 2019 Intel Corporation. All Rights Reserved.
// Copyright 2019-2021 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::result;
use std::sync::{Arc, RwLock};

use super::{VhostUserBackend, Vring};

/// Errors related to vring epoll event handling.
#[derive(Debug)]
pub enum VringEpollError {
    /// Failed to create epoll file descriptor.
    EpollCreateFd(io::Error),
    /// Failed while waiting for events.
    EpollWait(io::Error),
    /// Could not register exit event
    RegisterExitEvent(io::Error),
    /// Failed to read the event from kick EventFd.
    HandleEventReadKick(io::Error),
    /// Failed to handle the event from the backend.
    HandleEventBackendHandling(io::Error),
}

impl Display for VringEpollError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            VringEpollError::EpollCreateFd(e) => write!(f, "cannot create epoll fd: {}", e),
            VringEpollError::EpollWait(e) => write!(f, "failed to wait for epoll event: {}", e),
            VringEpollError::RegisterExitEvent(e) => write!(f, "cannot register exit event: {}", e),
            VringEpollError::HandleEventReadKick(e) => {
                write!(f, "cannot read vring kick event: {}", e)
            }
            VringEpollError::HandleEventBackendHandling(e) => {
                write!(f, "failed to handle epoll event: {}", e)
            }
        }
    }
}

impl std::error::Error for VringEpollError {}

/// Result of vring epoll operations.
pub type VringEpollResult<T> = std::result::Result<T, VringEpollError>;

/// Epoll event handler to manage and process epoll events for registered file descriptor.
///
/// The `VringEpollHandler` structure provides interfaces to:
/// - add file descriptors to be monitored by the epoll fd
/// - remove registered file descriptors from the epoll fd
/// - run the event loop to handle pending events on the epoll fd
pub struct VringEpollHandler<S: VhostUserBackend> {
    epoll_file: File,
    backend: S,
    vrings: Vec<Arc<RwLock<Vring>>>,
    thread_id: usize,
    exit_event_id: Option<u16>,
}

impl<S: VhostUserBackend> VringEpollHandler<S> {
    /// Create a `VringEpollHandler` instance.
    pub(crate) fn new(
        backend: S,
        vrings: Vec<Arc<RwLock<Vring>>>,
        thread_id: usize,
    ) -> VringEpollResult<Self> {
        let epoll_fd = epoll::create(true).map_err(VringEpollError::EpollCreateFd)?;
        let epoll_file = unsafe { File::from_raw_fd(epoll_fd) };
        let (exit_event_fd, exit_event_id) = match backend.exit_event(thread_id) {
            Some((exit_event_fd, exit_event_id)) => {
                (exit_event_fd.as_raw_fd(), Some(exit_event_id))
            }
            None => (-1, None),
        };
        let handler = VringEpollHandler {
            epoll_file,
            backend,
            vrings,
            thread_id,
            exit_event_id,
        };

        if let Some(exit_event_id) = exit_event_id {
            epoll::ctl(
                handler.epoll_file.as_raw_fd(),
                epoll::ControlOptions::EPOLL_CTL_ADD,
                exit_event_fd,
                epoll::Event::new(epoll::Events::EPOLLIN, u64::from(exit_event_id)),
            )
            .map_err(VringEpollError::RegisterExitEvent)?;
        }

        Ok(handler)
    }

    /// Register a custom event only meaningful to the caller into the epoll fd.
    ///
    /// When this event is later triggered, and because only the caller knows what to do about
    /// it, the backend implementation of `handle_event` will be called. This lets entire control
    /// to the caller about what needs to be done for this special event, without forcing it
    /// to run its own dedicated epoll loop for it.
    pub fn register_listener(
        &self,
        fd: RawFd,
        ev_type: epoll::Events,
        data: u64,
    ) -> result::Result<(), io::Error> {
        assert!(data >= self.vrings.len() as u64);
        epoll::ctl(
            self.epoll_file.as_raw_fd(),
            epoll::ControlOptions::EPOLL_CTL_ADD,
            fd,
            epoll::Event::new(ev_type, data),
        )
    }

    /// Unregister a custom event from the epoll fd.
    ///
    /// If the custom event is triggered after this function has been called, nothing will happen
    /// as it will be removed from the list of file descriptors the epoll loop is listening to.
    pub fn unregister_listener(
        &self,
        fd: RawFd,
        ev_type: epoll::Events,
        data: u64,
    ) -> result::Result<(), io::Error> {
        assert!(data >= self.vrings.len() as u64);
        epoll::ctl(
            self.epoll_file.as_raw_fd(),
            epoll::ControlOptions::EPOLL_CTL_DEL,
            fd,
            epoll::Event::new(ev_type, data),
        )
    }

    /// Run the event poll loop to handle all pending events on registered fds.
    ///
    /// The event loop will be terminated once an event is received from the `exit event fd`
    /// associated with the backend.
    pub(crate) fn run(&self) -> VringEpollResult<()> {
        const EPOLL_EVENTS_LEN: usize = 100;
        let mut events = vec![epoll::Event::new(epoll::Events::empty(), 0); EPOLL_EVENTS_LEN];

        'epoll: loop {
            let num_events = match epoll::wait(self.epoll_file.as_raw_fd(), -1, &mut events[..]) {
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
                    return Err(VringEpollError::EpollWait(e));
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

                // handle_event() returns true if an event is received from the exit event fd.
                if self.handle_event(ev_type, evset)? {
                    break 'epoll;
                }
            }
        }

        Ok(())
    }

    fn handle_event(&self, device_event: u16, evset: epoll::Events) -> VringEpollResult<bool> {
        if self.exit_event_id == Some(device_event) {
            return Ok(true);
        }

        let num_queues = self.vrings.len();
        if (device_event as usize) < num_queues {
            let vring = &self.vrings[device_event as usize].read().unwrap();
            if let Some(kick) = &vring.kick {
                kick.read().map_err(VringEpollError::HandleEventReadKick)?;
            }

            // If the vring is not enabled, it should not be processed.
            // The event is only read to be discarded.
            if !vring.enabled {
                return Ok(false);
            }
        }

        self.backend
            .handle_event(device_event, evset, &self.vrings, self.thread_id)
            .map_err(VringEpollError::HandleEventBackendHandling)
    }
}

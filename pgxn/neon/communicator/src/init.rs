//! Initialization functions. These are executed in the postmaster process,
//! at different stages of server startup.
//!
//!
//! Communicator initialization steps:
//!
//! 1. At postmaster startup, before shared memory is allocated,
//!    rcommunicator_shmem_size() is called to get the amount of
//!    shared memory that this module needs.
//!
//! 2. Later, after the shared memory has been allocated,
//!    rcommunicator_shmem_init() is called to initialize the shmem
//!    area.
//!
//! Per process initialization:
//!
//! When a backend process starts up, it calls rcommunicator_backend_init().
//! In the communicator worker process, other functions are called, see
//! `worker_process` module.

use std::ffi::c_int;
use std::mem;
use std::mem::MaybeUninit;
use std::os::fd::OwnedFd;

use neonart::allocator::r#static::alloc_array_from_slice;

use crate::backend_comms::NeonIOHandle;
use crate::integrated_cache::IntegratedCacheInitStruct;

const NUM_NEON_REQUEST_SLOTS_PER_BACKEND: u32 = 5;

/// This struct is created in the postmaster process, and inherited to
/// the communicator process and all backend processes through fork()
#[repr(C)]
pub struct CommunicatorInitStruct {
    #[allow(dead_code)]
    pub max_procs: u32,

    pub submission_pipe_read_fd: OwnedFd,
    pub submission_pipe_write_fd: OwnedFd,

    // Shared memory data structures
    pub num_neon_request_slots_per_backend: u32,

    pub neon_request_slots: &'static [NeonIOHandle],

    pub integrated_cache_init_struct: IntegratedCacheInitStruct<'static>,
}

impl std::fmt::Debug for CommunicatorInitStruct {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        fmt.debug_struct("CommunicatorInitStruct")
            .field("max_procs", &self.max_procs)
            .field("submission_pipe_read_fd", &self.submission_pipe_read_fd)
            .field("submission_pipe_write_fd", &self.submission_pipe_write_fd)
            .field(
                "num_neon_request_slots_per_backend",
                &self.num_neon_request_slots_per_backend,
            )
            .field("neon_request_slots length", &self.neon_request_slots.len())
            .finish()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_shmem_size(max_procs: u32) -> u64 {
    let mut size = 0;

    let num_neon_request_slots = max_procs * NUM_NEON_REQUEST_SLOTS_PER_BACKEND;
    size += mem::size_of::<NeonIOHandle>() * num_neon_request_slots as usize;

    // For integrated_cache's Allocator. TODO: make this adjustable
    size += IntegratedCacheInitStruct::shmem_size(max_procs);

    size as u64
}

/// Initialize the shared memory segment. Returns a backend-private
/// struct, which will be inherited by backend processes through fork
#[unsafe(no_mangle)]
pub extern "C" fn rcommunicator_shmem_init(
    submission_pipe_read_fd: c_int,
    submission_pipe_write_fd: c_int,
    max_procs: u32,
    shmem_area_ptr: *mut MaybeUninit<u8>,
    shmem_area_len: u64,
) -> &'static mut CommunicatorInitStruct {
    let shmem_area: &'static mut [MaybeUninit<u8>] =
        unsafe { std::slice::from_raw_parts_mut(shmem_area_ptr, shmem_area_len as usize) };

    // Carve out the request slots from the shmem area and initialize them
    let num_neon_request_slots_per_backend = NUM_NEON_REQUEST_SLOTS_PER_BACKEND as usize;
    let num_neon_request_slots = max_procs as usize * num_neon_request_slots_per_backend;

    let (neon_request_slots, remaining_area) =
        alloc_array_from_slice::<NeonIOHandle>(shmem_area, num_neon_request_slots);

    for i in 0..num_neon_request_slots {
        neon_request_slots[i].write(NeonIOHandle::default());
    }

    // 'neon_request_slots' is initialized now. (MaybeUninit::slice_assume_init_mut() is nightly-only
    // as of this writing.)
    let neon_request_slots = unsafe {
        std::mem::transmute::<&mut [MaybeUninit<NeonIOHandle>], &mut [NeonIOHandle]>(
            neon_request_slots,
        )
    };

    // Give the rest of the area to the integrated cache
    let integrated_cache_init_struct =
        IntegratedCacheInitStruct::shmem_init(max_procs, remaining_area);

    let (submission_pipe_read_fd, submission_pipe_write_fd) = unsafe {
        use std::os::fd::FromRawFd;
        (
            OwnedFd::from_raw_fd(submission_pipe_read_fd),
            OwnedFd::from_raw_fd(submission_pipe_write_fd),
        )
    };

    let cis: &'static mut CommunicatorInitStruct = Box::leak(Box::new(CommunicatorInitStruct {
        max_procs,
        submission_pipe_read_fd,
        submission_pipe_write_fd,

        num_neon_request_slots_per_backend: NUM_NEON_REQUEST_SLOTS_PER_BACKEND,
        neon_request_slots,

        integrated_cache_init_struct,
    }));

    cis
}

use std::io;

use tokio::net::TcpStream;

#[cfg(target_os = "linux")]
pub use linux::Pipe;

#[cfg(not(target_os = "linux"))]
pub use fallback::Pipe;

#[cfg(target_os = "linux")]
mod linux {
    use std::cell::RefCell;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::ptr;

    use super::*;

    const MIN_CAPACITY: usize = 64 * 1024;
    const MAX_IDLE: usize = 16;

    thread_local! {
        static IDLE: RefCell<Vec<Pipe>> = const { RefCell::new(Vec::new()) };
    }

    pub struct Pipe {
        read: OwnedFd,
        write: OwnedFd,
        capacity: usize,
    }

    impl Pipe {
        pub fn acquire() -> Option<Pipe> {
            IDLE.with_borrow_mut(Vec::pop).or_else(Self::create)
        }

        fn create() -> Option<Pipe> {
            let mut fds = [0; 2];
            if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) } != 0 {
                return None;
            }
            let (read, write) =
                unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
            let capacity = unsafe { libc::fcntl(write.as_raw_fd(), libc::F_GETPIPE_SZ) };
            if capacity < MIN_CAPACITY as libc::c_int {
                return None;
            }
            Some(Pipe {
                read,
                write,
                capacity: capacity as usize,
            })
        }

        pub fn release(self) {
            let rejected = IDLE.with_borrow_mut(|idle| {
                if idle.len() < MAX_IDLE {
                    idle.push(self);
                    None
                } else {
                    Some(self)
                }
            });
            drop(rejected);
        }

        pub fn fill_from(&self, src: &TcpStream) -> io::Result<usize> {
            splice(src.as_raw_fd(), self.write.as_raw_fd(), self.capacity)
        }

        pub fn drain_to(&self, dst: &TcpStream, len: usize) -> io::Result<usize> {
            splice(self.read.as_raw_fd(), dst.as_raw_fd(), len)
        }
    }

    fn splice(from: RawFd, to: RawFd, len: usize) -> io::Result<usize> {
        let n = unsafe {
            libc::splice(
                from,
                ptr::null_mut(),
                to,
                ptr::null_mut(),
                len,
                libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
            )
        };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod fallback {
    use std::convert::Infallible;

    use super::*;

    pub struct Pipe(Infallible);

    impl Pipe {
        pub fn acquire() -> Option<Pipe> {
            None
        }

        pub fn release(self) {
            match self.0 {}
        }

        pub fn fill_from(&self, _src: &TcpStream) -> io::Result<usize> {
            match self.0 {}
        }

        pub fn drain_to(&self, _dst: &TcpStream, _len: usize) -> io::Result<usize> {
            match self.0 {}
        }
    }
}

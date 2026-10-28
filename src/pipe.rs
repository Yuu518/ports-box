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
    const MAX_CAPACITY: usize = 256 * 1024;
    const GROW_AFTER: u8 = 4;
    const MAX_IDLE: usize = 16;

    thread_local! {
        static IDLE: RefCell<Vec<Pipe>> = const { RefCell::new(Vec::new()) };
    }

    pub struct Pipe {
        read: OwnedFd,
        write: OwnedFd,
        capacity: usize,
        busy: u8,
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
                busy: 0,
            })
        }

        fn grow(&mut self) {
            let size = unsafe {
                libc::fcntl(
                    self.write.as_raw_fd(),
                    libc::F_SETPIPE_SZ,
                    MAX_CAPACITY as libc::c_int,
                )
            };
            if size > 0 && size as usize > self.capacity {
                self.capacity = size as usize;
            }
        }

        pub fn release(mut self) {
            self.busy = 0;
            let _ = IDLE.try_with(|idle| {
                let mut idle = idle.borrow_mut();
                if idle.len() < MAX_IDLE {
                    idle.push(self);
                }
            });
        }

        #[cfg(test)]
        pub fn idle_count() -> usize {
            IDLE.with_borrow(Vec::len)
        }

        pub fn fill_from(&mut self, src: &TcpStream) -> io::Result<usize> {
            let n = splice(src.as_raw_fd(), self.write.as_raw_fd(), self.capacity)?;
            if n > 0 {
                self.busy = self.busy.saturating_add(1);
                if self.busy == GROW_AFTER && self.capacity < MAX_CAPACITY {
                    self.grow();
                }
            }
            Ok(n)
        }

        #[cfg(test)]
        pub fn capacity(&self) -> usize {
            self.capacity
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

    #[cfg(test)]
    mod tests {
        use std::time::Duration;

        use tokio::io::{AsyncWriteExt, Interest};
        use tokio::net::TcpListener;
        use tokio::time::timeout;

        use super::*;

        #[tokio::test]
        async fn sustained_fills_grow_the_pipe() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, _) = listener.accept().await.unwrap();
            let writer = tokio::spawn(async move {
                let _ = client.write_all(&vec![7u8; 16 << 20]).await;
            });
            let mut pipe = Pipe::acquire().unwrap();
            let initial = pipe.capacity();
            let mut sink = vec![0u8; 1 << 20];

            timeout(Duration::from_secs(10), async {
                while pipe.capacity() == initial {
                    server.readable().await.unwrap();
                    if let Ok(n) = server.try_io(Interest::READABLE, || pipe.fill_from(&server)) {
                        assert!(n > 0);
                        let read = unsafe {
                            libc::read(pipe.read.as_raw_fd(), sink.as_mut_ptr().cast(), sink.len())
                        };
                        assert_eq!(read as usize, n);
                    }
                }
            })
            .await
            .unwrap();

            assert!(pipe.capacity() > initial);
            writer.abort();
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

        pub fn fill_from(&mut self, _src: &TcpStream) -> io::Result<usize> {
            match self.0 {}
        }

        pub fn drain_to(&self, _dst: &TcpStream, _len: usize) -> io::Result<usize> {
            match self.0 {}
        }
    }
}

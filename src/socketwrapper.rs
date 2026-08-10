#[cfg(target_family = "unix")]
use std::fs;
#[cfg(target_family = "unix")]
use std::os::fd::FromRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{env, io, net};

use crate::config::Address;
use anyhow::{Context as _, Result};
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{tcp, TcpListener, TcpStream};
#[cfg(target_family = "unix")]
use tokio::net::{unix, UnixListener, UnixStream};

pub enum SocketAddr {
    Ip(#[allow(dead_code)] net::SocketAddr),
    #[cfg(target_family = "unix")]
    Unix(#[allow(dead_code)] tokio::net::unix::SocketAddr),
}

impl From<net::SocketAddr> for SocketAddr {
    fn from(val: net::SocketAddr) -> Self {
        SocketAddr::Ip(val)
    }
}

#[cfg(target_family = "unix")]
impl From<tokio::net::unix::SocketAddr> for SocketAddr {
    fn from(val: tokio::net::unix::SocketAddr) -> Self {
        SocketAddr::Unix(val)
    }
}

#[cfg(target_family = "unix")]
pin_project! {
    #[project = OwnedReadHalfProj]
    pub enum OwnedReadHalf {
        Tcp{#[pin] tcp: tcp::OwnedReadHalf},
        Unix{#[pin] unix: unix::OwnedReadHalf},
    }
}
#[cfg(not(target_family = "unix"))]
pin_project! {
    #[project = OwnedReadHalfProj]
    pub enum OwnedReadHalf {
        Tcp{#[pin] tcp: tcp::OwnedReadHalf},
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            OwnedReadHalfProj::Tcp { tcp } => tcp.poll_read(cx, buf),
            #[cfg(target_family = "unix")]
            OwnedReadHalfProj::Unix { unix } => unix.poll_read(cx, buf),
        }
    }
}

#[cfg(target_family = "unix")]
pin_project! {
    #[project = OwnedWriteHalfProj]
    pub enum OwnedWriteHalf {
        Tcp{#[pin] tcp: tcp::OwnedWriteHalf},
        Unix{#[pin] unix: unix::OwnedWriteHalf},
    }
}
#[cfg(not(target_family = "unix"))]
pin_project! {
    #[project = OwnedWriteHalfProj]
    pub enum OwnedWriteHalf {
        Tcp{#[pin] tcp: tcp::OwnedWriteHalf},
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.project() {
            OwnedWriteHalfProj::Tcp { tcp } => tcp.poll_write(cx, buf),
            #[cfg(target_family = "unix")]
            OwnedWriteHalfProj::Unix { unix } => unix.poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        match self.project() {
            OwnedWriteHalfProj::Tcp { tcp } => tcp.poll_write_vectored(cx, bufs),
            #[cfg(target_family = "unix")]
            OwnedWriteHalfProj::Unix { unix } => unix.poll_write_vectored(cx, bufs),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            OwnedWriteHalfProj::Tcp { tcp } => tcp.poll_flush(cx),
            #[cfg(target_family = "unix")]
            OwnedWriteHalfProj::Unix { unix } => unix.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            OwnedWriteHalfProj::Tcp { tcp } => tcp.poll_shutdown(cx),
            #[cfg(target_family = "unix")]
            OwnedWriteHalfProj::Unix { unix } => unix.poll_shutdown(cx),
        }
    }
}

#[cfg(target_family = "unix")]
pin_project! {
    #[project = StreamProj]
    pub enum Stream {
        Tcp{#[pin] tcp: TcpStream},
        Unix{#[pin] unix: UnixStream},
    }
}
#[cfg(not(target_family = "unix"))]
pin_project! {
    #[project = StreamProj]
    pub enum Stream {
        Tcp{#[pin] tcp: TcpStream},
    }
}

impl Stream {
    pub async fn connect(addr: &Address) -> Result<Stream> {
        match addr {
            Address::Tcp(ip_addr, port) => TcpStream::connect((*ip_addr, *port))
                .await
                .with_context(|| format!("connecting to tcp socket {ip_addr}:{port}"))
                .map(|tcp| Stream::Tcp { tcp }),
            #[cfg(target_family = "unix")]
            Address::Unix(path) => UnixStream::connect(path)
                .await
                .with_context(|| format!("connecting to unix socket {path:?}"))
                .map(|unix| Stream::Unix { unix }),
        }
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        match self {
            Stream::Tcp { tcp } => {
                let (read, write) = tcp.into_split();
                (
                    OwnedReadHalf::Tcp { tcp: read },
                    OwnedWriteHalf::Tcp { tcp: write },
                )
            }
            #[cfg(target_family = "unix")]
            Stream::Unix { unix } => {
                let (read, write) = unix.into_split();
                (
                    OwnedReadHalf::Unix { unix: read },
                    OwnedWriteHalf::Unix { unix: write },
                )
            }
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            StreamProj::Tcp { tcp } => tcp.poll_read(cx, buf),
            #[cfg(target_family = "unix")]
            StreamProj::Unix { unix } => unix.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.project() {
            StreamProj::Tcp { tcp } => tcp.poll_write(cx, buf),
            #[cfg(target_family = "unix")]
            StreamProj::Unix { unix } => unix.poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        match self.project() {
            StreamProj::Tcp { tcp } => tcp.poll_write_vectored(cx, bufs),
            #[cfg(target_family = "unix")]
            StreamProj::Unix { unix } => unix.poll_write_vectored(cx, bufs),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            StreamProj::Tcp { tcp } => tcp.poll_flush(cx),
            #[cfg(target_family = "unix")]
            StreamProj::Unix { unix } => unix.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match self.project() {
            StreamProj::Tcp { tcp } => tcp.poll_shutdown(cx),
            #[cfg(target_family = "unix")]
            StreamProj::Unix { unix } => unix.poll_shutdown(cx),
        }
    }
}

pub enum Listener {
    Tcp(TcpListener),
    #[cfg(target_family = "unix")]
    Unix(UnixListener),
}

impl Listener {
    /// Take the listening socket from systemd socket activation
    ///
    /// Returns `None` when the process was not started by systemd (no
    /// `LISTEN_PID`/`LISTEN_FDS` environment or the fd belongs to a
    /// different process), in which case the socket should be created as
    /// usual. The socket type is determined from the fd itself, so the
    /// `listen` config value may differ from the activated socket.
    #[cfg(target_family = "unix")]
    pub fn from_activation() -> Result<Option<Listener>> {
        // The first fd systemd passes is always 3.
        const START_FD: i32 = 3;

        // Only trust the environment when systemd really started this
        // process, it may leak into other processes otherwise.
        let listen_pid = env::var("LISTEN_PID").ok().and_then(|pid| pid.parse().ok());
        let listen_fds = env::var("LISTEN_FDS")
            .ok()
            .and_then(|fds| fds.parse::<u32>().ok());

        // Do not pass socket-activation state to a language-server child, and
        // do not let a stale environment activate this process's children.
        for variable in [
            "LISTEN_PID",
            "LISTEN_FDS",
            "LISTEN_FDNAMES",
            "LISTEN_PIDFDID",
        ] {
            env::remove_var(variable);
        }

        if listen_pid != Some(std::process::id()) {
            return Ok(None);
        }
        let Some(listen_fds) = listen_fds else {
            return Ok(None);
        };
        if listen_fds == 0 {
            return Ok(None);
        }
        if listen_fds != 1 {
            return Err(anyhow::anyhow!(
                "expected exactly one activated socket, got {listen_fds}"
            ));
        }

        // Determine the socket family from the fd itself instead of relying
        // on the config, this way a mismatched config can't cause the fd to
        // be interpreted wrongly. `from_raw_fd` takes ownership of the fd.
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let ret = unsafe {
            libc::getsockname(
                START_FD,
                &mut storage as *mut _ as *mut libc::sockaddr,
                &mut len,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error())
                .with_context(|| format!("getsockname on activated socket fd {START_FD}"));
        }

        // Prevent the activated listener from being inherited by a language
        // server child. This is normally done by sd_listen_fds().
        let descriptor_flags = unsafe { libc::fcntl(START_FD, libc::F_GETFD) };
        if descriptor_flags == -1
            || unsafe { libc::fcntl(START_FD, libc::F_SETFD, descriptor_flags | libc::FD_CLOEXEC) }
                == -1
        {
            return Err(io::Error::last_os_error()).with_context(|| {
                format!("setting close-on-exec on activated socket fd {START_FD}")
            });
        }

        // tokio's `*Listener::from_std` requires a non-blocking socket and
        // panics otherwise on the current_thread runtime (tokio issue #7172).
        // systemd passes its socket in blocking mode, so enable O_NONBLOCK
        // on the file description before wrapping it.
        let flags = unsafe { libc::fcntl(START_FD, libc::F_GETFL) };
        if flags == -1
            || unsafe { libc::fcntl(START_FD, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
        {
            return Err(io::Error::last_os_error())
                .with_context(|| format!("setting O_NONBLOCK on activated socket fd {START_FD}"));
        }

        let listener = match storage.ss_family as i32 {
            libc::AF_INET | libc::AF_INET6 => {
                let socket = unsafe { std::net::TcpListener::from_raw_fd(START_FD) };
                Listener::Tcp(TcpListener::from_std(socket).with_context(|| {
                    format!("tokio initialization of activated tcp socket fd {START_FD}")
                })?)
            }
            libc::AF_UNIX => {
                let socket = unsafe { std::os::unix::net::UnixListener::from_raw_fd(START_FD) };
                Listener::Unix(UnixListener::from_std(socket).with_context(|| {
                    format!("tokio initialization of activated unix socket fd {START_FD}")
                })?)
            }
            family => {
                return Err(anyhow::anyhow!(
                    "unsupported socket family {family} of activated socket fd {START_FD}"
                ))
            }
        };

        Ok(Some(listener))
    }

    #[cfg(not(target_family = "unix"))]
    pub fn from_activation() -> Result<Option<Listener>> {
        Ok(None)
    }

    pub async fn bind(addr: &Address) -> Result<Listener> {
        match addr {
            Address::Tcp(ip_addr, port) => TcpListener::bind((*ip_addr, *port))
                .await
                .with_context(|| format!("binding to tcp socket {ip_addr}:{port}"))
                .map(Listener::Tcp),
            #[cfg(target_family = "unix")]
            Address::Unix(path) => {
                match fs::remove_file(path) {
                    Ok(()) => (),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("removing old unix socket file {path:?}"))
                    }
                }
                UnixListener::bind(path)
                    .with_context(|| format!("binding to unix socket {path:?}"))
                    .map(Listener::Unix)
            }
        }
    }

    pub async fn accept(&self) -> io::Result<(Stream, SocketAddr)> {
        match self {
            Listener::Tcp(tcp) => {
                let (stream, addr) = tcp.accept().await?;
                Ok((Stream::Tcp { tcp: stream }, addr.into()))
            }
            #[cfg(target_family = "unix")]
            Listener::Unix(unix) => {
                let (stream, addr) = unix.accept().await?;
                Ok((Stream::Unix { unix: stream }, addr.into()))
            }
        }
    }
}

use std::future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::task;
use tokio::time::{interval, Duration};
use tracing::{error, info, info_span, warn, Instrument};

use crate::client;
use crate::config::Config;
use crate::instance::InstanceMap;
use crate::socketwrapper::Listener;

pub async fn run(config: &Config) -> Result<()> {
    let instance_map = InstanceMap::new(config);
    let next_client_id = AtomicUsize::new(0);
    let next_client_id = || next_client_id.fetch_add(1, Ordering::Relaxed);
    // Counts connected clients, including clients which haven't sent their
    // initialize request yet and therefore have no instance in the map yet.
    let active_clients = Arc::new(AtomicUsize::new(0));

    let (listener, socket_activated) = match Listener::from_activation()? {
        Some(listener) => {
            info!("listening on systemd-activated socket");
            (listener, true)
        }
        None => {
            let listener = Listener::bind(&config.listen).await.context("listen")?;
            info!(socket = ?config.listen, "listening");
            (listener, false)
        }
    };

    // Only socket-activated servers can be restarted on demand when they
    // become idle. A normally configured listener must remain available for
    // manual starts and supervisors such as launchd.
    let mut idle_check = if socket_activated {
        let mut idle_check = interval(Duration::from_secs(u64::from(config.gc_interval).max(1)));
        // The first tick fires immediately; consume it so a freshly started
        // server doesn't exit while a newly connected client is still
        // establishing its session.
        idle_check.tick().await;
        Some(idle_check)
    } else {
        None
    };

    loop {
        tokio::select! {
            // If a connection is already queued when the idle timer fires,
            // accept it before considering shutdown.
            biased;
            accept = listener.accept() => {
                let (socket, _addr) = match accept {
                    Ok((socket, _addr)) => (socket, _addr),
                    Err(err) => match err.kind() {
                        // ignore benign errors
                        std::io::ErrorKind::NotConnected => {
                            warn!("listener error {err}");
                            continue;
                        }
                        _ => Err(err).context("accept connection")?,
                    },
                };
                let client_id = next_client_id();
                let instance_map = instance_map.clone();
                let active_clients = active_clients.clone();
                // Increment before spawning so the idle check above can't
                // race with a freshly accepted client.
                active_clients.fetch_add(1, Ordering::Relaxed);

                task::spawn(
                    async move {
                        info!("client connected");
                        let result = client::process(socket, client_id, instance_map).await;
                        active_clients.fetch_sub(1, Ordering::Relaxed);
                        match result {
                            Ok(_) => {}
                            Err(err) => error!("client error: {err:?}"),
                        }
                    }
                    .instrument(info_span!("client", %client_id)),
                );
            }
            _ = async {
                match idle_check.as_mut() {
                    Some(idle_check) => idle_check.tick().await,
                    None => future::pending::<tokio::time::Instant>().await,
                }
            } => {
                let idle = instance_map.lock().await.is_empty()
                    && active_clients.load(Ordering::Relaxed) == 0;
                if idle {
                    info!("no language server instances left, exiting");
                    break;
                }
            }
        }
    }

    Ok(())
}

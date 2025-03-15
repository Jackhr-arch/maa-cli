use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub async fn start_daemon(path: Arc<tempfile::TempPath>, cancel_token: CancellationToken) {
    use maa_server::prelude::*;
    use tonic::transport::Server;

    let timeout = std::time::Duration::from_micros(100);

    let child_cancel_token = cancel_token.child_token();
    let server = Server::builder()
        // need to be the parent node
        .add_service(core_service(cancel_token))
        .add_service(task_service());
    let stream = tokio_stream::wrappers::UnixListenerStream::new(
        tokio::net::UnixListener::bind(&*path).unwrap(),
    );
    tokio::select!(
        _ = server.serve_with_incoming_shutdown(stream, async{
            // used to cancel running connections
            let token = child_cancel_token.child_token();
            token.cancelled().await
        }) => {}
        _ = child_cancel_token.cancelled() => {}
    );
    // make sure connection is closed
    tokio::time::sleep(timeout).await;
    println!("Exiting");
    if maa_sys::Assistant::loaded() {
        println!("Clean Up");
        maa_sys::Assistant::unload().unwrap();
    }
}

/// Waits for a signal that requests a graceful shutdown, like SIGTERM or SIGINT.
#[cfg(unix)]
async fn wait_for_signal_impl() {
    use tokio::signal::unix::{signal, SignalKind};

    // Infos here:
    // https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html
    let mut signal_terminate = signal(SignalKind::terminate()).unwrap();
    let mut signal_interrupt = signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = signal_terminate.recv() => tracing::debug!("Received SIGTERM."),
        _ = signal_interrupt.recv() => tracing::debug!("Received SIGINT."),
    };
}

/// Waits for a signal that requests a graceful shutdown, Ctrl-C (SIGINT).
#[cfg(windows)]
async fn wait_for_signal_impl() {
    use tokio::signal::windows;

    // Infos here:
    // https://learn.microsoft.com/en-us/windows/console/handlerroutine
    let mut signal_c = windows::ctrl_c().unwrap();
    let mut signal_break = windows::ctrl_break().unwrap();
    let mut signal_close = windows::ctrl_close().unwrap();
    let mut signal_shutdown = windows::ctrl_shutdown().unwrap();

    tokio::select! {
        _ = signal_c.recv() => tracing::debug!("Received CTRL_C."),
        _ = signal_break.recv() => tracing::debug!("Received CTRL_BREAK."),
        _ = signal_close.recv() => tracing::debug!("Received CTRL_CLOSE."),
        _ = signal_shutdown.recv() => tracing::debug!("Received CTRL_SHUTDOWN."),
    };
}

/// Registers signal handlers and waits for a signal that
/// indicates a shutdown request.
pub(crate) async fn wait_for_signal() {
    wait_for_signal_impl().await
}
